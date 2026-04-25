use crate::engine::{ProgressReporter, TileConfig};
use crate::error::{FreestilerError, Result};
use crate::postgis::partition::{plan_partitions, PartitionConfig};
use crate::postgis::reader::{open_partition_reader, PostgisSourceConfig};
use crate::postgis::schema::discover_layer_schema;
use crate::tileflow::accumulator::TileAccumulatorMap;
use crate::tileflow::closer::{collect_all_remaining_tiles, collect_closable_tiles};
use crate::tileflow::dispatcher::{dispatch_feature, DispatchConfig};
use crate::tileflow::finalizer::{finalize_tile, FinalizeConfig};

#[cfg(feature = "mongodb-out")]
use crate::sink::mongo::MongoTileSink;

pub trait TileSink {
    fn push(&mut self, tile: crate::model::EncodedTile) -> std::result::Result<(), String>;
    fn finish(&mut self) -> std::result::Result<u64, String>;
}

fn validate_streaming_tile_config(tile_config: &TileConfig) -> Result<()> {
    if tile_config.cluster_distance.map_or(false, |d| d > 0.0) {
        return Err(FreestilerError::Other(
            "流式Mongo路径暂不支持cluster_distance聚类，请关闭聚类后再使用streaming".to_string(),
        ));
    }
    if tile_config.drop_rate.map_or(false, |r| r > 0.0) {
        return Err(FreestilerError::Other(
            "流式Mongo路径暂不支持drop_rate抽稀，请关闭抽稀后再使用streaming".to_string(),
        ));
    }
    Ok(())
}

#[cfg(all(feature = "postgis", feature = "mongodb-out"))]
pub fn run_postgis_to_mongo_stream(
    pg_config: &crate::postgis_input::PostgisConfig,
    mongo_config: &crate::sink::mongo::MongoSinkConfig,
    tile_config: &TileConfig,
    partition_config: &PartitionConfig,
    layer_name: &str,
    sql: &str,
    geom_column_hint: Option<&str>,
    reporter: &dyn ProgressReporter,
) -> Result<u64> {
    let mut sink = MongoTileSink::open(mongo_config).map_err(FreestilerError::Database)?;
    if mongo_config.create_indexes {
        sink.ensure_indexes().map_err(FreestilerError::Database)?;
    }
    run_postgis_to_tile_sink_stream(
        pg_config,
        &mut sink,
        tile_config,
        partition_config,
        layer_name,
        sql,
        geom_column_hint,
        reporter,
    )
}

#[cfg(feature = "postgis")]
pub fn run_postgis_to_tile_sink_stream(
    pg_config: &crate::postgis_input::PostgisConfig,
    sink: &mut dyn TileSink,
    tile_config: &TileConfig,
    partition_config: &PartitionConfig,
    layer_name: &str,
    sql: &str,
    geom_column_hint: Option<&str>,
    reporter: &dyn ProgressReporter,
) -> Result<u64> {
    validate_streaming_tile_config(tile_config)?;
    let schema = discover_layer_schema(pg_config, sql, layer_name, geom_column_hint)
        .map_err(FreestilerError::Database)?;
    let mut reader = open_partition_reader(
        &PostgisSourceConfig::from(pg_config),
        sql,
        schema.clone(),
    )
    .map_err(FreestilerError::Database)?;
    let partitions = plan_partitions(tile_config, partition_config);
    let mut accum = TileAccumulatorMap::new();
    let dispatch_cfg = DispatchConfig {
        min_zoom: tile_config.min_zoom,
        max_zoom: tile_config.max_zoom,
    };
    let finalize_cfg = FinalizeConfig {
        tile_format: tile_config.tile_format,
        simplification: tile_config.simplification,
        coalesce: tile_config.coalesce,
        compress: flate2::Compression::default(),
    };

    let mut total_tiles = 0u64;

    for partition in &partitions {
        let batch = reader.read_partition(partition).map_err(FreestilerError::Database)?;
        reporter.report(&format!(
            "  Partition {} rows [{}..{}): {} features",
            partition.sequence,
            partition.row_start,
            partition.row_end,
            batch.features.len()
        ));

        for feature in &batch.features {
            dispatch_feature(
                feature,
                &schema.layer_name,
                &schema.prop_names,
                &dispatch_cfg,
                &mut accum,
                partition.sequence,
            )
            .map_err(FreestilerError::Other)?;
        }

        for key in collect_closable_tiles(&accum, partition) {
            if let Some(tile) = accum.take_tile(&key) {
                if let Some(encoded) = finalize_tile(tile, &finalize_cfg).map_err(FreestilerError::Other)? {
                    sink.push(encoded).map_err(FreestilerError::Database)?;
                    total_tiles += 1;
                }
            }
        }
    }

    for key in collect_all_remaining_tiles(&accum) {
        if let Some(tile) = accum.take_tile(&key) {
            if let Some(encoded) = finalize_tile(tile, &finalize_cfg).map_err(FreestilerError::Other)? {
                sink.push(encoded).map_err(FreestilerError::Database)?;
                total_tiles += 1;
            }
        }
    }

    sink.finish().map_err(FreestilerError::Database)?;
    if total_tiles == 0 {
        return Err(FreestilerError::NoTilesGenerated);
    }
    Ok(total_tiles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pmtiles_writer::TileFormat;

    #[test]
    fn pipeline_finalize_config_follows_tile_config() {
        let tile_config = TileConfig {
            tile_format: TileFormat::Mlt,
            min_zoom: 0,
            max_zoom: 1,
            base_zoom: None,
            simplification: true,
            drop_rate: None,
            cluster_distance: None,
            cluster_maxzoom: None,
            coalesce: false,
        };

        let finalize = FinalizeConfig {
            tile_format: tile_config.tile_format,
            simplification: tile_config.simplification,
            coalesce: tile_config.coalesce,
            compress: flate2::Compression::default(),
        };

        assert!(matches!(finalize.tile_format, TileFormat::Mlt));
        assert!(finalize.simplification);
        assert!(!finalize.coalesce);
    }

    #[test]
    fn pipeline_rejects_cluster_for_streaming() {
        let tile_config = TileConfig {
            tile_format: TileFormat::Mlt,
            min_zoom: 0,
            max_zoom: 1,
            base_zoom: None,
            simplification: true,
            drop_rate: None,
            cluster_distance: Some(32.0),
            cluster_maxzoom: Some(0),
            coalesce: false,
        };

        let err = validate_streaming_tile_config(&tile_config).expect_err("should reject clustering");
        assert!(err.to_string().contains("暂不支持cluster_distance"));
    }

    #[test]
    fn pipeline_rejects_drop_rate_for_streaming() {
        let tile_config = TileConfig {
            tile_format: TileFormat::Mlt,
            min_zoom: 0,
            max_zoom: 1,
            base_zoom: None,
            simplification: true,
            drop_rate: Some(1.5),
            cluster_distance: None,
            cluster_maxzoom: None,
            coalesce: false,
        };

        let err = validate_streaming_tile_config(&tile_config).expect_err("should reject drop rate");
        assert!(err.to_string().contains("暂不支持drop_rate"));
    }
}
