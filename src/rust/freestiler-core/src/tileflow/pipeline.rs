use crate::engine::{ProgressReporter, SilentReporter, TileConfig};
use crate::error::{FreestilerError, Result};
use crate::postgis::partition::{plan_partitions, PartitionConfig};
use crate::postgis::reader::{open_partition_reader, PostgisSourceConfig};
use crate::postgis::schema::discover_layer_schema;
use crate::tileflow::accumulator::TileAccumulatorMap;
use crate::tileflow::closer::{collect_all_remaining_tiles, collect_closable_tiles};
use crate::tileflow::dispatcher::{dispatch_feature, DispatchConfig};
use crate::tileflow::finalizer::{finalize_tile, FinalizeConfig};

#[cfg(feature = "mongodb-out")]
use crate::sink::mongo::{MongoSinkConfig, MongoTileSink};

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
    let mut sink = MongoTileSink::open(mongo_config).map_err(FreestilerError::Database)?;
    if mongo_config.create_indexes {
        sink.ensure_indexes().map_err(FreestilerError::Database)?;
    }
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
}
