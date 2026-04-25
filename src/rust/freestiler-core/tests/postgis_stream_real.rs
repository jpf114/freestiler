#![cfg(feature = "postgis")]

use std::collections::BTreeMap;

use flate2::read::GzDecoder;
use freestiler_core::engine::{generate_tiles, SilentReporter, TileConfig};
use freestiler_core::model::EncodedTile;
use freestiler_core::pmtiles_writer::TileFormat;
use freestiler_core::postgis::partition::PartitionConfig;
use freestiler_core::postgis_input::{postgis_query_to_layers_with_config, PostgisConfig};
use freestiler_core::run_postgis_to_tile_sink_stream;
use freestiler_core::tileflow::pipeline::TileSink;
use freestiler_core::mvt::Tile as MvtTile;
use freestiler_core::mvt::Value as MvtValue;
use prost::Message;

type TileKey = (u8, u32, u32);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LayerSummary {
    name: String,
    feature_count: usize,
    geometry_value_count: usize,
    property_count: usize,
    feature_signatures: Vec<FeatureSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FeatureSummary {
    id: Option<u64>,
    geom_type: Option<i32>,
    geometry_value_count: usize,
    property_count: usize,
    property_keys: Vec<String>,
}

#[derive(Default)]
struct VecTileSink {
    tiles: Vec<EncodedTile>,
}

impl TileSink for VecTileSink {
    fn push(&mut self, tile: EncodedTile) -> Result<(), String> {
        self.tiles.push(tile);
        Ok(())
    }

    fn finish(&mut self) -> Result<u64, String> {
        Ok(self.tiles.len() as u64)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn normalize_data(mut data: Vec<u8>) -> Vec<u8> {
    if data.starts_with(&[0x1f, 0x8b]) {
        let mut out = Vec::new();
        let mut decoder = GzDecoder::new(data.as_slice());
        if std::io::copy(&mut decoder, &mut out).is_ok() {
            data = out;
        }
    }
    data
}

fn decode_tile(data: &[u8]) -> MvtTile {
    MvtTile::decode(data).expect("decode mvt tile")
}

fn value_to_string(value: &MvtValue) -> String {
    if let Some(v) = &value.string_value {
        return format!("s:{v}");
    }
    if let Some(v) = value.float_value {
        return format!("f:{v}");
    }
    if let Some(v) = value.double_value {
        return format!("d:{v}");
    }
    if let Some(v) = value.int_value {
        return format!("i:{v}");
    }
    if let Some(v) = value.uint_value {
        return format!("u:{v}");
    }
    if let Some(v) = value.sint_value {
        return format!("si:{v}");
    }
    if let Some(v) = value.bool_value {
        return format!("b:{v}");
    }
    "null".to_string()
}

fn tile_summaries(raw_tiles: BTreeMap<TileKey, Vec<u8>>) -> BTreeMap<TileKey, Vec<LayerSummary>> {
    raw_tiles
        .into_iter()
        .map(|(key, data)| {
            let decoded = decode_tile(&data);
            let mut layers = Vec::new();
            for layer in decoded.layers {
                let mut feature_signatures = Vec::new();
                let mut geometry_value_count = 0usize;
                let mut property_count = 0usize;
                for feature in layer.features {
                    let mut property_keys = Vec::new();
                    for pair in feature.tags.chunks_exact(2) {
                        let key_name = layer.keys[pair[0] as usize].clone();
                        let _value = value_to_string(&layer.values[pair[1] as usize]);
                        property_keys.push(key_name);
                    }
                    property_keys.sort();
                    geometry_value_count += feature.geometry.len();
                    property_count += property_keys.len();
                    feature_signatures.push(FeatureSummary {
                        id: feature.id,
                        geom_type: feature.r#type,
                        geometry_value_count: feature.geometry.len(),
                        property_count: property_keys.len(),
                        property_keys,
                    });
                }
                feature_signatures.sort();
                layers.push(LayerSummary {
                    name: layer.name,
                    feature_count: feature_signatures.len(),
                    geometry_value_count,
                    property_count,
                    feature_signatures,
                });
            }
            layers.sort();
            (key, layers)
        })
        .collect()
}

#[test]
#[ignore = "需要真实 PostGIS 环境"]
fn postgis_stream_matches_reference_without_mongo() {
    let pg_conn = env_or("TEST_POSTGIS_CONN", "10.1.0.16:5433:geoc_data:postgres:postgres");
    let table = env_or("TEST_POSTGIS_TABLE", "ACHARE_polygon");
    let sql = env_or(
        "TEST_POSTGIS_SQL",
        &format!("SELECT * FROM public.\"{}\"", table),
    );
    let layer_name = env_or("TEST_LAYER_NAME", "test_layer");
    let min_zoom: u8 = env_or("TEST_MIN_ZOOM", "0").parse().expect("min zoom");
    let max_zoom: u8 = env_or("TEST_MAX_ZOOM", "10").parse().expect("max zoom");

    let pg_conn_str = format!(
        "postgresql://{}:{}@{}/{}",
        pg_conn.split(':').nth(3).expect("user"),
        pg_conn.split(':').nth(4).expect("password"),
        format!(
            "{}:{}",
            pg_conn.split(':').next().expect("host"),
            pg_conn.split(':').nth(1).expect("port")
        ),
        pg_conn.split(':').nth(2).expect("db"),
    );

    let reporter = SilentReporter;
    let pg_config = PostgisConfig::new(&pg_conn_str).batch_size(10_000);
    let tile_config = TileConfig {
        tile_format: TileFormat::Mvt,
        min_zoom,
        max_zoom,
        base_zoom: None,
        simplification: true,
        drop_rate: None,
        cluster_distance: None,
        cluster_maxzoom: None,
        coalesce: false,
    };
    let partition_cfg = PartitionConfig {
        partition_zoom: max_zoom,
        metatile_rows: 64,
    };

    let layers = postgis_query_to_layers_with_config(
        &pg_config,
        &sql,
        &layer_name,
        min_zoom,
        max_zoom,
        None,
    )
    .expect("load postgis layers");

    let reference_tiles = generate_tiles(&layers, &tile_config, &reporter).expect("generate reference tiles");
    let reference_map: BTreeMap<TileKey, Vec<u8>> = reference_tiles
        .into_iter()
        .map(|(coord, data)| ((coord.z, coord.x, coord.y), normalize_data(data)))
        .collect();

    let mut sink = VecTileSink::default();
    run_postgis_to_tile_sink_stream(
        &pg_config,
        &mut sink,
        &tile_config,
        &partition_cfg,
        &layer_name,
        &sql,
        None,
        &reporter,
    )
    .expect("run streaming into vec sink");

    let stream_map: BTreeMap<TileKey, Vec<u8>> = sink
        .tiles
        .into_iter()
        .map(|tile| ((tile.key.z, tile.key.x, tile.key.y), normalize_data(tile.data)))
        .collect();

    assert_eq!(tile_summaries(reference_map), tile_summaries(stream_map));
}
