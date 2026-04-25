#![cfg(feature = "postgis")]

use std::collections::BTreeMap;
use std::time::Instant;

use flate2::read::GzDecoder;
use freestiler_core::engine::{generate_tiles, SilentReporter, TileConfig};
use freestiler_core::model::EncodedTile;
use freestiler_core::pmtiles_writer::TileFormat;
use freestiler_core::postgis::normalize::normalize_rows;
use freestiler_core::postgis::partition::{plan_partitions, PartitionConfig};
use freestiler_core::postgis::reader::{open_partition_reader, PostgisSourceConfig};
use freestiler_core::postgis::schema::discover_layer_schema;
use freestiler_core::postgis_input::{postgis_query_to_layers_with_config, PostgisConfig};
use freestiler_core::run_postgis_to_tile_sink_stream;
use freestiler_core::tileflow::pipeline::TileSink;
use freestiler_core::mvt::Tile as MvtTile;
use freestiler_core::mvt::Value as MvtValue;
use postgres::{Client, NoTls};
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

fn pg_conn_string_from_compact(pg_conn: &str) -> String {
    format!(
        "postgresql://{}:{}@{}/{}",
        pg_conn.split(':').nth(3).expect("user"),
        pg_conn.split(':').nth(4).expect("password"),
        format!(
            "{}:{}",
            pg_conn.split(':').next().expect("host"),
            pg_conn.split(':').nth(1).expect("port")
        ),
        pg_conn.split(':').nth(2).expect("db"),
    )
}

fn connect_pg(pg_conn_str: &str) -> Client {
    Client::connect(pg_conn_str, NoTls).expect("connect postgis")
}

fn old_bbox_query(
    schema: &freestiler_core::model::LayerSchema,
    source_sql: &str,
    partition: &freestiler_core::model::PartitionSpec,
) -> String {
    let ordered_source_sql = match &schema.fid_column {
        Some(fid_column) => format!(
            "SELECT * FROM ({}) AS __ordered_perf_src ORDER BY \"{}\"",
            source_sql, fid_column
        ),
        None => source_sql.to_string(),
    };
    let geom_col = format!("\"{}\"", schema.geom_column);
    let geom_expr = match schema.source_srid {
        Some(4326) | None => geom_col.clone(),
        Some(_srid) => format!("ST_Transform({}, 4326)", geom_col),
    };
    let select_cols: Vec<String> = schema
        .prop_names
        .iter()
        .map(|name| format!("\"{}\"", name))
        .collect();
    let mut projections = select_cols;
    projections.push("\"__fid\"".to_string());
    projections.push(format!("ST_AsEWKB({}) AS \"__wkb\"", geom_expr));
    format!(
        "SELECT {} FROM ( \
            SELECT __stream_base.*, ROW_NUMBER() OVER ()::bigint AS \"__fid\" \
            FROM ({}) AS __stream_base \
         ) AS __stream_src \
         WHERE {} IS NOT NULL \
         AND ST_Intersects({}, ST_MakeEnvelope({}, {}, {}, {}, 4326))",
        projections.join(", "),
        ordered_source_sql,
        geom_col,
        geom_expr,
        partition.min_lon,
        partition.min_lat,
        partition.max_lon,
        partition.max_lat
    )
}

#[allow(dead_code)]
fn discover_large_table(pg_conn_str: &str) -> (String, i64) {
    if let Ok(table) = std::env::var("TEST_POSTGIS_TABLE_LARGE") {
        return (table, -1);
    }

    let mut conn = connect_pg(pg_conn_str);
    let row = conn
        .query_one(
            "
            SELECT
                format('%I.%I', n.nspname, c.relname) AS table_name,
                GREATEST(c.reltuples::bigint, 0) AS est_rows
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND t.typname IN ('geometry', 'geography')
            ORDER BY c.reltuples DESC
            LIMIT 1
            ",
            &[],
        )
        .expect("discover large geometry table");

    (row.get::<_, String>("table_name"), row.get::<_, i64>("est_rows"))
}

fn discover_large_table_with_fid(pg_conn_str: &str) -> (String, i64, String) {
    if let Ok(table) = std::env::var("TEST_POSTGIS_TABLE_LARGE_WITH_FID") {
        return (table, -1, "env_override".to_string());
    }

    let mut conn = connect_pg(pg_conn_str);
    let row = conn
        .query_one(
            "
            SELECT
                format('%I.%I', n.nspname, c.relname) AS table_name,
                GREATEST(c.reltuples::bigint, 0) AS est_rows,
                fid.attname AS fid_column
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute geom ON geom.attrelid = c.oid
            JOIN pg_type geom_t ON geom_t.oid = geom.atttypid
            JOIN pg_attribute fid ON fid.attrelid = c.oid
            JOIN pg_type fid_t ON fid_t.oid = fid.atttypid
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND geom.attnum > 0
              AND NOT geom.attisdropped
              AND geom_t.typname IN ('geometry', 'geography')
              AND fid.attnum > 0
              AND NOT fid.attisdropped
              AND lower(fid.attname) IN ('__fid', 'id', 'gid', 'fid', 'objectid', 'oid')
              AND fid_t.typname IN ('int2', 'int4', 'int8', 'oid')
            ORDER BY c.reltuples DESC
            LIMIT 1
            ",
            &[],
        )
        .expect("discover large geometry table with fid");

    (
        row.get::<_, String>("table_name"),
        row.get::<_, i64>("est_rows"),
        row.get::<_, String>("fid_column"),
    )
}

fn sample_partitions(
    partitions: Vec<freestiler_core::model::PartitionSpec>,
    sample_count: usize,
) -> Vec<freestiler_core::model::PartitionSpec> {
    if partitions.len() <= sample_count {
        return partitions;
    }

    let last = partitions.len() - 1;
    let mut indexes = vec![0usize, last / 2, last];
    indexes.sort_unstable();
    indexes.dedup();

    indexes
        .into_iter()
        .filter_map(|idx| partitions.get(idx).cloned())
        .collect()
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

    let pg_conn_str = pg_conn_string_from_compact(&pg_conn);

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

#[test]
#[ignore = "需要真实 PostGIS 大表环境"]
fn postgis_large_table_reader_profile() {
    let pg_conn = env_or("TEST_POSTGIS_CONN", "10.1.0.16:5433:geoc_data:postgres:postgres");
    let pg_conn_str = pg_conn_string_from_compact(&pg_conn);
    let (table_name, est_rows, expected_fid_column) = discover_large_table_with_fid(&pg_conn_str);
    let sql = format!("SELECT * FROM {}", table_name);
    let layer_name = env_or("TEST_LAYER_NAME", "perf_layer");
    let perf_zoom: u8 = env_or("TEST_PERF_MAX_ZOOM", "8").parse().expect("perf zoom");

    let pg_config = PostgisConfig::new(&pg_conn_str).batch_size(10_000);
    let schema = discover_layer_schema(&pg_config, &sql, &layer_name, None).expect("discover schema");
    let tile_config = TileConfig {
        tile_format: TileFormat::Mvt,
        min_zoom: 0,
        max_zoom: perf_zoom,
        base_zoom: None,
        simplification: true,
        drop_rate: None,
        cluster_distance: None,
        cluster_maxzoom: None,
        coalesce: false,
    };
    let partition_cfg = PartitionConfig {
        partition_zoom: perf_zoom,
        metatile_rows: 64,
    };
    let partitions = sample_partitions(plan_partitions(&tile_config, &partition_cfg), 3);
    println!(
        "profile target table={} est_rows={} fid_column={:?} expected_fid_column={} sampled_partitions={} zoom={}",
        table_name,
        est_rows,
        schema.fid_column,
        expected_fid_column,
        partitions.len(),
        perf_zoom
    );

    let mut old_conn = connect_pg(&pg_conn_str);
    old_conn
        .batch_execute("SET statement_timeout = '300s'")
        .expect("set old query timeout");
    let old_start = Instant::now();
    let mut old_total = 0usize;
    for partition in &partitions {
        println!("running old partition seq={}", partition.sequence);
        let partition_sql = old_bbox_query(&schema, &sql, partition);
        let rows = old_conn
            .query(&partition_sql, &[])
            .expect("run old partition query");
        old_total += normalize_rows(&rows, &schema)
            .expect("normalize old rows")
            .len();
    }
    let old_elapsed = old_start.elapsed();

    let mut reader = open_partition_reader(&PostgisSourceConfig::from(&pg_config), &sql, schema.clone())
        .expect("open optimized partition reader");
    let new_start = Instant::now();
    let mut new_total = 0usize;
    for partition in &partitions {
        println!("running optimized partition seq={}", partition.sequence);
        let batch = reader
            .read_partition(partition)
            .expect("read optimized partition");
        new_total += batch.features.len();
    }
    let new_elapsed = new_start.elapsed();

    assert_eq!(old_total, new_total, "优化前后读取到的 feature 总数不一致");
    println!(
        "large_table={} est_rows={} fid_column={:?} partitions={} old_ms={} new_ms={} speedup={:.2}",
        table_name,
        est_rows,
        schema.fid_column,
        partitions.len(),
        old_elapsed.as_millis(),
        new_elapsed.as_millis(),
        old_elapsed.as_secs_f64() / new_elapsed.as_secs_f64()
    );
}
