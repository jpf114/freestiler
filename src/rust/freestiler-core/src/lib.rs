pub mod clip;
pub mod cluster;
pub mod coalesce;
pub mod drop;
pub mod duckdb_util;
pub mod engine;
pub mod error;
#[cfg(any(feature = "geoparquet", feature = "duckdb"))]
pub mod file_input;
pub mod model;
#[cfg(feature = "mongodb-out")]
pub mod mongo_writer;
pub mod mlt;
pub mod mvt;
pub mod pmtiles_writer;
#[cfg(feature = "postgis")]
pub mod postgis;
#[cfg(feature = "postgis")]
pub mod postgis_input;
#[cfg(feature = "mongodb-out")]
pub mod sink;
pub mod simplify;
pub mod tile_spool;
#[cfg(feature = "postgis")]
pub mod tileflow;
#[cfg(any(feature = "geoparquet", feature = "duckdb", feature = "postgis"))]
pub mod wkb;
#[cfg(feature = "duckdb")]
pub mod streaming;
#[cfg(all(feature = "postgis", feature = "mongodb-out"))]
pub mod streaming_pipeline;
pub mod tiler;

pub use geo;
pub use geo_types;
pub use tile_spool::{TileSpool, unique_suffix};

#[cfg(feature = "mongodb-out")]
pub use mongo_writer::MongoConfig;

pub use engine::{OutputTarget, TileConfig};

#[cfg(feature = "postgis")]
pub use postgis_input::{
    postgis_query_count_with_config, postgis_query_each_batch_with_config,
    postgis_query_exceeds_with_config, postgis_query_to_layers_with_geom, PostgisBatchScanner,
    PostgisConfig, PostgisLayerSchema, postgis_probe_and_maybe_load_layers_with_config,
};

#[cfg(all(feature = "postgis", feature = "mongodb-out"))]
pub use streaming_pipeline::{stream_postgis_to_mongo, StreamingConfig, StreamingTilePipeline};

#[cfg(all(feature = "postgis", feature = "mongodb-out"))]
pub use tileflow::pipeline::run_postgis_to_mongo_stream;
#[cfg(feature = "postgis")]
pub use tileflow::pipeline::run_postgis_to_tile_sink_stream;
