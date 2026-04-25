#[cfg(feature = "mongodb-out")]
mod mongo_impl {
    use crate::tiler::TileCoord;
    use once_cell::sync::Lazy;
    use mongodb::bson::doc;
    use mongodb::options::{ClientOptions, IndexOptions};
    use mongodb::{Client, IndexModel};
    use tokio::runtime::Runtime;
    use crate::engine::ProgressReporter;
    use crate::sink::mongo::{tile_document_from_parts, validate_document_size};

    static TOKIO_RUNTIME: Lazy<Runtime> =
        Lazy::new(|| Runtime::new().expect("failed to create mongodb tokio runtime"));

    fn block_on_safe<F: std::future::Future>(f: F) -> F::Output {
        TOKIO_RUNTIME.block_on(f)
    }

    #[derive(Clone, Debug)]
    pub struct MongoConfig {
        pub uri: String,
        pub database: String,
        pub collection: String,
        pub connect_timeout_ms: Option<u64>,
        pub compress: Option<flate2::Compression>,
        pub batch_size: Option<usize>,
        pub write_concurrency: Option<usize>,
        pub create_indexes: Option<bool>,
        pub upsert: Option<bool>,
        pub flush_tile_threshold: Option<usize>,
        pub flush_byte_threshold: Option<u64>,
    }

    impl MongoConfig {
        pub fn new(uri: impl Into<String>, db: impl Into<String>, coll: impl Into<String>) -> Self {
            Self {
                uri: uri.into(), database: db.into(), collection: coll.into(),
                connect_timeout_ms: Some(10000), compress: None, batch_size: None,
                write_concurrency: None, create_indexes: None, upsert: None,
                flush_tile_threshold: None, flush_byte_threshold: None,
            }
        }
        pub fn batch_size(mut self, v: usize) -> Self { self.batch_size = Some(v); self }
        pub fn write_concurrency(mut self, v: usize) -> Self { self.write_concurrency = Some(v); self }
        pub fn create_indexes(mut self, v: bool) -> Self { self.create_indexes = Some(v); self }
        pub fn upsert(mut self, v: bool) -> Self { self.upsert = Some(v); self }
        pub fn compress(mut self, v: flate2::Compression) -> Self { self.compress = Some(v); self }
        pub fn flush_tile_threshold(mut self, v: usize) -> Self { self.flush_tile_threshold = Some(v); self }
        pub fn flush_byte_threshold(mut self, v: u64) -> Self { self.flush_byte_threshold = Some(v); self }
        pub fn effective_compress(&self) -> flate2::Compression { self.compress.unwrap_or(flate2::Compression::none()) }
        pub fn effective_upsert(&self) -> bool { self.upsert.unwrap_or(true) }
        pub fn effective_create_indexes(&self) -> bool { self.create_indexes.unwrap_or(true) }
        pub fn effective_flush_tile_threshold(&self) -> usize { self.flush_tile_threshold.unwrap_or(4096) }
        pub fn effective_flush_byte_threshold(&self) -> u64 { self.flush_byte_threshold.unwrap_or(64 * 1024 * 1024) }
        pub fn effective_index_fail_is_error(&self) -> bool { true }
        pub fn effective_batch_size(&self) -> usize { self.batch_size.unwrap_or(4096) }
    }

    #[derive(Clone)]
    pub struct MongoTileWriter { config: MongoConfig, client: Client }

    pub struct WriteResult { pub tiles_written: u64, pub bytes_written: u64 }

    impl MongoTileWriter {
        pub fn config(&self) -> &MongoConfig { &self.config }

        pub fn from_config(config: &MongoConfig) -> Result<Self, String> {
            let client = block_on_safe(async {
                let mut options = ClientOptions::parse(&config.uri).await.map_err(|e| e.to_string())?;
                if let Some(ms) = config.connect_timeout_ms {
                    options.connect_timeout = Some(std::time::Duration::from_millis(ms));
                }
                Client::with_options(options).map_err(|e| e.to_string())
            })?;
            Ok(Self { config: config.clone(), client })
        }

        pub fn ensure_indexes(&self) -> Result<(), String> {
            block_on_safe(async {
                let coll = self.client.database(&self.config.database).collection::<mongodb::bson::Document>(&self.config.collection);
                let model = IndexModel::builder()
                    .keys(doc! { "z": 1, "x": 1, "y": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build();
                coll.create_index(model).await.map(|_| ()).map_err(|e| e.to_string())
            })
        }

        fn gzip_compress(data: &[u8], compress: flate2::Compression) -> Vec<u8> {
            if compress.level() <= 0 {
                return data.to_vec();
            }
            use std::io::Write;
            let mut encoder = flate2::write::GzEncoder::new(Vec::new(), compress);
            encoder.write_all(data).ok();
            encoder.finish().unwrap_or_else(|_| data.to_vec())
        }

        pub fn write_tiles(&self, tiles: &[(TileCoord, Vec<u8>)], compress: flate2::Compression, reporter: Option<&dyn ProgressReporter>) -> Result<WriteResult, String> {
            if tiles.is_empty() { return Ok(WriteResult { tiles_written: 0, bytes_written: 0 }); }
            block_on_safe(async {
                let db = self.client.database(&self.config.database);
                let coll = db.collection::<mongodb::bson::Document>(&self.config.collection);
                let mut bytes_written = 0u64;

                for (coord, data) in tiles {
                    let compressed_data = Self::gzip_compress(data, compress);
                    let data_ref = if compressed_data.len() < data.len() { &compressed_data } else { data };
                    let replacement = tile_document_from_parts(coord.z, coord.x, coord.y, data_ref.clone());
                    validate_document_size(&replacement)?;
                    if self.config.effective_upsert() {
                        coll.replace_one(doc! { "id": replacement.get_str("id").unwrap_or_default() }, replacement)
                            .upsert(true)
                            .await
                            .map_err(|e| e.to_string())?;
                    } else {
                        coll.insert_one(replacement)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                    bytes_written += data_ref.len() as u64;
                    if let Some(reporter) = reporter {
                        reporter.report(&format!(
                            "           mongo write z={} x={} y={}",
                            coord.z, coord.x, coord.y
                        ));
                    }
                }

                Ok(WriteResult { tiles_written: tiles.len() as u64, bytes_written })
            })
        }
    }
}

#[cfg(feature = "mongodb-out")]
pub use mongo_impl::*;
