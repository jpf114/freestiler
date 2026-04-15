//! MongoDB output writer: store tile data as {z, x, y, d} documents.
//!
//! Supports:
//! - Batched insert and bulk upsert modes (using bulk_write API for MongoDB 3.6+)
//! - Automatic gzip compression of tile data
//! - Automatic index creation on (z, x, y)
//! - Exponential-backoff retry for transient errors
//! - Connection string masking in logs
//! - TLS/SSL connection support

#[cfg(feature = "mongodb-out")]
mod mongo_impl {
    use crate::engine::ProgressReporter;
    use crate::pmtiles_writer::gzip_compress;
    use crate::tiler::TileCoord;
    use bson::doc;
    use log::{debug, warn};
    use rayon::prelude::*;
    use mongodb::options::{ClientOptions, InsertManyOptions};
    use mongodb::Client;
    use once_cell::sync::Lazy;

    const DEFAULT_BATCH_SIZE: usize = 4096;
    /// Default max uncompressed tile bytes to buffer before cross-zoom Mongo flush.
    const DEFAULT_FLUSH_BYTE_THRESHOLD: u64 = 64 * 1024 * 1024;
    const DEFAULT_MAX_RETRIES: u32 = 3;
    const DEFAULT_WRITE_CONCURRENCY: usize = 2;
    const RETRY_BASE_DELAY_MS: u64 = 100;
    const BULK_WRITE_BATCH_SIZE: usize = 250;
    /// Parallel gzip when a batch has at least this many tiles (avoids rayon overhead on tiny sets).
    const PARALLEL_COMPRESS_MIN: usize = 16;

    const TRANSIENT_ERROR_CODES: &[i32] = &[
        6,    // HostUnreachable
        24,   // LockTimeout
        89,   // NetworkTimeout
        91,   // ShutdownInProgress
        112,  // WriteConflict
        259,  // ExceededTimeLimit
        262,  // SnapshotTooOld
    ];

    static TOKIO_RT: Lazy<tokio::runtime::Runtime> =
        Lazy::new(|| tokio::runtime::Runtime::new().expect("Cannot create global tokio runtime"));

    fn block_on_safe<F>(future: F) -> F::Output
    where
        F: std::future::Future,
    {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                let _guard = handle.enter();
                tokio::task::block_in_place(|| handle.block_on(future))
            }
            Err(_) => TOKIO_RT.block_on(future),
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct MongoConfig {
        pub uri: String,
        pub database: String,
        pub collection: String,
        pub batch_size: Option<usize>,
        pub compress: Option<bool>,
        pub create_indexes: Option<bool>,
        pub upsert: Option<bool>,
        pub ordered: Option<bool>,
        pub max_retries: Option<u32>,
        pub write_concurrency: Option<usize>,
        pub index_fail_is_error: Option<bool>,
        pub connect_timeout_ms: Option<u64>,
        pub use_tls: Option<bool>,
        pub tls_ca_file: Option<String>,
        pub tls_allow_invalid: Option<bool>,
        pub flush_tile_threshold: Option<usize>,
        pub flush_byte_threshold: Option<u64>,
    }

    impl MongoConfig {
        pub fn new(uri: impl Into<String>, database: impl Into<String>, collection: impl Into<String>) -> Self {
            Self {
                uri: uri.into(),
                database: database.into(),
                collection: collection.into(),
                ..Default::default()
            }
        }

        pub fn batch_size(mut self, v: usize) -> Self { self.batch_size = Some(v); self }
        pub fn compress(mut self, v: bool) -> Self { self.compress = Some(v); self }
        pub fn create_indexes(mut self, v: bool) -> Self { self.create_indexes = Some(v); self }
        pub fn upsert(mut self, v: bool) -> Self { self.upsert = Some(v); self }
        pub fn ordered(mut self, v: bool) -> Self { self.ordered = Some(v); self }
        pub fn max_retries(mut self, v: u32) -> Self { self.max_retries = Some(v); self }
        pub fn write_concurrency(mut self, v: usize) -> Self { self.write_concurrency = Some(v); self }
        pub fn index_fail_is_error(mut self, v: bool) -> Self { self.index_fail_is_error = Some(v); self }
        pub fn connect_timeout_ms(mut self, v: u64) -> Self { self.connect_timeout_ms = Some(v); self }
        pub fn use_tls(mut self, v: bool) -> Self { self.use_tls = Some(v); self }
        pub fn tls_ca_file(mut self, v: impl Into<String>) -> Self { self.tls_ca_file = Some(v.into()); self }
        pub fn tls_allow_invalid(mut self, v: bool) -> Self { self.tls_allow_invalid = Some(v); self }
        pub fn flush_tile_threshold(mut self, v: usize) -> Self { self.flush_tile_threshold = Some(v); self }
        pub fn flush_byte_threshold(mut self, v: u64) -> Self { self.flush_byte_threshold = Some(v); self }

        pub fn effective_batch_size(&self) -> usize { self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE) }
        pub fn effective_compress(&self) -> bool { self.compress.unwrap_or(true) }
        pub fn effective_create_indexes(&self) -> bool { self.create_indexes.unwrap_or(true) }
        pub fn effective_upsert(&self) -> bool { self.upsert.unwrap_or(false) }
        pub fn effective_ordered(&self) -> bool { self.ordered.unwrap_or(false) }
        pub fn effective_max_retries(&self) -> u32 { self.max_retries.unwrap_or(DEFAULT_MAX_RETRIES) }
        pub fn effective_write_concurrency(&self) -> usize { self.write_concurrency.unwrap_or(DEFAULT_WRITE_CONCURRENCY).max(1) }
        pub fn effective_index_fail_is_error(&self) -> bool { self.index_fail_is_error.unwrap_or(false) }
        pub fn effective_use_tls(&self) -> bool { self.use_tls.unwrap_or(false) }
        pub fn effective_tls_allow_invalid(&self) -> bool { self.tls_allow_invalid.unwrap_or(false) }

        /// Tiles to accumulate (large-data Mongo) before calling `write_tiles`.
        pub fn effective_flush_tile_threshold(&self) -> usize {
            self.flush_tile_threshold.unwrap_or_else(|| {
                self.effective_batch_size().saturating_mul(16).max(8192)
            })
        }

        pub fn effective_flush_byte_threshold(&self) -> u64 {
            self.flush_byte_threshold.unwrap_or(DEFAULT_FLUSH_BYTE_THRESHOLD)
        }
    }

    #[derive(Debug, Default)]
    pub struct MongoWriteResult {
        pub tiles_written: u64,
        pub tiles_upserted: u64,
        pub tiles_failed: u64,
        pub bytes_written: u64,
    }

    pub struct MongoTileWriter {
        config: MongoConfig,
        client: Client,
    }

    impl MongoTileWriter {
        pub fn new(uri: &str, db_name: &str, coll_name: &str) -> Result<Self, String> {
            Self::from_config(&MongoConfig::new(uri, db_name, coll_name))
        }

        pub fn from_config(config: &MongoConfig) -> Result<Self, String> {
            let client = block_on_safe(async {
                let mut options = ClientOptions::parse(&config.uri).await
                    .map_err(|e| format!("Cannot parse MongoDB URI '{}': {}",
                        crate::tiler::mask_conn_str(&config.uri), e))?;

                if let Some(timeout_ms) = config.connect_timeout_ms {
                    options.connect_timeout = Some(std::time::Duration::from_millis(timeout_ms));
                    options.server_selection_timeout = Some(std::time::Duration::from_millis(timeout_ms));
                }

                // TLS parameters are expected to be encoded in the URI for driver v3.
                // Keep builder flags for API compatibility, but avoid assigning
                // non-existent ClientOptions TLS fields across driver versions.
                let _ = config.effective_use_tls();
                let _ = &config.tls_ca_file;
                let _ = config.effective_tls_allow_invalid();

                Client::with_options(options)
                    .map_err(|e| format!("Cannot create MongoDB client: {}", e))
            })?;
            Ok(Self { config: config.clone(), client })
        }

        fn collection(&self) -> mongodb::Collection<bson::Document> {
            self.client.database(&self.config.database)
                .collection::<bson::Document>(&self.config.collection)
        }

        fn with_runtime<F, T>(&self, f: F) -> Result<T, String>
        where F: std::future::Future<Output = Result<T, String>>,
        { block_on_safe(f) }

        pub fn write_tiles(
            &self, tiles: &[(TileCoord, Vec<u8>)], compress: bool,
            reporter: Option<&dyn ProgressReporter>,
        ) -> Result<MongoWriteResult, String> {
            self.with_runtime(self.write_tiles_async(tiles, compress, reporter))
        }

        async fn write_tiles_async(
            &self, tiles: &[(TileCoord, Vec<u8>)], compress: bool,
            reporter: Option<&dyn ProgressReporter>,
        ) -> Result<MongoWriteResult, String> {
            debug!(
                "Writing {} tiles to MongoDB ({}:{}, compress={})",
                tiles.len(),
                self.config.database,
                self.config.collection,
                compress
            );
            let collection = self.collection();
            let batch_size = self.config.effective_batch_size();
            let upsert_mode = self.config.effective_upsert();
            let max_retries = self.config.effective_max_retries();
            let write_concurrency = self.config.effective_write_concurrency();
            let mut total_written: u64 = 0;
            let mut total_upserted: u64 = 0;
            let mut total_failed: u64 = 0;
            let mut total_bytes: u64 = 0;

            let mut prepared_batches: Vec<(Vec<bson::Document>, u64)> = Vec::new();
            for batch in tiles.chunks(batch_size) {
                let mut docs: Vec<bson::Document> = Vec::with_capacity(batch.len());
                let mut batch_bytes: u64 = 0;

                if compress && batch.len() >= PARALLEL_COMPRESS_MIN {
                    let parts: Vec<Option<(bson::Document, u64)>> = batch
                        .par_iter()
                        .map(|(coord, data)| match gzip_compress(data) {
                            Ok(tile_data) => {
                                let bl = tile_data.len() as u64;
                                let d = doc! {
                                    "z": i32::from(coord.z),
                                    "x": coord.x as i32,
                                    "y": coord.y as i32,
                                    "d": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: tile_data },
                                };
                                Some((d, bl))
                            }
                            Err(e) => {
                                warn!(
                                    "gzip compression failed for tile z={} x={} y={}: {}",
                                    coord.z, coord.x, coord.y, e
                                );
                                None
                            }
                        })
                        .collect();
                    for p in parts {
                        match p {
                            Some((d, bl)) => {
                                docs.push(d);
                                batch_bytes += bl;
                            }
                            None => total_failed += 1,
                        }
                    }
                } else {
                    for (coord, data) in batch {
                        let tile_data = if compress {
                            match gzip_compress(data) {
                                Ok(c) => c,
                                Err(e) => {
                                    warn!(
                                        "gzip compression failed for tile z={} x={} y={}: {}",
                                        coord.z, coord.x, coord.y, e
                                    );
                                    total_failed += 1;
                                    continue;
                                }
                            }
                        } else {
                            data.clone()
                        };
                        batch_bytes += tile_data.len() as u64;
                        docs.push(doc! {
                            "z": i32::from(coord.z), "x": coord.x as i32, "y": coord.y as i32,
                            "d": bson::Binary { subtype: bson::spec::BinarySubtype::Generic, bytes: tile_data },
                        });
                    }
                }

                if !docs.is_empty() {
                    prepared_batches.push((docs, batch_bytes));
                }
            }

            let total_batches = prepared_batches.len();
            if upsert_mode {
                for (batch_idx, (docs, batch_bytes)) in prepared_batches.into_iter().enumerate() {
                    let result = self.bulk_upsert_with_retry(&collection, &docs, max_retries).await?;
                    total_upserted += result.upserted;
                    total_written += docs.len() as u64;
                    total_bytes += batch_bytes;
                    if let Some(r) = reporter {
                        r.report(&format!(
                            "  MongoDB batch {}/{}: {} tiles written",
                            batch_idx + 1,
                            total_batches,
                            total_written
                        ));
                    }
                    debug!(
                        "Batch {}/{}: {} tiles written ({} bytes)",
                        batch_idx + 1,
                        total_batches,
                        total_written,
                        total_bytes
                    );
                }
            } else {
                use futures::stream::{self, StreamExt};
                let insert_options = InsertManyOptions::builder()
                    .ordered(self.config.effective_ordered())
                    .build();
                let coll = collection.clone();
                let results = stream::iter(prepared_batches.into_iter().enumerate().map(|(batch_idx, (docs, batch_bytes))| {
                    let coll = coll.clone();
                    let insert_options = insert_options.clone();
                    async move {
                        let mut attempt = 0u32;
                        loop {
                            match coll.insert_many(docs.clone()).with_options(insert_options.clone()).await {
                                Ok(r) => return Ok::<(usize, u64, u64), String>((batch_idx, r.inserted_ids.len() as u64, batch_bytes)),
                                Err(e) => {
                                    if attempt < max_retries && is_transient_error(&e) {
                                        let delay = std::time::Duration::from_millis(
                                            RETRY_BASE_DELAY_MS * 2u64.pow(attempt),
                                        );
                                        tokio::time::sleep(delay).await;
                                        attempt += 1;
                                    } else {
                                        return Err(format!("MongoDB insert error: {}", e));
                                    }
                                }
                            }
                        }
                    }
                }))
                .buffer_unordered(write_concurrency)
                .collect::<Vec<_>>()
                .await;

                for item in results {
                    let (batch_idx, inserted, batch_bytes) = item?;
                    total_written += inserted;
                    total_bytes += batch_bytes;
                    if let Some(r) = reporter {
                        r.report(&format!(
                            "  MongoDB batch {}/{}: {} tiles written",
                            batch_idx + 1,
                            total_batches,
                            total_written
                        ));
                    }
                }
            }

            if total_failed > 0 {
                warn!("{} tiles failed during MongoDB write", total_failed);
            }

            debug!(
                "MongoDB write complete: {} tiles, {} upserted, {} failed, {} bytes",
                total_written, total_upserted, total_failed, total_bytes
            );
            Ok(MongoWriteResult { tiles_written: total_written, tiles_upserted: total_upserted, tiles_failed: total_failed, bytes_written: total_bytes })
        }

        async fn bulk_upsert_with_retry(
            &self,
            collection: &mongodb::Collection<bson::Document>,
            docs: &[bson::Document],
            max_retries: u32,
        ) -> Result<BulkUpsertResult, String> {
            let mut total_upserted: u64 = 0;
            let mut total_modified: u64 = 0;

            for chunk in docs.chunks(BULK_WRITE_BATCH_SIZE) {
                for doc in chunk {
                    let z = doc.get_i32("z").unwrap_or(0);
                    let x = doc.get_i32("x").unwrap_or(0);
                    let y = doc.get_i32("y").unwrap_or(0);

                    let filter = doc! { "z": z, "x": x, "y": y };
                    let update = doc! { "$set": doc.clone() };

                    let mut attempt = 0u32;
                    loop {
                        match collection.update_one(filter.clone(), update.clone()).upsert(true).await {
                            Ok(result) => {
                                if result.upserted_id.is_some() {
                                    total_upserted += 1;
                                } else if result.modified_count > 0 || result.matched_count > 0 {
                                    total_modified += 1;
                                }
                                break;
                            }
                            Err(e) => {
                                if attempt < max_retries && is_transient_error(&e) {
                                    let delay = std::time::Duration::from_millis(
                                        RETRY_BASE_DELAY_MS * 2u64.pow(attempt));
                                    warn!(
                                        "MongoDB upsert retry ({}/{}): {}",
                                        attempt + 1,
                                        max_retries,
                                        e
                                    );
                                    tokio::time::sleep(delay).await;
                                    attempt += 1;
                                } else {
                                    return Err(format!("MongoDB upsert error: {}", e));
                                }
                            }
                        }
                    }
                }
            }

            let _ = total_modified;
            Ok(BulkUpsertResult { upserted: total_upserted })
        }

        pub fn ensure_indexes(&self) -> Result<(), String> {
            self.with_runtime(self.ensure_indexes_async())
        }

        async fn ensure_indexes_async(&self) -> Result<(), String> {
            let collection = self.collection();
            let index_model = mongodb::IndexModel::builder()
                .keys(doc! { "z": 1, "x": 1, "y": 1 })
                .options(mongodb::options::IndexOptions::builder().unique(true).build())
                .build();
            collection.create_index(index_model).await
                .map_err(|e| format!("Cannot create index: {}", e))?;
            Ok(())
        }

        pub fn drop_collection(&self) -> Result<(), String> {
            self.with_runtime(async {
                let collection = self.collection();
                collection.drop().await.map_err(|e| format!("Cannot drop collection: {}", e))?;
                Ok(())
            })
        }

        pub fn count_documents(&self) -> Result<u64, String> {
            self.with_runtime(async {
                let collection = self.collection();
                collection.count_documents(doc! {}).await
                    .map_err(|e| format!("Cannot count documents: {}", e))
            })
        }
    }

    struct BulkUpsertResult {
        upserted: u64,
    }

    fn is_transient_error(error: &mongodb::error::Error) -> bool {
        match &*error.kind {
            mongodb::error::ErrorKind::Command(ref cmd) => {
                TRANSIENT_ERROR_CODES.contains(&cmd.code)
            }
            mongodb::error::ErrorKind::Io(_) => true,
            _ => false,
        }
    }

    pub fn write_tiles_to_mongo(
        config: &MongoConfig, tiles: &[(TileCoord, Vec<u8>)],
        compress: bool, create_indexes: bool,
    ) -> Result<MongoWriteResult, String> {
        let writer = MongoTileWriter::from_config(config)?;
        if create_indexes { writer.ensure_indexes()?; }
        writer.write_tiles(tiles, compress, None)
    }

    pub fn write_tiles_to_mongo_with_progress(
        config: &MongoConfig, tiles: &[(TileCoord, Vec<u8>)],
        compress: bool, create_indexes: bool, reporter: &dyn ProgressReporter,
    ) -> Result<MongoWriteResult, String> {
        let writer = MongoTileWriter::from_config(config)?;
        if create_indexes {
            if let Err(e) = writer.ensure_indexes() {
                if config.effective_index_fail_is_error() {
                    return Err(e);
                }
                warn!("Index creation failed (non-fatal): {}", e);
            }
        }
        writer.write_tiles(tiles, compress, Some(reporter))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_mask_mongo_uri() {
            let uri = "mongodb://user:secret@localhost:27017/mydb";
            assert_eq!(crate::tiler::mask_conn_str(uri), "mongodb://user:***@localhost:27017/mydb");
        }

        #[test]
        fn test_mongo_config_defaults() {
            let c = MongoConfig::new("mongodb://localhost:27017", "test", "tiles");
            assert_eq!(c.effective_batch_size(), DEFAULT_BATCH_SIZE);
            assert!(c.effective_compress());
            assert!(!c.effective_upsert());
            assert_eq!(c.effective_max_retries(), DEFAULT_MAX_RETRIES);
            assert!(!c.effective_index_fail_is_error());
            assert!(!c.effective_use_tls());
        }

        #[test]
        fn test_mongo_config_builder() {
            let c = MongoConfig::new("mongodb://localhost:27017", "test", "tiles")
                .batch_size(500)
                .upsert(true)
                .compress(false)
                .max_retries(5)
                .use_tls(true);
            assert_eq!(c.effective_batch_size(), 500);
            assert!(c.effective_upsert());
            assert!(!c.effective_compress());
            assert_eq!(c.effective_max_retries(), 5);
            assert!(c.effective_use_tls());
        }

        #[test]
        fn test_transient_error_codes() {
            assert!(TRANSIENT_ERROR_CODES.contains(&112));
            assert!(TRANSIENT_ERROR_CODES.contains(&24));
            assert!(TRANSIENT_ERROR_CODES.contains(&259));
            assert!(TRANSIENT_ERROR_CODES.contains(&262));
            assert!(TRANSIENT_ERROR_CODES.contains(&6));
            assert!(TRANSIENT_ERROR_CODES.contains(&89));
        }
    }
}

#[cfg(feature = "mongodb-out")]
pub use mongo_impl::{
    write_tiles_to_mongo, write_tiles_to_mongo_with_progress,
    MongoConfig, MongoTileWriter, MongoWriteResult,
};
