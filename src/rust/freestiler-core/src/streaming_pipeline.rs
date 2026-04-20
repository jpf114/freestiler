//! Spatial-partitioned streaming pipeline from PostGIS to MongoDB.

#[cfg(all(feature = "postgis", feature = "mongodb-out"))]
mod streaming_impl {
    use crate::pmtiles_writer::TileFormat;
    use crate::{tiler, mvt, mlt};
    use log::info;
    use std::time::Instant;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crossbeam_channel::unbounded;
    use crate::tiler::TileCoord;

    #[derive(Clone, Copy, Debug)]
    pub struct StreamingConfig {
        pub spatial_strips: usize,
        pub concurrency: usize,
        pub flush_tile_threshold: usize,
        pub flush_byte_threshold: u64,
    }

    impl Default for StreamingConfig {
        fn default() -> Self {
            Self {
                spatial_strips: 32,
                concurrency: 4,
                flush_tile_threshold: 4096,
                flush_byte_threshold: 64 * 1024 * 1024,
            }
        }
    }

    pub struct StreamingTileBuffer {
        writer: crate::mongo_writer::MongoTileWriter,
        compress: flate2::Compression,
        tiles: Vec<(TileCoord, Vec<u8>)>,
        total_bytes: u64,
        config: StreamingConfig,
    }

    impl StreamingTileBuffer {
        pub fn new(writer: crate::mongo_writer::MongoTileWriter, compress: flate2::Compression, config: StreamingConfig) -> Result<Self, String> {
            Ok(Self { writer, compress, tiles: Vec::new(), total_bytes: 0, config })
        }

        pub fn add_tile(&mut self, coord: TileCoord, data: Vec<u8>) -> Result<(), String> {
            self.total_bytes += data.len() as u64;
            self.tiles.push((coord, data));
            if self.tiles.len() >= self.config.flush_tile_threshold || self.total_bytes >= self.config.flush_byte_threshold {
                self.flush()?;
            }
            Ok(())
        }

        pub fn flush(&mut self) -> Result<(), String> {
            if self.tiles.is_empty() { return Ok(()); }
            self.writer.write_tiles(&self.tiles, self.compress, None)?;
            self.tiles.clear();
            self.total_bytes = 0;
            Ok(())
        }
    }

    pub struct StreamingTilePipeline {
        mongo_writer: crate::mongo_writer::MongoTileWriter,
    }

    impl StreamingTilePipeline {
        pub fn new(_pg_config: &crate::postgis_input::PostgisConfig, mongo_config: &crate::mongo_writer::MongoConfig) -> Result<Self, String> {
            let mongo_writer = crate::mongo_writer::MongoTileWriter::from_config(mongo_config)?;
            if mongo_config.effective_create_indexes() { mongo_writer.ensure_indexes()?; }
            Ok(Self { mongo_writer })
        }

        pub fn stream_zoom(
            &self,
            zoom: u8,
            layer_name: String,
            sql: String,
            geom_column: Option<String>,
            tile_config: crate::engine::TileConfig,
            streaming_config: StreamingConfig,
            postgis_config: crate::postgis_input::PostgisConfig,
            reporter: Arc<dyn crate::engine::ProgressReporter>,
        ) -> Result<u64, String> {
            let start = Instant::now();
            let num_strips = streaming_config.spatial_strips;
            let concurrency = streaming_config.concurrency;
            let total_rows = (1u32 << zoom) as usize;
            let rows_per_strip = (total_rows + num_strips - 1) / num_strips;

            let (tx, rx) = unbounded::<(TileCoord, Vec<u8>)>();
            let strips_with_data = Arc::new(Mutex::new(0usize));
            let strip_counter = Arc::new(AtomicUsize::new(0));

            let mongo_writer = self.mongo_writer.clone();
            let compress = self.mongo_writer.config().effective_compress();
            
            let reporter_consumer = reporter.clone();
            let consumer_handle = std::thread::spawn(move || -> Result<u64, String> {
                let mut buffer = StreamingTileBuffer::new(mongo_writer, compress, streaming_config)?;
                let mut local_count = 0u64;
                while let Ok((coord, data)) = rx.recv() {
                    let res = buffer.add_tile(coord, data)?;
                    local_count += 1;
                }
                buffer.flush()?;
                reporter_consumer.report("Consumer thread finished");
                Ok(local_count)
            });

            let mut producers = Vec::new();
            for _ in 0..concurrency {
                let tx = tx.clone();
                let sc = strip_counter.clone();
                let postgis_config = postgis_config.clone();
                let sql = sql.clone();
                let geom_column = geom_column.clone();
                let layer_name = layer_name.clone();
                let tile_config = tile_config.clone();
                let strips_with_data = strips_with_data.clone();

                producers.push(std::thread::spawn(move || -> Result<(), String> {
                    loop {
                        let strip_idx = sc.fetch_add(1, Ordering::SeqCst);
                        if strip_idx >= num_strips { break; }
                        let y_start = (strip_idx * rows_per_strip) as u32;
                        let y_end = ((strip_idx + 1) * rows_per_strip).min(total_rows) as u32;
                        if y_start >= total_rows as u32 { continue; }

                        let max_y_lat = tiler::tile_y_to_lat(y_start, zoom);
                        let min_y_lat = tiler::tile_y_to_lat(y_end, zoom);

                        let scanner = crate::postgis_input::PostgisBatchScanner::new(&postgis_config, &sql, geom_column.as_deref())?;

                        let features = scanner.query_features_in_bbox(-180.0, min_y_lat, 180.0, max_y_lat)?;
                        if features.is_empty() { continue; }
                        { *strips_with_data.lock().unwrap() += 1; }

                        let prop_names = scanner.schema().prop_names.clone();
                        let tile_map = tiler::assign_features_to_tiles(&features, zoom);
                        
                        for (coord, tile_features) in tile_map {
                            let data = match tile_config.tile_format {
                                TileFormat::Mlt => mlt::encode_tile(&coord, &tile_features, &layer_name, &prop_names),
                                TileFormat::Mvt => mvt::encode_tile(&coord, &tile_features, &layer_name, &prop_names),
                            };
                            if !data.is_empty() {
                                if tx.send((coord, data)).is_err() { break; }
                            }
                        }
                    }
                    Ok(())
                }));
            }
            drop(tx);

            for p in producers { p.join().map_err(|_| "Producer panic")??; }
            let total_tiles = consumer_handle.join().map_err(|_| "Consumer panic")??;

            let duration = start.elapsed();
            reporter.report(&format!("Zoom {} complete: {} tiles produced from {} strips in {:?}", zoom, total_tiles, *strips_with_data.lock().unwrap(), duration));
            Ok(total_tiles)
        }
    }

    pub fn stream_postgis_to_mongo(
        pg_config: &crate::postgis_input::PostgisConfig,
        sql: &str,
        geom_column: Option<&str>,
        mongo_config: &crate::mongo_writer::MongoConfig,
        tile_config: crate::engine::TileConfig,
        streaming_config: Option<StreamingConfig>,
        layer_name: &str,
        reporter: Arc<dyn crate::engine::ProgressReporter>,
    ) -> Result<u64, String> {
        let pipeline = StreamingTilePipeline::new(pg_config, mongo_config)?;
        let mut total = 0;
        let s_config = streaming_config.unwrap_or_default();
        for z in tile_config.min_zoom..=tile_config.max_zoom {
            total += pipeline.stream_zoom(z, layer_name.to_string(), sql.to_string(), geom_column.map(|s| s.to_string()), tile_config.clone(), s_config, pg_config.clone(), reporter.clone())?;
        }
        Ok(total)
    }
}

pub use streaming_impl::*;
