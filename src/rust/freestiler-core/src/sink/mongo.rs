#[cfg(feature = "mongodb-out")]
mod mongo_impl {
    use mongodb::bson::{doc, spec::BinarySubtype, Binary, Document};
    use mongodb::options::{ClientOptions, IndexOptions};
    use mongodb::{Client, Collection, IndexModel};
    use once_cell::sync::Lazy;
    use tokio::runtime::Runtime;

    use crate::model::EncodedTile;
    use crate::tileflow::pipeline::TileSink;

    static TOKIO_RUNTIME: Lazy<Runtime> =
        Lazy::new(|| Runtime::new().expect("failed to create mongodb tokio runtime"));
    const MONGO_MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;

    fn block_on_safe<F: std::future::Future>(f: F) -> F::Output {
        TOKIO_RUNTIME.block_on(f)
    }

    #[derive(Clone, Debug)]
    pub struct MongoSinkConfig {
        pub uri: String,
        pub database: String,
        pub collection: String,
        pub batch_size: usize,
        pub create_indexes: bool,
        pub upsert: bool,
    }

    impl MongoSinkConfig {
        pub fn new(uri: impl Into<String>, db: impl Into<String>, coll: impl Into<String>) -> Self {
            Self {
                uri: uri.into(),
                database: db.into(),
                collection: coll.into(),
                batch_size: 4096,
                create_indexes: true,
                upsert: true,
            }
        }
    }

    pub struct MongoTileSink {
        collection: Collection<Document>,
        config: MongoSinkConfig,
        pending: Vec<EncodedTile>,
    }

    impl MongoTileSink {
        pub fn open(config: &MongoSinkConfig) -> Result<Self, String> {
            let collection = block_on_safe(async {
                let options = ClientOptions::parse(&config.uri)
                    .await
                    .map_err(|e| e.to_string())?;
                let client = Client::with_options(options).map_err(|e| e.to_string())?;
                Ok::<Collection<Document>, String>(
                    client
                        .database(&config.database)
                        .collection::<Document>(&config.collection),
                )
            })?;

            Ok(Self {
                collection,
                config: config.clone(),
                pending: Vec::new(),
            })
        }

        pub fn ensure_indexes(&self) -> Result<(), String> {
            block_on_safe(async {
                let model = IndexModel::builder()
                    .keys(doc! { "id": 1 })
                    .options(IndexOptions::builder().unique(true).build())
                    .build();
                self.collection
                    .create_index(model)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            })
        }

        pub fn push(&mut self, tile: EncodedTile) -> Result<(), String> {
            self.pending.push(tile);
            if self.pending.len() >= self.config.batch_size {
                self.flush()?;
            }
            Ok(())
        }

        pub fn flush(&mut self) -> Result<u64, String> {
            if self.pending.is_empty() {
                return Ok(0);
            }
            let tiles = std::mem::take(&mut self.pending);
            let coll = self.collection.clone();
            let upsert = self.config.upsert;
            block_on_safe(async move {
                for tile in &tiles {
                    let doc = validated_encoded_tile_document(tile)?;
                    if upsert {
                        coll.replace_one(doc! { "id": doc.get_str("id").unwrap_or_default() }, doc)
                            .upsert(true)
                            .await
                            .map_err(|e| e.to_string())?;
                    } else {
                        coll.insert_one(doc)
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
                Ok::<u64, String>(tiles.len() as u64)
            })
        }

        pub fn finish(&mut self) -> Result<u64, String> {
            self.flush()
        }
    }

    impl TileSink for MongoTileSink {
        fn validate_tile(&self, tile: &EncodedTile) -> Result<(), String> {
            validate_encoded_tile_for_mongo(tile)
        }

        fn push(&mut self, tile: EncodedTile) -> Result<(), String> {
            MongoTileSink::push(self, tile)
        }

        fn finish(&mut self) -> Result<u64, String> {
            MongoTileSink::finish(self)
        }
    }

    pub fn encoded_tile_to_document(tile: &EncodedTile) -> Document {
        tile_document_from_parts(tile.key.z, tile.key.x, tile.key.y, tile.data.clone())
    }

    pub(crate) fn validate_encoded_tile_for_mongo(tile: &EncodedTile) -> Result<(), String> {
        let doc = encoded_tile_to_document(tile);
        validate_document_size(&doc)
    }

    pub(crate) fn tile_document_from_parts(z: u8, x: u32, y: u32, data: Vec<u8>) -> Document {
        doc! {
            "id": format!("{}/{}/{}", z, x, y),
            "z": z as i32,
            "x": x as i32,
            "y": y as i32,
            "data": Binary {
                subtype: BinarySubtype::Generic,
                bytes: data,
            }
        }
    }

    pub(crate) fn validate_document_size(doc: &Document) -> Result<(), String> {
        let data_len = doc
            .get_binary_generic("data")
            .map(|bytes| bytes.len())
            .unwrap_or_default();
        if data_len >= MONGO_MAX_DOCUMENT_BYTES {
            return Err(format!(
                "tile z={} x={} y={} data size {} exceeded MongoDB 16MB document limit; increase min_zoom or reduce tile density",
                doc.get_i32("z").unwrap_or_default(),
                doc.get_i32("x").unwrap_or_default(),
                doc.get_i32("y").unwrap_or_default(),
                data_len
            ));
        }

        let bytes = mongodb::bson::to_vec(doc).map_err(|_| {
            format!(
                "tile z={} x={} y={} exceeded MongoDB 16MB document limit; increase min_zoom or reduce tile density",
                doc.get_i32("z").unwrap_or_default(),
                doc.get_i32("x").unwrap_or_default(),
                doc.get_i32("y").unwrap_or_default()
            )
        })?;
        if bytes.len() > MONGO_MAX_DOCUMENT_BYTES {
            return Err(format!(
                "tile z={} x={} y={} encoded document size {} exceeded MongoDB 16MB document limit; increase min_zoom or reduce tile density",
                doc.get_i32("z").unwrap_or_default(),
                doc.get_i32("x").unwrap_or_default(),
                doc.get_i32("y").unwrap_or_default(),
                bytes.len()
            ));
        }
        Ok(())
    }

    pub(crate) fn validated_encoded_tile_document(tile: &EncodedTile) -> Result<Document, String> {
        validate_encoded_tile_for_mongo(tile)?;
        let doc = encoded_tile_to_document(tile);
        Ok(doc)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::model::{ContentEncoding, EncodedTile, TileKey};
        use crate::pmtiles_writer::TileFormat;

        #[test]
        fn mongo_document_keeps_only_id_xyz_data() {
            let tile = EncodedTile {
                key: TileKey { z: 3, x: 2, y: 1 },
                data: vec![1, 2, 3],
                tile_format: TileFormat::Mlt,
                content_encoding: ContentEncoding::Gzip,
            };

            let doc = encoded_tile_to_document(&tile);
            let keys: Vec<&str> = doc.keys().map(|k| k.as_str()).collect();
            assert_eq!(keys, vec!["id", "z", "x", "y", "data"]);
            assert_eq!(doc.get_str("id").unwrap(), "3/2/1");
        }

        #[test]
        fn mongo_rejects_oversized_tile_document() {
            let doc = tile_document_from_parts(8, 10, 20, vec![0u8; 16 * 1024 * 1024]);
            let err = validate_document_size(&doc).expect_err("oversized tile must be rejected");

            assert!(err.contains("z=8"));
            assert!(err.contains("x=10"));
            assert!(err.contains("y=20"));
            assert!(err.contains("16MB"));
        }
    }
}

#[cfg(feature = "mongodb-out")]
pub use mongo_impl::*;
