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
                    let doc = encoded_tile_to_document(tile);
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
        fn push(&mut self, tile: EncodedTile) -> Result<(), String> {
            MongoTileSink::push(self, tile)
        }

        fn finish(&mut self) -> Result<u64, String> {
            MongoTileSink::finish(self)
        }
    }

    pub fn encoded_tile_to_document(tile: &EncodedTile) -> Document {
        doc! {
            "id": format!("{}/{}/{}", tile.key.z, tile.key.x, tile.key.y),
            "z": tile.key.z as i32,
            "x": tile.key.x as i32,
            "y": tile.key.y as i32,
            "data": Binary {
                subtype: BinarySubtype::Generic,
                bytes: tile.data.clone(),
            }
        }
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
    }
}

#[cfg(feature = "mongodb-out")]
pub use mongo_impl::*;
