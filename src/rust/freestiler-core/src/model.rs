use geo_types::Rect;

use crate::pmtiles_writer::TileFormat;
use crate::tiler::{Feature, Geometry, PropertyValue, TileCoord};

#[derive(Clone, Debug)]
pub struct LayerSchema {
    pub layer_name: String,
    pub geom_column: String,
    pub prop_names: Vec<String>,
    pub prop_types: Vec<String>,
    pub source_srid: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct BBox4326 {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl From<Rect<f64>> for BBox4326 {
    fn from(value: Rect<f64>) -> Self {
        Self {
            min_lon: value.min().x,
            min_lat: value.min().y,
            max_lon: value.max().x,
            max_lat: value.max().y,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NormalizedFeature {
    pub id: Option<u64>,
    pub geometry: Geometry,
    pub properties: Vec<PropertyValue>,
    pub bbox: BBox4326,
}

impl NormalizedFeature {
    pub fn to_feature(&self) -> Feature {
        Feature {
            id: self.id,
            geometry: self.geometry.clone(),
            properties: self.properties.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PartitionSpec {
    pub sequence: u64,
    pub zoom: u8,
    pub row_start: u32,
    pub row_end: u32,
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

#[derive(Clone, Debug)]
pub struct FeatureBatch {
    pub partition: PartitionSpec,
    pub features: Vec<NormalizedFeature>,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct TileKey {
    pub z: u8,
    pub x: u32,
    pub y: u32,
}

impl From<TileKey> for TileCoord {
    fn from(value: TileKey) -> Self {
        Self {
            z: value.z,
            x: value.x,
            y: value.y,
        }
    }
}

impl From<TileCoord> for TileKey {
    fn from(value: TileCoord) -> Self {
        Self {
            z: value.z,
            x: value.x,
            y: value.y,
        }
    }
}

#[derive(Clone, Debug)]
pub enum ContentEncoding {
    Identity,
    Gzip,
}

impl ContentEncoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            ContentEncoding::Identity => "identity",
            ContentEncoding::Gzip => "gzip",
        }
    }
}

#[derive(Clone, Debug)]
pub struct EncodedTile {
    pub key: TileKey,
    pub data: Vec<u8>,
    pub tile_format: TileFormat,
    pub content_encoding: ContentEncoding,
}
