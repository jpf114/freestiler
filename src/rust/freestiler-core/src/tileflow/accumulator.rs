use std::collections::HashMap;

use crate::model::{NormalizedFeature, TileKey};

pub struct TileAccum {
    pub key: TileKey,
    pub layer_name: String,
    pub prop_names: Vec<String>,
    pub features: Vec<NormalizedFeature>,
    pub approx_bytes: usize,
    pub last_partition_seq: u64,
}

pub struct TileAccumulatorMap {
    tiles: HashMap<TileKey, TileAccum>,
}

impl TileAccumulatorMap {
    pub fn new() -> Self {
        Self {
            tiles: HashMap::new(),
        }
    }

    pub fn insert_feature(
        &mut self,
        key: TileKey,
        layer_name: &str,
        prop_names: &[String],
        feature: NormalizedFeature,
        partition_seq: u64,
    ) {
        let approx_bytes = estimate_feature_bytes(&feature);
        let entry = self.tiles.entry(key).or_insert_with(|| TileAccum {
            key,
            layer_name: layer_name.to_string(),
            prop_names: prop_names.to_vec(),
            features: Vec::new(),
            approx_bytes: 0,
            last_partition_seq: partition_seq,
        });
        entry.features.push(feature);
        entry.approx_bytes += approx_bytes;
        entry.last_partition_seq = partition_seq;
    }

    pub fn take_tile(&mut self, key: &TileKey) -> Option<TileAccum> {
        self.tiles.remove(key)
    }

    pub fn keys(&self) -> Vec<TileKey> {
        self.tiles.keys().copied().collect()
    }

    pub fn len(&self) -> usize {
        self.tiles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tiles.is_empty()
    }
}

fn estimate_feature_bytes(feature: &NormalizedFeature) -> usize {
    let mut bytes = std::mem::size_of::<NormalizedFeature>();
    for prop in &feature.properties {
        bytes += match prop {
            crate::tiler::PropertyValue::String(s) => s.len(),
            _ => std::mem::size_of_val(prop),
        };
    }
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BBox4326, NormalizedFeature};
    use crate::tiler::{Geometry, PropertyValue};
    use geo_types::Point;

    #[test]
    fn accumulator_groups_and_extracts_tiles() {
        let mut accum = TileAccumulatorMap::new();
        let key = TileKey { z: 2, x: 1, y: 1 };
        let feature = NormalizedFeature {
            id: Some(1),
            geometry: Geometry::Point(Point::new(0.0, 0.0)),
            properties: vec![PropertyValue::Int(1)],
            bbox: BBox4326 {
                min_lon: 0.0,
                min_lat: 0.0,
                max_lon: 0.0,
                max_lat: 0.0,
            },
        };

        accum.insert_feature(key, "test", &["value".to_string()], feature, 3);

        assert_eq!(accum.len(), 1);
        let tile = accum.take_tile(&key).expect("tile exists");
        assert_eq!(tile.features.len(), 1);
        assert_eq!(tile.layer_name, "test");
        assert_eq!(tile.prop_names, vec!["value".to_string()]);
        assert!(accum.is_empty());
    }
}
