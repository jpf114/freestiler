use crate::model::{PartitionSpec, TileKey};
use crate::tileflow::accumulator::TileAccumulatorMap;

pub fn collect_closable_tiles(
    accum: &TileAccumulatorMap,
    current_partition: &PartitionSpec,
) -> Vec<TileKey> {
    accum
        .keys()
        .into_iter()
        .filter(|key| projected_row_end(*key, current_partition.zoom) < current_partition.row_start)
        .collect()
}

pub fn collect_all_remaining_tiles(accum: &TileAccumulatorMap) -> Vec<TileKey> {
    accum.keys()
}

fn projected_row_end(key: TileKey, partition_zoom: u8) -> u32 {
    if key.z >= partition_zoom {
        let shift = key.z - partition_zoom;
        key.y >> shift
    } else {
        let shift = partition_zoom - key.z;
        ((key.y + 1) << shift) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{BBox4326, NormalizedFeature};
    use crate::tileflow::accumulator::TileAccumulatorMap;
    use crate::tiler::{Geometry, PropertyValue};
    use geo_types::Point;

    fn sample_feature() -> NormalizedFeature {
        NormalizedFeature {
            id: Some(1),
            geometry: Geometry::Point(Point::new(0.0, 0.0)),
            properties: vec![PropertyValue::Int(1)],
            bbox: BBox4326 {
                min_lon: 0.0,
                min_lat: 0.0,
                max_lon: 0.0,
                max_lat: 0.0,
            },
        }
    }

    #[test]
    fn closer_marks_tiles_above_partition_as_closable() {
        let mut accum = TileAccumulatorMap::new();
        accum.insert_feature(
            TileKey { z: 3, x: 0, y: 0 },
            "test",
            &["v".to_string()],
            sample_feature(),
            0,
        );
        accum.insert_feature(
            TileKey { z: 3, x: 0, y: 4 },
            "test",
            &["v".to_string()],
            sample_feature(),
            1,
        );

        let closable = collect_closable_tiles(
            &accum,
            &PartitionSpec {
                sequence: 2,
                zoom: 3,
                row_start: 3,
                row_end: 6,
                min_lon: -180.0,
                min_lat: -10.0,
                max_lon: 180.0,
                max_lat: 10.0,
            },
        );

        assert!(closable.contains(&TileKey { z: 3, x: 0, y: 0 }));
        assert!(!closable.contains(&TileKey { z: 3, x: 0, y: 4 }));
    }
}
