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
