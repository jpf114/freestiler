use crate::engine::TileConfig;
use crate::model::PartitionSpec;
use crate::tiler::{tile_y_to_lat};

#[derive(Clone, Debug)]
pub struct PartitionConfig {
    pub partition_zoom: u8,
    pub metatile_rows: u32,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            partition_zoom: 0,
            metatile_rows: 64,
        }
    }
}

pub fn plan_partitions(tile_config: &TileConfig, partition_config: &PartitionConfig) -> Vec<PartitionSpec> {
    let zoom = if partition_config.partition_zoom == 0 {
        tile_config.max_zoom
    } else {
        partition_config.partition_zoom
    };
    let total_rows = 1u32 << zoom;
    let rows_per_partition = partition_config.metatile_rows.max(1);
    let mut out = Vec::new();
    let mut row_start = 0u32;
    let mut sequence = 0u64;

    while row_start < total_rows {
        let row_end = (row_start + rows_per_partition).min(total_rows);
        let max_lat = tile_y_to_lat(row_start, zoom);
        let min_lat = tile_y_to_lat(row_end, zoom);
        out.push(PartitionSpec {
            sequence,
            zoom,
            row_start,
            row_end,
            min_lon: -180.0,
            min_lat,
            max_lon: 180.0,
            max_lat,
        });
        row_start = row_end;
        sequence += 1;
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pmtiles_writer::TileFormat;

    #[test]
    fn partition_plan_covers_rows_without_overlap() {
        let cfg = TileConfig {
            tile_format: TileFormat::Mlt,
            min_zoom: 0,
            max_zoom: 3,
            base_zoom: None,
            simplification: true,
            drop_rate: None,
            cluster_distance: None,
            cluster_maxzoom: None,
            coalesce: false,
        };

        let partitions = plan_partitions(
            &cfg,
            &PartitionConfig {
                partition_zoom: 3,
                metatile_rows: 3,
            },
        );

        assert_eq!(partitions.first().unwrap().row_start, 0);
        assert_eq!(partitions.last().unwrap().row_end, 8);
        for pair in partitions.windows(2) {
            assert_eq!(pair[0].row_end, pair[1].row_start);
        }
    }
}
