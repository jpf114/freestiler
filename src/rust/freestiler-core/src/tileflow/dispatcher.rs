use crate::model::{NormalizedFeature, TileKey};
use crate::tileflow::accumulator::TileAccumulatorMap;
use crate::tiler::{lat_to_tile_y, lon_to_tile_x, ASSIGN_BUFFER_FRACTION};

#[derive(Clone, Debug)]
pub struct DispatchConfig {
    pub min_zoom: u8,
    pub max_zoom: u8,
}

pub fn dispatch_feature(
    feature: &NormalizedFeature,
    layer_name: &str,
    prop_names: &[String],
    config: &DispatchConfig,
    accum: &mut TileAccumulatorMap,
    partition_seq: u64,
) -> Result<(), String> {
    for z in config.min_zoom..=config.max_zoom {
        let tile_width_deg = 360.0 / (1u64 << z) as f64;
        let buf_x = tile_width_deg * ASSIGN_BUFFER_FRACTION;
        let buf_y = tile_width_deg * ASSIGN_BUFFER_FRACTION;
        let min_x = lon_to_tile_x((feature.bbox.min_lon - buf_x).max(-180.0), z);
        let max_x = lon_to_tile_x((feature.bbox.max_lon + buf_x).min(180.0), z);
        let min_y = lat_to_tile_y((feature.bbox.max_lat + buf_y).min(85.051), z);
        let max_y = lat_to_tile_y((feature.bbox.min_lat - buf_y).max(-85.051), z);
        let max_tiles = (1u32 << z) - 1;

        for x in min_x..=max_x.min(max_tiles) {
            for y in min_y..=max_y.min(max_tiles) {
                accum.insert_feature(
                    TileKey { z, x, y },
                    layer_name,
                    prop_names,
                    feature.clone(),
                    partition_seq,
                );
            }
        }
    }
    Ok(())
}
