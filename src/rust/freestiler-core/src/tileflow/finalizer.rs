use std::io::Write;

use crate::model::{ContentEncoding, EncodedTile};
use crate::pmtiles_writer::TileFormat;
use crate::tileflow::accumulator::TileAccum;
use crate::tiler::{tile_bounds, tile_morton_key, Feature, TileCoord};

#[derive(Clone, Debug)]
pub struct FinalizeConfig {
    pub tile_format: TileFormat,
    pub simplification: bool,
    pub coalesce: bool,
    pub compress: flate2::Compression,
}

pub fn finalize_tile(
    accum: TileAccum,
    config: &FinalizeConfig,
) -> Result<Option<EncodedTile>, String> {
    if accum.features.is_empty() {
        return Ok(None);
    }

    let coord = TileCoord::from(accum.key);
    let mut tile_features: Vec<Feature> = Vec::new();

    for feature in &accum.features {
        let Some(clipped) = crate::clip::clip_geometry_to_tile(&feature.geometry, &coord) else {
            continue;
        };
        let geometry = if config.simplification {
            crate::simplify::simplify_geometry(&clipped, &coord)
        } else {
            clipped
        };
        tile_features.push(Feature {
            id: feature.id,
            geometry,
            properties: feature.properties.clone(),
        });
    }

    if tile_features.is_empty() {
        return Ok(None);
    }

    if tile_features.len() > 1 {
        let bounds = tile_bounds(&coord);
        let west = bounds.min().x;
        let east = bounds.max().x;
        let south = bounds.min().y;
        let north = bounds.max().y;
        tile_features.sort_by(|a, b| {
            let ka = tile_morton_key(&a.geometry, west, east, south, north);
            let kb = tile_morton_key(&b.geometry, west, east, south, north);
            ka.cmp(&kb).then(a.id.cmp(&b.id))
        });
    }

    if config.coalesce {
        tile_features = crate::coalesce::coalesce_features(tile_features, &accum.prop_names);
    }

    let tile_bytes = match config.tile_format {
        TileFormat::Mlt => crate::mlt::encode_tile(&coord, &tile_features, &accum.layer_name, &accum.prop_names),
        TileFormat::Mvt => crate::mvt::encode_tile(&coord, &tile_features, &accum.layer_name, &accum.prop_names),
    };

    if tile_bytes.is_empty() {
        return Ok(None);
    }

    let (data, content_encoding) = compress_if_needed(&tile_bytes, config.compress)?;

    Ok(Some(EncodedTile {
        key: accum.key,
        data,
        tile_format: config.tile_format,
        content_encoding,
    }))
}

fn compress_if_needed(
    input: &[u8],
    compress: flate2::Compression,
) -> Result<(Vec<u8>, ContentEncoding), String> {
    if compress.level() == 0 {
        return Ok((input.to_vec(), ContentEncoding::Identity));
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), compress);
    encoder
        .write_all(input)
        .map_err(|e| format!("Cannot gzip tile bytes: {}", e))?;
    let bytes = encoder
        .finish()
        .map_err(|e| format!("Cannot finish gzip tile bytes: {}", e))?;
    Ok((bytes, ContentEncoding::Gzip))
}
