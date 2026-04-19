use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::error::{FreestilerError, Result};
use crate::pmtiles_writer::TileFormat;
use crate::tiler::{Feature, Geometry, LayerData, TileCoord};
use crate::{clip, cluster, coalesce, drop, mlt, mvt, pmtiles_writer, simplify, tiler};

#[cfg(feature = "mongodb-out")]
use crate::mongo_writer::MongoConfig;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum OutputTarget {
    Pmtiles { path: String },
    #[cfg(feature = "mongodb-out")]
    MongoDB { config: MongoConfig },
}

pub struct TileConfig {
    pub tile_format: TileFormat,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub base_zoom: Option<u8>,
    pub simplification: bool,
    pub drop_rate: Option<f64>,
    pub cluster_distance: Option<f64>,
    pub cluster_maxzoom: Option<u8>,
    pub coalesce: bool,
}

impl TileConfig {
    pub fn from_binding_params(
        tile_format: &str,
        min_zoom: u8,
        max_zoom: u8,
        base_zoom: i32,
        do_simplify: bool,
        drop_rate: f64,
        cluster_distance: f64,
        cluster_maxzoom: i32,
        do_coalesce: bool,
    ) -> Self {
        Self {
            tile_format: match tile_format {
                "mlt" => TileFormat::Mlt,
                _ => TileFormat::Mvt,
            },
            min_zoom,
            max_zoom,
            base_zoom: if base_zoom < 0 { None } else { Some(base_zoom as u8) },
            simplification: do_simplify,
            drop_rate: if drop_rate > 0.0 { Some(drop_rate) } else { None },
            cluster_distance: if cluster_distance > 0.0 { Some(cluster_distance) } else { None },
            cluster_maxzoom: if cluster_maxzoom >= 0 { Some(cluster_maxzoom as u8) } else { None },
            coalesce: do_coalesce,
        }
    }
}

pub trait ProgressReporter: Send + Sync {
    fn report(&self, msg: &str);
}

pub struct SilentReporter;
impl ProgressReporter for SilentReporter {
    fn report(&self, _msg: &str) {}
}

fn detect_point_layers(layers: &[LayerData]) -> Vec<bool> {
    layers.iter().map(|l| {
        !l.features.is_empty()
            && l.features
                .iter()
                .all(|f| matches!(&f.geometry, Geometry::Point(_) | Geometry::MultiPoint(_)))
    }).collect()
}

fn build_layer_metas(
    layers: &[LayerData],
    is_point_layer: &[bool],
    use_cluster: bool,
) -> Vec<pmtiles_writer::LayerMeta> {
    layers.iter().enumerate().map(|(li, l)| {
        let mut names = l.prop_names.clone();
        if use_cluster && is_point_layer[li] {
            names.push("point_count".to_string());
        }
        let geometry_type = l.features.first().map(|f| match &f.geometry {
            Geometry::Point(_) | Geometry::MultiPoint(_) => "Point".to_string(),
            Geometry::LineString(_) | Geometry::MultiLineString(_) => "Line".to_string(),
            Geometry::Polygon(_) | Geometry::MultiPolygon(_) => "Polygon".to_string(),
        });
        pmtiles_writer::LayerMeta {
            name: l.name.clone(),
            property_names: names,
            min_zoom: l.min_zoom,
            max_zoom: l.max_zoom,
            geometry_type,
        }
    }).collect()
}

pub fn compute_all_bounds(layers: &[LayerData]) -> (f64, f64, f64, f64) {
    use crate::geo::BoundingRect;
    let mut west = f64::MAX;
    let mut south = f64::MAX;
    let mut east = f64::MIN;
    let mut north = f64::MIN;

    for layer in layers {
        for feature in &layer.features {
            let bbox = match &feature.geometry {
                Geometry::Point(p) => Some(geo_types::Rect::new(p.0, p.0)),
                Geometry::MultiPoint(mp) => mp.bounding_rect(),
                Geometry::LineString(ls) => ls.bounding_rect(),
                Geometry::MultiLineString(mls) => mls.bounding_rect(),
                Geometry::Polygon(p) => p.bounding_rect(),
                Geometry::MultiPolygon(mp) => mp.bounding_rect(),
            };
            if let Some(bb) = bbox {
                west = west.min(bb.min().x);
                south = south.min(bb.min().y);
                east = east.max(bb.max().x);
                north = north.max(bb.max().y);
            }
        }
    }

    (west, south, east, north)
}

pub fn generate_tiles(
    layers: &[LayerData],
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<Vec<(TileCoord, Vec<u8>)>> {
    let min_z = config.min_zoom;
    let max_z = config.max_zoom;

    let use_drop = config.drop_rate.map_or(false, |r| r > 0.0);
    let drop_rate = config.drop_rate.unwrap_or(-1.0);
    let spatial_indices: Vec<Vec<(usize, u64)>> = if use_drop {
        layers
            .iter()
            .map(|l| drop::compute_spatial_indices(&l.features))
            .collect()
    } else {
        layers.iter().map(|_| Vec::new()).collect()
    };

    let use_cluster = config.cluster_distance.map_or(false, |d| d > 0.0);
    let cluster_distance = config.cluster_distance.unwrap_or(-1.0);
    let cluster_max_z = config
        .cluster_maxzoom
        .unwrap_or_else(|| max_z.saturating_sub(1));

    let is_point_layer = detect_point_layers(layers);

    let cluster_results: Vec<HashMap<u8, Vec<Feature>>> = if use_cluster {
        layers
            .iter()
            .enumerate()
            .map(|(li, layer)| {
                if is_point_layer[li] {
                    let cfg = cluster::ClusterConfig {
                        distance: cluster_distance,
                        max_zoom: cluster_max_z,
                    };
                    cluster::cluster_points(&layer.features, &cfg, min_z, layer.prop_names.len())
                } else {
                    HashMap::new()
                }
            })
            .collect()
    } else {
        layers.iter().map(|_| HashMap::new()).collect()
    };

    let cluster_prop_names: Vec<Vec<String>> = layers
        .iter()
        .enumerate()
        .map(|(li, layer)| {
            if use_cluster && is_point_layer[li] {
                let mut names = layer.prop_names.clone();
                names.push("point_count".to_string());
                names
            } else {
                layer.prop_names.clone()
            }
        })
        .collect();

    let do_simplify = config.simplification;
    let do_coalesce = config.coalesce;
    let format = config.tile_format;

    let mut all_tiles: Vec<(TileCoord, Vec<u8>)> = Vec::new();
    let total_start = Instant::now();

    for zoom in min_z..=max_z {
        let zoom_start = Instant::now();
        let pixel_deg = 360.0 / ((1u64 << zoom) as f64 * 4096.0);

        struct ActiveLayer<'a> {
            layer_idx: usize,
            features: &'a [Feature],
            prop_names: &'a [String],
            tile_map: HashMap<TileCoord, Vec<usize>>,
            simplified_geoms: Vec<Option<Geometry>>,
            drop_mask: Option<Vec<bool>>,
        }

        let mut active_layers: Vec<ActiveLayer> = Vec::new();

        for (li, layer) in layers.iter().enumerate() {
            if zoom < layer.min_zoom || zoom > layer.max_zoom {
                continue;
            }

            let using_clusters = use_cluster && is_point_layer[li] && zoom <= cluster_max_z;
            let features: &[Feature] = if using_clusters {
                cluster_results[li]
                    .get(&zoom)
                    .map(|v| v.as_slice())
                    .unwrap_or(&layer.features)
            } else {
                &layer.features
            };

            let prop_names: &[String] = if using_clusters {
                &cluster_prop_names[li]
            } else {
                &layer.prop_names
            };

            let vw_tol = simplify::vw_tolerance_for_zoom(zoom);
            let simplified_geoms: Vec<Option<Geometry>> = features
                .par_iter()
                .map(|f| match &f.geometry {
                    Geometry::LineString(_) | Geometry::MultiLineString(_) if do_simplify => {
                        Some(simplify::presimplify_line_vw(&f.geometry, vw_tol))
                    }
                    _ => None,
                })
                .collect();

            let base_zoom_val = config.base_zoom;
            let layer_base_z = base_zoom_val.unwrap_or(layer.max_zoom);
            let drop_mask = if use_drop && !using_clusters && zoom < layer_base_z {
                Some(drop::compute_drop_mask(
                    features,
                    &spatial_indices[li],
                    zoom,
                    layer_base_z,
                    drop_rate,
                    pixel_deg,
                ))
            } else {
                None
            };

            let tile_map =
                tiler::assign_features_to_tiles_with_geoms(features, &simplified_geoms, zoom);

            active_layers.push(ActiveLayer {
                layer_idx: li,
                features,
                prop_names,
                tile_map,
                simplified_geoms,
                drop_mask,
            });
        }

        let mut all_coords: HashSet<TileCoord> = HashSet::new();
        for al in &active_layers {
            for coord in al.tile_map.keys() {
                all_coords.insert(*coord);
            }
        }

        let n_tiles = all_coords.len();
        reporter.report(&format!(
            "  Zoom {:>2}/{}: {:>6} tiles ...",
            zoom, max_z, n_tiles
        ));

        let tile_coords: Vec<TileCoord> = all_coords.into_iter().collect();
        let zoom_tiles: Vec<(TileCoord, Vec<u8>)> = tile_coords
            .into_par_iter()
            .filter_map(|coord| {
                let mut tile_layer_data: Vec<(&str, &[String], Vec<Feature>)> = Vec::new();

                for al in &active_layers {
                    let layer = &layers[al.layer_idx];

                    if let Some(feature_indices) = al.tile_map.get(&coord) {
                        let mut tile_feats: Vec<Feature> = feature_indices
                            .par_iter()
                            .filter_map(|&idx| {
                                if let Some(ref mask) = al.drop_mask {
                                    if !mask[idx] {
                                        return None;
                                    }
                                }

                                let feature = &al.features[idx];
                                let geom_to_process = match &al.simplified_geoms[idx] {
                                    Some(g) => g,
                                    None => &feature.geometry,
                                };

                                let clipped = clip::clip_geometry_to_tile(geom_to_process, &coord)?;

                                let geometry = if do_simplify {
                                    simplify::simplify_geometry(&clipped, &coord)
                                } else {
                                    clipped
                                };

                                Some(Feature {
                                    id: feature.id,
                                    geometry,
                                    properties: feature.properties.clone(),
                                })
                            })
                            .collect();

                        if tile_feats.len() > 1 {
                            let tb = tiler::tile_bounds(&coord);
                            let tw = tb.min().x;
                            let te = tb.max().x;
                            let ts = tb.min().y;
                            let tn = tb.max().y;
                            tile_feats.sort_by(|a, b| {
                                let key_a = tiler::tile_morton_key(&a.geometry, tw, te, ts, tn);
                                let key_b = tiler::tile_morton_key(&b.geometry, tw, te, ts, tn);
                                key_a.cmp(&key_b).then(a.id.cmp(&b.id))
                            });
                        }

                        if do_coalesce && !tile_feats.is_empty() {
                            tile_feats = coalesce::coalesce_features(tile_feats, al.prop_names);
                        }

                        if !tile_feats.is_empty() {
                            tile_layer_data.push((&layer.name, al.prop_names, tile_feats));
                        }
                    }
                }

                if tile_layer_data.is_empty() {
                    return None;
                }

                let layer_refs: Vec<(&str, &[String], &[Feature])> = tile_layer_data
                    .iter()
                    .map(|(name, props, feats)| (*name, *props, feats.as_slice()))
                    .collect();

                let tile_bytes = match format {
                    TileFormat::Mvt => mvt::encode_tile_multilayer(&coord, &layer_refs),
                    TileFormat::Mlt => mlt::encode_tile_multilayer(&coord, &layer_refs),
                };

                if tile_bytes.is_empty() {
                    return None;
                }

                Some((coord, tile_bytes))
            })
            .collect();

        let n_encoded = zoom_tiles.len();
        all_tiles.extend(zoom_tiles);

        let elapsed = zoom_start.elapsed().as_secs_f64();
        reporter.report(&format!(
            "           {:>6} encoded ({:.1}s)",
            n_encoded, elapsed
        ));
    }

    reporter.report(&format!(
        "  Total: {} tiles in {:.1}s",
        all_tiles.len(),
        total_start.elapsed().as_secs_f64()
    ));

    Ok(all_tiles)
}

pub fn generate_pmtiles(
    layers: &[LayerData],
    output_path: &str,
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<()> {
    let output = OutputTarget::Pmtiles { path: output_path.to_string() };
    generate_tiles_to_target(layers, &output, config, reporter)?;
    Ok(())
}

pub fn generate_tiles_to_target(
    layers: &[LayerData],
    output: &OutputTarget,
    config: &TileConfig,
    reporter: &dyn ProgressReporter,
) -> Result<u64> {
    match output {
        OutputTarget::Pmtiles { path } => {
            let bounds = compute_all_bounds(layers);
            let use_cluster = config.cluster_distance.map_or(false, |d| d > 0.0);
            let is_point_layer = detect_point_layers(layers);
            let layer_metas = build_layer_metas(layers, &is_point_layer, use_cluster);

            let all_tiles = generate_tiles(layers, config, reporter)?;

            if all_tiles.is_empty() {
                return Err(FreestilerError::NoTilesGenerated);
            }

            let tile_count = all_tiles.len() as u64;

            reporter.report(&format!(
                "  Writing PMTiles archive ({} tiles) ...",
                all_tiles.len()
            ));
            let write_start = Instant::now();
            pmtiles_writer::write_pmtiles(
                path,
                all_tiles,
                config.tile_format,
                &layer_metas,
                config.min_zoom,
                config.max_zoom,
                bounds,
            ).map_err(FreestilerError::Other)?;
            reporter.report(&format!(
                "  PMTiles write: {:.1}s",
                write_start.elapsed().as_secs_f64()
            ));

            Ok(tile_count)
        }
        #[cfg(feature = "mongodb-out")]
        OutputTarget::MongoDB { config: mongo_cfg } => {
            use crate::mongo_writer::write_tiles_to_mongo_with_progress;

            let all_tiles = generate_tiles(layers, config, reporter)?;

            if all_tiles.is_empty() {
                return Err(FreestilerError::NoTilesGenerated);
            }

            let tile_count = all_tiles.len() as u64;
            let compress = mongo_cfg.effective_compress();
            let create_indexes = mongo_cfg.effective_create_indexes();

            reporter.report(&format!(
                "  Writing to MongoDB ({} tiles) ...",
                all_tiles.len()
            ));
            let write_start = Instant::now();

            let result = write_tiles_to_mongo_with_progress(
                mongo_cfg,
                &all_tiles,
                compress,
                create_indexes,
                reporter,
            ).map_err(FreestilerError::Other)?;

            reporter.report(&format!(
                "  MongoDB write: {:.1}s ({} tiles, {} bytes)",
                write_start.elapsed().as_secs_f64(),
                result.tiles_written,
                result.bytes_written
            ));

            Ok(tile_count)
        }
    }
}
