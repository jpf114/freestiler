use pyo3::prelude::*;
use std::time::Instant;

use freestiler_core::engine::{self, ProgressReporter, TileConfig};
use freestiler_core::tiler::{Feature, LayerData, PropertyValue};

fn init_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();
}

fn make_reporter(quiet: bool) -> Box<dyn ProgressReporter> {
    if quiet {
        Box::new(engine::SilentReporter)
    } else {
        Box::new(PyReporter)
    }
}

struct PyReporter;

impl ProgressReporter for PyReporter {
    fn report(&self, msg: &str) {
        eprintln!("{}", msg);
    }
}

fn parse_layers_from_py(
    py: Python<'_>,
    layers: &[Py<PyAny>],
    generate_ids: bool,
) -> PyResult<Vec<LayerData>> {
    let mut result = Vec::new();
    let mut id_offset: u64 = 0;

    for (_layer_idx, layer_obj) in layers.iter().enumerate() {
        let layer = layer_obj.bind(py);

        let name: String = layer.get_item("name")?.extract()?;
        let wkb_list: Vec<Vec<u8>> = layer.get_item("wkb")?.extract()?;
        let _geom_types: Vec<String> = layer.get_item("geom_types")?.extract()?;
        let prop_names: Vec<String> = layer.get_item("prop_names")?.extract()?;
        let prop_types: Vec<String> = layer.get_item("prop_types")?.extract()?;
        let string_columns: Vec<Vec<Option<String>>> =
            layer.get_item("string_columns")?.extract()?;
        let int_columns: Vec<Vec<Option<i64>>> = layer.get_item("int_columns")?.extract()?;
        let float_columns: Vec<Vec<Option<f64>>> = layer.get_item("float_columns")?.extract()?;
        let bool_columns: Vec<Vec<Option<bool>>> = layer.get_item("bool_columns")?.extract()?;
        let layer_min_zoom: u8 = layer.get_item("min_zoom")?.extract()?;
        let layer_max_zoom: u8 = layer.get_item("max_zoom")?.extract()?;

        let n_features = wkb_list.len();

        let mut string_col_idx = 0usize;
        let mut int_col_idx = 0usize;
        let mut float_col_idx = 0usize;
        let mut bool_col_idx = 0usize;

        struct ColMapping {
            kind: &'static str,
            col_index: usize,
        }

        let mut mappings: Vec<ColMapping> = Vec::new();
        for ptype in &prop_types {
            match ptype.as_str() {
                "string" => {
                    mappings.push(ColMapping { kind: "string", col_index: string_col_idx });
                    string_col_idx += 1;
                }
                "integer" => {
                    mappings.push(ColMapping { kind: "integer", col_index: int_col_idx });
                    int_col_idx += 1;
                }
                "double" => {
                    mappings.push(ColMapping { kind: "double", col_index: float_col_idx });
                    float_col_idx += 1;
                }
                "boolean" => {
                    mappings.push(ColMapping { kind: "boolean", col_index: bool_col_idx });
                    bool_col_idx += 1;
                }
                _ => {
                    mappings.push(ColMapping { kind: "string", col_index: string_col_idx });
                    string_col_idx += 1;
                }
            }
        }

        let mut features = Vec::with_capacity(n_features);
        for i in 0..n_features {
            let geom = freestiler_core::wkb::wkb_to_geometry(&wkb_list[i]);
            if let Some(geometry) = geom {
                let mut properties = Vec::with_capacity(prop_names.len());
                for mapping in &mappings {
                    let prop = match mapping.kind {
                        "string" => {
                            if mapping.col_index < string_columns.len() {
                                let col = &string_columns[mapping.col_index];
                                if i < col.len() {
                                    match &col[i] {
                                        Some(s) => PropertyValue::String(s.clone()),
                                        None => PropertyValue::Null,
                                    }
                                } else { PropertyValue::Null }
                            } else { PropertyValue::Null }
                        }
                        "integer" => {
                            if mapping.col_index < int_columns.len() {
                                let col = &int_columns[mapping.col_index];
                                if i < col.len() {
                                    match col[i] {
                                        Some(v) => PropertyValue::Int(v),
                                        None => PropertyValue::Null,
                                    }
                                } else { PropertyValue::Null }
                            } else { PropertyValue::Null }
                        }
                        "double" => {
                            if mapping.col_index < float_columns.len() {
                                let col = &float_columns[mapping.col_index];
                                if i < col.len() {
                                    match col[i] {
                                        Some(v) if v.is_nan() => PropertyValue::Null,
                                        Some(v) => PropertyValue::Double(v),
                                        None => PropertyValue::Null,
                                    }
                                } else { PropertyValue::Null }
                            } else { PropertyValue::Null }
                        }
                        "boolean" => {
                            if mapping.col_index < bool_columns.len() {
                                let col = &bool_columns[mapping.col_index];
                                if i < col.len() {
                                    match col[i] {
                                        Some(v) => PropertyValue::Bool(v),
                                        None => PropertyValue::Null,
                                    }
                                } else { PropertyValue::Null }
                            } else { PropertyValue::Null }
                        }
                        _ => PropertyValue::Null,
                    };
                    properties.push(prop);
                }

                let id = if generate_ids {
                    Some((i as u64 + 1) + id_offset)
                } else {
                    None
                };

                features.push(Feature { id, geometry, properties });
            }
        }

        if generate_ids {
            id_offset += features.len() as u64;
        }

        result.push(LayerData {
            name,
            features,
            prop_names,
            prop_types,
            min_zoom: layer_min_zoom,
            max_zoom: layer_max_zoom,
        });
    }

    Ok(result)
}

#[pyfunction]
#[pyo3(signature = (layers, output_path, tile_format, min_zoom, max_zoom,
    base_zoom, do_simplify, generate_ids, quiet, drop_rate, cluster_distance,
    cluster_maxzoom, do_coalesce))]
fn _freestile(
    py: Python<'_>,
    layers: Vec<Py<PyAny>>,
    output_path: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    generate_ids: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
) -> PyResult<String> {
    let parse_start = Instant::now();
    let layer_data = parse_layers_from_py(py, &layers, generate_ids)?;

    let reporter = make_reporter(quiet);

    if !quiet {
        let total_features: usize = layer_data.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!(
            "  Parsed {} features across {} layer{} in {:.1}s",
            total_features, layer_data.len(),
            if layer_data.len() != 1 { "s" } else { "" },
            parse_start.elapsed().as_secs_f64()
        ));
    }

    if layer_data.iter().all(|l| l.features.is_empty()) {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("No valid features to tile"));
    }

    let config = TileConfig::from_binding_params(
        tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
        drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    );

    match engine::generate_pmtiles(&layer_data, output_path, &config, reporter.as_ref()) {
        Ok(()) => Ok(output_path.to_string()),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Error: {}", e))),
    }
}

#[cfg(feature = "geoparquet")]
#[pyfunction]
#[pyo3(signature = (input_path, output_path, layer_name, tile_format, min_zoom,
    max_zoom, base_zoom, do_simplify, quiet, drop_rate, cluster_distance,
    cluster_maxzoom, do_coalesce))]
fn _freestile_file(
    input_path: &str,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
) -> PyResult<String> {
    let reporter = make_reporter(quiet);

    let layers =
        freestiler_core::file_input::parquet_to_layers(input_path, layer_name, min_zoom, max_zoom)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if !quiet {
        let total: usize = layers.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!("  Read {} features from {}", total, input_path));
    }

    let config = TileConfig::from_binding_params(
        tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
        drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    );

    match engine::generate_pmtiles(&layers, output_path, &config, reporter.as_ref()) {
        Ok(()) => Ok(output_path.to_string()),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
    }
}

#[cfg(feature = "duckdb")]
#[pyfunction]
#[pyo3(signature = (sql, db_path, output_path, layer_name, tile_format, min_zoom,
    max_zoom, base_zoom, do_simplify, quiet, drop_rate, cluster_distance,
    cluster_maxzoom, do_coalesce, streaming_mode))]
fn _freestile_duckdb_query(
    sql: &str,
    db_path: Option<&str>,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
    streaming_mode: &str,
) -> PyResult<String> {
    let reporter = make_reporter(quiet);

    let config = TileConfig::from_binding_params(
        tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
        drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    );

    let maybe_stream = match streaming_mode {
        "always" => true,
        "auto" if cluster_distance <= 0.0 => {
            freestiler_core::streaming::query_feature_count(db_path, sql)
                .map(|count| count >= freestiler_core::streaming::auto_threshold())
                .unwrap_or(false)
        }
        _ => false,
    };

    if maybe_stream {
        match freestiler_core::streaming::generate_pmtiles_from_duckdb_query(
            db_path, sql, output_path, layer_name, &config, reporter.as_ref(),
        ) {
            Ok(_) => return Ok(output_path.to_string()),
            Err(e) => {
                let can_fallback = streaming_mode == "auto"
                    && (e.contains("POINT geometries only")
                        || e.contains("does not support clustering"));
                if !can_fallback {
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e));
                }
                if !quiet {
                    reporter.report("  Streaming unavailable for this query, falling back to in-memory tiling");
                }
            }
        }
    }

    let layers = freestiler_core::file_input::duckdb_query_to_layers(
        db_path, sql, layer_name, min_zoom, max_zoom,
    ).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if !quiet {
        let total: usize = layers.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!("  Query returned {} features", total));
    }

    match engine::generate_pmtiles(&layers, output_path, &config, reporter.as_ref()) {
        Ok(()) => Ok(output_path.to_string()),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
    }
}

#[cfg(feature = "duckdb")]
#[pyfunction]
#[pyo3(signature = (input_path, output_path, layer_name, tile_format, min_zoom,
    max_zoom, base_zoom, do_simplify, quiet, drop_rate, cluster_distance,
    cluster_maxzoom, do_coalesce))]
fn _freestile_duckdb(
    input_path: &str,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
) -> PyResult<String> {
    let reporter = make_reporter(quiet);

    let layers = freestiler_core::file_input::duckdb_file_to_layers(
        input_path, layer_name, min_zoom, max_zoom,
    ).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if !quiet {
        let total: usize = layers.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!("  Read {} features from {}", total, input_path));
    }

    let config = TileConfig::from_binding_params(
        tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
        drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    );

    match engine::generate_pmtiles(&layers, output_path, &config, reporter.as_ref()) {
        Ok(()) => Ok(output_path.to_string()),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
    }
}

#[cfg(feature = "postgis")]
#[pyfunction]
#[pyo3(signature = (conn_str, sql, output_path, layer_name, tile_format, min_zoom,
    max_zoom, base_zoom, do_simplify, quiet, drop_rate, cluster_distance,
    cluster_maxzoom, do_coalesce, batch_size, geom_column=None))]
fn _freestile_postgis(
    conn_str: &str,
    sql: &str,
    output_path: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
    batch_size: Option<usize>,
    geom_column: Option<&str>,
) -> PyResult<String> {
    let reporter = make_reporter(quiet);

    let output = freestiler_core::OutputTarget::Pmtiles { path: output_path.to_string() };

    if !quiet {
        reporter.report(&format!("  Connecting to PostGIS: {}",
            freestiler_core::tiler::mask_conn_str(conn_str)));
    }

    let layers = freestiler_core::postgis_input::postgis_query_to_layers_with_geom(
        conn_str, sql, layer_name, min_zoom, max_zoom, batch_size, geom_column,
    ).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if !quiet {
        let total: usize = layers.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!("  Query returned {} features", total));
    }

    let config = TileConfig::from_binding_params(
        tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
        drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    );

    match engine::generate_tiles_to_target(&layers, &output, &config, reporter.as_ref()) {
        Ok(count) => Ok(format!("{} tiles written to {}", count, output_path)),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
    }
}

#[cfg(all(feature = "postgis", feature = "mongodb-out"))]
#[pyfunction]
#[pyo3(signature = (conn_str, sql, mongo_uri, mongo_db, mongo_collection,
    layer_name, tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
    quiet, drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    batch_size, upsert, geom_column=None))]
fn _freestile_postgis_to_mongo(
    conn_str: &str,
    sql: &str,
    mongo_uri: &str,
    mongo_db: &str,
    mongo_collection: &str,
    layer_name: &str,
    tile_format: &str,
    min_zoom: u8,
    max_zoom: u8,
    base_zoom: i32,
    do_simplify: bool,
    quiet: bool,
    drop_rate: f64,
    cluster_distance: f64,
    cluster_maxzoom: i32,
    do_coalesce: bool,
    batch_size: Option<usize>,
    upsert: bool,
    geom_column: Option<&str>,
) -> PyResult<String> {
    let reporter = make_reporter(quiet);

    let mongo_config = freestiler_core::MongoConfig::new(mongo_uri, mongo_db, mongo_collection)
        .compress(true)
        .create_indexes(true)
        .upsert(upsert);

    let output = freestiler_core::OutputTarget::MongoDB { config: mongo_config };

    if !quiet {
        reporter.report(&format!("  Connecting to PostGIS: {}",
            freestiler_core::tiler::mask_conn_str(conn_str)));
    }

    let layers = freestiler_core::postgis_input::postgis_query_to_layers_with_geom(
        conn_str, sql, layer_name, min_zoom, max_zoom, batch_size, geom_column,
    ).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e))?;

    if !quiet {
        let total: usize = layers.iter().map(|l| l.features.len()).sum();
        reporter.report(&format!("  Query returned {} features", total));
    }

    let config = TileConfig::from_binding_params(
        tile_format, min_zoom, max_zoom, base_zoom, do_simplify,
        drop_rate, cluster_distance, cluster_maxzoom, do_coalesce,
    );

    match engine::generate_tiles_to_target(&layers, &output, &config, reporter.as_ref()) {
        Ok(count) => Ok(format!("{} tiles written to MongoDB", count)),
        Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
    }
}

#[pymodule]
fn _freestiler(m: &Bound<'_, PyModule>) -> PyResult<()> {
    init_logging();
    m.add_function(wrap_pyfunction!(_freestile, m)?)?;
    #[cfg(feature = "geoparquet")]
    m.add_function(wrap_pyfunction!(_freestile_file, m)?)?;
    #[cfg(feature = "duckdb")]
    m.add_function(wrap_pyfunction!(_freestile_duckdb, m)?)?;
    #[cfg(feature = "duckdb")]
    m.add_function(wrap_pyfunction!(_freestile_duckdb_query, m)?)?;
    #[cfg(feature = "postgis")]
    m.add_function(wrap_pyfunction!(_freestile_postgis, m)?)?;
    #[cfg(all(feature = "postgis", feature = "mongodb-out"))]
    m.add_function(wrap_pyfunction!(_freestile_postgis_to_mongo, m)?)?;
    Ok(())
}
