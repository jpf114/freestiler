//! PostGIS database input: read spatial data from PostgreSQL/PostGIS into LayerData.
//!
//! Supports:
//! - Direct table or arbitrary SQL query input
//! - Automatic SRID detection and transformation to WGS84 (EPSG:4326)
//! - Cursor-based batched/streaming reads for large tables
//! - WKB geometry parsing via shared geozero-based parser
//! - Transaction cleanup on error (ROLLBACK) for safe cursor operations
//! - Connection string masking in logs for security
//! - Read-only transaction mode to prevent accidental DDL/DML

#[cfg(feature = "postgis")]
mod postgis_impl {
    use crate::tiler::{Feature, LayerData, PropertyValue};
    use log::{debug, info, warn};
    use postgres::{Client, NoTls, Row};

    const WKB_ALIAS: &str = "__wkb";
    const CURSOR_NAME: &str = "__freestiler_cursor";
    #[derive(Clone, Copy, Debug)]
    enum PgValueKind {
        String,
        Int,
        Double,
        Bool,
    }

    struct PgColumn {
        name: String,
        type_name: String,
    }

    fn validate_identifier(name: &str, label: &str) -> Result<(), String> {
        if name.is_empty() {
            return Err(format!("{} must not be empty", label));
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!(
                "{} '{}' contains invalid characters; only alphanumeric and underscore are allowed",
                label, name
            ));
        }
        Ok(())
    }

    fn discover_columns_via_prepare(conn: &mut Client, sql: &str) -> Result<Vec<PgColumn>, String> {
        let stmt = conn.prepare(sql).map_err(|e| {
            let code = e.code().map(|c| c.code()).unwrap_or("unknown");
            let db_msg = e.as_db_error().map(|db| db.message()).unwrap_or("no db message");
            format!("Cannot prepare statement for column discovery: code={}, msg={}, full: {:?}", code, db_msg, e)
        })?;

        let columns: Vec<PgColumn> = stmt.columns().iter().map(|c| {
            PgColumn {
                name: c.name().to_string(),
                type_name: c.type_().name().to_string(),
            }
        }).collect();

        debug!("Discovered {} columns via prepared statement", columns.len());
        Ok(columns)
    }

    fn detect_geom_column_and_srid(
        conn: &mut Client,
        columns: &[PgColumn],
        sql: &str,
        geom_column_hint: Option<&str>,
    ) -> Result<(String, Option<i32>), String> {
        let geom_candidates: Vec<&PgColumn> = if let Some(hint) = geom_column_hint {
            columns.iter().filter(|c| c.name == hint).collect()
        } else {
            columns
                .iter()
                .filter(|c| {
                    let type_lower = c.type_name.to_lowercase();
                    type_lower.contains("geometry")
                        || type_lower.contains("geography")
                        || type_lower == "user-defined"
                })
                .collect()
        };

        if geom_candidates.is_empty() {
            if geom_column_hint.is_some() {
                return Err(format!(
                    "Specified geometry column '{}' not found in query result. Available columns: {:?}",
                    geom_column_hint.unwrap(),
                    columns.iter().map(|c| &c.name).collect::<Vec<_>>()
                ));
            }
            return Err("No geometry column found in query result. Ensure your query returns a PostGIS geometry column.".to_string());
        }

        if geom_candidates.len() > 1 && geom_column_hint.is_none() {
            let names: Vec<&str> = geom_candidates.iter().map(|c| c.name.as_str()).collect();
            warn!(
                "Multiple geometry columns detected {:?}; using '{}'. \
                 Specify 'geom_column' parameter to select a different one.",
                names, geom_candidates[0].name
            );
        }

        let geom_col_name = geom_candidates[0].name.clone();
        let srid = try_srid_from_geometry_columns(conn, &geom_col_name);

        let source_srid = match srid {
            Some(srid) => {
                debug!("Detected SRID {} from geometry_columns for column '{}'", srid, geom_col_name);
                Some(srid)
            }
            None => {
                debug!("geometry_columns lookup failed, falling back to ST_SRID query for column '{}'", geom_col_name);
                let srid_sql = format!(
                    "SELECT ST_SRID(\"{}\") AS __srid FROM ({}) AS __t WHERE \"{}\" IS NOT NULL LIMIT 1",
                    geom_col_name, sql, geom_col_name
                );
                conn.query_opt(&srid_sql, &[])
                    .ok()
                    .and_then(|r| r.and_then(|row| row.get(0)))
            }
        };

        Ok((geom_col_name, source_srid))
    }

    fn try_srid_from_geometry_columns(conn: &mut Client, geom_col_name: &str) -> Option<i32> {
        let rows = conn
            .query(
                "SELECT srid FROM geometry_columns WHERE f_geometry_column = $1 LIMIT 1",
                &[&geom_col_name],
            )
            .ok()?;

        if let Some(row) = rows.first() {
            let srid: i32 = row.get(0);
            if srid != 0 {
                return Some(srid);
            }
        }
        None
    }

    fn build_prop_columns(
        columns: &[PgColumn],
        geom_col_name: &str,
    ) -> Vec<(usize, PgValueKind)> {
        columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.name.to_lowercase() != geom_col_name.to_lowercase())
            .map(|(i, c)| (i, pg_type_to_value_kind(&c.type_name)))
            .collect()
    }

    fn pg_type_to_value_kind(type_name: &str) -> PgValueKind {
        let dt = type_name.trim().to_uppercase();
        match dt.as_str() {
            "BOOLEAN" | "BOOL" => return PgValueKind::Bool,
            "SMALLINT" | "INTEGER" | "INT" | "INT4" | "BIGINT" | "INT8" | "INT2"
            | "SERIAL" | "BIGSERIAL" | "SMALLSERIAL" => return PgValueKind::Int,
            "REAL" | "DOUBLE PRECISION" | "FLOAT8" | "FLOAT4" | "NUMERIC" | "DECIMAL" | "FLOAT"
            => return PgValueKind::Double,
            _ => {}
        }
        if dt.starts_with("VARCHAR") || dt.starts_with("CHAR(") || dt.starts_with("CHARACTER")
            || dt.starts_with("TEXT") || dt.starts_with("UUID") || dt.starts_with("JSON")
            || dt.starts_with("TIMESTAMP") || dt.starts_with("DATE") || dt.starts_with("TIME")
            || dt.starts_with("BYTEA") || dt.starts_with("BIT") || dt.starts_with("XML")
        {
            return PgValueKind::String;
        }
        if dt.starts_with("INT") || dt.starts_with("SMALLINT") || dt.starts_with("BIGINT")
            || dt.starts_with("SERIAL")
        {
            return PgValueKind::Int;
        }
        if dt.starts_with("FLOAT") || dt.starts_with("DOUBLE") || dt.starts_with("NUMERIC")
            || dt.starts_with("DECIMAL") || dt.starts_with("REAL")
        {
            return PgValueKind::Double;
        }
        PgValueKind::String
    }

    pub fn postgis_query_to_layers(
        conn_str: &str,
        sql: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
        batch_size: Option<usize>,
    ) -> Result<Vec<LayerData>, String> {
        postgis_query_to_layers_with_geom(conn_str, sql, layer_name, min_zoom, max_zoom, batch_size, None)
    }

    pub fn postgis_query_to_layers_with_geom(
        conn_str: &str,
        sql: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
        batch_size: Option<usize>,
        geom_column: Option<&str>,
    ) -> Result<Vec<LayerData>, String> {
        info!("Connecting to PostGIS: {}", crate::tiler::mask_conn_str(conn_str));

        let mut conn = Client::connect(conn_str, NoTls)
            .map_err(|e| format!("Cannot connect to PostgreSQL: {:?}", e))?;

        if let Some(ref gc) = geom_column {
            validate_identifier(gc, "geom_column")?;
        }

        debug!("Discovering columns for query: {}", sql);
        let columns = discover_columns_via_prepare(&mut conn, sql)?;
        debug!("Found {} columns: {:?}", columns.len(), columns.iter().map(|c| (&c.name, &c.type_name)).collect::<Vec<_>>());

        let (geom_col_name, source_srid) = detect_geom_column_and_srid(&mut conn, &columns, sql, geom_column)?;
        debug!("Detected geometry column '{}' with SRID {:?}", geom_col_name, source_srid);

        let _ = conn.execute("SET default_transaction_read_only = on", &[]);
        debug!("Set PostgreSQL session to read-only mode");

        let needs_transform = match source_srid {
            None | Some(0) | Some(4326) => {
                if source_srid.is_none() || source_srid == Some(0) {
                    warn!("SRID is unknown for geometry column '{}', assuming WGS84 (EPSG:4326)", geom_col_name);
                }
                false
            }
            Some(srid) => {
                info!("Source SRID is {}, will transform to EPSG:4326", srid);
                true
            }
        };

        let geom_expr = if needs_transform {
            format!("ST_AsEWKB(ST_Transform(\"{}\", 4326))", geom_col_name)
        } else {
            format!("ST_AsEWKB(\"{}\")", geom_col_name)
        };

        let prop_cols = build_prop_columns(&columns, &geom_col_name);
        let prop_names: Vec<String> = prop_cols.iter().map(|&(idx, _)| columns[idx].name.clone()).collect();
        let prop_types: Vec<String> = prop_cols.iter().map(|&(idx, _)| pg_type_to_property_type(&columns[idx].type_name)).collect();

        let geom_col_lower = geom_col_name.to_lowercase();
        let select_cols: Vec<String> = columns
            .iter()
            .enumerate()
            .filter(|(_i, c)| c.name.to_lowercase() != geom_col_lower)
            .map(|(_i, c)| format!("\"{}\"" , c.name))
            .chain(std::iter::once(format!("{} AS \"{}\"", geom_expr, WKB_ALIAS)))
            .collect();
        let full_sql = format!("SELECT {} FROM ({}) AS __t", select_cols.join(", "), sql);

        let effective_batch_size = effective_batch_size(batch_size);
        let features = if let Some(batch) = effective_batch_size {
            info!("Using cursor-based batched reading (batch_size={})", batch);
            cursor_batch_read(&mut conn, &full_sql, &prop_cols, batch)?
        } else {
            debug!("Using single-shot read (no cursor)");
            single_read(&mut conn, &full_sql, &prop_cols)?
        };

        if features.is_empty() {
            return Err("No valid features found in query result".to_string());
        }

        info!("PostGIS query returned {} features for layer '{}'", features.len(), layer_name);

        Ok(vec![LayerData {
            name: layer_name.to_string(),
            features,
            prop_names,
            prop_types,
            min_zoom,
            max_zoom,
        }])
    }

    fn single_read(
        conn: &mut Client,
        sql: &str,
        prop_cols: &[(usize, PgValueKind)],
    ) -> Result<Vec<Feature>, String> {
        let rows = conn.query(sql, &[]).map_err(|e| format!("Query error: {}", e))?;
        parse_rows(&rows, prop_cols)
    }

    fn cursor_batch_read(
        conn: &mut Client,
        sql: &str,
        prop_cols: &[(usize, PgValueKind)],
        batch_size: usize,
    ) -> Result<Vec<Feature>, String> {
        conn.execute("BEGIN", &[])
            .map_err(|e| format!("Cannot start transaction: {}", e))?;

        let result = (|| -> Result<Vec<Feature>, String> {
            conn.execute(&format!("DECLARE {} CURSOR FOR {}", CURSOR_NAME, sql), &[])
                .map_err(|e| format!("Cannot declare cursor: {}", e))?;

            let mut all_features = Vec::new();

            loop {
                let fetch_sql = format!("FETCH {} FROM {}", batch_size, CURSOR_NAME);
                let rows = conn.query(&fetch_sql, &[])
                    .map_err(|e| format!("Cursor fetch error: {}", e))?;

                if rows.is_empty() {
                    break;
                }

                let mut batch_features = parse_rows(&rows, prop_cols)?;
                debug!("Cursor batch: fetched {} features", batch_features.len());
                all_features.append(&mut batch_features);
            }

            let _ = conn.execute(&format!("CLOSE {}", CURSOR_NAME), &[]);
            Ok(all_features)
        })();

        match &result {
            Ok(_) => {
                conn.execute("COMMIT", &[])
                    .map_err(|e| format!("Cannot commit transaction: {}", e))?;
            }
            Err(_) => {
                let _ = conn.execute("ROLLBACK", &[]);
                debug!("Rolled back PostgreSQL transaction after error");
            }
        }

        result
    }

    fn parse_rows(
        rows: &[Row],
        prop_cols: &[(usize, PgValueKind)],
    ) -> Result<Vec<Feature>, String> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        debug!("parse_rows: {} rows, {} prop_cols", rows.len(), prop_cols.len());

        let wkb_col_idx = rows[0]
            .columns()
            .iter()
            .position(|c| c.name() == WKB_ALIAS)
            .ok_or_else(|| {
                let col_names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
                format!("WKB column '{}' not found in result. Available columns: {:?}", WKB_ALIAS, col_names)
            })?;

        let mut features = Vec::with_capacity(rows.len());

        for row in rows {
            let wkb_bytes: Option<Vec<u8>> = row.get(wkb_col_idx);
            let wkb_bytes = match wkb_bytes {
                Some(b) => b,
                None => {
                    debug!("WKB bytes is None, skipping row");
                    continue;
                }
            };

            let geometry = match crate::wkb::wkb_to_geometry(&wkb_bytes) {
                Some(g) => g,
                None => {
                    warn!("WKB parsing failed for bytes length {}, skipping row", wkb_bytes.len());
                    continue;
                }
            };

            let mut properties = Vec::with_capacity(prop_cols.len());
            for &(col_idx, kind) in prop_cols {
                properties.push(extract_pg_value(row, col_idx, kind));
            }

            features.push(Feature {
                id: Some((features.len() + 1) as u64),
                geometry,
                properties,
            });
        }

        Ok(features)
    }

    fn extract_pg_value(row: &Row, col_idx: usize, kind: PgValueKind) -> PropertyValue {
        match kind {
            PgValueKind::String => row
                .try_get::<_, Option<String>>(col_idx)
                .ok().flatten()
                .map(PropertyValue::String)
                .unwrap_or(PropertyValue::Null),
            PgValueKind::Int => row
                .try_get::<_, Option<i64>>(col_idx)
                .ok().flatten()
                .map(PropertyValue::Int)
                .unwrap_or(PropertyValue::Null),
            PgValueKind::Double => row
                .try_get::<_, Option<f64>>(col_idx)
                .ok().flatten()
                .map(|v| if v.is_nan() { PropertyValue::Null } else { PropertyValue::Double(v) })
                .unwrap_or(PropertyValue::Null),
            PgValueKind::Bool => row
                .try_get::<_, Option<bool>>(col_idx)
                .ok().flatten()
                .map(PropertyValue::Bool)
                .unwrap_or(PropertyValue::Null),
        }
    }

    fn pg_type_to_property_type(type_name: &str) -> String {
        match pg_type_to_value_kind(type_name) {
            PgValueKind::String => "character".to_string(),
            PgValueKind::Int => "integer".to_string(),
            PgValueKind::Double => "numeric".to_string(),
            PgValueKind::Bool => "logical".to_string(),
        }
    }

    fn effective_batch_size(batch_size: Option<usize>) -> Option<usize> {
        batch_size.filter(|size| *size > 0)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_mask_conn_str_with_password() {
            assert_eq!(
                crate::tiler::mask_conn_str("postgresql://user:secret@localhost:5432/mydb"),
                "postgresql://user:***@localhost:5432/mydb"
            );
        }

        #[test]
        fn test_mask_conn_str_without_password() {
            assert_eq!(
                crate::tiler::mask_conn_str("postgresql://localhost:5432/mydb"),
                "postgresql://localhost:5432/mydb"
            );
        }

        #[test]
        fn test_mask_conn_str_with_special_chars() {
            let masked = crate::tiler::mask_conn_str("postgresql://admin:p@ss:w0rd@db.example.com:5432/gis");
            assert!(masked.contains("***"), "Password should be masked, got: {}", masked);
            assert!(!masked.contains("p@ss"), "Original password should not appear, got: {}", masked);
        }

        #[test]
        fn test_pg_type_to_value_kind_exact() {
            assert!(matches!(pg_type_to_value_kind("INTEGER"), PgValueKind::Int));
            assert!(matches!(pg_type_to_value_kind("DOUBLE PRECISION"), PgValueKind::Double));
            assert!(matches!(pg_type_to_value_kind("BOOLEAN"), PgValueKind::Bool));
            assert!(matches!(pg_type_to_value_kind("TEXT"), PgValueKind::String));
            assert!(matches!(pg_type_to_value_kind("BIGINT"), PgValueKind::Int));
            assert!(matches!(pg_type_to_value_kind("SERIAL"), PgValueKind::Int));
            assert!(matches!(pg_type_to_value_kind("BIGSERIAL"), PgValueKind::Int));
        }

        #[test]
        fn test_pg_type_to_value_kind_parametric() {
            assert!(matches!(pg_type_to_value_kind("VARCHAR(255)"), PgValueKind::String));
            assert!(matches!(pg_type_to_value_kind("CHARACTER VARYING(100)"), PgValueKind::String));
            assert!(matches!(pg_type_to_value_kind("TIMESTAMP WITHOUT TIME ZONE"), PgValueKind::String));
            assert!(matches!(pg_type_to_value_kind("UUID"), PgValueKind::String));
            assert!(matches!(pg_type_to_value_kind("JSONB"), PgValueKind::String));
            assert!(matches!(pg_type_to_value_kind("NUMERIC(10,2)"), PgValueKind::Double));
        }

        #[test]
        fn test_build_prop_columns_excludes_geom() {
            let columns = vec![
                PgColumn { name: "id".to_string(), type_name: "INTEGER".to_string() },
                PgColumn { name: "geom".to_string(), type_name: "geometry(Point,4326)".to_string() },
                PgColumn { name: "name".to_string(), type_name: "TEXT".to_string() },
            ];
            let props = build_prop_columns(&columns, "geom");
            assert_eq!(props.len(), 2);
            assert_eq!(props[0].0, 0);
            assert_eq!(props[1].0, 2);
        }

        #[test]
        fn test_validate_identifier_valid() {
            assert!(validate_identifier("my_column", "test").is_ok());
            assert!(validate_identifier("col_123", "test").is_ok());
        }

        #[test]
        fn test_validate_identifier_invalid() {
            assert!(validate_identifier("", "test").is_err());
            assert!(validate_identifier("col; DROP TABLE", "test").is_err());
            assert!(validate_identifier("col-name", "test").is_err());
            assert!(validate_identifier("col name", "test").is_err());
        }

        #[test]
        fn test_effective_batch_size_preserves_none() {
            assert_eq!(effective_batch_size(None), None);
            assert_eq!(effective_batch_size(Some(10000)), Some(10000));
            assert_eq!(effective_batch_size(Some(0)), None);
        }
    }
}

#[cfg(feature = "postgis")]
pub use postgis_impl::{postgis_query_to_layers, postgis_query_to_layers_with_geom};
