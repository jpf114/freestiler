//! PostGIS database input: read spatial data from PostgreSQL/PostGIS into LayerData.
//!
//! Supports:
//! - Direct table or arbitrary SQL query input
//! - Automatic SRID detection and transformation to WGS84 (EPSG:4326)
//! - Cursor-based batched/streaming reads for large tables
//! - WKB geometry parsing via shared geozero-based parser (parallel parsing enabled)
//! - Transaction cleanup on error (ROLLBACK) for safe cursor operations
//! - Connection string masking in logs for security
//! - Read-only transaction mode to prevent accidental DDL/DML
//! - SSL/TLS connection support
//! - Connection pooling support

#[cfg(feature = "postgis")]
mod postgis_impl {
    use crate::tiler::{Feature, LayerData, PropertyValue};
    use log::{debug, info, warn};
    use postgres::{Client, NoTls, Row};
    use rayon::prelude::*;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::str::FromStr;

    const WKB_ALIAS: &str = "__wkb";
    const CURSOR_NAME: &str = "__freestiler_cursor";
    const DEFAULT_BATCH_SIZE: usize = 10000;

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

    #[derive(Clone, Debug)]
    pub struct PostgisLayerSchema {
        pub geom_column: String,
        pub prop_names: Vec<String>,
        pub prop_types: Vec<String>,
        pub source_srid: Option<i32>,
    }

    pub struct PostgisBatchScanner {
        conn: Client,
        full_sql: String,
        prop_cols: Vec<(usize, PgValueKind)>,
        schema: PostgisLayerSchema,
        batch_size: usize,
        temp_snapshot_table: Option<String>,
    }

    #[derive(Clone, Debug, Default)]
    pub struct PostgisConfig {
        pub conn_str: String,
        pub use_ssl: bool,
        pub ssl_ca_file: Option<String>,
        pub ssl_cert_file: Option<String>,
        pub ssl_key_file: Option<String>,
        pub ssl_allow_invalid: bool,
        pub connect_timeout_ms: Option<u64>,
        pub batch_size: Option<usize>,
    }

    impl PostgisConfig {
        pub fn new(conn_str: impl Into<String>) -> Self {
            Self {
                conn_str: conn_str.into(),
                ..Default::default()
            }
        }

        pub fn use_ssl(mut self, v: bool) -> Self { self.use_ssl = v; self }
        pub fn ssl_ca_file(mut self, v: impl Into<String>) -> Self { self.ssl_ca_file = Some(v.into()); self }
        pub fn ssl_cert_file(mut self, v: impl Into<String>) -> Self { self.ssl_cert_file = Some(v.into()); self }
        pub fn ssl_key_file(mut self, v: impl Into<String>) -> Self { self.ssl_key_file = Some(v.into()); self }
        pub fn ssl_allow_invalid(mut self, v: bool) -> Self { self.ssl_allow_invalid = v; self }
        pub fn connect_timeout_ms(mut self, v: u64) -> Self { self.connect_timeout_ms = Some(v); self }
        pub fn batch_size(mut self, v: usize) -> Self { self.batch_size = Some(v); self }

        pub fn effective_batch_size(&self) -> usize { self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE) }
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

    fn connect_with_config(config: &PostgisConfig) -> Result<Client, String> {
        if config.use_ssl {
            connect_with_ssl(config)
        } else {
            let mut builder = postgres::Config::from_str(&config.conn_str)
                .map_err(|e| format!("Invalid connection string: {}", e))?;
            
            if let Some(timeout_ms) = config.connect_timeout_ms {
                builder.connect_timeout(std::time::Duration::from_millis(timeout_ms));
            }
            
            builder.connect(NoTls)
                .map_err(|e| format!("Cannot connect to PostgreSQL: {:?}", e))
        }
    }

    fn connect_with_ssl(config: &PostgisConfig) -> Result<Client, String> {
        use postgres_native_tls::MakeTlsConnector;
        use native_tls::{TlsConnector, Certificate};
        use std::fs::File;
        use std::io::Read;

        let mut builder = TlsConnector::builder();
        
        if let Some(ref ca_file) = config.ssl_ca_file {
            let mut ca_data = Vec::new();
            File::open(ca_file)
                .map_err(|e| format!("Cannot open CA file '{}': {}", ca_file, e))?
                .read_to_end(&mut ca_data)
                .map_err(|e| format!("Cannot read CA file: {}", e))?;
            let cert = Certificate::from_pem(&ca_data)
                .map_err(|e| format!("Invalid CA certificate: {}", e))?;
            builder.add_root_certificate(cert);
        }

        if config.ssl_allow_invalid {
            builder.danger_accept_invalid_certs(true);
            builder.danger_accept_invalid_hostnames(true);
        }

        let connector = builder.build()
            .map_err(|e| format!("Cannot create TLS connector: {}", e))?;
        let tls = MakeTlsConnector::new(connector);

        let mut pg_config = postgres::Config::from_str(&config.conn_str)
            .map_err(|e| format!("Invalid connection string: {}", e))?;
        
        if let Some(timeout_ms) = config.connect_timeout_ms {
            pg_config.connect_timeout(std::time::Duration::from_millis(timeout_ms));
        }

        pg_config.connect(tls)
            .map_err(|e| format!("Cannot connect to PostgreSQL with SSL: {:?}", e))
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
        // Prefer probing SRID from the actual query result first; this avoids
        // false hits when geometry_columns has duplicate column names.
        let srid_sql = format!(
            "SELECT ST_SRID(\"{}\") AS __srid FROM ({}) AS __t WHERE \"{}\" IS NOT NULL LIMIT 1",
            geom_col_name, sql, geom_col_name
        );
        let source_srid = conn
            .query_opt(&srid_sql, &[])
            .ok()
            .and_then(|r| r.and_then(|row| row.get(0)))
            .filter(|srid| *srid > 0)
            .or_else(|| {
                debug!(
                    "ST_SRID probe returned no valid SRID, trying geometry_columns for '{}'",
                    geom_col_name
                );
                try_srid_from_geometry_columns(conn, &geom_col_name)
            });

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

    /// Fail fast on empty / null-only geometry, SRID=0 rows, and invalid geometry in a bounded sample.
    fn precheck_postgis_layer(
        conn: &mut Client,
        sql: &str,
        geom_col: &str,
        is_geography: bool,
    ) -> Result<(), String> {
        validate_identifier(geom_col, "geom_column")?;
        let q = format!("\"{}\"", geom_col);

        let stats_sql = format!(
            "SELECT COALESCE(COUNT(*) FILTER (WHERE {q} IS NOT NULL), 0)::bigint, COUNT(*)::bigint FROM ({}) AS __precheck_base",
            sql
        );
        let row = conn
            .query_one(&stats_sql, &[])
            .map_err(|e| format!("PostGIS input precheck (row counts) failed: {}", e))?;
        let nn: i64 = row.get(0);
        let tot: i64 = row.get(1);
        if tot == 0 {
            return Err("PostGIS query returned no rows.".to_string());
        }
        if nn == 0 {
            return Err(format!(
                "PostGIS query has no non-null geometries in column '{}'.",
                geom_col
            ));
        }

        let bad_srid_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM ({}) AS __t WHERE {q} IS NOT NULL AND ST_SRID({q}) = 0 LIMIT 1)",
            sql
        );
        let row = conn
            .query_one(&bad_srid_sql, &[])
            .map_err(|e| format!("PostGIS input precheck (SRID) failed: {}", e))?;
        if row.get::<_, bool>(0) {
            return Err(format!(
                "Geometry column '{}' contains rows with SRID=0 (unknown). Use ST_SetSRID / fix metadata, or exclude those rows.",
                geom_col
            ));
        }

        if !is_geography {
            let invalid_sql = format!(
                "SELECT EXISTS (
                    SELECT 1 FROM (
                        SELECT {q} AS __g FROM ({}) AS __b WHERE {q} IS NOT NULL LIMIT 50000
                    ) AS __s
                    WHERE NOT ST_IsValid(__g)
                    LIMIT 1
                )",
                sql
            );
            let row = conn
                .query_one(&invalid_sql, &[])
                .map_err(|e| format!("PostGIS input precheck (validity sample) failed: {}", e))?;
            if row.get::<_, bool>(0) {
                return Err(format!(
                    "Sample (up to 50000 rows) contains invalid geometries in '{}'. Consider ST_MakeValid or filtering.",
                    geom_col
                ));
            }
        }

        Ok(())
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

    pub fn postgis_query_count_with_config(
        config: &PostgisConfig,
        sql: &str,
    ) -> Result<u64, String> {
        let mut conn = connect_with_config(config)?;
        let count_sql = format!("SELECT COUNT(*) FROM ({}) AS __freestiler_count", sql);
        let row = conn
            .query_one(&count_sql, &[])
            .map_err(|e| format!("Cannot count PostGIS query rows: {}", e))?;
        let count: i64 = row.get(0);
        Ok(count.max(0) as u64)
    }

    pub fn postgis_query_exceeds_with_config(
        config: &PostgisConfig,
        sql: &str,
        threshold: u64,
    ) -> Result<bool, String> {
        let mut conn = connect_with_config(config)?;
        let probe_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM ({}) AS __freestiler_probe OFFSET {} LIMIT 1)",
            sql, threshold
        );
        let row = conn
            .query_one(&probe_sql, &[])
            .map_err(|e| format!("Cannot probe PostGIS query size: {}", e))?;
        let exceeds: bool = row.get(0);
        Ok(exceeds)
    }

    pub fn postgis_query_to_layers_with_config(
        config: &PostgisConfig,
        sql: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
        geom_column: Option<&str>,
    ) -> Result<Vec<LayerData>, String> {
        info!("Connecting to PostGIS: {}", crate::tiler::mask_conn_str(&config.conn_str));

        let mut conn = connect_with_config(config)?;

        if let Some(ref gc) = geom_column {
            validate_identifier(gc, "geom_column")?;
        }

        let prepared = prepare_query_plan(&mut conn, sql, geom_column)?;

        let batch_size = config.batch_size.or(Some(config.effective_batch_size()));
        let features = if let Some(batch) = batch_size {
            info!("Using cursor-based batched reading (batch_size={})", batch);
            cursor_batch_read(&mut conn, &prepared.full_sql, &prepared.prop_cols, batch)?
        } else {
            debug!("Using single-shot read (no cursor)");
            single_read(&mut conn, &prepared.full_sql, &prepared.prop_cols)?
        };

        if features.is_empty() {
            return Err("No valid features found in query result".to_string());
        }

        info!("PostGIS query returned {} features for layer '{}'", features.len(), layer_name);

        Ok(vec![LayerData {
            name: layer_name.to_string(),
            features,
            prop_names: prepared.schema.prop_names,
            prop_types: prepared.schema.prop_types,
            min_zoom,
            max_zoom,
        }])
    }

    struct PreparedPostgisQuery {
        full_sql: String,
        prop_cols: Vec<(usize, PgValueKind)>,
        schema: PostgisLayerSchema,
    }

    fn prepare_query_plan(
        conn: &mut Client,
        sql: &str,
        geom_column: Option<&str>,
    ) -> Result<PreparedPostgisQuery, String> {
        debug!("Discovering columns for query: {}", sql);
        let columns = discover_columns_via_prepare(conn, sql)?;
        debug!(
            "Found {} columns: {:?}",
            columns.len(),
            columns.iter().map(|c| (&c.name, &c.type_name)).collect::<Vec<_>>()
        );

        let (geom_col_name, source_srid) = detect_geom_column_and_srid(conn, &columns, sql, geom_column)?;
        let is_geography = columns.iter().any(|c| {
            c.name == geom_col_name && c.type_name.to_lowercase().contains("geography")
        });
        precheck_postgis_layer(conn, sql, &geom_col_name, is_geography)?;
        debug!("Detected geometry column '{}' with SRID {:?}", geom_col_name, source_srid);

        let _ = conn.execute("SET default_transaction_read_only = on", &[]);
        debug!("Set PostgreSQL session to read-only mode");

        let needs_transform = match source_srid {
            None | Some(0) => {
                return Err(format!(
                    "SRID is unknown (SRID=0) for geometry column '{}'. \
                     Please fix your source data (e.g. ST_SetSRID) or return a geometry with a valid SRID.",
                    geom_col_name
                ));
            }
            Some(4326) => false,
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
        let prop_types: Vec<String> = prop_cols
            .iter()
            .map(|&(idx, _)| pg_type_to_property_type(&columns[idx].type_name))
            .collect();

        let geom_col_lower = geom_col_name.to_lowercase();
        let select_cols: Vec<String> = columns
            .iter()
            .enumerate()
            .filter(|(_i, c)| c.name.to_lowercase() != geom_col_lower)
            .map(|(_i, c)| format!("\"{}\"", c.name))
            .chain(std::iter::once(format!("{} AS \"{}\"", geom_expr, WKB_ALIAS)))
            .collect();
        let full_sql = format!("SELECT {} FROM ({}) AS __t", select_cols.join(", "), sql);

        Ok(PreparedPostgisQuery {
            full_sql,
            prop_cols,
            schema: PostgisLayerSchema {
                geom_column: geom_col_name,
                prop_names,
                prop_types,
                source_srid,
            },
        })
    }

    pub fn postgis_query_each_batch_with_config<F>(
        config: &PostgisConfig,
        sql: &str,
        geom_column: Option<&str>,
        mut on_batch: F,
    ) -> Result<PostgisLayerSchema, String>
    where
        F: FnMut(Vec<Feature>) -> Result<(), String>,
    {
        info!("Connecting to PostGIS: {}", crate::tiler::mask_conn_str(&config.conn_str));
        let mut conn = connect_with_config(config)?;

        if let Some(ref gc) = geom_column {
            validate_identifier(gc, "geom_column")?;
        }

        let prepared = prepare_query_plan(&mut conn, sql, geom_column)?;
        let batch_size = config.effective_batch_size();

        conn.execute("BEGIN", &[])
            .map_err(|e| format!("Cannot start transaction: {}", e))?;

        let result = (|| -> Result<(), String> {
            conn.execute(&format!("DECLARE {} CURSOR FOR {}", CURSOR_NAME, prepared.full_sql), &[])
                .map_err(|e| format!("Cannot declare cursor: {}", e))?;

            let mut next_id: u64 = 1;
            loop {
                let fetch_sql = format!("FETCH {} FROM {}", batch_size, CURSOR_NAME);
                let rows = conn
                    .query(&fetch_sql, &[])
                    .map_err(|e| format!("Cursor fetch error: {}", e))?;

                if rows.is_empty() {
                    break;
                }

                let batch_features = parse_rows_parallel(&rows, &prepared.prop_cols, next_id)?;
                next_id += batch_features.len() as u64;
                on_batch(batch_features)?;
            }

            let _ = conn.execute(&format!("CLOSE {}", CURSOR_NAME), &[]);
            Ok(())
        })();

        match &result {
            Ok(_) => {
                conn.execute("COMMIT", &[])
                    .map_err(|e| format!("Cannot commit transaction: {}", e))?;
            }
            Err(_) => {
                let _ = conn.execute("ROLLBACK", &[]);
            }
        }

        result?;
        Ok(prepared.schema)
    }

    pub fn postgis_probe_and_maybe_load_layers_with_config(
        config: &PostgisConfig,
        sql: &str,
        layer_name: &str,
        min_zoom: u8,
        max_zoom: u8,
        geom_column: Option<&str>,
        threshold: u64,
    ) -> Result<(bool, Option<Vec<LayerData>>), String> {
        let mut conn = connect_with_config(config)?;
        if let Some(ref gc) = geom_column {
            validate_identifier(gc, "geom_column")?;
        }

        // Probe using source SQL with DB-side offset/exists.
        let probe_sql = format!(
            "SELECT EXISTS (SELECT 1 FROM ({}) AS __freestiler_probe OFFSET {} LIMIT 1)",
            sql, threshold
        );
        let row = conn
            .query_one(&probe_sql, &[])
            .map_err(|e| format!("Cannot probe PostGIS query size: {}", e))?;
        let exceeds: bool = row.get(0);
        if exceeds {
            return Ok((true, None));
        }

        let prepared = prepare_query_plan(&mut conn, sql, geom_column)?;
        let batch_size = config.batch_size.or(Some(config.effective_batch_size()));
        let features = if let Some(batch) = batch_size {
            cursor_batch_read(&mut conn, &prepared.full_sql, &prepared.prop_cols, batch)?
        } else {
            single_read(&mut conn, &prepared.full_sql, &prepared.prop_cols)?
        };

        if features.is_empty() {
            return Err("No valid features found in query result".to_string());
        }

        let layers = vec![LayerData {
            name: layer_name.to_string(),
            features,
            prop_names: prepared.schema.prop_names,
            prop_types: prepared.schema.prop_types,
            min_zoom,
            max_zoom,
        }];
        Ok((false, Some(layers)))
    }

    impl PostgisBatchScanner {
        pub fn new(
            config: &PostgisConfig,
            sql: &str,
            geom_column: Option<&str>,
        ) -> Result<Self, String> {
            info!("Connecting to PostGIS: {}", crate::tiler::mask_conn_str(&config.conn_str));
            let mut conn = connect_with_config(config)?;
            if let Some(ref gc) = geom_column {
                validate_identifier(gc, "geom_column")?;
            }
            let prepared = prepare_query_plan(&mut conn, sql, geom_column)?;
            Ok(Self {
                conn,
                full_sql: prepared.full_sql,
                prop_cols: prepared.prop_cols,
                schema: prepared.schema,
                batch_size: config.effective_batch_size(),
                temp_snapshot_table: None,
            })
        }

        pub fn schema(&self) -> &PostgisLayerSchema {
            &self.schema
        }

        pub fn scan_batches<F>(&mut self, mut on_batch: F) -> Result<(), String>
        where
            F: FnMut(Vec<Feature>) -> Result<(), String>,
        {
            self.conn
                .execute("BEGIN", &[])
                .map_err(|e| format!("Cannot start transaction: {}", e))?;

            let result = (|| -> Result<(), String> {
                self.conn
                    .execute(&format!("DECLARE {} CURSOR FOR {}", CURSOR_NAME, self.full_sql), &[])
                    .map_err(|e| format!("Cannot declare cursor: {}", e))?;

                let mut next_id: u64 = 1;
                loop {
                    let fetch_sql = format!("FETCH {} FROM {}", self.batch_size, CURSOR_NAME);
                    let rows = self
                        .conn
                        .query(&fetch_sql, &[])
                        .map_err(|e| format!("Cursor fetch error: {}", e))?;
                    if rows.is_empty() {
                        break;
                    }
                    let batch_features = parse_rows_parallel(&rows, &self.prop_cols, next_id)?;
                    next_id += batch_features.len() as u64;
                    on_batch(batch_features)?;
                }
                let _ = self.conn.execute(&format!("CLOSE {}", CURSOR_NAME), &[]);
                Ok(())
            })();

            match &result {
                Ok(_) => {
                    self.conn
                        .execute("COMMIT", &[])
                        .map_err(|e| format!("Cannot commit transaction: {}", e))?;
                }
                Err(_) => {
                    let _ = self.conn.execute("ROLLBACK", &[]);
                }
            }

            result
        }

        pub fn materialize_temp_snapshot(&mut self) -> Result<(), String> {
            if self.temp_snapshot_table.is_some() {
                return Ok(());
            }

            let _ = self
                .conn
                .execute("SET default_transaction_read_only = off", &[]);
            let table_name = format!("__freestiler_pg_snap_{}", unique_suffix());
            let create_sql = format!(
                "CREATE TEMP TABLE {} AS {}",
                table_name, self.full_sql
            );
            self.conn
                .execute(&create_sql, &[])
                .map_err(|e| format!("Cannot materialize PostGIS temp snapshot: {}", e))?;

            // Subsequent scans read from the temp table, avoiding repeated
            // execution of the original source SQL and geometry transforms.
            self.full_sql = format!("SELECT * FROM {}", table_name);
            self.temp_snapshot_table = Some(table_name);
            let _ = self
                .conn
                .execute("SET default_transaction_read_only = on", &[]);
            Ok(())
        }
    }

    impl Drop for PostgisBatchScanner {
        fn drop(&mut self) {
            if let Some(ref t) = self.temp_snapshot_table {
                let _ = self.conn.execute(&format!("DROP TABLE IF EXISTS {}", t), &[]);
            }
        }
    }

    fn unique_suffix() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{}_{}", std::process::id(), nanos)
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
        let config = PostgisConfig::new(conn_str)
            .batch_size(batch_size.unwrap_or(DEFAULT_BATCH_SIZE));
        postgis_query_to_layers_with_config(&config, sql, layer_name, min_zoom, max_zoom, geom_column)
    }

    fn single_read(
        conn: &mut Client,
        sql: &str,
        prop_cols: &[(usize, PgValueKind)],
    ) -> Result<Vec<Feature>, String> {
        let rows = conn.query(sql, &[]).map_err(|e| format!("Query error: {}", e))?;
        parse_rows_parallel(&rows, prop_cols, 1)
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
            let mut next_id: u64 = 1;

            loop {
                let fetch_sql = format!("FETCH {} FROM {}", batch_size, CURSOR_NAME);
                let rows = conn.query(&fetch_sql, &[])
                    .map_err(|e| format!("Cursor fetch error: {}", e))?;

                if rows.is_empty() {
                    break;
                }

                let mut batch_features = parse_rows_parallel(&rows, prop_cols, next_id)?;
                debug!("Cursor batch: fetched {} features", batch_features.len());
                next_id += batch_features.len() as u64;
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

    fn parse_rows_parallel(
        rows: &[Row],
        prop_cols: &[(usize, PgValueKind)],
        start_id: u64,
    ) -> Result<Vec<Feature>, String> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        debug!("parse_rows_parallel: {} rows, {} prop_cols", rows.len(), prop_cols.len());

        let wkb_col_idx = rows[0]
            .columns()
            .iter()
            .position(|c| c.name() == WKB_ALIAS)
            .ok_or_else(|| {
                let col_names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
                format!("WKB column '{}' not found in result. Available columns: {:?}", WKB_ALIAS, col_names)
            })?;

        let features: Vec<Option<Feature>> = rows
            .par_iter()
            .enumerate()
            .map(|(idx, row)| {
                let wkb_bytes: Option<Vec<u8>> = row.get(wkb_col_idx);
                let wkb_bytes = match wkb_bytes {
                    Some(b) => b,
                    None => {
                        debug!("WKB bytes is None, skipping row");
                        return None;
                    }
                };

                let geometry = match crate::wkb::wkb_to_geometry(&wkb_bytes) {
                    Some(g) => g,
                    None => {
                        warn!("WKB parsing failed for bytes length {}, skipping row", wkb_bytes.len());
                        return None;
                    }
                };

                let mut properties = Vec::with_capacity(prop_cols.len());
                for &(col_idx, kind) in prop_cols {
                    properties.push(extract_pg_value(row, col_idx, kind));
                }

                Some(Feature {
                    id: Some(start_id + idx as u64),
                    geometry,
                    properties,
                })
            })
            .collect();

        let valid_features: Vec<Feature> = features.into_iter().flatten().collect();
        Ok(valid_features)
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
        fn test_postgis_config_defaults() {
            let c = PostgisConfig::new("postgresql://localhost/mydb");
            assert!(!c.use_ssl);
            assert!(c.ssl_ca_file.is_none());
            assert_eq!(c.effective_batch_size(), DEFAULT_BATCH_SIZE);
        }

        #[test]
        fn test_postgis_config_builder() {
            let c = PostgisConfig::new("postgresql://localhost/mydb")
                .use_ssl(true)
                .ssl_ca_file("/path/to/ca.pem")
                .batch_size(5000);
            assert!(c.use_ssl);
            assert_eq!(c.ssl_ca_file, Some("/path/to/ca.pem".to_string()));
            assert_eq!(c.effective_batch_size(), 5000);
        }
    }
}

#[cfg(feature = "postgis")]
pub use postgis_impl::{
    postgis_query_count_with_config, postgis_query_each_batch_with_config,
    postgis_query_exceeds_with_config, postgis_query_to_layers,
    postgis_query_to_layers_with_geom, postgis_query_to_layers_with_config, PostgisConfig,
    PostgisBatchScanner, PostgisLayerSchema, postgis_probe_and_maybe_load_layers_with_config,
};
