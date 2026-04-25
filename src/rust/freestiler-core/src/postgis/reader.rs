use std::str::FromStr;

use crate::model::{FeatureBatch, LayerSchema, PartitionSpec};
use crate::unique_suffix;

#[derive(Clone, Debug)]
pub struct PostgisSourceConfig {
    pub conn_str: String,
    pub batch_size: usize,
    pub connect_timeout_ms: Option<u64>,
    pub use_ssl: bool,
}

impl From<&crate::postgis_input::PostgisConfig> for PostgisSourceConfig {
    fn from(value: &crate::postgis_input::PostgisConfig) -> Self {
        Self {
            conn_str: value.conn_str.clone(),
            batch_size: value.batch_size.unwrap_or(10_000),
            connect_timeout_ms: value.connect_timeout_ms,
            use_ssl: value.use_ssl,
        }
    }
}

#[cfg(feature = "postgis")]
pub struct PostgisPartitionReader {
    conn: postgres::Client,
    schema: LayerSchema,
    plan: FidReadPlan,
}

#[cfg(feature = "postgis")]
#[derive(Clone, Debug)]
enum FidReadPlan {
    SnapshotTable { table_name: String },
}

#[cfg(feature = "postgis")]
fn wrap_sql_with_stable_order(source_sql: &str, schema: &LayerSchema) -> String {
    match &schema.fid_column {
        Some(fid_column) => format!(
            "SELECT * FROM ({}) AS __ordered_stream_src ORDER BY \"{}\"",
            source_sql, fid_column
        ),
        None => source_sql.to_string(),
    }
}

#[cfg(feature = "postgis")]
fn connect(config: &PostgisSourceConfig) -> Result<postgres::Client, String> {
    if config.use_ssl {
        return Err("streaming pipeline does not support SSL PostGIS connections yet".to_string());
    }
    let mut builder = postgres::Config::from_str(&config.conn_str)
        .map_err(|e| format!("Invalid connection string: {}", e))?;
    if let Some(timeout_ms) = config.connect_timeout_ms {
        builder.connect_timeout(std::time::Duration::from_millis(timeout_ms));
    }
    builder
        .connect(postgres::NoTls)
        .map_err(|e| format!("Cannot connect to PostgreSQL: {:?}", e))
}

#[cfg(feature = "postgis")]
pub fn open_partition_reader(
    config: &PostgisSourceConfig,
    sql: &str,
    schema: LayerSchema,
) -> Result<PostgisPartitionReader, String> {
    let mut conn = connect(config)?;
    let plan = build_fid_read_plan(sql, &schema, &format!("__freestiler_stream_snap_{}", unique_suffix()));
    let FidReadPlan::SnapshotTable { table_name } = &plan;
    materialize_snapshot(&mut conn, sql, &schema, table_name)?;
    Ok(PostgisPartitionReader {
        conn,
        schema,
        plan,
    })
}

#[cfg(feature = "postgis")]
fn build_fid_read_plan(source_sql: &str, schema: &LayerSchema, snapshot_table_name: &str) -> FidReadPlan {
    let _ = source_sql;
    let _ = schema;
    FidReadPlan::SnapshotTable {
        table_name: snapshot_table_name.to_string(),
    }
}

#[cfg(feature = "postgis")]
fn build_bbox_query(schema: &LayerSchema, plan: &FidReadPlan, partition: &PartitionSpec) -> String {
    let geom_col = format!("\"{}\"", schema.geom_column);
    let geom_expr = match schema.source_srid {
        Some(4326) | None => geom_col.clone(),
        Some(_srid) => format!("ST_Transform({}, 4326)", geom_col),
    };
    let select_cols: Vec<String> = schema
        .prop_names
        .iter()
        .map(|name| format!("\"{}\"", name))
        .collect();
    let mut projections = select_cols;
    let from_sql = match plan {
        FidReadPlan::SnapshotTable { table_name } => {
            projections.push("\"__fid\"".to_string());
            table_name.to_string()
        }
    };
    projections.push(format!("ST_AsEWKB({}) AS \"__wkb\"", geom_expr));
    format!(
        "SELECT {} FROM {} \
         WHERE {} IS NOT NULL \
         AND ST_Intersects({}, ST_MakeEnvelope({}, {}, {}, {}, 4326))",
        projections.join(", "),
        from_sql,
        geom_col,
        geom_expr,
        partition.min_lon,
        partition.min_lat,
        partition.max_lon,
        partition.max_lat
    )
}

#[cfg(feature = "postgis")]
fn materialize_snapshot(
    conn: &mut postgres::Client,
    source_sql: &str,
    schema: &LayerSchema,
    table_name: &str,
) -> Result<(), String> {
    let ordered_source_sql = wrap_sql_with_stable_order(source_sql, schema);
    let _ = conn.execute("SET default_transaction_read_only = off", &[]);
    let create_sql = format!(
        "CREATE TEMP TABLE {} AS \
         SELECT __stream_base.*, ROW_NUMBER() OVER ()::bigint AS \"__fid\" \
         FROM ({}) AS __stream_base",
        table_name, ordered_source_sql
    );
    conn.execute(&create_sql, &[])
        .map_err(|e| format!("Cannot materialize streaming temp snapshot: {}", e))?;

    conn.execute(
        &format!(
            "CREATE INDEX {} ON {} (\"__fid\")",
            format!("{}_fid_idx", table_name),
            table_name
        ),
        &[],
    )
    .map_err(|e| format!("Cannot create temp snapshot fid index: {}", e))?;

    let geom_expr = match schema.source_srid {
        Some(4326) | None => format!("\"{}\"", schema.geom_column),
        Some(_srid) => format!("ST_Transform(\"{}\", 4326)", schema.geom_column),
    };
    conn.execute(
        &format!(
            "CREATE INDEX {} ON {} USING GIST ({})",
            format!("{}_geom_idx", table_name),
            table_name,
            geom_expr
        ),
        &[],
    )
    .map_err(|e| format!("Cannot create temp snapshot geometry index: {}", e))?;

    conn.execute(&format!("ANALYZE {}", table_name), &[])
        .map_err(|e| format!("Cannot analyze temp snapshot: {}", e))?;
    let _ = conn.execute("SET default_transaction_read_only = on", &[]);
    Ok(())
}

#[cfg(feature = "postgis")]
impl PostgisPartitionReader {
    fn bbox_query(&self, partition: &PartitionSpec) -> String {
        build_bbox_query(&self.schema, &self.plan, partition)
    }

    pub fn read_partition(&mut self, partition: &PartitionSpec) -> Result<FeatureBatch, String> {
        let sql = self.bbox_query(partition);
        let rows = self
            .conn
            .query(&sql, &[])
            .map_err(|e| format!("Partition query failed: {}", e))?;
        let features = super::normalize::normalize_rows(&rows, &self.schema)?;
        Ok(FeatureBatch {
            partition: partition.clone(),
            features,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::LayerSchema;

    fn sample_schema(fid_column: Option<&str>) -> LayerSchema {
        LayerSchema {
            layer_name: "test".to_string(),
            geom_column: "geom".to_string(),
            prop_names: vec!["gid".to_string(), "name".to_string()],
            prop_types: vec!["integer".to_string(), "string".to_string()],
            source_srid: Some(4326),
            fid_column: fid_column.map(|s| s.to_string()),
        }
    }

    fn sample_partition() -> PartitionSpec {
        PartitionSpec {
            sequence: 0,
            zoom: 10,
            row_start: 0,
            row_end: 64,
            min_lon: -180.0,
            min_lat: -10.0,
            max_lon: 180.0,
            max_lat: 10.0,
        }
    }

    #[test]
    fn reader_uses_snapshot_even_when_fid_column_exists() {
        let schema = sample_schema(Some("gid"));
        let plan = build_fid_read_plan("SELECT * FROM demo", &schema, "__snap_demo");
        let sql = build_bbox_query(&schema, &plan, &sample_partition());

        assert!(matches!(plan, FidReadPlan::SnapshotTable { .. }));
        assert!(sql.contains("FROM __snap_demo"));
        assert!(!sql.contains("ROW_NUMBER() OVER ()"));
    }

    #[test]
    fn reader_uses_snapshot_table_when_no_fid_column_exists() {
        let schema = sample_schema(None);
        let plan = build_fid_read_plan("SELECT * FROM demo", &schema, "__snap_demo");
        let sql = build_bbox_query(&schema, &plan, &sample_partition());

        assert!(matches!(plan, FidReadPlan::SnapshotTable { .. }));
        assert!(sql.contains("FROM __snap_demo"));
        assert!(!sql.contains("ROW_NUMBER() OVER ()"));
    }
}
