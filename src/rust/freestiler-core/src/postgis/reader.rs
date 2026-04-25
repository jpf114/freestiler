use std::str::FromStr;

use crate::model::{FeatureBatch, LayerSchema, PartitionSpec};

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
    source_sql: String,
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
    Ok(PostgisPartitionReader {
        conn: connect(config)?,
        schema,
        source_sql: sql.to_string(),
    })
}

#[cfg(feature = "postgis")]
impl PostgisPartitionReader {
    fn bbox_query(&self, partition: &PartitionSpec) -> String {
        let geom_col = format!("\"{}\"", self.schema.geom_column);
        let geom_expr = match self.schema.source_srid {
            Some(4326) | None => geom_col.clone(),
            Some(_srid) => format!("ST_Transform({}, 4326)", geom_col),
        };
        let select_cols: Vec<String> = self
            .schema
            .prop_names
            .iter()
            .map(|name| format!("\"{}\"", name))
            .collect();
        let mut projections = select_cols;
        projections.push("\"__fid\"".to_string());
        projections.push(format!("ST_AsEWKB({}) AS \"__wkb\"", geom_expr));
        format!(
            "SELECT {} FROM ( \
                SELECT __stream_base.*, ROW_NUMBER() OVER () AS \"__fid\" \
                FROM ({}) AS __stream_base \
             ) AS __stream_src \
             WHERE {} IS NOT NULL \
             AND ST_Intersects({}, ST_MakeEnvelope({}, {}, {}, {}, 4326))",
            projections.join(", "),
            self.source_sql,
            geom_col,
            geom_expr,
            partition.min_lon,
            partition.min_lat,
            partition.max_lon,
            partition.max_lat
        )
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
