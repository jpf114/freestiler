use crate::tiler::PropertyValue;

#[derive(Clone, Copy)]
pub enum DuckDbValueKind {
    String,
    Int,
    Double,
    Bool,
}

pub fn duckdb_type_to_value_kind(dtype: &str) -> DuckDbValueKind {
    let dt = dtype.trim().to_uppercase();
    if matches!(dt.as_str(), "BOOLEAN" | "BOOL" | "LOGICAL") {
        DuckDbValueKind::Bool
    } else if matches!(
        dt.as_str(),
        "TINYINT"
            | "SMALLINT"
            | "INTEGER"
            | "INT"
            | "BIGINT"
            | "UTINYINT"
            | "USMALLINT"
            | "UINTEGER"
    ) {
        DuckDbValueKind::Int
    } else if matches!(dt.as_str(), "REAL" | "FLOAT" | "DOUBLE") || dt.starts_with("DECIMAL") {
        DuckDbValueKind::Double
    } else {
        DuckDbValueKind::String
    }
}

pub fn duckdb_type_to_property_type(dtype: &str) -> String {
    match duckdb_type_to_value_kind(dtype) {
        DuckDbValueKind::String => "character".to_string(),
        DuckDbValueKind::Int => "integer".to_string(),
        DuckDbValueKind::Double => "numeric".to_string(),
        DuckDbValueKind::Bool => "logical".to_string(),
    }
}

#[cfg(feature = "duckdb")]
pub fn extract_value(row: &duckdb::Row, col_idx: usize, kind: DuckDbValueKind) -> PropertyValue {
    match kind {
        DuckDbValueKind::String => row
            .get::<_, Option<String>>(col_idx)
            .ok()
            .flatten()
            .map(PropertyValue::String)
            .unwrap_or(PropertyValue::Null),
        DuckDbValueKind::Int => row
            .get::<_, Option<i64>>(col_idx)
            .ok()
            .flatten()
            .map(PropertyValue::Int)
            .unwrap_or(PropertyValue::Null),
        DuckDbValueKind::Double => row
            .get::<_, Option<f64>>(col_idx)
            .ok()
            .flatten()
            .map(|v| {
                if v.is_nan() {
                    PropertyValue::Null
                } else {
                    PropertyValue::Double(v)
                }
            })
            .unwrap_or(PropertyValue::Null),
        DuckDbValueKind::Bool => row
            .get::<_, Option<bool>>(col_idx)
            .ok()
            .flatten()
            .map(PropertyValue::Bool)
            .unwrap_or(PropertyValue::Null),
    }
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}
