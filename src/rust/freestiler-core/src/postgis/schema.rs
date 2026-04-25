use crate::model::LayerSchema;

#[cfg(feature = "postgis")]
pub fn discover_layer_schema(
    config: &crate::postgis_input::PostgisConfig,
    sql: &str,
    layer_name: &str,
    geom_column_hint: Option<&str>,
) -> Result<LayerSchema, String> {
    let scanner = crate::postgis_input::PostgisBatchScanner::new(config, sql, geom_column_hint)?;
    let schema = scanner.schema().clone();
    Ok(LayerSchema {
        layer_name: layer_name.to_string(),
        geom_column: schema.geom_column,
        prop_names: schema.prop_names,
        prop_types: schema.prop_types,
        source_srid: schema.source_srid,
        fid_column: schema.fid_column,
    })
}
