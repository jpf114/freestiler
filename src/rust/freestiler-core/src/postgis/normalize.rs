use crate::model::{BBox4326, NormalizedFeature};
use crate::tiler::{geometry_bbox, Geometry, PropertyValue};

#[derive(Clone, Copy, Debug)]
enum PropKind {
    String,
    Int,
    Double,
    Bool,
}

fn prop_kind(type_name: &str) -> PropKind {
    match type_name {
        "integer" => PropKind::Int,
        "numeric" => PropKind::Double,
        "logical" => PropKind::Bool,
        _ => PropKind::String,
    }
}

#[cfg(feature = "postgis")]
pub fn normalize_rows(
    rows: &[postgres::Row],
    schema: &crate::model::LayerSchema,
    start_id: u64,
) -> Result<Vec<NormalizedFeature>, String> {
    let prop_kinds: Vec<PropKind> = schema.prop_types.iter().map(|t| prop_kind(t)).collect();
    let wkb_idx = schema.prop_names.len();
    let mut out = Vec::with_capacity(rows.len());

    for (idx, row) in rows.iter().enumerate() {
        let wkb_bytes: Option<Vec<u8>> = row.get(wkb_idx);
        let Some(wkb_bytes) = wkb_bytes else {
            continue;
        };

        let Some(geometry) = crate::wkb::wkb_to_geometry(&wkb_bytes) else {
            continue;
        };

        let mut properties = Vec::with_capacity(prop_kinds.len());
        for (col_idx, kind) in prop_kinds.iter().enumerate() {
            let value = match kind {
                PropKind::String => row
                    .try_get::<_, Option<String>>(col_idx)
                    .ok()
                    .flatten()
                    .map(PropertyValue::String)
                    .unwrap_or(PropertyValue::Null),
                PropKind::Int => row
                    .try_get::<_, Option<i64>>(col_idx)
                    .ok()
                    .flatten()
                    .map(PropertyValue::Int)
                    .unwrap_or(PropertyValue::Null),
                PropKind::Double => row
                    .try_get::<_, Option<f64>>(col_idx)
                    .ok()
                    .flatten()
                    .map(PropertyValue::Double)
                    .unwrap_or(PropertyValue::Null),
                PropKind::Bool => row
                    .try_get::<_, Option<bool>>(col_idx)
                    .ok()
                    .flatten()
                    .map(PropertyValue::Bool)
                    .unwrap_or(PropertyValue::Null),
            };
            properties.push(value);
        }

        let bbox = BBox4326::from(geometry_bbox(&geometry));
        out.push(NormalizedFeature {
            id: Some(start_id + idx as u64),
            geometry,
            properties,
            bbox,
        });
    }

    Ok(out)
}

#[allow(dead_code)]
pub fn normalized_feature_from_parts(
    id: Option<u64>,
    geometry: Geometry,
    properties: Vec<PropertyValue>,
) -> NormalizedFeature {
    let bbox = BBox4326::from(geometry_bbox(&geometry));
    NormalizedFeature {
        id,
        geometry,
        properties,
        bbox,
    }
}
