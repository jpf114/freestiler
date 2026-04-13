#[cfg(any(feature = "geoparquet", feature = "duckdb", feature = "postgis"))]
use crate::tiler::Geometry;

#[cfg(any(feature = "geoparquet", feature = "duckdb", feature = "postgis"))]
pub fn wkb_to_geometry(wkb_bytes: &[u8]) -> Option<Geometry> {
    use geozero::wkb::Ewkb;
    use geozero::ToGeo;

    let geo_geom = Ewkb(wkb_bytes).to_geo().ok()?;
    match geo_geom {
        geo_types::Geometry::Point(p) => Some(Geometry::Point(p)),
        geo_types::Geometry::MultiPoint(mp) => Some(Geometry::MultiPoint(mp)),
        geo_types::Geometry::LineString(ls) => Some(Geometry::LineString(ls)),
        geo_types::Geometry::MultiLineString(mls) => Some(Geometry::MultiLineString(mls)),
        geo_types::Geometry::Polygon(p) => Some(Geometry::Polygon(p)),
        geo_types::Geometry::MultiPolygon(mp) => Some(Geometry::MultiPolygon(mp)),
        _ => None,
    }
}
