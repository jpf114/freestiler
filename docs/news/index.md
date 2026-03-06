# Changelog

## freestiler 0.1.0

Initial release.

### Tile generation

- [`freestile()`](https://walker-data.com/freestiler/reference/freestile.md)
  creates PMTiles archives from sf data frames with zero external
  dependencies (no tippecanoe, no Java, no Go).
- Supports **MapLibre Tiles (MLT)** and **Mapbox Vector Tiles (MVT)**
  output formats.
- Multi-layer output via named lists or
  [`freestile_layer()`](https://walker-data.com/freestiler/reference/freestile_layer.md)
  per-layer zoom control.

### Geometry types

- POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON, MULTIPOLYGON.
- Automatic CRS transformation to WGS84.
- Z/M dimension handling (dropped automatically).

### Performance features

- Parallel tile encoding with rayon (across tiles and within tiles).
- Tile-pixel grid snapping for zoom-adaptive simplification without
  slivers.
- Buffered tile assignment and clipping for seamless tile boundaries.

### Feature management

- `drop_rate` exponential feature thinning with Morton-curve spatial
  ordering for points and area-based ordering for polygons/lines.
- `base_zoom` control for ensuring all features present at higher zooms.
- `cluster_distance` point clustering with `point_count` attribute.
- `coalesce` line merging and polygon grouping.

### MLT encoder

- Spec-compliant MapLibre Tile encoder with varint, delta, RLE, and
  dictionary encoding.
- Validated against mlt-core 0.1.2 reference decoder.
