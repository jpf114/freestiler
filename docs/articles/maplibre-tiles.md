# MapLibre Tiles (MLT)

freestiler defaults to the [MapLibre Tiles
(MLT)](https://github.com/maplibre/maplibre-tile-spec) format, a
columnar binary tile encoding announced in January 2026 by the MapLibre
organization. This article explains what MLT is, how it compares to MVT,
and when you might want to use one vs the other.

### Vector tiles in brief

Vector tiles divide geographic data into a grid of square tiles at
multiple zoom levels. Unlike raster tiles (pre-rendered images), vector
tiles store the actual geometry and attribute data, so the client can
style, interact with, and dynamically render the data. The standard
container for vector tilesets is
[PMTiles](https://github.com/protomaps/PMTiles) - a single-file archive
you can serve from any static file host.

### MVT: the standard

[Mapbox Vector Tiles (MVT)](https://github.com/mapbox/vector-tile-spec)
has been the dominant vector tile encoding since Mapbox introduced it in
2014. It uses Protocol Buffers to serialize geometry and attributes, and
it’s supported by every major mapping library: MapLibre GL, Mapbox GL,
Leaflet, deck.gl, and more. When you need maximum compatibility, MVT is
the safe choice.

### MLT: what’s different

Where MVT stores data row-by-row (one feature at a time), MLT organizes
data column-by-column - all x-coordinates together, all y-coordinates
together, all values of a given attribute together. This columnar layout
opens up compression techniques that aren’t available in MVT’s
row-oriented format: delta encoding for coordinates, run-length encoding
for repeated values, and dictionary encoding for string columns with low
cardinality.

The practical result is smaller tiles, especially for polygon-heavy
datasets:

| Dataset                 | MVT    | MLT    | Savings |
|-------------------------|--------|--------|---------|
| NC counties (z0-14)     | 78 KB  | 65 KB  | 17%     |
| US block groups (z4-12) | 310 MB | 259 MB | 16%     |
| Education dots (z0-12)  | 238 MB | 237 MB | ~0%     |

Point-only datasets see minimal savings because coordinate data
dominates and compresses similarly in both formats.

### Which format should I use?

Use **MLT** (the default) when you’re working in R with
[mapgl](https://walker-data.com/mapgl/) or viewing in a browser with
MapLibre GL JS 5.17+. MLT produces smaller files for polygon and line
data, and mapgl supports it natively.

Use **MVT** when you need the widest viewer compatibility - particularly
for Python-facing examples, older MapLibre versions, or when sharing
tilesets with users on tools you don’t control. You can switch formats
with a single argument:

``` r
freestile(nc, "nc_mvt.pmtiles", layer_name = "counties", tile_format = "mvt")
```

### Ecosystem status

MLT is still new (January 2026), and library support is growing:

- **Encoding**: freestiler (Rust, R + Python), mlt-core reference
  encoder (Java/C++)
- **Decoding**: MapLibre GL JS (experimental), mlt-core decoder
- **Viewing**: MapLibre GL JS with MLT plugin, mapgl in R

As MLT support expands across mapping libraries, the default will become
directly viewable in more places without needing to switch to MVT.
