# freestiler

<!-- badges: start -->
<!-- badges: end -->

**freestiler** is a Rust-powered vector tile engine for R. It takes sf data frames and produces [PMTiles](https://github.com/protomaps/PMTiles) archives with zero external dependencies --- no tippecanoe, no Java, no Go.

## Features

- **Two tile formats**: MapLibre Tiles (MLT) and Mapbox Vector Tiles (MVT)
- **Fast**: parallel Rust backend via extendr; ~8x faster than tippecanoe for in-R workflows
- **Multi-layer**: combine multiple sf layers into a single tileset
- **Feature management**: exponential drop rate, point clustering, line/polygon coalescing
- **Simplification**: tile-pixel grid snapping prevents slivers between adjacent polygons
- **Self-contained**: everything runs in-memory, no temp files or external processes

## Installation

Install from [r-universe](https://walkerke.r-universe.dev):

```r
install.packages("freestiler", repos = "https://walkerke.r-universe.dev")
```

Or install the development version from GitHub:

```r
# install.packages("devtools")
devtools::install_github("walkerke/freestiler")
```

## Quick start

```r
library(sf)
library(freestiler)

nc <- st_read(system.file("shape/nc.shp", package = "sf"))

# Create an MLT tileset
freestile(nc, "nc_counties.pmtiles", layer_name = "counties")

# Or use MVT format
freestile(nc, "nc_mvt.pmtiles", layer_name = "counties", tile_format = "mvt")
```

View with [mapgl](https://walker-data.com/mapgl/):

```r
library(mapgl)

maplibre() |>
  add_vector_source(
    id = "counties",
    url = paste0("pmtiles://", normalizePath("nc_counties.pmtiles"))
  ) |>
  add_fill_layer(
    id = "county-fill",
    source = "counties",
    source_layer = "counties",
    fill_color = "#00897b",
    fill_opacity = 0.5
  )
```

## Multi-layer tiles

```r
pts <- st_centroid(nc)

freestile(
  list(
    counties = freestile_layer(nc, min_zoom = 0, max_zoom = 10),
    centroids = freestile_layer(pts, min_zoom = 6, max_zoom = 14)
  ),
  "nc_layers.pmtiles"
)
```

## Learn more

- [Getting Started](https://walker-data.com/freestiler/articles/getting-started.html) --- installation, usage, and all features
- [MapLibre Tiles (MLT)](https://walker-data.com/freestiler/articles/maplibre-tiles.html) --- the MLT format and how freestiler encodes it
- [Python Companion](https://walker-data.com/freestiler/articles/python.html) --- using freestiler from Python

## Python

freestiler also has a Python companion package sharing the same Rust engine. See the [Python article](https://walker-data.com/freestiler/articles/python.html) for details.

```python
from freestiler import freestile
import geopandas as gpd

gdf = gpd.read_file("nc.shp")
freestile(gdf, "nc_counties.pmtiles", layer_name="counties")
```
