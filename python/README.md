# freestiler for Python

`freestiler` builds PMTiles vector tile archives from GeoPandas data,
GeoParquet files, and DuckDB spatial queries using a Rust tiling engine.

Features:

- MapLibre Tiles (`mlt`) and Mapbox Vector Tiles (`mvt`)
- Multi-layer tilesets
- Point clustering
- Feature coalescing
- Exponential feature dropping for low zoom levels

## Why this package exists

- Python-native API backed by the same Rust tiler as the R package
- PMTiles output instead of tile directory trees
- Direct DuckDB SQL tiling
- Streaming point tiling for large DuckDB query results

## Installation

Install from PyPI:

```bash
pip install freestiler
```

Published PyPI wheels ship the native feature set for Python 3.9 through 3.14:

- GeoPandas input
- Multi-layer tiling and feature management
- Direct GeoParquet file input
- DuckDB-backed file input
- DuckDB SQL query support
- PostGIS query input
- MongoDB tile output

If a wheel is not available for your platform, `pip` will build from source and
requires a Rust toolchain.

## Quick Start

```python
import geopandas as gpd
from freestiler import freestile

gdf = gpd.read_file("counties.shp")

freestile(gdf, "counties.pmtiles", layer_name="counties")
```

That example is intentionally small. The more interesting path is tiling
directly from DuckDB:

```python
from freestiler import freestile_query

freestile_query(
    query="SELECT * FROM read_parquet('blocks.parquet') WHERE state = 'NC'",
    output="nc_blocks.pmtiles",
    layer_name="blocks",
)
```

For very large point tables, use `streaming="always"` and prefer
`tile_format="mvt"` for maximum viewer compatibility.

## PostGIS to MongoDB

The Python binding also supports streaming tiles directly from PostGIS into
MongoDB. This is the custom path used for large online tile delivery workloads.

```python
from freestiler import freestile_postgis

result = freestile_postgis(
    "postgresql://postgres:postgres@10.1.0.16:5433/geoc_data",
    "SELECT * FROM public.ht_tyg5c32ihg_sys_ht_mark ORDER BY ogc_fid LIMIT 100",
    {
        "uri": "mongodb://localhost:27017",
        "database": "freestiler_test",
        "collection": "py_tiles",
    },
    layer_name="default",
    streaming=True,
    mongo_profile="recommended",
)

print(result)
```

Mongo output documents keep the business fields `id`, `x`, `y`, `z`, and
`data`. MongoDB also adds its own internal `_id` field.

Built-in `mongo_profile` presets:

- `recommended`: `tile_format="mvt"`, zoom `10..12`
- `safe`: `tile_format="mvt"`, zoom `6..12`
- `high_detail`: `tile_format="mvt"`, zoom `14..15`

Operational guidance:

- Prefer `mongo_profile="recommended"` for the default production path.
- Use `streaming=True` for bounded memory usage on large tables.
- Avoid Mongo output with `min_zoom <= 5`; very large low-zoom tiles can exceed
  MongoDB's 16 MB document limit.

## Minimal CLI for PostGIS to MongoDB

For source checkouts, a minimal CLI is available for the same pipeline:

```bash
cargo run --manifest-path python/Cargo.toml --bin freestiler-postgis-mongo -- \
  --postgis "10.1.0.16:5433:geoc_data:postgres:postgres" \
  --sql "SELECT * FROM public.ht_tyg5c32ihg_sys_ht_mark ORDER BY ogc_fid LIMIT 100" \
  --mongo "localhost:27017" \
  --mongo-db "freestiler_test" \
  --mongo-collection "cli_tiles" \
  --mongo-profile recommended \
  --streaming true
```

Notes:

- `--postgis` accepts `ip:port:dbname:user:password` or a full
  `postgresql://...` URL.
- `--mongo` accepts `host:port` or a full `mongodb://...` URI.
- The CLI and Python API have been parity-checked against the same Mongo tile
  output using `python/scripts/verify_cli_python_mongo_parity.py`.

Performance note:

- `freestile(gdf, ...)` is convenient for GeoDataFrames that already fit comfortably in memory.
- For larger datasets, `freestile_file()` and `freestile_query()` are usually faster because they avoid a heavier GeoPandas-to-Rust handoff.
- If your GeoDataFrame still needs `to_crs(4326)` before tiling, that reprojection step can dominate startup time on large layers.

## Source Builds

Published wheels include the default native feature set, including GeoParquet,
DuckDB, PostGIS, and MongoDB output support. To build from a local checkout:

```bash
git clone https://github.com/walkerke/freestiler.git
cd freestiler/python
python3 -m venv .venv
source .venv/bin/activate
pip install maturin
python3 -m maturin develop --release
```

To build an installable wheel instead of using an editable install:

```bash
python3 -m maturin build --release --out dist
pip install dist/freestiler-*.whl
```

From the repository root, verify the installed PostGIS + Mongo binding with:

```bash
python python/scripts/verify_installed_postgis_mongo_binding.py
```

## Links

- Documentation: https://walker-data.com/freestiler/articles/python.html
- Source: https://github.com/walkerke/freestiler
- Issues: https://github.com/walkerke/freestiler/issues
