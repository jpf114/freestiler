#!/bin/bash
# Setup the freestiler Python dev environment
# Usage: cd python && bash setup_env.sh [--duckdb]
#
# Opens in Positron: set Python interpreter to python/.venv/bin/python

set -e

cd "$(dirname "$0")"

echo "Creating venv..."
uv venv .venv

echo "Installing dependencies..."
uv pip install maturin geopandas shapely pyproj numpy pytest

FEATURES=""
if [[ "$1" == "--duckdb" ]]; then
    FEATURES="--features duckdb"
    echo "Building with DuckDB support (this takes a while first time)..."
else
    echo "Building (use --duckdb to enable DuckDB support)..."
fi

python3 -m maturin develop $FEATURES

echo ""
echo "Done! To use in Positron:"
echo "  1. Open Command Palette → 'Python: Select Interpreter'"
echo "  2. Choose: $(pwd)/.venv/bin/python"
echo ""
echo "Or from terminal:"
echo "  source .venv/bin/activate"
