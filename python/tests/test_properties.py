"""Tests for property type handling."""

import pytest
import geopandas as gpd
import numpy as np
from shapely.geometry import Point

from freestiler import freestile


def test_all_property_types(tmp_path):
    """Test string, integer, float, and boolean properties."""
    gdf = gpd.GeoDataFrame(
        {
            "str_col": ["hello", "world", "test"],
            "int_col": [1, 2, 3],
            "float_col": [1.1, 2.2, 3.3],
            "bool_col": [True, False, True],
        },
        geometry=[Point(-78, 35), Point(-79, 36), Point(-80, 37)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(gdf, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_na_handling(tmp_path):
    """Test that NA/None values are handled correctly."""
    gdf = gpd.GeoDataFrame(
        {
            "str_col": ["hello", None, "test"],
            "int_col": [1, None, 3],
            "float_col": [1.1, np.nan, 3.3],
        },
        geometry=[Point(-78, 35), Point(-79, 36), Point(-80, 37)],
        crs="EPSG:4326",
    )
    # Use nullable integer type
    gdf["int_col"] = gdf["int_col"].astype("Int64")
    output = tmp_path / "test.pmtiles"
    freestile(gdf, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_no_properties(tmp_path):
    """Test GeoDataFrame with no attribute columns."""
    gdf = gpd.GeoDataFrame(
        geometry=[Point(-78, 35), Point(-79, 36)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(gdf, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_mlt_properties(tmp_path):
    """Test property encoding in MLT format."""
    gdf = gpd.GeoDataFrame(
        {
            "str_col": ["hello", "world"],
            "int_col": [1, 2],
            "float_col": [1.1, 2.2],
            "bool_col": [True, False],
        },
        geometry=[Point(-78, 35), Point(-79, 36)],
        crs="EPSG:4326",
    )
    output = tmp_path / "test.pmtiles"
    freestile(gdf, output, tile_format="mlt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0
