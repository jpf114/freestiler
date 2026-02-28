"""Tests for CRS handling."""

import warnings

import pytest
import geopandas as gpd
from shapely.geometry import box

from freestiler import freestile


def test_auto_transform_from_3857(tmp_path):
    """Input in EPSG:3857 should be auto-transformed to WGS84."""
    gdf_4326 = gpd.GeoDataFrame(
        {"name": ["a"]},
        geometry=[box(-80, 35, -78, 37)],
        crs="EPSG:4326",
    )
    gdf_3857 = gdf_4326.to_crs(3857)
    output = tmp_path / "test.pmtiles"
    freestile(gdf_3857, output, tile_format="mvt", max_zoom=4, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_missing_crs_warning(tmp_path):
    """Missing CRS should emit a warning."""
    gdf = gpd.GeoDataFrame(
        {"name": ["a"]},
        geometry=[box(-80, 35, -78, 37)],
    )
    output = tmp_path / "test.pmtiles"
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        freestile(gdf, output, tile_format="mvt", max_zoom=4, quiet=True)
        assert any("no CRS" in str(warning.message) for warning in w)
    assert output.exists()
