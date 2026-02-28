"""Tests for multi-layer tile generation."""

import pytest
import geopandas as gpd
from shapely.geometry import Point, box

from freestiler import freestile


def test_multilayer_dict(tmp_path):
    polys = gpd.GeoDataFrame(
        {"name": ["a", "b"]},
        geometry=[box(-80, 35, -78, 37), box(-82, 34, -79, 36)],
        crs="EPSG:4326",
    )
    pts = gpd.GeoDataFrame(
        {"label": ["p1", "p2"]},
        geometry=[Point(-79, 36), Point(-81, 35)],
        crs="EPSG:4326",
    )
    output = tmp_path / "multi.pmtiles"
    freestile(
        {"polygons": polys, "points": pts},
        output,
        tile_format="mvt",
        max_zoom=6,
        quiet=True,
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_multilayer_mlt(tmp_path):
    polys = gpd.GeoDataFrame(
        {"name": ["a"]},
        geometry=[box(-80, 35, -78, 37)],
        crs="EPSG:4326",
    )
    pts = gpd.GeoDataFrame(
        {"label": ["p1"]},
        geometry=[Point(-79, 36)],
        crs="EPSG:4326",
    )
    output = tmp_path / "multi_mlt.pmtiles"
    freestile(
        {"polygons": polys, "points": pts},
        output,
        tile_format="mlt",
        max_zoom=6,
        quiet=True,
    )
    assert output.exists()
    assert output.stat().st_size > 0


def test_invalid_input_type(tmp_path):
    with pytest.raises(TypeError):
        freestile("not a gdf", tmp_path / "out.pmtiles", quiet=True)
