"""Basic tests for freestiler."""

import pytest
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, LineString, Polygon, box

from freestiler import freestile


@pytest.fixture
def polygon_gdf():
    """Create a simple polygon GeoDataFrame."""
    polys = [
        box(-80, 35, -78, 37),
        box(-82, 34, -79, 36),
        box(-84, 33, -81, 35),
    ]
    return gpd.GeoDataFrame(
        {"name": ["a", "b", "c"], "value": [1, 2, 3]},
        geometry=polys,
        crs="EPSG:4326",
    )


@pytest.fixture
def point_gdf():
    """Create a simple point GeoDataFrame."""
    points = [Point(-78.6, 35.8), Point(-80.2, 36.1), Point(-82.5, 34.2)]
    return gpd.GeoDataFrame(
        {"name": ["p1", "p2", "p3"], "score": [10.5, 20.3, 30.1]},
        geometry=points,
        crs="EPSG:4326",
    )


@pytest.fixture
def line_gdf():
    """Create a simple linestring GeoDataFrame."""
    lines = [
        LineString([(-78, 35), (-79, 36), (-80, 35)]),
        LineString([(-81, 34), (-82, 35), (-83, 34)]),
    ]
    return gpd.GeoDataFrame(
        {"road": ["r1", "r2"]},
        geometry=lines,
        crs="EPSG:4326",
    )


def test_polygon_mvt(tmp_path, polygon_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(polygon_gdf, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_polygon_mlt(tmp_path, polygon_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(polygon_gdf, output, tile_format="mlt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_point_mvt(tmp_path, point_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(point_gdf, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_point_mlt(tmp_path, point_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(point_gdf, output, tile_format="mlt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_linestring_mvt(tmp_path, line_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(line_gdf, output, tile_format="mvt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_linestring_mlt(tmp_path, line_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(line_gdf, output, tile_format="mlt", max_zoom=6, quiet=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_overwrite(tmp_path, polygon_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(polygon_gdf, output, max_zoom=4, quiet=True)
    size1 = output.stat().st_size
    freestile(polygon_gdf, output, max_zoom=4, quiet=True, overwrite=True)
    assert output.exists()
    assert output.stat().st_size > 0


def test_no_overwrite(tmp_path, polygon_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(polygon_gdf, output, max_zoom=4, quiet=True)
    with pytest.raises(FileExistsError):
        freestile(polygon_gdf, output, max_zoom=4, quiet=True, overwrite=False)


def test_invalid_format(tmp_path, polygon_gdf):
    output = tmp_path / "test.pmtiles"
    with pytest.raises(ValueError, match="tile_format"):
        freestile(polygon_gdf, output, tile_format="xyz", quiet=True)


def test_layer_name(tmp_path, polygon_gdf):
    output = tmp_path / "test.pmtiles"
    freestile(
        polygon_gdf, output, layer_name="counties", max_zoom=4, quiet=True
    )
    assert output.exists()
