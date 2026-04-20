"""Smoke tests for optional PostGIS bindings."""

import pytest

from freestiler import freestile_postgis

try:
    from freestiler._freestiler import _freestile_postgis  # noqa: F401

    _HAS_POSTGIS = True
except ImportError:
    _HAS_POSTGIS = False


requires_postgis = pytest.mark.skipif(
    not _HAS_POSTGIS, reason="PostGIS feature not compiled"
)


@requires_postgis
def test_postgis_binding_is_exposed():
    """PostGIS-enabled builds should expose the Python API."""
    assert callable(freestile_postgis)


@requires_postgis
def test_postgis_mongo_output_requires_mongodb_feature():
    """PostGIS-only builds should fail fast on MongoDB output requests."""
    with pytest.raises(RuntimeError, match="PostGIS \\+ MongoDB support"):
        freestile_postgis(
            "postgresql://user:pass@localhost:5432/gis",
            "SELECT 1",
            {
                "uri": "mongodb://localhost:27017",
                "database": "tiles",
                "collection": "cities",
            },
            quiet=True,
            batch_size=None,
        )
