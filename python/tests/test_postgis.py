"""Smoke tests for optional PostGIS bindings."""

import pytest

from freestiler import freestile_postgis

try:
    from freestiler._freestiler import _freestile_postgis  # noqa: F401

    _HAS_POSTGIS = True
except ImportError:
    _HAS_POSTGIS = False

try:
    from freestiler._freestiler import _freestile_postgis_to_mongo  # noqa: F401

    _HAS_POSTGIS_MONGO = True
except ImportError:
    _HAS_POSTGIS_MONGO = False


requires_postgis = pytest.mark.skipif(
    not _HAS_POSTGIS, reason="PostGIS feature not compiled"
)


@requires_postgis
def test_postgis_binding_is_exposed():
    """PostGIS-enabled builds should expose the Python API."""
    assert callable(freestile_postgis)


@requires_postgis
def test_postgis_mongo_output_requires_mongodb_feature():
    """Mongo output should either fail fast on missing feature or reach the backend."""
    expected = (
        "Cannot connect to PostgreSQL|password authentication failed"
        if _HAS_POSTGIS_MONGO
        else "PostGIS \\+ MongoDB support"
    )
    with pytest.raises(RuntimeError, match=expected):
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
