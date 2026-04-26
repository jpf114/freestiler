"""Tests for the Python PostGIS wrapper layer."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path


def _load_freestiler_with_fake_extension(monkeypatch):
    captured: dict[str, object] = {}

    fake_ext = types.ModuleType("freestiler._freestiler")
    fake_ext._freestile = lambda **kwargs: "pmtiles-ok"
    fake_ext._freestile_postgis = lambda **kwargs: "postgis-ok"

    def fake_postgis_to_mongo(**kwargs):
        captured.update(kwargs)
        return "mongo-ok"

    fake_ext._freestile_postgis_to_mongo = fake_postgis_to_mongo

    package_init = (
        Path(__file__).resolve().parents[1] / "python" / "freestiler" / "__init__.py"
    )
    spec = importlib.util.spec_from_file_location(
        "freestiler",
        package_init,
        submodule_search_locations=[str(package_init.parent)],
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "freestiler", module)
    monkeypatch.setitem(sys.modules, "freestiler._freestiler", fake_ext)
    spec.loader.exec_module(module)
    return module, captured


def test_postgis_mongo_profile_is_forwarded(monkeypatch):
    module, captured = _load_freestiler_with_fake_extension(monkeypatch)

    result = module.freestile_postgis(
        "postgresql://user:pass@localhost:5432/gis",
        "SELECT 1",
        {
            "uri": "mongodb://localhost:27017",
            "database": "tiles",
            "collection": "cities",
        },
        quiet=True,
        mongo_profile="recommended",
    )

    assert result == {"status": "ok", "result": "mongo-ok"}
    assert captured["mongo_profile"] == "recommended"
