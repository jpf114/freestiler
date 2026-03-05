# Python Release Checklist

This package publishes base-feature Python distributions to PyPI.

Current wheel policy:

- Published wheels include the default feature set.
- GeoParquet support is included.
- DuckDB support is source-build only and is not published in PyPI wheels.

## Before releasing

1. Bump the Python version in `python/pyproject.toml` and `python/Cargo.toml`.
2. Review `python/README.md` for any release-specific wording.
3. Ensure the git tree is clean enough for a release build.

## Build and validate locally

```bash
cd python
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
python -m pytest -q tests
python -m maturin build --release --sdist --out dist
python -m twine check dist/*
```

## TestPyPI upload

```bash
python -m twine upload --repository testpypi dist/*
```

## PyPI upload

Preferred path: push a `python-v*` tag and let GitHub Actions publish via the
trusted publisher workflow in `.github/workflows/python-package.yml`.

Manual fallback:

```bash
python -m twine upload dist/*
```
