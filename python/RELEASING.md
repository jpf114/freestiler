# Python Release Checklist

This package publishes Python distributions to PyPI with the default native
feature set.

Current wheel policy:

- Published wheels include the default feature set.
- GeoParquet support is included.
- DuckDB support is included.
- PostGIS support is included.
- MongoDB output support is included.
- Supported wheel targets are Python 3.9 through 3.14.

## Before releasing

1. Bump the Python version in `python/pyproject.toml` and `python/Cargo.toml`.
2. Review `python/README.md` for any release-specific wording.
3. Ensure the git tree is clean enough for a release build.
4. Confirm the GitHub Actions trusted publisher path is still what you want:
   repo `walkerke/freestiler`, workflow `.github/workflows/python-package.yml`,
   environment `pypi`.

## Build and validate locally

```bash
cd python
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
python -m pytest -q tests
cargo test --manifest-path Cargo.toml --bin freestiler-postgis-mongo
python scripts/verify_installed_postgis_mongo_binding.py
python -m maturin build --release --sdist --out dist
python -m twine check dist/*
```

If you have access to a real PostGIS and MongoDB environment, run the parity
check before publishing changes to the custom PostGIS -> Mongo pipeline:

```bash
python scripts/verify_cli_python_mongo_parity.py \
  --postgis "10.1.0.16:5433:geoc_data:postgres:postgres" \
  --sql "SELECT * FROM public.ht_tyg5c32ihg_sys_ht_mark ORDER BY ogc_fid LIMIT 100" \
  --mongo "localhost:27017" \
  --mongo-db "freestiler_test" \
  --cli-collection "release_cli_parity" \
  --python-collection "release_python_parity" \
  --mongo-profile recommended \
  --streaming \
  --cleanup
```

## TestPyPI upload

```bash
python -m twine upload --repository testpypi dist/*
```

## PyPI upload

Preferred path: push a `python-v*` tag and let GitHub Actions publish via the
trusted publisher workflow in `.github/workflows/python-package.yml`.

Typical release flow:

```bash
git add .
git commit -m "python release v0.1.0"
git push origin main
git tag python-v0.1.0
git push origin python-v0.1.0
```

If trusted publishing is configured on PyPI for this repository and workflow,
you do not need to sign in locally or remember an API token. GitHub Actions
publishes directly.

If you are unsure whether trusted publishing is configured, check the PyPI
project settings once in the browser before tagging. The project should list a
trusted publisher for this repository/workflow combination.

Manual fallback:

```bash
python -m twine upload dist/*
```

For the manual fallback, use a PyPI API token rather than a password.
