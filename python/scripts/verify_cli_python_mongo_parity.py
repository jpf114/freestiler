"""Verify that the CLI and Python API produce identical Mongo tile documents."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from pymongo import MongoClient


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SRC = REPO_ROOT / "python" / "python"

if str(PYTHON_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_SRC))

from freestiler import freestile_postgis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare Mongo tile output from freestiler CLI and Python API."
    )
    parser.add_argument("--postgis", required=True)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--mongo", required=True)
    parser.add_argument("--mongo-db", required=True)
    parser.add_argument("--cli-collection", required=True)
    parser.add_argument("--python-collection", required=True)
    parser.add_argument("--layer-name", default="default")
    parser.add_argument("--mongo-profile", default="recommended")
    parser.add_argument("--batch-size", type=int, default=20_000)
    parser.add_argument("--mongo-batch-size", type=int, default=8_192)
    parser.add_argument("--geom-column")
    parser.add_argument("--streaming", action="store_true", default=False)
    parser.add_argument("--create-indexes", action="store_true", default=False)
    parser.add_argument("--upsert", action="store_true", default=False)
    parser.add_argument("--cleanup", action="store_true", default=False)
    return parser.parse_args()


def normalize_postgis(value: str) -> str:
    if "://" in value:
        return value

    parts = value.split(":", 4)
    if len(parts) != 5 or any(not part for part in parts):
        raise SystemExit(
            "Invalid --postgis value. Expected ip:port:dbname:user:password or postgresql://..."
        )
    host, port, dbname, user, password = parts
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


def normalize_mongo(value: str) -> str:
    if "://" in value:
        return value

    parts = value.split(":")
    if len(parts) != 2 or any(not part for part in parts):
        raise SystemExit("Invalid --mongo value. Expected host:port or mongodb://...")
    return f"mongodb://{value}"


def run_cli(args: argparse.Namespace) -> None:
    command = [
        "cargo",
        "run",
        "--manifest-path",
        "python/Cargo.toml",
        "--bin",
        "freestiler-postgis-mongo",
        "--",
        "--postgis",
        args.postgis,
        "--sql",
        args.sql,
        "--mongo",
        args.mongo,
        "--mongo-db",
        args.mongo_db,
        "--mongo-collection",
        args.cli_collection,
        "--layer-name",
        args.layer_name,
        "--mongo-profile",
        args.mongo_profile,
        "--batch-size",
        str(args.batch_size),
        "--mongo-batch-size",
        str(args.mongo_batch_size),
        "--streaming",
        "true" if args.streaming else "false",
        "--create-indexes",
        "true" if args.create_indexes else "false",
    ]
    if args.upsert:
        command.append("--upsert")
    if args.geom_column:
        command.extend(["--geom-column", args.geom_column])

    print("running-cli:", " ".join(command))
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def run_python_api(args: argparse.Namespace) -> None:
    print("running-python-api: freestile_postgis(...)")
    result = freestile_postgis(
        normalize_postgis(args.postgis),
        args.sql,
        {
            "uri": normalize_mongo(args.mongo),
            "database": args.mongo_db,
            "collection": args.python_collection,
            "batch_size": args.mongo_batch_size,
            "create_indexes": args.create_indexes,
        },
        layer_name=args.layer_name,
        quiet=False,
        batch_size=args.batch_size,
        upsert=args.upsert,
        geom_column=args.geom_column,
        streaming=args.streaming,
        mongo_profile=args.mongo_profile,
    )
    print("python-api-result:", result)


def load_docs(collection) -> dict[str, tuple[int, int, int, bytes]]:
    docs: dict[str, tuple[int, int, int, bytes]] = {}
    cursor = collection.find({}, {"_id": 0, "id": 1, "x": 1, "y": 1, "z": 1, "data": 1})
    for doc in cursor:
        docs[doc["id"]] = (doc["z"], doc["x"], doc["y"], bytes(doc["data"]))
    return docs


def compare_outputs(args: argparse.Namespace) -> None:
    client = MongoClient(normalize_mongo(args.mongo))
    db = client[args.mongo_db]
    cli_collection = db[args.cli_collection]
    python_collection = db[args.python_collection]

    cli_docs = load_docs(cli_collection)
    python_docs = load_docs(python_collection)

    if len(cli_docs) != len(python_docs):
        raise SystemExit(
            f"count-mismatch: cli={len(cli_docs)} python={len(python_docs)}"
        )

    cli_ids = set(cli_docs)
    python_ids = set(python_docs)
    if cli_ids != python_ids:
        missing_in_python = sorted(cli_ids - python_ids)[:5]
        missing_in_cli = sorted(python_ids - cli_ids)[:5]
        raise SystemExit(
            "id-set-mismatch: "
            f"missing_in_python={missing_in_python} missing_in_cli={missing_in_cli}"
        )

    for tile_id in sorted(cli_ids):
        if cli_docs[tile_id] != python_docs[tile_id]:
            raise SystemExit(
                f"tile-mismatch: id={tile_id} cli={cli_docs[tile_id]} python={python_docs[tile_id]}"
            )

    print(f"parity-ok: {len(cli_docs)} tiles matched")

    if args.cleanup:
        cli_collection.drop()
        python_collection.drop()
        print("cleanup-ok")


def main() -> int:
    args = parse_args()
    client = MongoClient(normalize_mongo(args.mongo))
    db = client[args.mongo_db]
    db[args.cli_collection].drop()
    db[args.python_collection].drop()

    run_cli(args)
    run_python_api(args)
    compare_outputs(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
