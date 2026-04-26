"""Benchmark PostGIS -> MongoDB profile runs and collect collection statistics."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from pymongo import MongoClient


REPO_ROOT = Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run CLI profile benchmarks for PostGIS -> MongoDB."
    )
    parser.add_argument("--postgis", required=True)
    parser.add_argument("--sql", required=True)
    parser.add_argument("--mongo", required=True)
    parser.add_argument("--mongo-db", required=True)
    parser.add_argument("--collection-prefix", required=True)
    parser.add_argument("--layer-name", default="default")
    parser.add_argument(
        "--profiles",
        nargs="+",
        default=["recommended", "safe", "high_detail"],
    )
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--mongo-batch-size", type=int, default=4_096)
    parser.add_argument("--streaming", action="store_true", default=False)
    parser.add_argument("--create-indexes", action="store_true", default=False)
    parser.add_argument("--upsert", action="store_true", default=False)
    parser.add_argument("--geom-column")
    parser.add_argument("--cleanup", action="store_true", default=False)
    parser.add_argument("--output-json")
    return parser.parse_args()


def normalize_mongo(value: str) -> str:
    if "://" in value:
        return value
    host, port = value.split(":")
    return f"mongodb://{host}:{port}"


def profile_zoom_range(profile: str) -> tuple[int, int]:
    if profile == "recommended":
        return 10, 12
    if profile == "safe":
        return 6, 12
    if profile == "high_detail":
        return 14, 15
    raise ValueError(f"unsupported profile: {profile}")


def run_profile(args: argparse.Namespace, profile: str) -> dict[str, object]:
    collection = f"{args.collection_prefix}_{profile}"
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
        collection,
        "--layer-name",
        args.layer_name,
        "--mongo-profile",
        profile,
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

    client = MongoClient(normalize_mongo(args.mongo))
    db = client[args.mongo_db]
    db[collection].drop()

    started_at = datetime.now().isoformat(timespec="seconds")
    start = time.perf_counter()
    proc = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    elapsed_seconds = round(time.perf_counter() - start, 2)

    result: dict[str, object] = {
        "profile": profile,
        "zoom_range": profile_zoom_range(profile),
        "collection": collection,
        "started_at": started_at,
        "elapsed_seconds": elapsed_seconds,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }

    if proc.returncode != 0:
        return result

    coll = db[collection]
    stats = db.command("collStats", collection)
    pipeline = [
        {
            "$group": {
                "_id": "$z",
                "count": {"$sum": 1},
                "max_data_size": {"$max": {"$binarySize": "$data"}},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    per_zoom = list(coll.aggregate(pipeline))
    max_doc = coll.aggregate(
        [
            {
                "$project": {
                    "_id": 0,
                    "id": 1,
                    "z": 1,
                    "x": 1,
                    "y": 1,
                    "data_size": {"$binarySize": "$data"},
                }
            },
            {"$sort": {"data_size": -1}},
            {"$limit": 1},
        ]
    )
    max_doc_list = list(max_doc)

    result.update(
        {
            "document_count": coll.count_documents({}),
            "storage_size": stats.get("storageSize"),
            "size": stats.get("size"),
            "total_index_size": stats.get("totalIndexSize"),
            "avg_obj_size": stats.get("avgObjSize"),
            "per_zoom": per_zoom,
            "max_data_doc": max_doc_list[0] if max_doc_list else None,
        }
    )

    if args.cleanup:
        coll.drop()

    return result


def main() -> int:
    args = parse_args()
    results = [run_profile(args, profile) for profile in args.profiles]

    output = {
        "sql": args.sql,
        "streaming": args.streaming,
        "create_indexes": args.create_indexes,
        "batch_size": args.batch_size,
        "mongo_batch_size": args.mongo_batch_size,
        "results": results,
    }

    text = json.dumps(output, ensure_ascii=False, indent=2)
    print(text)

    if args.output_json:
        Path(args.output_json).write_text(text, encoding="utf-8")

    failures = [item for item in results if item["returncode"] != 0]
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
