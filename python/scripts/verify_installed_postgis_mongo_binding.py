"""Verify that the installed Python binding exposes the Mongo profile API."""

from __future__ import annotations

import importlib
import inspect
import sys


def require_contains(text: str, needle: str, label: str) -> None:
    if needle not in text:
        raise SystemExit(f"{label} is missing '{needle}': {text}")


def main() -> int:
    freestiler = importlib.import_module("freestiler")
    extension = importlib.import_module("freestiler._freestiler")

    public_signature = str(inspect.signature(freestiler.freestile_postgis))
    private_signature = extension._freestile_postgis_to_mongo.__text_signature__ or ""

    require_contains(public_signature, "mongo_profile", "freestile_postgis signature")
    require_contains(
        private_signature,
        "mongo_profile=None",
        "_freestile_postgis_to_mongo signature",
    )

    print("binding-check-ok")
    print(public_signature)
    print(private_signature)
    return 0


if __name__ == "__main__":
    sys.exit(main())
