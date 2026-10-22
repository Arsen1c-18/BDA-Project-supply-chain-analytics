#!/usr/bin/env python3
"""Validate the CSV header and print column positions for the Hadoop job.

Stdout (three integers, space separated):
    <status_col> <order_id_col> <shipping_mode_col>

The values are 0-based, matching the C++ mapper. Diagnostics go to stderr.
"""

from __future__ import annotations

import csv
import os
import sys

REQUIRED = {
    "status": ("deliverystatus",),
    "order_id": ("orderid",),
    "shipping_mode": ("shippingmode",),
}


def normalize(name: str) -> str:
    return "".join(ch.lower() for ch in name if ch.isalnum())


def read_header(path: str) -> list[str]:
    # The DataCo file is ISO-8859-1; utf-8-sig is tried first so a BOM is
    # removed cleanly for files that do have one.
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            with open(path, "r", newline="", encoding=encoding) as handle:
                return next(csv.reader(handle))
        except UnicodeDecodeError:
            continue
    raise RuntimeError("could not decode the CSV header")


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {os.path.basename(sys.argv[0])} <csv-file>", file=sys.stderr)
        return 2

    path = sys.argv[1]
    if not os.path.isfile(path):
        print(f"ERROR: input file not found: {path}", file=sys.stderr)
        return 2

    try:
        header = read_header(path)
    except Exception as exc:  # noqa: BLE001 - report the real cause to the user
        print(f"ERROR: failed to read CSV header: {exc}", file=sys.stderr)
        return 2

    normalized = [normalize(name) for name in header]
    positions: dict[str, int] = {}
    for key, aliases in REQUIRED.items():
        for index, name in enumerate(normalized):
            if name in aliases:
                positions[key] = index
                break
        if key not in positions:
            print(
                f"ERROR: required column '{key}' is missing from the header",
                file=sys.stderr,
            )
            print(f"Header: {header}", file=sys.stderr)
            return 2

    if len(header) <= max(positions.values()):
        print("ERROR: header is shorter than the resolved columns", file=sys.stderr)
        return 2

    print(
        "schema OK: "
        f"{len(header)} columns; "
        f"status={positions['status']} "
        f"order_id={positions['order_id']} "
        f"shipping_mode={positions['shipping_mode']}",
        file=sys.stderr,
    )
    print(
        positions["status"],
        positions["order_id"],
        positions["shipping_mode"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
