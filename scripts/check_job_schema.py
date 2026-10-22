#!/usr/bin/env python3
"""Validate CSV headers for the expanded Hadoop Streaming jobs.

Usage:
    python3 scripts/check_job_schema.py <job> <input.csv>

The job-specific column positions are written to stdout as a single
space-separated line, in the order consumed by the corresponding submit script.
Diagnostics go to stderr so they can never become mapper output.
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

JOBS = {
    "shipping-performance": [
        ("shipping_mode", ("shippingmode",)),
        ("order_id", ("orderid",)),
        ("status", ("deliverystatus",)),
        ("actual_days", ("daysforshippingreal",)),
        ("scheduled_days", ("daysforshipmentscheduled",)),
        ("sales", ("sales",)),
    ],
    "category-demand": [
        ("category", ("categoryname",)),
        ("order_id", ("orderid",)),
        ("quantity", ("orderitemquantity",)),
        ("sales", ("sales",)),
        ("status", ("deliverystatus",)),
    ],
}


def normalize(value: str) -> str:
    return "".join(character.lower() for character in value if character.isalnum())


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="latin-1", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV is empty") from None
    if header:
        header[0] = header[0].lstrip("\ufeff")
    return header


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(
            "Usage: python3 scripts/check_job_schema.py "
            "<shipping-performance|category-demand> <input.csv>",
            file=sys.stderr,
        )
        return 2

    job = argv[1]
    if job not in JOBS:
        print(f"ERROR: unknown job: {job}", file=sys.stderr)
        return 2

    path = Path(argv[2])
    if not path.is_file():
        print(f"ERROR: input file not found: {path}", file=sys.stderr)
        return 2

    try:
        header = read_header(path)
    except (OSError, ValueError, csv.Error) as exc:
        print(f"ERROR: could not read header from {path}: {exc}", file=sys.stderr)
        return 2

    normalized = [normalize(field) for field in header]
    positions: list[int] = []
    missing: list[str] = []

    for label, aliases in JOBS[job]:
        position = next(
            (index for index, name in enumerate(normalized) if name in aliases),
            None,
        )
        if position is None:
            missing.append(f"{label} ({', '.join(aliases)})")
        else:
            positions.append(position)

    if missing:
        print(
            f"ERROR: {path.name} is missing required columns for {job}: "
            + "; ".join(missing),
            file=sys.stderr,
        )
        return 2

    print(
        f"Schema OK for {job}: {len(header)} columns; "
        + ", ".join(
            f"{label}={positions[index]}"
            for index, (label, _) in enumerate(JOBS[job])
        ),
        file=sys.stderr,
    )
    print(" ".join(str(position) for position in positions))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
