#!/usr/bin/env bash
# Run the mapper/reducer locally, imitating Hadoop's shuffle with sort.
#
# Usage:
#   bash scripts/run_local_test.sh            # small synthetic sample
#   bash scripts/run_local_test.sh --real     # full DataCo dataset (slower)

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

CASE="${1:-sample}"
case "$CASE" in
    --sample|sample|"")
        INPUT="tests/input/deliveries_sample.csv"
        EXPECTED="tests/expected/delivery_counts.tsv"
        LABEL="synthetic sample"
        ;;
    --real|real)
        INPUT="data/DataCoSupplyChainDataset.csv"
        EXPECTED="tests/expected/real_dataset_counts.tsv"
        LABEL="full DataCo dataset"
        ;;
    *)
        echo "usage: bash scripts/run_local_test.sh [--sample|--real]" >&2
        exit 2
        ;;
esac

for binary in cpp/bin/delivery_status_mapper cpp/bin/delivery_status_reducer; do
    if [ ! -x "$binary" ]; then
        echo "ERROR: $binary is missing or not executable." >&2
        echo "Run: bash scripts/build_cpp.sh" >&2
        exit 2
    fi
done

if [ ! -f "$INPUT" ]; then
    echo "ERROR: input file not found: $INPUT" >&2
    exit 2
fi

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/bda-local-test.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

# Hadoop sorts map output by key before the reducer sees it. LC_ALL=C matches
# Hadoop's byte-wise ordering.
LC_ALL=C cpp/bin/delivery_status_mapper < "$INPUT" \
    | LC_ALL=C sort \
    | cpp/bin/delivery_status_reducer > "$TMP_DIR/actual.tsv"

LC_ALL=C sort "$EXPECTED" > "$TMP_DIR/expected.tsv"

if diff -u "$TMP_DIR/expected.tsv" "$TMP_DIR/actual.tsv"; then
    echo "PASS: $LABEL"
    echo "--- output ---"
    cat "$TMP_DIR/actual.tsv"
else
    echo "FAIL: $LABEL (see diff above)" >&2
    exit 1
fi
