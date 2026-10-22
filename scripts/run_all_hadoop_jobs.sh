#!/usr/bin/env bash
# Run all three C++ Hadoop Streaming jobs for one run-id.
#
# Usage:
#   bash scripts/run_all_hadoop_jobs.sh [input.csv] [run-id]
#
# Job 1 writes the existing flat result (results/<run-id>.tsv); this wrapper
# also copies it into results/hadoop/<run-id>/delivery-mode.tsv so the notebook
# and dashboard can read one consistent result layout.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

INPUT="${1:-$ROOT_DIR/data/DataCoSupplyChainDataset.csv}"
RUN_ID="${2:-$(date -u +%Y%m%dT%H%M%SZ)}"

if ! [[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: run-id may contain only letters, digits, dot, underscore and dash." >&2
    exit 2
fi
if [ ! -f "$INPUT" ]; then
    echo "ERROR: input file not found: $INPUT" >&2
    exit 2
fi

RESULT_DIR="$ROOT_DIR/results/hadoop/$RUN_ID"
DELIVERY_RESULT="$RESULT_DIR/delivery-mode.tsv"
if [ -e "$DELIVERY_RESULT" ]; then
    echo "ERROR: delivery result already exists: $DELIVERY_RESULT" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi

echo "=== Job 1/3: delivery status ==="
bash "$ROOT_DIR/scripts/run_streaming_job.sh" "$INPUT" "$RUN_ID"

mkdir -p "$RESULT_DIR"
FLAT_RESULT="$ROOT_DIR/results/${RUN_ID}.tsv"
if [ ! -f "$FLAT_RESULT" ]; then
    echo "ERROR: expected delivery result was not written: $FLAT_RESULT" >&2
    exit 5
fi
cp "$FLAT_RESULT" "$DELIVERY_RESULT"
echo "Delivery result copied to: $DELIVERY_RESULT"
echo

echo "=== Job 2/3: shipping performance ==="
bash "$ROOT_DIR/scripts/run_analysis_job.sh" shipping-performance "$INPUT" "$RUN_ID"
echo

echo "=== Job 3/3: category demand ==="
bash "$ROOT_DIR/scripts/run_analysis_job.sh" category-demand "$INPUT" "$RUN_ID"

echo
echo "All three jobs completed through YARN for run-id: $RUN_ID"
echo "Notebook/dashboard result directory: $RESULT_DIR"
