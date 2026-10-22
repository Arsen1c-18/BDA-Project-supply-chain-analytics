#!/usr/bin/env bash
# Run a new C++ Streaming mapper/reducer locally, with the same key ordering
# that Hadoop uses (bytewise sort on the first tab-separated field).
#
# Usage:
#   bash scripts/run_local_analysis_test.sh <job> [--sample|--real]
#
# Jobs: shipping-performance, category-demand
# Default: --sample

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

JOB="${1:-}"
DATASET="${2:---sample}"
if [ "$DATASET" = "sample" ]; then
    DATASET="--sample"
elif [ "$DATASET" = "real" ]; then
    DATASET="--real"
fi

case "$JOB" in
    shipping-performance)
        MAPPER="cpp/bin/shipping_performance_mapper"
        REDUCER="cpp/bin/shipping_performance_reducer"
        ;;
    category-demand)
        MAPPER="cpp/bin/category_demand_mapper"
        REDUCER="cpp/bin/category_demand_reducer"
        ;;
    *)
        echo "Usage: bash scripts/run_local_analysis_test.sh <shipping-performance|category-demand> [--sample|--real]" >&2
        exit 2
        ;;
esac

case "$DATASET" in
    --sample)
        INPUT="tests/input/${JOB//-/_}_sample.csv"
        EXPECTED="tests/expected/${JOB//-/_}_sample.tsv"
        ;;
    --real)
        INPUT="data/DataCoSupplyChainDataset.csv"
        EXPECTED="tests/expected/real_${JOB//-/_}.tsv"
        ;;
    *)
        echo "ERROR: dataset must be --sample or --real" >&2
        exit 2
        ;;
esac

if [ ! -x "$MAPPER" ] || [ ! -x "$REDUCER" ]; then
    echo "ERROR: missing binaries; run: bash scripts/build_cpp.sh" >&2
    exit 2
fi
if [ ! -f "$INPUT" ]; then
    echo "ERROR: input not found: $INPUT" >&2
    exit 2
fi

TMP_DIR="$(mktemp -d /tmp/opencode/bda-${JOB}.XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT

"$MAPPER" < "$INPUT" 2> "$TMP_DIR/mapper.err" \
    | LC_ALL=C sort -t $'\t' -k1,1 \
    | "$REDUCER" > "$TMP_DIR/actual.tsv" \
    2> "$TMP_DIR/reducer.err"
LC_ALL=C sort -t $'\t' -k1,1 "$EXPECTED" > "$TMP_DIR/expected.tsv"

if ! diff -u "$TMP_DIR/expected.tsv" "$TMP_DIR/actual.tsv"; then
    echo "FAILED: local $JOB output differs from $EXPECTED" >&2
    exit 1
fi

if [ "$DATASET" = "--sample" ] && ! grep -q "malformed=1" "$TMP_DIR/mapper.err"; then
    cat "$TMP_DIR/mapper.err" >&2
    echo "FAILED: sample malformed-row diagnostic was not emitted" >&2
    exit 1
fi

if [ "$DATASET" = "--sample" ]; then
    # Simulate the non-first HDFS split: no header, all positions supplied via
    # the same environment variables used by -cmdenv.
    SCHEMA_OUTPUT="$(python3 scripts/check_job_schema.py "$JOB" "$INPUT")"
    read -r -a COLS <<< "$SCHEMA_OUTPUT"
    if [ "$JOB" = "shipping-performance" ]; then
        ENV_ARGS=(
            "BDA_SHIPPING_MODE_COL=${COLS[0]}"
            "BDA_ORDER_ID_COL=${COLS[1]}"
            "BDA_STATUS_COL=${COLS[2]}"
            "BDA_ACTUAL_DAYS_COL=${COLS[3]}"
            "BDA_SCHEDULED_DAYS_COL=${COLS[4]}"
            "BDA_SALES_COL=${COLS[5]}"
        )
    else
        ENV_ARGS=(
            "BDA_CATEGORY_COL=${COLS[0]}"
            "BDA_ORDER_ID_COL=${COLS[1]}"
            "BDA_QUANTITY_COL=${COLS[2]}"
            "BDA_SALES_COL=${COLS[3]}"
            "BDA_STATUS_COL=${COLS[4]}"
        )
    fi
    tail -n +2 "$INPUT" \
        | env "${ENV_ARGS[@]}" "$MAPPER" 2> "$TMP_DIR/headerless.mapper.err" \
        | LC_ALL=C sort -t $'\t' -k1,1 \
        | "$REDUCER" > "$TMP_DIR/headerless.tsv" 2> "$TMP_DIR/headerless.reducer.err"
    if ! diff -u "$TMP_DIR/expected.tsv" "$TMP_DIR/headerless.tsv"; then
        echo "FAILED: $JOB headerless environment-column path differs from $EXPECTED" >&2
        exit 1
    fi
    if ! grep -q "malformed=1" "$TMP_DIR/headerless.mapper.err"; then
        cat "$TMP_DIR/headerless.mapper.err" >&2
        echo "FAILED: headerless malformed-row diagnostic was not emitted" >&2
        exit 1
    fi
    echo "PASS: $JOB headerless -cmdenv-style path"
fi

cat "$TMP_DIR/mapper.err" >&2
cat "$TMP_DIR/reducer.err" >&2
echo "PASS: $JOB ($DATASET) matches $EXPECTED"
echo "Output rows: $(wc -l < "$TMP_DIR/actual.tsv")"
