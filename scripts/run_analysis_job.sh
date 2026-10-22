#!/usr/bin/env bash
# Submit one of the expanded C++ mapper/reducer pairs through Hadoop Streaming.
#
# Usage:
#   bash scripts/run_analysis_job.sh <shipping-performance|category-demand> \
#       [input.csv] [run-id]
#
# The input is uploaded once per run-id and reused by subsequent jobs that use
# the same run-id. Each job gets a separate HDFS output directory and a separate
# local result file. Existing data is never deleted or overwritten.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

JOB="${1:-}"
if [ -z "$JOB" ]; then
    echo "Usage: bash scripts/run_analysis_job.sh <shipping-performance|category-demand> [input.csv] [run-id]" >&2
    exit 2
fi
INPUT="${2:-$ROOT_DIR/data/DataCoSupplyChainDataset.csv}"
RUN_ID="${3:-$(date -u +%Y%m%dT%H%M%SZ)}"

case "$JOB" in
    shipping-performance)
        MAPPER="cpp/bin/shipping_performance_mapper"
        REDUCER="cpp/bin/shipping_performance_reducer"
        SCHEMA_JOB="shipping-performance"
        EXPECTED_REAL="tests/expected/real_shipping_performance.tsv"
        ;;
    category-demand)
        MAPPER="cpp/bin/category_demand_mapper"
        REDUCER="cpp/bin/category_demand_reducer"
        SCHEMA_JOB="category-demand"
        EXPECTED_REAL="tests/expected/real_category_demand.tsv"
        ;;
    *)
        echo "ERROR: unknown job: $JOB" >&2
        echo "Choose shipping-performance or category-demand." >&2
        exit 2
        ;;
esac

if ! [[ "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "ERROR: run-id may contain only letters, digits, dot, underscore and dash." >&2
    exit 2
fi
if [ ! -f "$INPUT" ]; then
    echo "ERROR: input file not found: $INPUT" >&2
    exit 2
fi
INPUT="$(cd -- "$(dirname -- "$INPUT")" && pwd)/$(basename -- "$INPUT")"
BASENAME="$(basename -- "$INPUT")"

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_HOME
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_CONF_DIR}"

if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/lib/jvm/java-8-openjdk/bin/java ]; then
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
fi

for command_name in hadoop hdfs python3; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "ERROR: $command_name is not on PATH." >&2
        exit 2
    fi
done

STREAMING_JAR="$(find "$HADOOP_HOME" -type f -name 'hadoop-streaming-*.jar' \
    ! -name '*sources*' ! -name '*tests*' 2>/dev/null | sort -V | tail -n 1)"
if [ -z "$STREAMING_JAR" ]; then
    echo "ERROR: no hadoop-streaming-*.jar found under $HADOOP_HOME." >&2
    exit 2
fi

if ! timeout 10 hdfs dfs -ls / >/dev/null 2>&1; then
    echo "ERROR: HDFS is not reachable." >&2
    echo "Start HDFS and YARN first (start-dfs.sh / start-yarn.sh)." >&2
    exit 3
fi

if [ ! -x "$MAPPER" ] || [ ! -x "$REDUCER" ]; then
    echo "ERROR: $MAPPER or $REDUCER is missing or not executable." >&2
    echo "Run: bash scripts/build_cpp.sh" >&2
    exit 2
fi

# Resolve header positions once. Every split receives the positions through
# -cmdenv because only the split starting at byte zero sees the header row.
SCHEMA_OUTPUT="$(python3 "$ROOT_DIR/scripts/check_job_schema.py" "$SCHEMA_JOB" "$INPUT")" || {
    echo "ERROR: schema validation failed; refusing to submit the job." >&2
    exit 2
}
read -r -a COLUMNS <<< "$SCHEMA_OUTPUT"

MAPPER_ENV=()
case "$JOB" in
    shipping-performance)
        if [ "${#COLUMNS[@]}" -ne 6 ]; then
            echo "ERROR: unexpected schema output for shipping-performance" >&2
            exit 2
        fi
        read -r SHIPPING_MODE_COL ORDER_ID_COL STATUS_COL ACTUAL_DAYS_COL \
            SCHEDULED_DAYS_COL SALES_COL <<< "$SCHEMA_OUTPUT"
        MAPPER_ENV=(
            "BDA_SHIPPING_MODE_COL=$SHIPPING_MODE_COL"
            "BDA_ORDER_ID_COL=$ORDER_ID_COL"
            "BDA_STATUS_COL=$STATUS_COL"
            "BDA_ACTUAL_DAYS_COL=$ACTUAL_DAYS_COL"
            "BDA_SCHEDULED_DAYS_COL=$SCHEDULED_DAYS_COL"
            "BDA_SALES_COL=$SALES_COL"
        )
        SCHEMA_LABEL="shipping_mode=$SHIPPING_MODE_COL order_id=$ORDER_ID_COL status=$STATUS_COL actual_days=$ACTUAL_DAYS_COL scheduled_days=$SCHEDULED_DAYS_COL sales=$SALES_COL"
        ;;
    category-demand)
        if [ "${#COLUMNS[@]}" -ne 5 ]; then
            echo "ERROR: unexpected schema output for category-demand" >&2
            exit 2
        fi
        read -r CATEGORY_COL ORDER_ID_COL QUANTITY_COL SALES_COL STATUS_COL \
            <<< "$SCHEMA_OUTPUT"
        MAPPER_ENV=(
            "BDA_CATEGORY_COL=$CATEGORY_COL"
            "BDA_ORDER_ID_COL=$ORDER_ID_COL"
            "BDA_QUANTITY_COL=$QUANTITY_COL"
            "BDA_SALES_COL=$SALES_COL"
            "BDA_STATUS_COL=$STATUS_COL"
        )
        SCHEMA_LABEL="category=$CATEGORY_COL order_id=$ORDER_ID_COL quantity=$QUANTITY_COL sales=$SALES_COL status=$STATUS_COL"
        ;;
esac

HDFS_USER="${HDFS_USER:-$(id -un)}"
HDFS_BASE="${BDA_HDFS_ANALYTICS_BASE:-/user/$HDFS_USER/bda/analytics}"
INPUT_DIR="$HDFS_BASE/input/$RUN_ID"
HDFS_INPUT_FILE="$INPUT_DIR/$BASENAME"
OUTPUT_DIR="$HDFS_BASE/output/$RUN_ID/$JOB"
LOG="$ROOT_DIR/logs/${JOB}_${RUN_ID}.log"
RESULT_DIR="$ROOT_DIR/results/hadoop/$RUN_ID"
RESULT_FILE="$RESULT_DIR/${JOB}.tsv"

if hdfs dfs -test -e "$OUTPUT_DIR"; then
    echo "ERROR: HDFS output directory already exists: $OUTPUT_DIR" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi
if [ -e "$LOG" ]; then
    echo "ERROR: local log already exists: $LOG" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi
if [ -e "$RESULT_FILE" ]; then
    echo "ERROR: local result already exists: $RESULT_FILE" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi

mkdir -p "$ROOT_DIR/logs" "$RESULT_DIR"

echo "Job          : $JOB"
echo "Input        : $INPUT"
echo "Streaming JAR: $STREAMING_JAR"
echo "HDFS input   : $HDFS_INPUT_FILE"
echo "HDFS output  : $OUTPUT_DIR"
echo "Schema cols  : $SCHEMA_LABEL"
echo

if hdfs dfs -test -e "$HDFS_INPUT_FILE"; then
    echo "Reusing existing HDFS input for run-id $RUN_ID."
else
    hdfs dfs -mkdir -p "$INPUT_DIR"
    hdfs dfs -put "$INPUT" "$HDFS_INPUT_FILE"
fi

if ! hdfs dfs -test -e "$HDFS_INPUT_FILE"; then
    echo "ERROR: HDFS upload was not confirmed: $HDFS_INPUT_FILE" >&2
    exit 5
fi
echo "HDFS input ready."
echo

echo "Submitting Hadoop Streaming job ..."
CMD_ENV=()
for assignment in "${MAPPER_ENV[@]}"; do
    CMD_ENV+=(-cmdenv "$assignment")
done

set +e
hadoop jar "$STREAMING_JAR" \
    -D "mapreduce.job.name=bda-${JOB}-${RUN_ID}" \
    -D "mapreduce.input.fileinputformat.split.maxsize=33554432" \
    -D "mapreduce.job.reduces=1" \
    -files "$ROOT_DIR/$MAPPER,$ROOT_DIR/$REDUCER" \
    "${CMD_ENV[@]}" \
    -mapper "./$(basename -- "$MAPPER")" \
    -reducer "./$(basename -- "$REDUCER")" \
    -input "$HDFS_INPUT_FILE" \
    -output "$OUTPUT_DIR" 2>&1 | tee "$LOG"
JOB_STATUS=${PIPESTATUS[0]}
set -e

if [ "$JOB_STATUS" -ne 0 ]; then
    echo
    echo "ERROR: Hadoop Streaming job failed (exit $JOB_STATUS)." >&2
    echo "Log: $LOG" >&2
    echo "No distributed result is claimed." >&2
    exit "$JOB_STATUS"
fi

echo
echo "=== Job result ==="
JOB_ID="$(grep -m1 -oE 'job_[0-9_]+' "$LOG" || true)"
if [ -n "$JOB_ID" ]; then
    echo "Job ID: $JOB_ID"
fi
grep -E 'Launched map tasks|Launched reduce tasks|completed successfully' "$LOG" || true
echo "Log: $LOG"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/bda-analysis-result.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

if ! hdfs dfs -cat "$OUTPUT_DIR/part-*" > "$TMP_DIR/actual.tsv"; then
    echo "ERROR: could not read job output from $OUTPUT_DIR" >&2
    exit 6
fi
LC_ALL=C sort -o "$TMP_DIR/actual.tsv" "$TMP_DIR/actual.tsv"

cp "$TMP_DIR/actual.tsv" "$RESULT_FILE"
echo "HDFS output: $OUTPUT_DIR"
echo "Local result: $RESULT_FILE"
echo "--- output rows ---"
cat "$TMP_DIR/actual.tsv"

EXPECTED=""
if [ "$BASENAME" = "DataCoSupplyChainDataset.csv" ]; then
    EXPECTED="$ROOT_DIR/$EXPECTED_REAL"
fi

if [ -n "$EXPECTED" ] && [ -f "$EXPECTED" ]; then
    LC_ALL=C sort -t $'\t' -k1,1 "$EXPECTED" > "$TMP_DIR/expected.tsv"
    if diff -u "$TMP_DIR/expected.tsv" "$TMP_DIR/actual.tsv"; then
        echo "VERIFIED: output matches $EXPECTED"
    else
        echo "ERROR: output does not match $EXPECTED" >&2
        exit 7
    fi
else
    echo "No expected fixture for this input; output was saved without comparison."
fi

echo
echo "Analysis job completed through YARN: $JOB"
