#!/usr/bin/env bash
# Submit the C++ mapper/reducer through Hadoop Streaming to the existing
# pseudo-distributed HDFS/YARN installation.
#
# Usage:
#   bash scripts/run_streaming_job.sh [input.csv] [run-id]
#
# Defaults:
#   input  = data/DataCoSupplyChainDataset.csv
#   run-id = UTC timestamp
#
# The script refuses to reuse an existing HDFS input/output directory or local
# log/result file. It never deletes or overwrites existing data.

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
INPUT="$(cd -- "$(dirname -- "$INPUT")" && pwd)/$(basename -- "$INPUT")"
BASENAME="$(basename -- "$INPUT")"

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_HOME
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_CONF_DIR}"

if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/lib/jvm/java-8-openjdk/bin/java ]; then
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
fi

if ! command -v hadoop >/dev/null 2>&1; then
    echo "ERROR: hadoop is not on PATH." >&2
    exit 2
fi
if ! command -v hdfs >/dev/null 2>&1; then
    echo "ERROR: hdfs is not on PATH." >&2
    exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 is required for the schema check." >&2
    exit 2
fi

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

for binary in cpp/bin/delivery_status_mapper cpp/bin/delivery_status_reducer; do
    if [ ! -x "$binary" ]; then
        echo "ERROR: $binary is missing or not executable." >&2
        echo "Run: bash scripts/build_cpp.sh" >&2
        exit 2
    fi
done

# Resolve the schema from the real header. The positions are passed to every
# mapper because only the split starting at byte 0 sees the header.
SCHEMA_OUTPUT="$(python3 "$ROOT_DIR/scripts/check_schema.py" "$INPUT")" || {
    echo "ERROR: schema validation failed; refusing to submit the job." >&2
    exit 2
}
read -r STATUS_COL ORDER_COL MODE_COL <<< "$SCHEMA_OUTPUT"

HDFS_USER="${HDFS_USER:-$(id -un)}"
HDFS_BASE="${BDA_HDFS_BASE:-/user/$HDFS_USER/bda/streaming}"
INPUT_DIR="$HDFS_BASE/input/$RUN_ID"
OUTPUT_DIR="$HDFS_BASE/output/$RUN_ID"

if hdfs dfs -test -e "$INPUT_DIR"; then
    echo "ERROR: HDFS input directory already exists: $INPUT_DIR" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi
if hdfs dfs -test -e "$OUTPUT_DIR"; then
    echo "ERROR: HDFS output directory already exists: $OUTPUT_DIR" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi

mkdir -p "$ROOT_DIR/logs"
LOG="$ROOT_DIR/logs/streaming_${RUN_ID}.log"
if [ -e "$LOG" ]; then
    echo "ERROR: local log already exists: $LOG" >&2
    echo "Refusing to overwrite it. Choose another run-id." >&2
    exit 4
fi

echo "Input        : $INPUT"
echo "Streaming JAR: $STREAMING_JAR"
echo "HDFS input   : $INPUT_DIR"
echo "HDFS output  : $OUTPUT_DIR"
echo "Schema cols  : status=$STATUS_COL order_id=$ORDER_COL shipping_mode=$MODE_COL"
echo

echo "Creating HDFS input directory and uploading the file ..."
hdfs dfs -mkdir -p "$INPUT_DIR"
hdfs dfs -put "$INPUT" "$INPUT_DIR/$BASENAME"

if ! hdfs dfs -test -e "$INPUT_DIR/$BASENAME"; then
    echo "ERROR: HDFS upload was not confirmed: $INPUT_DIR/$BASENAME" >&2
    exit 5
fi
echo "Upload complete."
echo

echo "Submitting Hadoop Streaming job ..."
set +e
hadoop jar "$STREAMING_JAR" \
    -D "mapreduce.job.name=bda-delivery-status-$RUN_ID" \
    -D "mapreduce.input.fileinputformat.split.maxsize=33554432" \
    -D "mapreduce.job.reduces=1" \
    -files "$ROOT_DIR/cpp/bin/delivery_status_mapper,$ROOT_DIR/cpp/bin/delivery_status_reducer" \
    -cmdenv "BDA_STATUS_COL=$STATUS_COL" \
    -cmdenv "BDA_ORDER_ID_COL=$ORDER_COL" \
    -cmdenv "BDA_SHIPPING_MODE_COL=$MODE_COL" \
    -mapper ./delivery_status_mapper \
    -reducer ./delivery_status_reducer \
    -input "$INPUT_DIR" \
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

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/bda-streaming-result.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

if ! hdfs dfs -cat "$OUTPUT_DIR/part-*" > "$TMP_DIR/actual.tsv"; then
    echo "ERROR: could not read job output from $OUTPUT_DIR" >&2
    exit 6
fi
LC_ALL=C sort -o "$TMP_DIR/actual.tsv" "$TMP_DIR/actual.tsv"

echo
echo "HDFS output: $OUTPUT_DIR"
echo "--- output rows ---"
cat "$TMP_DIR/actual.tsv"

EXPECTED=""
if [ "$BASENAME" = "DataCoSupplyChainDataset.csv" ]; then
    EXPECTED="$ROOT_DIR/tests/expected/real_dataset_counts.tsv"
fi

if [ -n "$EXPECTED" ] && [ -f "$EXPECTED" ]; then
    LC_ALL=C sort "$EXPECTED" > "$TMP_DIR/expected.tsv"
    if diff -u "$TMP_DIR/expected.tsv" "$TMP_DIR/actual.tsv"; then
        echo "VERIFIED: output matches $EXPECTED"
    else
        echo "ERROR: output does not match $EXPECTED" >&2
        exit 7
    fi
fi

mkdir -p "$ROOT_DIR/results"
RESULT_FILE="$ROOT_DIR/results/${RUN_ID}.tsv"
if [ -e "$RESULT_FILE" ]; then
    echo "Local result file already exists; not overwriting: $RESULT_FILE"
else
    cp "$TMP_DIR/actual.tsv" "$RESULT_FILE"
    echo "Local result: $RESULT_FILE"
fi

echo
echo "Streaming job completed through YARN."
