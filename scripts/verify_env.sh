#!/usr/bin/env bash
# Read-only environment check. Does not start Hadoop services or submit jobs.

set -u

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"

section() {
    printf '\n=== %s ===\n' "$1"
}

section "C++ compiler"
if command -v g++ >/dev/null 2>&1; then
    echo "path    : $(command -v g++)"
    g++ --version | head -n 1
else
    echo "ERROR: g++ not found"
fi
if ls /usr/lib/gcc/*/*/libstdc++.a >/dev/null 2>&1; then
    echo "static  : libstdc++.a available"
else
    echo "static  : libstdc++.a NOT found (static linking may fail)"
fi
echo "glibc   : $(ldd --version 2>/dev/null | head -n 1)"

section "C++ job binaries"
for binary in \
    cpp/bin/delivery_status_mapper \
    cpp/bin/delivery_status_reducer \
    cpp/bin/shipping_performance_mapper \
    cpp/bin/shipping_performance_reducer \
    cpp/bin/category_demand_mapper \
    cpp/bin/category_demand_reducer; do
    if [ -x "$binary" ]; then
        echo "OK      : $binary"
    else
        echo "MISSING : $binary (run bash scripts/build_cpp.sh)"
    fi
done

section "Java"
echo "JAVA_HOME=${JAVA_HOME:-<unset>}"
if command -v java >/dev/null 2>&1; then
    java -version 2>&1 | head -n 2
fi
for candidate in /usr/lib/jvm/java-8-openjdk /usr/lib/jvm/java-11-openjdk; do
    if [ -x "$candidate/bin/java" ]; then
        echo "candidate: $candidate -> $("$candidate/bin/java" -version 2>&1 | head -n 1)"
    fi
done

section "Hadoop"
echo "HADOOP_HOME=${HADOOP_HOME:-<unset>}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-<unset>}"
if command -v hadoop >/dev/null 2>&1; then
    hadoop version 2>&1 | head -n 2
else
    echo "ERROR: hadoop not found on PATH"
fi

STREAMING_JAR="$(find "$HADOOP_HOME" -type f -name 'hadoop-streaming-*.jar' \
    ! -name '*sources*' ! -name '*tests*' 2>/dev/null | sort -V | tail -n 1)"
if [ -n "$STREAMING_JAR" ]; then
    echo "Streaming JAR: $STREAMING_JAR"
    ls -lh "$STREAMING_JAR"
else
    echo "ERROR: no hadoop-streaming-*.jar found under $HADOOP_HOME"
fi

section "Hadoop services"
if command -v jps >/dev/null 2>&1; then
    jps -l 2>/dev/null
else
    echo "jps not found"
fi

if command -v hdfs >/dev/null 2>&1; then
    if timeout 10 hdfs dfs -ls / >/dev/null 2>&1; then
        echo "HDFS: reachable"
    else
        echo "HDFS: NOT reachable (run: bash scripts/start_hadoop.sh)"
    fi
fi

if [ -x "$HADOOP_HOME/bin/yarn" ]; then
    YARN_BIN="$HADOOP_HOME/bin/yarn"
elif command -v yarn >/dev/null 2>&1; then
    YARN_BIN="$(command -v yarn)"
else
    YARN_BIN=""
fi

if [ -n "$YARN_BIN" ]; then
    if timeout 10 "$YARN_BIN" node -list >/dev/null 2>&1; then
        echo "YARN: reachable via $YARN_BIN"
        "$YARN_BIN" node -list 2>/dev/null | head -n 5
    else
        echo "YARN: NOT reachable (run: bash scripts/start_hadoop.sh)"
    fi
else
    echo "YARN: command not found"
fi

section "Data"
DATA="$ROOT_DIR/data/DataCoSupplyChainDataset.csv"
if [ -f "$DATA" ]; then
    ls -lh "$DATA"
    echo "physical lines: $(wc -l < "$DATA")"
    if command -v python3 >/dev/null 2>&1; then
        python3 "$ROOT_DIR/scripts/check_schema.py" "$DATA" 2>&1 || true
        python3 "$ROOT_DIR/scripts/check_job_schema.py" shipping-performance "$DATA" 2>&1 || true
        python3 "$ROOT_DIR/scripts/check_job_schema.py" category-demand "$DATA" 2>&1 || true
    else
        echo "python3 not found; schema checks skipped"
    fi
else
    echo "missing: $DATA"
fi

section "Resources"
echo "cores: $(nproc)"
free -h 2>/dev/null | head -n 2
df -h "$ROOT_DIR" 2>/dev/null | tail -n 1

section "Note"
echo "This was a read-only check; no Hadoop service was started."
