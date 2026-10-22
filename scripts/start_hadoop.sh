#!/usr/bin/env bash
# Start the existing pseudo-distributed HDFS and YARN services.
# It is idempotent: already-running daemons are left alone.
# It never formats the NameNode (formatting is destructive).

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_HOME
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export YARN_CONF_DIR="${YARN_CONF_DIR:-$HADOOP_CONF_DIR}"

if [ ! -x "$HADOOP_HOME/bin/hdfs" ]; then
    echo "ERROR: $HADOOP_HOME/bin/hdfs is missing." >&2
    exit 1
fi

if [ -z "${JAVA_HOME:-}" ] && [ -x /usr/lib/jvm/java-8-openjdk/bin/java ]; then
    export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
fi

echo "HADOOP_HOME     = $HADOOP_HOME"
echo "HADOOP_CONF_DIR = $HADOOP_CONF_DIR"
echo "JAVA_HOME       = ${JAVA_HOME:-<unset>}"
echo

# Refuse to continue if the NameNode storage has never been formatted. The
# script must not run `hdfs namenode -format` because that can destroy data.
NN_DIR="$(hdfs getconf -confKey dfs.namenode.name.dir 2>/dev/null || true)"
NN_DIR="${NN_DIR%%,*}"
NN_DIR="${NN_DIR#file://}"
NN_DIR="${NN_DIR#file:/}"
if [ -z "$NN_DIR" ]; then
    NN_DIR="$HADOOP_HOME/data/namenode"
fi
if [ ! -f "$NN_DIR/current/VERSION" ]; then
    echo "ERROR: NameNode storage is not formatted: $NN_DIR/current/VERSION" >&2
    echo "Refusing to format automatically (it is destructive)." >&2
    exit 2
fi

is_running() {
    jps -l 2>/dev/null | grep -q "$1"
}

if is_running 'org.apache.hadoop.hdfs.server.namenode.NameNode' &&
   is_running 'org.apache.hadoop.hdfs.server.datanode.DataNode'; then
    echo "HDFS daemons already running; skipping start-dfs.sh."
else
    echo "Starting HDFS ..."
    "$HADOOP_HOME/sbin/start-dfs.sh"
fi

HDFS_READY=0
for _ in $(seq 1 60); do
    if hdfs dfs -ls / >/dev/null 2>&1; then
        HDFS_READY=1
        break
    fi
    sleep 1
done
if [ "$HDFS_READY" -ne 1 ]; then
    echo "ERROR: HDFS did not become ready. Inspect $HADOOP_HOME/logs." >&2
    exit 3
fi
echo "HDFS is ready."

if is_running 'org.apache.hadoop.yarn.server.resourcemanager.ResourceManager' &&
   is_running 'org.apache.hadoop.yarn.server.nodemanager.NodeManager'; then
    echo "YARN daemons already running; skipping start-yarn.sh."
else
    echo "Starting YARN ..."
    "$HADOOP_HOME/sbin/start-yarn.sh"
fi

YARN_READY=0
for _ in $(seq 1 60); do
    if is_running 'ResourceManager' && is_running 'NodeManager'; then
        YARN_READY=1
        break
    fi
    sleep 1
done
if [ "$YARN_READY" -ne 1 ]; then
    echo "ERROR: YARN did not become ready. Inspect $HADOOP_HOME/logs." >&2
    exit 4
fi
echo "YARN is ready."

echo
echo "=== jps ==="
jps -l

echo
echo "=== HDFS report ==="
hdfs dfsadmin -report 2>/dev/null | head -n 20 || true

echo
echo "=== YARN nodes ==="
yarn node -list 2>/dev/null || true

echo
echo "Hadoop services are running."
