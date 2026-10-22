# Manual Execution and Verification Log

Project: `/home/arsen1c/Documents/Sem_5_Labs/BDA/Project`

This document records the manual steps and commands used to take the project
from compiled C++ binaries to three verified Hadoop Streaming/YARN jobs, then
through the PySpark notebook and Streamlit dashboard.

No project submission script was used for the successful distributed run. The
commands below are the manual equivalents. The project submit scripts were
corrected afterward for future use.

For `fish`, use:

```fish
set -gx NAME value
```

For `bash`, use:

```bash
export NAME=value
```

---

## 1. Final status

| Job | YARN job ID | Maps | Reduces | Output rows | Result |
|---|---|---:|---:|---:|---|
| Job 1 — delivery status | `job_1792688515026_0001` | 2 | 1 | 11 | **PASS** |
| Job 2 — shipping performance | `job_1792688515026_0004` | 2 | 1 | 4 | **PASS** |
| Job 3 — category demand | `job_1792688515026_0005` | 2 | 1 | 50 | **PASS** |

Successful HDFS locations:

```text
Input:
/user/arsen1c/bda/manual/input/DataCoSupplyChainDataset.csv

Job 1:
/user/arsen1c/bda/manual/output/manual-20261022T172132Z/delivery

Job 2:
/user/arsen1c/bda/manual/output/retry-20261022T175034Z/shipping

Job 3:
/user/arsen1c/bda/manual/output/retry-20261022T175034Z/category
```

Local verified bundle:

```text
results/hadoop/retry-20261022T175034Z/delivery-mode.tsv         (11 rows)
results/hadoop/retry-20261022T175034Z/shipping-performance.tsv  (4 rows)
results/hadoop/retry-20261022T175034Z/category-demand.tsv       (50 rows)

results/retry-20261022T175034Z.tsv                              (11 rows, flat Job 1 copy)
```

---

## 2. Start HDFS and YARN

From the project terminal:

```bash
start-dfs.sh
start-yarn.sh
jps
```

Expected daemons included:

```text
NameNode
DataNode
SecondaryNameNode
ResourceManager
NodeManager
```

Optionally inspect YARN applications:

```bash
/opt/hadoop/bin/yarn application -list -appStates ALL
```

The `yarn` executable on `PATH` can resolve to JavaScript Yarn on some systems.
Using `/opt/hadoop/bin/yarn` avoids that ambiguity.

---

## 3. Project and environment variables

From the project root:

```fish
cd /home/arsen1c/Documents/Sem_5_Labs/BDA/Project

set -gx HADOOP_HOME /opt/hadoop
set -gx HDFS_BASE "/user/$USER/bda/manual"
set -gx RUN_ID "manual-"(date -u +%Y%m%dT%H%M%SZ)
set -gx INPUT_HDFS "$HDFS_BASE/input/DataCoSupplyChainDataset.csv"
set -gx OUTPUT_DIR "$HDFS_BASE/output/$RUN_ID/delivery"

echo "HADOOP_HOME=$HADOOP_HOME"
echo "HDFS_BASE=$HDFS_BASE"
echo "RUN_ID=$RUN_ID"
echo "INPUT_HDFS=$INPUT_HDFS"
echo "OUTPUT_DIR=$OUTPUT_DIR"
```

The successful Job 1 run used:

```text
RUN_ID=manual-20261022T172132Z
```

Keep the terminal open so the variables remain available.

### Java environment separation

Hadoop uses Java 8 from:

```text
/opt/hadoop/etc/hadoop/hadoop-env.sh
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk
```

PySpark uses Java 17 from:

```text
/usr/lib/jvm/java-17-openjdk
```

PySpark remains local (`local[*]`); it is not submitted to YARN. This keeps the
two Java environments in separate processes.

Useful Java checks:

```bash
java -version
/usr/lib/jvm/java-8-openjdk/bin/java -version
bash -c 'source /opt/hadoop/etc/hadoop/hadoop-env.sh; echo "$JAVA_HOME"; "$JAVA_HOME/bin/java" -version'
```

---

## 4. Compile the C++ mappers and reducers

The working mapper binaries were compiled from `cpp/mapper/`:

```bash
cd cpp/mapper

g++ category_demand_mapper.cpp -o mapper1
g++ delivery_status_mapper.cpp -o mapper2
g++ shipping_performance_mapper.cpp -o mapper3
```

The matching reducers were compiled with:

```bash
g++ -std=c++17 -O2 ../reducer/delivery_status_reducer.cpp -o reducer2
g++ -std=c++17 -O2 ../reducer/shipping_performance_reducer.cpp -o reducer3
g++ -std=c++17 -O2 ../reducer/category_demand_reducer.cpp -o reducer1
```

Confirm all six binaries:

```bash
ls -lh mapper1 mapper2 mapper3 reducer1 reducer2 reducer3
```

Binary mapping:

| Binary | Job |
|---|---|
| `mapper1` / `reducer1` | Job 3 — category demand |
| `mapper2` / `reducer2` | Job 1 — delivery status |
| `mapper3` / `reducer3` | Job 2 — shipping performance |

Return to the project root:

```bash
cd ../..
```

---

## 5. Verify the CSV header with a tolerant encoding

The DataCo CSV is ISO-8859-1, not valid UTF-8. A strict `utf-8-sig` read failed
with a decoding error at byte `0xe1`. Use `latin-1` for header inspection.

Zero-based header positions in this 53-column file:

| Field | Index |
|---|---:|
| `Days for shipping (real)` | **1** |
| `Days for shipment (scheduled)` | **2** |
| `Delivery Status` | **5** |
| `Category Name` | **8** |
| `Order Id` | **29** |
| `Order Item Quantity` | **36** |
| `Sales` | **37** |
| `Shipping Mode` | **52** |

Reproducible header inspection:

```bash
python3 - <<'PY'
import csv
from pathlib import Path

path = Path("data/DataCoSupplyChainDataset.csv")
with path.open("r", encoding="latin-1", newline="") as handle:
    header = next(csv.reader(handle))

wanted = {
    "Delivery Status",
    "Order Id",
    "Shipping Mode",
    "Days for shipping (real)",
    "Days for shipment (scheduled)",
    "Sales",
    "Category Name",
    "Order Item Quantity",
}
print("columns:", len(header))
for index, name in enumerate(header):
    if name in wanted:
        print(f"{index:2d}  {name!r}")
PY
```

---

## 6. Stage the input in HDFS

### 6.1 Earlier generic test-path cleanup

Before switching to the dedicated manual paths, the earlier generic test paths
were cleaned:

```bash
hdfs dfs -rm -f /input/input.txt
hdfs dfs -rm -r -f /output
```

These commands removed only the earlier test file and test output directory. They
did not touch the raw dataset or the final manual output paths. Do not use these
commands against a successful project output.

### 6.2 Create and upload the manual input

Inspect the HDFS root if needed:

```bash
hdfs dfs -ls /
hdfs dfs -ls /user
hdfs dfs -ls "/user/$USER"
```

Create the manual input directory:

```bash
hdfs dfs -mkdir -p "$HDFS_BASE/input"
hdfs dfs -ls "$HDFS_BASE"
```

Upload the dataset only if it does not already exist:

```fish
if hdfs dfs -test -e "$INPUT_HDFS"
    echo "Input already exists; reusing it."
else
    hdfs dfs -put data/DataCoSupplyChainDataset.csv "$INPUT_HDFS"
end

hdfs dfs -ls -h "$INPUT_HDFS"
```

The verified upload was:

```text
-rw-r--r--   1 arsen1c supergroup     91.5 M  ... /user/arsen1c/bda/manual/input/DataCoSupplyChainDataset.csv
```

Check the intended Job 1 output path is unused:

```fish
if hdfs dfs -test -e "$OUTPUT_DIR"
    echo "OUTPUT EXISTS — choose a new RUN_ID before submitting."
else
    echo "Output path is free: $OUTPUT_DIR"
end
```

Do not reuse or delete an existing output directory. Choose a new `RUN_ID` or a
new output subdirectory instead.

---

## 7. Job 1 — delivery status

The mapper and reducer were `mapper2` and `reducer2`.

```fish
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar" \
  -D "mapreduce.job.name=bda-manual-delivery-$RUN_ID" \
  -D "mapreduce.input.fileinputformat.split.maxsize=33554432" \
  -D "mapreduce.job.reduces=1" \
  -files cpp/mapper/mapper2,cpp/reducer/reducer2 \
  -cmdenv BDA_STATUS_COL=5 \
  -cmdenv BDA_ORDER_ID_COL=29 \
  -cmdenv BDA_SHIPPING_MODE_COL=52 \
  -mapper ./mapper2 \
  -reducer ./reducer2 \
  -input "$INPUT_HDFS" \
  -output "$OUTPUT_DIR"
```

Important: generic Hadoop options such as `-D` and `-files` must appear before
Streaming options such as `-cmdenv`.

Observed result:

```text
job_1792688515026_0001
Launched map tasks=2
Launched reduce tasks=1
Map input records=180,520
Map output records=180,519
Reduce input groups=11
Reduce output records=11
```

### Verify Job 1

```bash
hdfs dfs -ls "$OUTPUT_DIR"

hdfs dfs -cat "$OUTPUT_DIR/part-00000" > /tmp/job1.tsv
LC_ALL=C sort /tmp/job1.tsv -o /tmp/job1.tsv

LC_ALL=C sort tests/expected/real_dataset_counts.tsv -o /tmp/job1.expected.tsv

wc -l /tmp/job1.tsv
cat /tmp/job1.tsv
diff -u /tmp/job1.expected.tsv /tmp/job1.tsv
```

Expected:

```text
11 rows
diff is silent
Late:     36,048
On time:  26,849
Canceled:  2,855
```

The result matched `tests/expected/real_dataset_counts.tsv`.

---

## 8. Job 2 and Job 3 failures

The first Job 2 and Job 3 attempts failed with mapper exit code `2`.

### 8.1 Incomplete environment schema

Job 2 had blank values for:

```text
BDA_ACTUAL_DAYS_COL
BDA_SCHEDULED_DAYS_COL
BDA_SALES_COL
```

The mapper requires all six variables. If any are missing, `have_env_schema` is
false. The first input split can still use the CSV header, but other splits have
no header and exit with:

```text
ERROR: no CSV header in this input split and no header-derived
BDA_*_COL environment variables were supplied
```

Job 3 has the same fail-closed behavior and requires all five variables.

The failed HDFS output directories were empty and were left untouched:

```text
/user/arsen1c/bda/manual/output/manual-20261022T172132Z/shipping
/user/arsen1c/bda/manual/output/manual-20261022T172132Z/category
```

The failed YARN applications were:

```text
application_1792688515026_0002  category  FAILED
application_1792688515026_0003  shipping  FAILED
```

### 8.2 Wrong option ordering

A retry with the correct schema values initially failed before submission:

```text
ERROR streaming.StreamJob: Unrecognized option: -files
```

Cause: the command placed `-cmdenv` before `-files`. Hadoop parses generic
options first; `-files` must precede `-cmdenv`.

The failed retry output path was not reused. The corrected retry used a new
retry ID.

---

## 9. Local full-dataset verification

These commands verified that the C++ binaries and exact schema values were
correct before retrying YARN.

### Job 2

```bash
env BDA_SHIPPING_MODE_COL=52 \
    BDA_ORDER_ID_COL=29 \
    BDA_STATUS_COL=5 \
    BDA_ACTUAL_DAYS_COL=1 \
    BDA_SCHEDULED_DAYS_COL=2 \
    BDA_SALES_COL=37 \
    cpp/mapper/mapper3 < data/DataCoSupplyChainDataset.csv \
  | LC_ALL=C sort -t "$(printf '\t')" -k1,1 \
  | cpp/reducer/reducer3 > /tmp/job2_local.tsv

LC_ALL=C sort -t "$(printf '\t')" -k1,1 /tmp/job2_local.tsv -o /tmp/job2_local.tsv
LC_ALL=C sort -t "$(printf '\t')" -k1,1 tests/expected/real_shipping_performance.tsv -o /tmp/job2_expected.tsv

wc -l /tmp/job2_local.tsv /tmp/job2_expected.tsv
diff -u /tmp/job2_expected.tsv /tmp/job2_local.tsv
```

Observed:

```text
shipping reducer summary: groups=4 bad_values=0
4 /tmp/job2_local.tsv
4 /tmp/job2_expected.tsv
diff is silent
```

### Job 3

```bash
env BDA_CATEGORY_COL=8 \
    BDA_ORDER_ID_COL=29 \
    BDA_QUANTITY_COL=36 \
    BDA_SALES_COL=37 \
    BDA_STATUS_COL=5 \
    cpp/mapper/mapper1 < data/DataCoSupplyChainDataset.csv \
  | LC_ALL=C sort -t "$(printf '\t')" -k1,1 \
  | cpp/reducer/reducer1 > /tmp/job3_local.tsv

LC_ALL=C sort -t "$(printf '\t')" -k1,1 /tmp/job3_local.tsv -o /tmp/job3_local.tsv
LC_ALL=C sort -t "$(printf '\t')" -k1,1 tests/expected/real_category_demand.tsv -o /tmp/job3_expected.tsv

wc -l /tmp/job3_local.tsv /tmp/job3_expected.tsv
diff -u /tmp/job3_expected.tsv /tmp/job3_local.tsv
```

Observed:

```text
category reducer summary: groups=50 bad_values=0
50 /tmp/job3_local.tsv
50 /tmp/job3_expected.tsv
diff is silent
```

---

## 10. Successful Job 2 retry

New retry variables:

```fish
set -gx RETRY_ID "retry-"(date -u +%Y%m%dT%H%M%SZ)
set -gx SHIPPING_OUT "$HDFS_BASE/output/$RETRY_ID/shipping"
set -gx CATEGORY_OUT "$HDFS_BASE/output/$RETRY_ID/category"

echo "$RETRY_ID"
echo "$SHIPPING_OUT"
echo "$CATEGORY_OUT"
```

The successful values were:

```text
RETRY_ID=retry-20261022T175034Z
SHIPPING_OUT=/user/arsen1c/bda/manual/output/retry-20261022T175034Z/shipping
CATEGORY_OUT=/user/arsen1c/bda/manual/output/retry-20261022T175034Z/category
```

Check both output paths are free:

```fish
if hdfs dfs -test -e "$SHIPPING_OUT"
    echo "EXISTS: $SHIPPING_OUT"
else
    echo "FREE: $SHIPPING_OUT"
end

if hdfs dfs -test -e "$CATEGORY_OUT"
    echo "EXISTS: $CATEGORY_OUT"
else
    echo "FREE: $CATEGORY_OUT"
end
```

Submit Job 2:

```fish
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar" \
  -D "mapreduce.job.name=bda-manual-shipping-$RETRY_ID" \
  -D "mapreduce.input.fileinputformat.split.maxsize=33554432" \
  -D "mapreduce.job.reduces=1" \
  -files cpp/mapper/mapper3,cpp/reducer/reducer3 \
  -cmdenv BDA_SHIPPING_MODE_COL=52 \
  -cmdenv BDA_ORDER_ID_COL=29 \
  -cmdenv BDA_STATUS_COL=5 \
  -cmdenv BDA_ACTUAL_DAYS_COL=1 \
  -cmdenv BDA_SCHEDULED_DAYS_COL=2 \
  -cmdenv BDA_SALES_COL=37 \
  -mapper ./mapper3 \
  -reducer ./reducer3 \
  -input "$INPUT_HDFS" \
  -output "$SHIPPING_OUT"
```

Observed:

```text
job_1792688515026_0004
Launched map tasks=2
Launched reduce tasks=1
4 rows
```

### Verify Job 2

```bash
hdfs dfs -ls "$SHIPPING_OUT"

hdfs dfs -cat "$SHIPPING_OUT/part-00000" > /tmp/job2.tsv
LC_ALL=C sort /tmp/job2.tsv -o /tmp/job2.tsv
LC_ALL=C sort tests/expected/real_shipping_performance.tsv -o /tmp/job2.expected.tsv

wc -l /tmp/job2.tsv
diff -u /tmp/job2.expected.tsv /tmp/job2.tsv
```

A silent `diff` means Job 2 matched the expected fixture.

---

## 11. Successful Job 3 retry

Submit Job 3:

```fish
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar" \
  -D "mapreduce.job.name=bda-manual-category-$RETRY_ID" \
  -D "mapreduce.input.fileinputformat.split.maxsize=33554432" \
  -D "mapreduce.job.reduces=1" \
  -files cpp/mapper/mapper1,cpp/reducer/reducer1 \
  -cmdenv BDA_CATEGORY_COL=8 \
  -cmdenv BDA_ORDER_ID_COL=29 \
  -cmdenv BDA_QUANTITY_COL=36 \
  -cmdenv BDA_SALES_COL=37 \
  -cmdenv BDA_STATUS_COL=5 \
  -mapper ./mapper1 \
  -reducer ./reducer1 \
  -input "$INPUT_HDFS" \
  -output "$CATEGORY_OUT"
```

Observed:

```text
job_1792688515026_0005
Launched map tasks=2
Launched reduce tasks=1
50 rows
```

### Verify Job 3

```bash
hdfs dfs -ls "$CATEGORY_OUT"

hdfs dfs -cat "$CATEGORY_OUT/part-00000" > /tmp/job3.tsv
LC_ALL=C sort /tmp/job3.tsv -o /tmp/job3.tsv
LC_ALL=C sort tests/expected/real_category_demand.tsv -o /tmp/job3.expected.tsv

wc -l /tmp/job3.tsv
diff -u /tmp/job3.expected.tsv /tmp/job3.tsv
```

A silent `diff` means Job 3 matched the expected fixture.

---

## 12. Assemble the local result bundle

Job 1 and Jobs 2/3 used separate HDFS run paths, so their verified outputs were
combined into one local bundle for the notebook and dashboard.

```bash
RUN_ID=retry-20261022T175034Z
BUNDLE="results/hadoop/$RUN_ID"

mkdir -p "$BUNDLE"

hdfs dfs -cat \
  /user/arsen1c/bda/manual/output/manual-20261022T172132Z/delivery/part-00000 \
  > "$BUNDLE/delivery-mode.tsv"

cp "/tmp/job2-$RUN_ID.tsv" "$BUNDLE/shipping-performance.tsv"
cp "/tmp/job3-$RUN_ID.tsv" "$BUNDLE/category-demand.tsv"

cp "$BUNDLE/delivery-mode.tsv" "results/$RUN_ID.tsv"

wc -l "$BUNDLE"/*.tsv "results/$RUN_ID.tsv"
```

Expected local row counts:

```text
11 delivery-mode.tsv
 4 shipping-performance.tsv
50 category-demand.tsv
11 results/retry-20261022T175034Z.tsv
```

---

## 13. Run the PySpark notebook

The notebook uses Java 17 and the project virtual environment. It reads the
verified local bundle through `BDA_HADOOP_RUN_ID`.

```bash
export BDA_HADOOP_RUN_ID=retry-20261022T175034Z
export BDA_HADOOP_RESULTS="$PWD/results/hadoop"
export BDA_SPARK_JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"

.venv/bin/jupyter nbconvert \
  --to notebook \
  --execute notebooks/supply_chain_analysis.ipynb \
  --output-dir=/tmp \
  --output=supply_chain_analysis.executed.ipynb \
  --ExecutePreprocessor.timeout=900 \
  > /tmp/notebook-execute.log 2>&1
```

Observed:

```text
notebook exit code 0
15/15 code cells
0 errors
```

The notebook loaded:

```text
results/hadoop/retry-20261022T175034Z/delivery-mode.tsv
results/hadoop/retry-20261022T175034Z/shipping-performance.tsv
results/hadoop/retry-20261022T175034Z/category-demand.tsv
```

All Hadoop-vs-PySpark comparison differences were `0`.

Copy the executed notebook back into the project:

```bash
cp /tmp/supply_chain_analysis.executed.ipynb \
   notebooks/supply_chain_analysis.ipynb
```

Model results:

```text
Accuracy: 0.6915
F1 score: 0.6870
AUC:      0.7130
```

Artefacts are under `outputs/model/`.

---

## 14. Verify the Streamlit dashboard

### AppTest with a pinned run ID

```bash
env BDA_HADOOP_RUN_ID=retry-20261022T175034Z \
    BDA_HADOOP_RESULTS="$PWD/results/hadoop" \
    .venv/bin/python - <<'PY'
from streamlit.testing.v1 import AppTest

at = AppTest.from_file("dashboard/app.py", default_timeout=120)
at.run()

print("exceptions:", len(at.exception))
for exc in at.exception:
    print("EXCEPTION:", exc.message)

print("metrics:", len(at.metric))
print("dataframes:", len(at.dataframe))
print("tabs:", [tab.label for tab in at.tabs])
PY
```

Observed:

```text
exceptions: 0
metrics: 8
dataframes: 11
tabs: ['Overview', 'Shipping performance', 'Product categories', 'Model', 'Hadoop jobs']
```

### AppTest with default latest-bundle selection

```bash
env -u BDA_HADOOP_RUN_ID \
    .venv/bin/python - <<'PY'
from streamlit.testing.v1 import AppTest

at = AppTest.from_file("dashboard/app.py", default_timeout=120)
at.run()

print("exceptions:", len(at.exception))
print("metrics:", len(at.metric))
print("dataframes:", len(at.dataframe))
print("tabs:", [tab.label for tab in at.tabs])
PY
```

This also returned `0` exceptions and selected the newest bundle.

### Server health check

```bash
PORT=8502
LOG=/tmp/bda-streamlit-$PORT.log

BDA_HADOOP_RUN_ID=retry-20261022T175034Z \
  .venv/bin/streamlit run dashboard/app.py \
  --server.headless true \
  --server.address 127.0.0.1 \
  --server.port "$PORT" \
  --browser.gatherUsageStats false \
  > "$LOG" 2>&1 &

PID=$!

for i in $(seq 1 30); do
    if curl -fsS "http://127.0.0.1:$PORT/_stcore/health" >/tmp/bda-health.txt 2>/dev/null; then
        break
    fi
    sleep 1
done

curl -sS -o /dev/null -w 'root_status=%{http_code}\n' "http://127.0.0.1:$PORT/"
curl -sS -o /dev/null -w 'health_status=%{http_code}\n' "http://127.0.0.1:$PORT/_stcore/health"
cat /tmp/bda-health.txt

kill "$PID" 2>/dev/null || true
wait "$PID" 2>/dev/null || true
```

Observed:

```text
root_status=200
health_status=200
health_body=ok
```

The test server was stopped after the check. For normal use:

```bash
source .venv/bin/activate
streamlit run dashboard/app.py
```

---

## 15. Final verification checks

```bash
/opt/hadoop/bin/yarn application -list -appStates ALL
jps -l
```

The three successful applications were:

```text
application_1792688515026_0001  delivery  SUCCEEDED
application_1792688515026_0004  shipping  SUCCEEDED
application_1792688515026_0005  category  SUCCEEDED
```

The two earlier failed applications remained:

```text
application_1792688515026_0002  category  FAILED
application_1792688515026_0003  shipping  FAILED
```

Failed HDFS output directories were left untouched. The successful retries used
new output directories.

---

## 16. Files changed after verification

- `README.md` — distributed job IDs, HDFS paths, verification status, corrected
  map-task count, and troubleshooting notes.
- `docs/SETUP_REPORT.md` — replaced pending YARN fields with actual job IDs,
  task counts, HDFS paths, and notebook/dashboard results.
- `scripts/run_streaming_job.sh` — moved generic `-files` before `-cmdenv`.
- `scripts/run_analysis_job.sh` — moved generic `-files` before `-cmdenv`.
- `notebooks/supply_chain_analysis.ipynb` — updated with execution against the
  distributed result bundle.
- `docs/WORKFLOW_LOG.md` — this manual execution record.

Syntax check for the corrected submit scripts:

```bash
bash -n scripts/run_streaming_job.sh scripts/run_analysis_job.sh
```

No C++ source changes were required.

---

## 17. Important operational notes

1. Use a new `RUN_ID` or a new output subdirectory for every retry.
2. Never delete or overwrite a successful HDFS output directory.
3. `-files` must come before `-cmdenv` in the `hadoop jar` command.
4. Supply the complete `BDA_*_COL` set to every mapper when using more than one
   input split.
5. Use `latin-1` when inspecting the DataCo CSV header.
6. Hadoop runs on Java 8; local PySpark runs on Java 17. Do not submit PySpark to
   YARN with this setup.
7. The notebook and dashboard should only read verified Hadoop outputs; the
   dashboard must not retrain the model during interaction.
