# Supply Chain Delivery Disruption and Shipping Performance Analysis

**Hadoop MapReduce (C++), PySpark and Streamlit**

This project analyses the DataCo supply-chain dataset with three genuine C++
Hadoop Streaming jobs designed to run through HDFS and YARN. The jobs write
separate TSV results and feed a PySpark notebook and a Streamlit dashboard.

The title deliberately focuses on **shipping performance**. The dataset has no
shipping-cost column and no inventory-stock levels, so the project does not make
shipping-cost, stockout or inventory-risk claims.

## Current verification status

- All six C++ binaries build successfully with the portable static runtime flags.
- All three jobs have completed successfully through **HDFS/YARN** using the full
  95 MB CSV:
  - Job 1: `job_1792688515026_0001` — 2 maps, 1 reduce, 11 result groups.
  - Job 2: `job_1792688515026_0004` — 2 maps, 1 reduce, 4 rows.
  - Job 3: `job_1792688515026_0005` — 2 maps, 1 reduce, 50 rows.
- Each distributed output matched its expected TSV with a sorted `diff`.
- The notebook executed end to end with **64/64 code cells and 0 errors** against
  the distributed bundle `results/hadoop/retry-20261022T175034Z/`; all
  Hadoop-vs-Spark comparison differences were **0**.
- The Streamlit `AppTest` reported **0 exceptions** across the four tabs,
  filters/reset/refresh, all three Hadoop verification views, and the saved-model
  prediction form. Earlier server checks returned HTTP 200 on both the root and
  health endpoints.
- The first two Job 2/3 attempts failed because their `BDA_*_COL` configuration
  was incomplete. Their output directories were left untouched; new directories
  were used for the successful retry.

## Architecture

```text
data/DataCoSupplyChainDataset.csv
        |
        |  scripts/run_streaming_job.sh
        v
/user/<user>/bda/streaming/input/<run-id>/...        (Job 1 input)
        |
        |  Hadoop Streaming / YARN (verified: 2 map tasks, 1 reducer)
        v
/user/<user>/bda/streaming/output/<run-id>/         (Job 1 delivery status)

        +-- separate analytics path for Jobs 2 and 3 ----------------------+
        |                                                                  |
        |  scripts/run_analysis_job.sh                                    |
        v                                                                  v
/user/<user>/bda/analytics/input/<run-id>/...        (shared uploaded CSV)
        |
        +--> /user/<user>/bda/analytics/output/<run-id>/shipping-performance
        +--> /user/<user>/bda/analytics/output/<run-id>/category-demand

Downloaded/local result bundle
        results/hadoop/<run-id>/delivery-mode.tsv
        results/hadoop/<run-id>/shipping-performance.tsv
        results/hadoop/<run-id>/category-demand.tsv
        |
        +--> notebooks/supply_chain_analysis.ipynb
        +--> dashboard/app.py
```

`scripts/run_all_hadoop_jobs.sh` runs all three jobs with one run-id and copies
the Job 1 result into the same bundle layout. The original working Job 1 files and
the original `scripts/run_streaming_job.sh` were not rewritten.

## The three jobs

### Job 1 — delivery status

Files:

- `cpp/mapper/delivery_status_mapper.cpp`
- `cpp/reducer/delivery_status_reducer.cpp`

Purpose: count **distinct orders** by shipping mode and delivery-status group.

Input columns: `Delivery Status`, `Order Id`, `Shipping Mode`.

Output columns (tab-separated, no header):

```text
shipping_mode<TAB>status<TAB>distinct_order_count
```

`status` is `late`, `on_time`, `canceled` or `other`. `Advance shipping` is
grouped with on-time. Repeated order-item rows for one order are counted once.

### Job 2 — shipping-mode performance

Files:

- `cpp/mapper/shipping_performance_mapper.cpp`
- `cpp/reducer/shipping_performance_reducer.cpp`
- shared parser/helpers: `cpp/csv.hpp`

Purpose: aggregate shipping performance by shipping mode. The job keeps both
order-item record counts and distinct-order counts because they answer different
questions.

Input columns: `Shipping Mode`, `Order Id`, `Delivery Status`,
`Days for shipping (real)`, `Days for shipment (scheduled)`, `Sales`.

Output columns (tab-separated, no header):

```text
shipping_mode
records
distinct_orders
late_records
on_time_records
canceled_records
other_records
avg_actual_days
avg_scheduled_days
avg_delay_days
total_sales
avg_sales_per_order
```

`total_sales` and `avg_sales_per_order` are an explicitly labelled order-value
proxy. **They are not shipping charges.** DataCo has no shipping-cost field.

### Job 3 — product-category demand

Files:

- `cpp/mapper/category_demand_mapper.cpp`
- `cpp/reducer/category_demand_reducer.cpp`
- shared parser/helpers: `cpp/csv.hpp`

Purpose: measure demand and revenue proxies by category while retaining
distinct-order and late-item counts.

Input columns: `Category Name`, `Order Id`, `Order Item Quantity`, `Sales`,
`Delivery Status`.

Output columns (tab-separated, no header):

```text
category
records
distinct_orders
total_quantity
units_per_order
total_sales
avg_sales_per_item
late_records
late_rate_pct
```

Quantity and sales are demand/revenue proxies. **No inventory-stock level exists
in the dataset**, so this job does not measure stockouts or inventory risk.

All new mappers use an RFC-4180-style parser (quoted commas, escaped quotes and
quoted newlines), resolve headers case-insensitively, receive header-derived
column positions through `-cmdenv` for non-first HDFS splits, and write
diagnostics only to stderr.

## Dataset

`data/DataCoSupplyChainDataset.csv`

The raw CSV is **not committed** because it is about 96 MB. See
`docs/DATASET.md` for the download source, expected filename and SHA-256 checksum.
No files under `data/` are tracked in Git.

- 95,910,149 bytes (~95.9 MB), 180,520 physical lines, 53 columns.
- 180,519 data rows; **65,752 unique `Order Id`** values.
- ISO-8859-1 encoding, CRLF line endings.
- Order-item grain: counting rows instead of distinct orders overstates deliveries.
- No `Shipping Cost` column.
- No inventory-stock or stockout columns.
- All rows of an order share the same shipping mode and delivery status (verified:
  0 inconsistent orders), so order-level deduplication is safe.

Job 1 expected full-data output:

| Shipping mode | Late | On time* | Canceled | Total |
|---|---:|---:|---:|---:|
| First Class | 9,602 | 0 | 477 | 10,079 |
| Same Day | 1,648 | 1,759 | 164 | 3,571 |
| Second Class | 9,803 | 2,453 | 522 | 12,778 |
| Standard Class | 14,995 | 22,637 | 1,692 | 39,324 |
| **Total** | **36,048** | **26,849** | **2,855** | **65,752** |

\* `Shipping on time` + `Advance shipping`; canceled orders are separate.

Job 2 totals: **180,519 records**, **65,752 distinct orders**, and four shipping
modes. Job 3 produces **50 category rows**. The notebook and dashboard compare
their distinct-order counts with equivalent pandas/PySpark aggregations.

## Project layout

```text
cpp/csv.hpp                                # shared RFC-4180 parser and helpers
cpp/mapper/delivery_status_mapper.cpp      # Job 1
cpp/reducer/delivery_status_reducer.cpp    # Job 1
cpp/mapper/shipping_performance_mapper.cpp # Job 2
cpp/reducer/shipping_performance_reducer.cpp
cpp/mapper/category_demand_mapper.cpp      # Job 3
cpp/reducer/category_demand_reducer.cpp
cpp/bin/                                   # compiled binaries

scripts/build_cpp.sh                       # build all six binaries
scripts/check_schema.py                    # Job 1 header check
scripts/check_job_schema.py                # Job 2/3 header checks
scripts/run_streaming_job.sh               # Job 1 HDFS + YARN submission
scripts/run_analysis_job.sh                # Job 2/3 HDFS + YARN submission
scripts/run_all_hadoop_jobs.sh             # all three jobs, one run-id

tests/expected/real_dataset_counts.tsv
tests/expected/real_shipping_performance.tsv
tests/expected/real_category_demand.tsv

notebooks/supply_chain_analysis.ipynb      # PySpark + Hadoop-result comparison
dashboard/app.py                           # Streamlit dashboard
docs/DATASET.md                            # dataset download / checksum
docs/SETUP_REPORT.md
docs/WORKFLOW_LOG.md
requirements.txt
```

## Requirements

| Component | Version / location |
|---|---|
| C++ compiler | g++ 16.2.1, C++17 |
| C++ runtime flags | `-static-libstdc++ -static-libgcc` |
| Hadoop | 3.3.4 at `/opt/hadoop` |
| Hadoop Java | `/usr/lib/jvm/java-8-openjdk`, pinned in `hadoop-env.sh` |
| Streaming JAR | `/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar` |
| HDFS | `hdfs://localhost:9000` |
| Python virtualenv | `.venv`, Python 3.13 |
| PySpark | 4.2.0 |
| pandas / Jupyter / Streamlit / Plotly | versions pinned in `requirements.txt` |
| Spark Java | Java 17+; local PySpark, not Spark-on-YARN |

Recreate the Python environment if needed:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Build

Build all C++ jobs:

```bash
bash scripts/build_cpp.sh
```

The submit scripts compare full-dataset output with the three fixtures under
`tests/expected/` when the input basename is `DataCoSupplyChainDataset.csv`.

## Hadoop startup and submission

Start HDFS and YARN with your Hadoop installation before submitting jobs:

```bash
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
```

Run all three jobs with one run-id:

```bash
bash scripts/run_all_hadoop_jobs.sh data/DataCoSupplyChainDataset.csv
# optional explicit run-id:
bash scripts/run_all_hadoop_jobs.sh data/DataCoSupplyChainDataset.csv bda-20261022T220000Z
```

Run one job at a time:

```bash
bash scripts/run_streaming_job.sh data/DataCoSupplyChainDataset.csv my-run
bash scripts/run_analysis_job.sh shipping-performance data/DataCoSupplyChainDataset.csv my-run
bash scripts/run_analysis_job.sh category-demand data/DataCoSupplyChainDataset.csv my-run
```

### HDFS paths

| Job | HDFS input | HDFS output |
|---|---|---|
| Job 1 delivery | `/user/<user>/bda/streaming/input/<run-id>/<file>` | `/user/<user>/bda/streaming/output/<run-id>` |
| Job 2 shipping | `/user/<user>/bda/analytics/input/<run-id>/<file>` | `/user/<user>/bda/analytics/output/<run-id>/shipping-performance` |
| Job 3 category | `/user/<user>/bda/analytics/input/<run-id>/<file>` | `/user/<user>/bda/analytics/output/<run-id>/category-demand` |

The new-job script uploads the input once per run-id and reuses it for the second
job. It refuses to overwrite an existing output, log or result bundle. Both submit
scripts set a 32 MB maximum split size; the verified 95 MB run used **2 map tasks**
and one reducer.

The distributed verification was run manually rather than through those scripts.
Its HDFS locations were:

| Job | HDFS input | HDFS output |
|---|---|---|
| Job 1 | `/user/arsen1c/bda/manual/input/DataCoSupplyChainDataset.csv` | `/user/arsen1c/bda/manual/output/manual-20261022T172132Z/delivery` |
| Job 2 | same input file | `/user/arsen1c/bda/manual/output/retry-20261022T175034Z/shipping` |
| Job 3 | same input file | `/user/arsen1c/bda/manual/output/retry-20261022T175034Z/category` |

The scripts print the YARN job id, launched map/reduce task counts, output rows and
a diff against the expected fixture. They only claim success after the YARN job
exits successfully. Downloaded results are written to:

```text
results/hadoop/<run-id>/delivery-mode.tsv
results/hadoop/<run-id>/shipping-performance.tsv
results/hadoop/<run-id>/category-demand.tsv
```

Job 1 additionally keeps its original flat result at `results/<run-id>.tsv`.

## Notebook

Launch Jupyter from the project virtual environment:

```bash
cd /home/arsen1c/Documents/Sem_5_Labs/BDA/Project
source .venv/bin/activate
jupyter notebook --no-browser --ip=127.0.0.1
```

The notebook sets Spark's worker and driver Python to the interpreter that launches
Jupyter. This avoids a worker/driver Python-version mismatch. If you execute it from
another Python environment, set both explicitly:

```bash
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python"
```

PySpark 4.2 needs Java 17+. Hadoop 3.3.4 stays on Java 8; do not point Hadoop at the
Spark JVM.

Validated non-interactive run:

```bash
PYSPARK_PYTHON="$PWD/.venv/bin/python" \
PYSPARK_DRIVER_PYTHON="$PWD/.venv/bin/python" \
  .venv/bin/jupyter nbconvert --to notebook --execute --inplace \
  notebooks/supply_chain_analysis.ipynb
```

Useful environment variables:

```bash
# Dataset path override
export BDA_DATA_PATH=/path/to/DataCoSupplyChainDataset.csv

# Optional HDFS read; the notebook falls back to local CSV on failure
export BDA_USE_HDFS=1
export BDA_HDFS_INPUT=hdfs://localhost:9000/user/$USER/bda/input/DataCoSupplyChainDataset.csv
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop

# Point the notebook at a downloaded C++ result bundle
export BDA_HADOOP_RESULTS="$PWD/results/hadoop"
export BDA_HADOOP_RUN_ID=my-run
# Optional Job 1 HDFS output base for the notebook's HDFS comparison branch
export BDA_HDFS_BASE=hdfs://localhost:9000/user/$USER/bda/streaming

# Optional Spark settings
export SPARK_MASTER='local[*]'
export SPARK_DRIVER_MEMORY=2g
```

The notebook covers SparkSession setup, local/HDFS loading with fallback, RDD
operations, DataFrame cleaning and aggregation, Spark SQL, Parquet output,
Hadoop-result comparison, and a late-delivery classifier with accuracy/F1/AUC.
The model excludes delivery outcomes, actual shipping days, shipping date,
`Order Status` and profit fields to avoid target leakage.

Current model result (local PySpark run against the distributed Hadoop bundle):
**accuracy 0.6915**, **F1 0.6870**, **AUC 0.7130**. Artefacts are written under
`outputs/model/`; the dashboard reads them, can load the saved `PipelineModel`
for a single-order prediction, and never retrains the model on interaction.
Interactive prediction requires Java 17+; set `BDA_SPARK_JAVA_HOME` if the
default `java` is older.

## Streamlit dashboard

```bash
cd /home/arsen1c/Documents/Sem_5_Labs/BDA/Project
source .venv/bin/activate
streamlit run dashboard/app.py
```

The dashboard:

- Filters order-item rows by shipping mode, delivery status and category.
- Shows distinct-order KPIs, late rates, shipping-mode performance and category
  demand proxies across **Overview** and **Analysis** tabs.
- Has a **Verification** tab for the three downloaded C++ result tables, charts,
  row-count checks and local pandas comparisons.
- Reads the pinned bundle `results/hadoop/retry-20261022T175034Z/` by default.
  `BDA_HADOOP_RESULTS` overrides the bundle root and `BDA_HADOOP_RUN_ID` selects
  another run.
- Uses saved model metrics and sample predictions, and can restore the saved
  Spark `PipelineModel` for a one-order late-delivery prediction. It never
  retrains the model.
- Labels `Order Item Total`/`Sales` as proxies and never presents them as shipping
  charges.
- Does not read intermediate mapper data.

A `local-verification` bundle exists for UI testing but is not evidence of a
distributed YARN run. The dashboard pins the verified distributed bundle
`retry-20261022T175034Z`; use `BDA_HADOOP_RUN_ID` to inspect a different verified
run.

## Limitations

- The dataset has no shipping-cost field; monetary results are order-value/sales
  proxies.
- The dataset has no inventory-stock levels; category quantity/sales are demand
  proxies, not stockout or inventory-risk measurements.
- The analysis is observational and does not establish causal effects.
- First Class has no on-time/advance rows in this dataset, an unusually extreme
  result that may be a dataset artefact.
- The model uses a random split, not a time-based split, and treats indexed
  categorical variables as ordinal in the decision tree.
- The distributed run used the full 95 MB file but only 2 map tasks per job; it is
  a correctness verification, not a performance benchmark.
- The first Job 2/3 submissions failed because `BDA_*_COL` values were incomplete.
  The mappers intentionally refuse to guess schema positions on split boundaries.

## Troubleshooting

- **HDFS is not reachable** — start HDFS/YARN (`start-dfs.sh` and `start-yarn.sh`), then retry.
- **Output/result already exists** — this is intentional. Choose a new run-id; the
  scripts never delete or overwrite existing data.
- **No CSV header in this input split** — submit through the scripts so header-
  derived `BDA_*_COL` variables are supplied to every mapper.
- **`Unrecognized option: -files`** — generic options such as `-D` and `-files`
  must appear before Streaming options such as `-cmdenv`. Both submit scripts were
  corrected after this was reproduced.
- **Only one map task** — check the `Launched map tasks` line; the scripts set a
  32 MB maximum split size.
- **PySpark worker Python mismatch** — use the `.venv` Python for both driver and
  worker; the notebook configures this automatically from `sys.executable`.
- **Java complaints** — Hadoop uses Java 8 via `/opt/hadoop/etc/hadoop/hadoop-env.sh`;
  PySpark uses Java 17+. Keep the two environments separate.

See `docs/SETUP_REPORT.md` for the full distributed verification record.
