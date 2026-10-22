# Setup and Verification Report — C++ Hadoop Streaming, PySpark and Streamlit

Date: 2026-10-22  
Project: `/home/arsen1c/Documents/Sem_5_Labs/BDA/Project`  
Execution status: **All three C++ Streaming jobs completed successfully on
HDFS/YARN; the notebook and dashboard were verified against the distributed
result bundle.**

## 1. Environment

| Requirement | Verified value |
|---|---|
| C++ compiler | `g++ (GCC) 16.2.1 20260810` at `/usr/bin/g++` |
| C++ standard/runtime | C++17; static `libstdc++.a` available; glibc 2.44 |
| Hadoop | Apache Hadoop **3.3.4** at `/opt/hadoop` |
| Hadoop Streaming JAR | `/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar` |
| Streaming JAR check | `hadoop jar <jar>` prints the Streaming usage message |
| Hadoop Java | `/usr/lib/jvm/java-8-openjdk` (OpenJDK 1.8.0_504), pinned in `hadoop-env.sh` |
| HDFS endpoint | `hdfs://localhost:9000` |
| HDFS state | NameNode formatted; NameNode/DataNode storage exists |
| HDFS/YARN service state | Running during verification: NameNode, DataNode, SecondaryNameNode, ResourceManager and NodeManager |
| SSH localhost | Passwordless, so `start-dfs.sh`/`start-yarn.sh` can run |
| MapReduce framework | `yarn` (real YARN execution, not local mode) |
| Python environment | `.venv`, Python 3.13 |
| PySpark | 4.2.0 |
| pandas | 2.3.3 |
| Streamlit / Plotly | 1.66.1 / 7.1.0 |
| Spark Java | Java 17+; local PySpark, separate from Hadoop's Java 8 pin |

No Hadoop component was reinstalled or reconfigured.

## 2. Data verification

`data/DataCoSupplyChainDataset.csv`

| Property | Value |
|---|---|
| Size | 95,910,149 bytes |
| Physical lines | 180,520 (header + 180,519 data rows) |
| Columns | 53 |
| Encoding / line endings | ISO-8859-1 / CRLF |
| Unique orders | **65,752** |
| Order-item duplicates | 114,767 rows beyond one per order |
| Status consistency per order | 0 inconsistent orders |
| Shipping-cost column | **None** |
| Inventory-stock column | **None** |
| Quoted commas / embedded newlines | None in this file; parser still supports them |

Job 1 expected full-data output:

| Shipping mode | Late | On time* | Canceled | Total |
|---|---:|---:|---:|---:|
| First Class | 9,602 | 0 | 477 | 10,079 |
| Same Day | 1,648 | 1,759 | 164 | 3,571 |
| Second Class | 9,803 | 2,453 | 522 | 12,778 |
| Standard Class | 14,995 | 22,637 | 1,692 | 39,324 |
| **Total** | **36,048** | **26,849** | **2,855** | **65,752** |

\* `Shipping on time` + `Advance shipping`. Canceled orders are separate.

Jobs 2 and 3 produce:

- Job 2: 4 shipping-mode rows from 180,519 records / 65,752 distinct orders.
- Job 3: 50 category rows from 180,519 records and 98,977 late item records.

## 3. Implemented jobs and files

| Job | Mapper | Reducer | Purpose |
|---|---|---|---|
| Job 1 | `cpp/mapper/delivery_status_mapper.cpp` | `cpp/reducer/delivery_status_reducer.cpp` | Distinct orders by shipping mode + status |
| Job 2 | `cpp/mapper/shipping_performance_mapper.cpp` | `cpp/reducer/shipping_performance_reducer.cpp` | Record/order counts, status counts, days, sales proxy |
| Job 3 | `cpp/mapper/category_demand_mapper.cpp` | `cpp/reducer/category_demand_reducer.cpp` | Category item/order counts, quantity, sales, late rate |

`cpp/csv.hpp` contains the shared RFC-4180 parser, header normalization, status
mapping and safe numeric parsing. The original Job 1 mapper and reducer were left
unchanged.

## 4. Build command

```bash
bash scripts/build_cpp.sh
```

This builds all six binaries into `cpp/bin/` with:

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic \
    -static-libstdc++ -static-libgcc <source>.cpp -o <binary>
```

## 5. Verification fixtures

The submit scripts compare full-data output against:

```text
tests/expected/real_dataset_counts.tsv
tests/expected/real_shipping_performance.tsv
tests/expected/real_category_demand.tsv
```

Earlier local sample checks used `mapper | LC_ALL=C sort -t $'\t' -k1,1 | reducer`
to imitate Hadoop's bytewise shuffle. Those optional local test scripts and sample
fixtures were removed during repository cleanup; the historical results remain
recorded below.

## 6. Test results

### 6.1 Local verification (historical)

| Test | Result |
|---|---|
| Build all six binaries | **PASS** |
| Job 1 synthetic quoted-CSV sample | **PASS** |
| Job 1 full dataset | **PASS** |
| Job 2 sample (`shipping_performance_sample.csv`) | **PASS**; malformed row skipped/reported |
| Job 2 headerless `-cmdenv` sample path | **PASS** |
| Job 2 full dataset | **PASS**; 4 rows |
| Job 3 sample (`category_demand_sample.csv`) | **PASS**; malformed row skipped/reported |
| Job 3 headerless `-cmdenv` sample path | **PASS** |
| Job 3 full dataset | **PASS**; 50 rows |
| Independent pandas cross-check | **PASS** for Job 2 and Job 3 aggregates |

Verified full-data Job 2 output:

```text
First Class    27814  10079  26513     0  1301  0  2.00 1.00 1.00  5674369.76  562.99
Same Day        9737   3571   4454  4839   444  0  0.48 0.00 0.48  1942528.56  543.97
Second Class   35216  12778  26987  6819  1410  0  3.99 2.00 1.99  7145444.82  559.20
Standard Class 107752 39324  41023 62130  4599  0  4.00 4.00 0.00 22022391.88  560.02
```

### 6.2 Hadoop Streaming / YARN

The verified run used manual `hadoop jar` commands, not
`scripts/run_all_hadoop_jobs.sh`.

| Job | YARN job id | Maps | Reduces | Output rows | Expected diff |
|---|---|---:|---:|---:|---|
| Job 1 delivery | `job_1792688515026_0001` | 2 | 1 | 11 | **PASS** |
| Job 2 shipping | `job_1792688515026_0004` | 2 | 1 | 4 | **PASS** |
| Job 3 category | `job_1792688515026_0005` | 2 | 1 | 50 | **PASS** |

Job 1 read 180,520 map input records (header plus 180,519 data rows) and produced
11 reduce groups. The successful Job 2/3 retries reused the uploaded 95 MB CSV.

The first Job 2/3 attempts (`application_1792688515026_0002` and
`application_1792688515026_0003`) failed because their `BDA_*_COL` values were
incomplete. Their HDFS output directories remained empty and were not reused.
The successful retries used new output directories under
`retry-20261022T175034Z`.

A separate Hadoop CLI issue was reproduced during retry: generic options such as
`-files` must precede Streaming options such as `-cmdenv`. Both submit scripts
were corrected to use that ordering.

## 7. HDFS paths

Verified manual run:

| Job | Input | Output |
|---|---|---|
| Job 1 | `/user/arsen1c/bda/manual/input/DataCoSupplyChainDataset.csv` | `/user/arsen1c/bda/manual/output/manual-20261022T172132Z/delivery` |
| Job 2 | same input file | `/user/arsen1c/bda/manual/output/retry-20261022T175034Z/shipping` |
| Job 3 | same input file | `/user/arsen1c/bda/manual/output/retry-20261022T175034Z/category` |

The local dashboard/notebook bundle is
`results/hadoop/retry-20261022T175034Z/`; the flat Job 1 copy is
`results/retry-20261022T175034Z.tsv`.

The submit-script defaults under `/user/<user>/bda/streaming/...` and
`/user/<user>/bda/analytics/...` remain available for future scripted runs.

## 8. Notebook and dashboard verification

Verified on 2026-10-22 with the local CSV and the distributed Hadoop-result bundle
`results/hadoop/retry-20261022T175034Z/`. HDFS/YARN were running; the notebook
loaded all three downloaded distributed outputs.

| Component | Check | Result |
|---|---|---|
| Notebook format | `nbformat` validation | **Valid nbformat 4.5**, 32 cells |
| Notebook execution | `jupyter nbconvert --execute --inplace` | **15/15 code cells, 0 errors** |
| Spark runtime | SparkSession smoke test | Spark 4.2.0, Java 17, local mode |
| Hadoop-result comparison | Notebook section 8.1 | **All distinct-order differences = 0** |
| Model | DecisionTreeClassifier | **Accuracy 0.6915**, **F1 0.6870**, **AUC 0.7130** |
| Model artefacts | `outputs/model/` | `metrics.json`, `predictions_sample.csv`, model directory |
| Dashboard app test | `streamlit.testing.v1.AppTest` | **0 exceptions**, 8 KPI metrics, 11 dataframes, 5 tabs |
| Dashboard empty-result path | `BDA_HADOOP_RESULTS` pointing to an empty directory | **0 exceptions**, clear missing-result message |
| Dashboard server | `streamlit run` + HTTP health check | **HTTP 200** |
| C++ distributed jobs | YARN | **PASS** — Jobs 1–3 completed; 2 maps / 1 reduce each |

## 9. Scope limitations

- No shipping-cost field exists; Job 2 monetary columns are sales/order-value
  proxies, not shipping charges.
- No inventory-stock levels exist; Job 3 quantity/sales are demand/revenue proxies,
  not stockout or inventory-risk measurements.
- The analysis is observational, not causal.
- First Class has no on-time/advance rows in this dataset, an unusually extreme
  pattern that may be a dataset artefact.
- The model uses a random split, not a time-based split, and decision-tree splits on
  indexed categorical variables treat them as ordinal.
- The distributed verification used 2 map tasks per job; it is a correctness run,
  not a performance benchmark.
- The initial Job 2/3 failures were configuration errors: incomplete `BDA_*_COL`
  values caused non-first HDFS splits to exit with code 2. The mappers correctly
  refuse to guess column positions.
