# Data

The full raw dataset is intentionally **not tracked in Git** because it is about
96 MB. Keep it in this directory locally before running the C++, PySpark or
Hadoop jobs.

## Required input

| Property | Value |
| --- | --- |
| File | `data/DataCoSupplyChainDataset.csv` |
| Size | 95,910,149 bytes (~95.9 MB) |
| SHA-256 | `fa6d022ed437155e1a2f0378710602848703c8a7f203f7ff5d77805bf8480aa6` |
| Encoding | ISO-8859-1 / `latin-1` |
| Rows | 180,519 data rows plus the header |
| Columns | 53 |

Download the original dataset from Kaggle:

- Dataset: **DataCo Smart Supply Chain for Big Data Analysis**
- URL: <https://www.kaggle.com/datasets/shashwatwork/dataco-smart-supply-chain-for-big-data-analysis>

Place the downloaded file at:

```text
data/DataCoSupplyChainDataset.csv
```

Then verify the checksum:

```bash
sha256sum data/DataCoSupplyChainDataset.csv
```

The expected value must match the SHA-256 in the table above.

## Included files

- `DescriptionDataCoSupplyChain.csv` contains the field dictionary for the raw
  dataset and is small enough to track in Git.
- `tokenized_access_logs.csv` was removed because it is unrelated to this
  project and is not used by any job, notebook or dashboard component.
