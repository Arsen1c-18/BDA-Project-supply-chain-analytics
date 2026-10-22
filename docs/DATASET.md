# Data

The full raw dataset is intentionally **not tracked in Git** because it is about
96 MB. Keep it in `data/` locally before running the C++, PySpark or Hadoop jobs.
No files under `data/` are committed; see the repository policy below.

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

## Repository policy

- `data/` is intentionally ignored by `.gitignore`; the raw CSV is never pushed.
- The optional local field dictionary `data/DescriptionDataCoSupplyChain.csv`
  may be kept on disk, but it is not required to run the project and is not
  tracked.
- `tokenized_access_logs.csv` was removed because it is unrelated to this
  project and is not used by any job, notebook or dashboard component.
