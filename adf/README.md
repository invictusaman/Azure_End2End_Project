# Azure Data Factory — `dfend2end`

## Pipeline Design

The ADF pipeline uses a **ForEach** activity to iterate over each table defined in `config/loop_input.json`. For each table it:

1. Reads `lastLoadTime` from `config/cdc.json` for that table
2. Runs a SQL query: `WHERE <watermarkColumn> > lastLoadTime`
3. Copies only the changed rows to `bronze-end2end/<TableName>/` as Parquet
4. Updates `cdc.json` with the new `lastLoadTime`

## Config Files

| File | Purpose |
|---|---|
| `config/loop_input.json` | Table list — drives the ForEach activity |
| `config/cdc.json` | CDC watermark state — stores `lastLoadTime` per table, updated after each run |
| `config/empty.json` | Reset file — copy over `cdc.json` in ADLS to trigger a full reload |

## How to Deploy

**Option A — Git Integration (recommended)**
Connect ADF to this repo under **Manage → Git configuration**. ADF reads pipeline changes directly from the `adf/` folder.

**Option B — Manual import**
Copy JSON files from `pipeline/`, `linkedService/`, and `dataset/` into ADF Studio.

## Notes

- Linked service credentials (SQL password, ADLS connection) are **not stored here** — configure those in ADF after deployment
- The Access Connector (`acess-end2endAzure`) must exist in Azure before deploying the ADLS linked service
- To force a full reload of any table, copy `config/empty.json` over `config/cdc.json` in ADLS, then re-run the pipeline
