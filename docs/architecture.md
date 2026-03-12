# Architecture & Design Notes

## Full Pipeline Flow

```
azureprojectDB (SQL Server)
  └── dbo.DimUser / DimArtist / DimTrack / DimDate / FactStream
          │
          │  ADF ForEach Activity
          │  ├── Reads loop_input.json  (table list)
          │  ├── Reads cdc.json         (lastLoadTime per table)
          │  ├── SQL: WHERE watermarkColumn > lastLoadTime
          │  ├── Copies delta rows → bronze-end2end/<TableName>/
          │  └── Updates cdc.json with new lastLoadTime
          ▼
  bronze-end2end/  (ADLS Gen2 — raw Parquet)
  ├── DimUser/
  ├── DimArtist/
  ├── DimTrack/
  ├── DimDate/
  └── FactStream/
          │
          │  silver_Dims.py — Auto Loader (cloudFiles)
          │  ├── readStream → cloudFiles format: parquet
          │  ├── schemaLocation checkpoint in silver-end2end/
          │  ├── Transformations (upper, regex, durationFlag, dedup)
          │  ├── writeStream → Delta, trigger(once=True)
          │  └── writeCheckpoint in silver-end2end/<Table>/write_checkpoints
          ▼
  silver-end2end/  (ADLS Gen2 — Delta tables)
  └── end2endCata.silver.*
          │
          │  Databricks DLT Pipeline (end2end_dab_etl)
          │  ├── @dlt.table → <table>_stg   (staging, readStream from Silver)
          │  ├── dlt.create_streaming_table  (target Gold table)
          │  └── dlt.create_auto_cdc_flow    (MERGE with SCD logic)
          ▼
  gold-end2end/  (ADLS Gen2 — Delta tables with history)
  └── end2endCata.gold.*
          │
          └── jinja_notebook.py — dynamic SQL analytics queries
```

---

## CDC Pipeline Design (ADF)

### loop_input.json
Drives the ForEach activity. Each item contains the table name, schema, and which column to use as the watermark for CDC.

### cdc.json
Acts as the CDC state store. Before each run, ADF reads `lastLoadTime` per table and uses it in the SQL WHERE clause. After a successful copy, ADF writes the new max watermark back to this file in ADLS.

### empty.json
A reset file — identical structure to `cdc.json` but with `1900-01-01` as `lastLoadTime` for all tables. Copying `empty.json` over `cdc.json` in ADLS forces a full reload on the next pipeline run. Useful for reprocessing or recovering from data issues.

---

## Silver Layer — Streaming Details

Auto Loader (`cloudFiles`) monitors new Parquet files in Bronze using ADLS file notifications. It maintains two checkpoints per table:

- **Read checkpoint** (`schemaLocation`) — tracks the inferred schema across runs
- **Write checkpoint** — tracks stream position so the job can resume safely after failure

`trigger(once=True)` means the stream processes all available data and stops — ideal for scheduled batch-style incremental runs rather than always-on streaming.

### DimTrack — `durationFlag` logic
```
duration_sec >= 300  → "Long"
duration_sec >= 120  → "Medium"
else                 → "Short"
```

### ReusableClass (`utils/transformations.py`)
A shared utility imported across all Silver notebooks. `dropColumns(df, columns)` accepts either a single string or a list, avoiding repetitive drop logic per notebook.

---

## Gold Layer — DLT + SCD Design

### DLT Staging Tables (`@dlt.table`)
Each transformation file defines a staging table (`dimuser_stg`, `dimtrack_stg`, etc.) that reads from Silver using `spark.readStream.table()`. The staging table applies data quality expectations before the CDC merge.

### `dlt.create_auto_cdc_flow()`
This is the core of the Gold layer. It implements CDC-based MERGE logic automatically:

- **SCD Type 2** (DimUser, DimTrack, DimDate) — every change creates a new row with `__START_AT` and `__END_AT` metadata columns, preserving full history
- **SCD Type 1** (FactStream) — records are upserted in place, no history kept (appropriate for fact tables)

The `sequence_by` column (e.g. `updated_at`) determines the ordering when multiple changes arrive for the same key.

### DimUser — Data Quality
`DimUser.py` adds an `expect_all_or_drop` expectation: `user_id IS NOT NULL`. Records failing this check are dropped before CDC merge.

### Email Validation UDF (`utils.py`)
A PySpark UDF that validates email format using regex. Returns `True`/`False` and can be applied as a column transformation or DLT expectation filter.

---

## Jinja Templating

`jinja_notebook.py` builds a `SELECT ... FROM ... LEFT JOIN` query dynamically from a Python list of parameter dicts. The template loops over the list using Jinja2's `{% for %}` syntax to:
- Render the SELECT column list (comma-separated, last item has no trailing comma)
- Render the FROM clause (only the first item)
- Render each subsequent LEFT JOIN with its ON condition

This pattern avoids hardcoded multi-table SQL and makes it trivial to add new tables by appending to the `parameters` list.

---

## Databricks Asset Bundle

`databricks.yml` configures the project as a Databricks Asset Bundle with two targets:

| Target | Mode | Behaviour |
|---|---|---|
| `dev` | development | Resources prefixed with `[dev username]`, all triggers paused |
| `prod` | production | Deployed to `/Workspace/PROD/`, managed permissions set |

Both targets point to the same workspace (`adb-7405607491444467.7.azuredatabricks.net`). The DLT pipeline resource (`end2end_dab_etl.pipeline.yml`) is serverless and sources its notebooks via glob from the `src/` directory.
