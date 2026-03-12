# Azure End-to-End Data Engineering Pipeline

Designed and implemented a production-style data pipeline on Microsoft Azure — ingesting data from **Azure SQL Server** into **ADLS Gen2** and processing it through a **Medallion Architecture (Bronze → Silver → Gold)** using **Azure Data Factory** and **Azure Databricks**.

---

## Architecture

```
Azure SQL Server
        │
        │  ADF — Watermark-based CDC (ForEach + incremental copy)
        ▼
   Bronze Layer        Raw Parquet files — no transformations
        │
        │  Databricks Auto Loader (Structured Streaming → Delta)
        ▼
   Silver Layer        Cleansed Delta tables — end2endCata.silver
        │
        │  Databricks DLT Pipeline (CDC Auto Flow)
        ▼
   Gold Layer          Analytics-ready Delta tables — end2endCata.gold
```

---

## What's in This Repo

```
azure-end2end-project/
├── README.md
├── adf/
│   ├── pipeline/            ← ADF pipeline JSON (ForEach + Copy Activity)
│   ├── linkedService/       ← SQL Server + ADLS Gen2 linked services
│   ├── dataset/             ← Source and sink dataset definitions
│   └── config/
│       ├── loop_input.json  ← Table list driving the ForEach loop
│       ├── cdc.json         ← Watermark state per table (lastLoadTime)
│       └── empty.json       ← Reset file for full reload
├── databricks/
│   ├── end2end_dab.dbc      ← Databricks Asset Bundle (import into workspace)
│   ├── databricks.yml       ← Bundle config — dev/prod targets
│   ├── src/
│   │   ├── silver/          ← Auto Loader streaming notebooks (one per table)
│   │   └── gold/dlt/        ← DLT pipeline notebooks (SCD Type 1 & 2)
│   ├── utils/               ← Shared transformation utilities
│   ├── jinja/               ← Jinja-templated dynamic SQL notebook
│   └── resources/           ← DLT pipeline YAML definition
├── docs/
│   └── architecture.md
└── screenshots/
    └── azure-resource-group.png
```

---

## Medallion Architecture

### 🥉 Bronze — Incremental Ingestion

Built an ADF pipeline using a **ForEach** activity to loop over all source tables. Used a **watermark-based CDC pattern** — a JSON state file tracks `lastLoadTime` per table so only new or updated rows are copied each run. A separate reset file allows a full reload when needed.

### 🥈 Silver — Structured Streaming

Used **Databricks Auto Loader** (`cloudFiles`) to read incrementally from Bronze. Each table streams independently with isolated read/write checkpoints, ensuring fault-tolerant exactly-once delivery. Data lands as managed **Delta tables** registered in **Unity Catalog**.

### 🥇 Gold — Delta Live Tables + SCD

Built a **DLT pipeline** using `dlt.create_auto_cdc_flow()` to apply CDC-driven MERGE from Silver into Gold. Dimension tables use **SCD Type 2** — preserving full change history via `__START_AT` / `__END_AT` metadata. The fact table uses **SCD Type 1** for efficient upserts. Also built a **Jinja-templated notebook** that generates dynamic SQL at runtime — no hardcoded query logic per table.

---

## Techniques & Concepts Applied

| Area          | Technique                                                          |
| ------------- | ------------------------------------------------------------------ |
| Ingestion     | Watermark-based CDC — ADF ForEach + JSON state management          |
| Storage       | Medallion Architecture on ADLS Gen2 (Bronze / Silver / Gold)       |
| Streaming     | Auto Loader with per-table read/write checkpointing                |
| Processing    | Spark Structured Streaming with Delta Lake writes                  |
| Orchestration | Databricks Asset Bundle (DAB) — dev/prod deployment via YAML       |
| DLT           | `dlt.create_auto_cdc_flow()` — declarative CDC pipeline management |
| SCD           | Type 2 for dimension history, Type 1 for fact table upserts        |
| Data Quality  | DLT Expectations — invalid records dropped before reaching Gold    |
| Templating    | Jinja2 for parameterized, reusable SQL generation                  |
| Security      | Access Connector — credential-free Databricks → ADLS identity auth |
| Catalog       | Unity Catalog — managed schemas across Silver and Gold layers      |

---

## Azure Services

| Service            | Role                                                      |
| ------------------ | --------------------------------------------------------- |
| Azure SQL Server   | Source OLTP database                                      |
| ADLS Gen2          | Medallion storage — Bronze, Silver, Gold containers       |
| Azure Data Factory | CDC incremental ingestion pipeline                        |
| Azure Databricks   | Auto Loader (Silver) + DLT pipeline (Gold)                |
| Access Connector   | Credential-free identity auth between Databricks and ADLS |
| Unity Catalog      | Centralized governance and schema management              |

---

![Azure Resource Group](screenshots/azure-resource-group.png)
