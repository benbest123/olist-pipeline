# Elio technical assessment — Olist on the Databricks Lakehouse

A medallion pipeline over the Olist Brazilian e-commerce dataset: raw CSVs land in bronze, are
conformed into a 3NF silver model, and are denormalised into a gold star schema for analytics.

**Status:** Part A1–A3 complete (bronze, data model, silver). Gold, analytics SQL and Part B in
progress.

## Dataset

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
(Kaggle) — 9 linked CSVs covering ~99k orders placed between 2016 and 2018.

Chosen over the Databricks-native `retail-org` sample because it is real data across multiple
linked tables, which gives a genuine normalisation exercise (a many-to-many between reviews and
orders, a category translation table, a geographic lookup) and a commercial story that stands up
to scrutiny.

## Environment

| | |
|---|---|
| Workspace | Databricks Free Edition, serverless compute only |
| Serverless environment | Standard v5 — Python 3.12, Spark 4.2.0 |
| Catalogs | `elio_dev` (development), `elio_prod` (deployed via CD) |
| Schemas | `bronze`, `silver`, `gold`; raw files land in the `bronze.raw` volume |
| Local / CI | Python 3.12, Java 17, pinned in `requirements-dev.txt` |
| Databricks CLI | v1.16.1 |

## Repository layout

```
.github/workflows/   ci.yml (lint, tests, bundle validate), cd.yml (deploy on merge)
ingest/              load_bronze.py, profile_bronze.py, README.md, requirements.txt
pipeline/            00_setup.sql, 01_create_tables.sql, 02_bronze_to_silver.py,
                     transforms/silver.py, data_model.md
resources/           pipeline_job.yml — the job definition
tests/               unit tests for the transforms, run in CI without Databricks
databricks.yml       asset bundle: dev and prod targets
```

The repo is synced to the workspace as a Databricks Git folder; notebooks are stored in source
format (`.py` / `.sql`) so they diff cleanly.

## Running it

### 1. Load the source data

A one-off manual step — see [`ingest/README.md`](ingest/README.md). Free Edition serverless has no
outbound internet access, so the CSVs are downloaded locally and uploaded to each catalog's landing
volume.

### 2. Deploy and run the pipeline

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run elio_pipeline -t dev
```

The job runs four tasks in order:

| Task | Notebook | What it does |
|---|---|---|
| `setup` | `pipeline/00_setup.sql` | Creates the catalog, the three schemas and the landing volume |
| `ingest_bronze` | `ingest/load_bronze.py` | Lands the 9 CSVs as-is into bronze Delta tables |
| `create_tables` | `pipeline/01_create_tables.sql` | Declares the silver tables, types and constraints |
| `bronze_to_silver` | `pipeline/02_bronze_to_silver.py` | Cleans, conforms and deduplicates into silver |

Every task takes a `catalog` parameter, supplied by the bundle (`elio_dev` or `elio_prod`), so the
same code runs against either environment. Each stage fully overwrites its output, so the pipeline
is re-runnable and re-runs produce identical results.

### 3. Run the tests

```bash
uv venv --python 3.12 && source .venv/bin/activate
uv pip install -r requirements-dev.txt
ruff check . && pytest -q
```

The transform logic is plain functions taking and returning DataFrames, so it runs on a local
Spark session with no Databricks connection.

## The layers

**Bronze** — every column as STRING, no cleaning, plus `_source_file` and `_loaded_at`. Row counts
match the published dataset. Read options and assumptions are documented in
[`ingest/README.md`](ingest/README.md).

**Silver** — ten tables in third normal form. The design, ERD and rationale are in
[`pipeline/data_model.md`](pipeline/data_model.md). Keys are the source's natural keys, which
profiling confirmed are unique; enforcement comes from `NOT NULL` and `CHECK` constraints.

**Gold** — star schema. _In progress._

## CI/CD

| Trigger | Workflow | What runs |
|---|---|---|
| Pull request into `main` | `ci.yml` | `ruff`, `pytest`, `bundle validate` against both targets |
| Merge to `main` | `cd.yml` | `bundle deploy -t prod` |
| Manual dispatch | `cd.yml` | Optionally `bundle run` against prod |

Deploying and running are deliberately separate: a merge updates the prod job definition but never
reprocesses data. Production runs are triggered explicitly.

## Assumptions and shortcuts

- **Uploading the source files is manual**, for the network reason above. In production the source
  system would drop files into the landing volume and Auto Loader would pick them up incrementally.
- **Every stage is a full overwrite.** Olist is a static snapshot, so this is the simplest
  idempotent option.
- **Dev and prod are two catalogs in one workspace**, since Free Edition provides a single
  workspace. The bundle switches between them with one variable.
