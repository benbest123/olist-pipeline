# Elio technical assessment — Olist on the Databricks Lakehouse

A medallion pipeline over the Olist Brazilian e-commerce dataset: raw CSVs land in bronze, are
conformed into a 3NF silver model, and are denormalised into a gold star schema for analytics.


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
sql/                 analytics_queries.sql
resources/           pipeline_job.yml — the job definition
tests/               unit tests for the transforms, run in CI without Databricks
analysis_answers.md  data analysis and recommendations
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

The job runs five tasks in order:

| Task | Notebook | What it does |
|---|---|---|
| `setup` | `pipeline/00_setup.sql` | Creates the catalog, the three schemas and the landing volume |
| `ingest_bronze` | `ingest/load_bronze.py` | Lands the 9 CSVs as-is into bronze Delta tables |
| `create_tables` | `pipeline/01_create_tables.sql` | Declares the silver tables, types and constraints |
| `bronze_to_silver` | `pipeline/02_bronze_to_silver.py` | Cleans, conforms and deduplicates into silver |
| `silver_to_gold` | `pipeline/03_silver_to_gold.py` | Denormalises silver into the gold star schema |

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

### 4. Run the analytics

sql/analytics_queries.sql runs against the gold layer, either in a SQL editor or as a notebook. Set the catalog at the top (USE CATALOG elio_dev;) and run the cells in order.

## The layers

**Bronze** — every column as STRING, no cleaning, plus `_source_file` and `_loaded_at`. Row counts
match the published dataset. Read options and assumptions are documented in
[`ingest/README.md`](ingest/README.md).

**Silver** — ten tables in third normal form. The design, ERD and rationale are in
[`pipeline/data_model.md`](pipeline/data_model.md). Keys are the source's natural keys, which
profiling confirmed are unique; enforcement comes from `NOT NULL` and `CHECK` constraints.

**Gold** — 6 tables ready for analytics (4 dim, 2 fact):

| Table | Grain | Holds |
|---|---|---|
| `fact_order_item` | one line item | Revenue attributable to a product, category and seller. All measures additive |
| `fact_order` | one order | Delivery time, review score and payment — order-grain measures that would be non-additive if repeated on every item row |
| `dim_customer` | one real person | Regrained from silver's per-order customer records: 99,441 records, 96,096 people |
| `dim_product`, `dim_seller`, `dim_date` | | Categories and locations flattened in; `dim_date` generated so there are no gaps |



## Analysis

analysis_answers.md contains the commercial findings, the recommendation and the caveats behind them, written for a non-technical reader. Every figure cites the query that produced it.

## CI/CD

| Trigger | Workflow | What runs |
|---|---|---|
| Pull request into `main` | `ci.yml` | `ruff`, `pytest`, `bundle validate` against both targets |
| Merge to `main` | `cd.yml` | `bundle deploy -t prod` |
| Manual dispatch | `cd.yml` | Optionally `bundle run` against prod |

Deploying and running are deliberately separate: a merge updates the prod job definition but never
reprocesses data. Production runs are triggered explicitly.

## Idempotency and environments

Every stage fully overwrites its output, so running the pipeline twice produces the same result as running it once. This holds because each layer is derived deterministically from the one below it:

- Bronze overwrites from the source files. Schema overwrite is deliberately not enabled, so a changed source fails the load rather than silently reshaping the table.
- Silver deduplicates on each table's primary key using an explicit ordering, so a re-run selects the same row rather than an arbitrary one.
- Gold derives surrogate keys as sha2 hashes of the natural key. A rebuild reproduces identical keys, so facts and dimensions can be rebuilt independently without orphaning rows.
- Writes use insertInto, which replaces rows but keeps the table definition, so the declared types and constraints are enforced on every run and a transform that drifts from the DDL fails.

Verified by running the full pipeline twice and comparing row counts, revenue totals and a checksum of the surrogate keys, and again by comparing elio_dev against elio_prod, with two separate runs of the same code producing identical output.

Dev and prod are separate Unity Catalog catalogs, selected by a single bundle variable. The dev target deploys under the developer's own workspace folder and prefixes the job name; the prod target deploys to a fixed path.

### What would change with a live source.
The full overwrite suits a static snapshot and keeps no history. For incremental file drops, bronze would move to Auto Loader, which tracks which files it has already processed and so stays idempotent while appending. Silver would MERGE on its keys rather than overwrite, and gold would either continue rebuilding or move to MERGE driven by Delta change data feed. Dimensions would also need Type 2 history to answer point-in-time questions, which the current Type 1 rebuild cannot.