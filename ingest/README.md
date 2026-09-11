# Ingest: raw Olist CSVs → bronze

`load_bronze.py` lands each source CSV as-is into `<catalog>.bronze.<table>`: every column as
STRING, plus `_source_file` and `_loaded_at`. It runs as the `ingest_bronze` task of the
`elio_pipeline` job, after `setup`. Each run fully overwrites the tables, so re-runs are idempotent.

## Manual steps

The only manual step is getting the source files into each environment's landing volume.

1. **Download** the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
   from Kaggle (9 CSV files). Unzip to a folder **outside the repo**:
   raw data is never committed, and `.gitignore` also excludes `*.csv` and `*.zip`.
2. **Create the landing volume** by running the job's `setup` task once, so `<catalog>.bronze.raw` exists.
3. **Upload** the CSVs directly into the volume, in each environment, in the Databricks UI:

   - Catalog Explorer → `<catalog>` → `bronze` → `raw` → **Upload to this volume**.