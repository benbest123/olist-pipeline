# Elio Data & Growth Engineer Assessment

## Environment

- **Databricks:** Free Edition, serverless compute only
- **Serverless environment:** Standard v5 (Python 3.12, Spark 4.2.0)
- **Catalogs:** `elio_dev` (development), `elio_prod` (deployed via CD)
- **Schemas:** `bronze`, `silver`, `gold`; raw files land in the `bronze.raw` volume
- **Local dev / CI:** Python 3.12, Java 17, `requirements-dev.txt`
- **Databricks CLI:** v1.16.1

## Ingest (bronze)

1. Upload the source CSVs as described in [`ingest/README.md`](ingest/README.md).
2. Deploy and run the pipeline, which runs `setup` then `ingest_bronze`:

```bash
   databricks bundle deploy -t dev
   databricks bundle run elio_pipeline -t dev
```

3. Each CSV lands as-is in `<catalog>.bronze.<table>`: all columns STRING, plus `_source_file`
   and `_loaded_at`. Runs fully overwrite the tables, so re-running is idempotent.