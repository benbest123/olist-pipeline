# Elio Data & Growth Engineer Assessment

## Environment

- **Databricks:** Free Edition, serverless compute only
- **Serverless environment:** Standard v5 (Python 3.12, Spark 4.2.0)
- **Catalogs:** `elio_dev` (development), `elio_prod` (deployed via CD)
- **Schemas:** `bronze`, `silver`, `gold`; raw files land in the `bronze.raw` volume
- **Local dev / CI:** Python 3.12, Java 17, `requirements-dev.txt`
- **Databricks CLI:** v1.16.1