# Databricks notebook source
# MAGIC %md
# MAGIC # Load raw Olist CSVs into bronze
# MAGIC
# MAGIC Lands each CSV as-is into `<catalog>.bronze.<table>`: every column as STRING, plus
# MAGIC `_source_file` and `_loaded_at`. Each run fully overwrites the tables, so re-running is
# MAGIC idempotent. Assumptions are recorded in `ingest/README.md`.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "elio_dev")
CATALOG = dbutils.widgets.get("catalog")
RAW_PATH = f"/Volumes/{CATALOG}/bronze/raw/olist"

# bronze table -> source file
FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}

# COMMAND ----------

for table, file_name in FILES.items():
    df = (
        spark.read.csv(
            f"{RAW_PATH}/{file_name}",
            header=True,
            inferSchema=False,  # keep everything as STRING; typing happens in silver
            multiLine=True,  # review text contains line breaks inside quoted fields
            escape='"',  # embedded quotes are doubled ("")
        )
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_loaded_at", F.current_timestamp())
    )
    target = f"{CATALOG}.bronze.{table}"
    df.write.format("delta").mode("overwrite").saveAsTable(target)
    print(f"{target}: {spark.table(target).count():,} rows")
