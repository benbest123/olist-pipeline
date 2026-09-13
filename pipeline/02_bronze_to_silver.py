# Databricks notebook source
# Bronze -> silver. The transform logic lives in transforms/silver.py so it can be unit tested
# without Databricks; this notebook only reads, calls, and writes.
#
# Writes with insertInto(overwrite=True), which replaces the rows but keeps the table definition
# from 01_create_tables.sql - so the declared types, NOT NULL and CHECK constraints are enforced
# on every write, and a transform that drifts from the DDL fails loudly. Full overwrite each run
# makes re-runs idempotent.
#
# Tables are written parents-first so the (informational) foreign keys line up.

from transforms.silver import (
    build_customers,
    build_geography,
    build_order_items,
    build_order_payments,
    build_order_reviews,
    build_orders,
    build_product_categories,
    build_products,
    build_reviews,
    build_sellers,
)

dbutils.widgets.text("catalog", "elio_dev")
CATALOG = dbutils.widgets.get("catalog")


def bronze(table: str):
    return spark.table(f"{CATALOG}.bronze.{table}")


def write_silver(df, table: str) -> int:
    """Overwrite one silver table, matching columns to the target schema by name."""
    target = f"{CATALOG}.silver.{table}"
    columns = [field.name for field in spark.table(target).schema.fields]
    df.select(*columns).write.insertInto(target, overwrite=True)
    return spark.table(target).count()


# COMMAND ----------

silver = {
    "geography": build_geography(bronze("customers"), bronze("sellers"), bronze("geolocation")),
    "product_categories": build_product_categories(
        bronze("products"), bronze("product_category_translation")
    ),
    "customers": build_customers(bronze("customers")),
    "sellers": build_sellers(bronze("sellers")),
    "products": build_products(bronze("products")),
    "orders": build_orders(bronze("orders")),
    "order_items": build_order_items(bronze("order_items")),
    "order_payments": build_order_payments(bronze("order_payments")),
    "reviews": build_reviews(bronze("order_reviews")),
    "order_reviews": build_order_reviews(bronze("order_reviews")),
}

for table, df in silver.items():
    print(f"{CATALOG}.silver.{table}: {write_silver(df, table):,} rows")
