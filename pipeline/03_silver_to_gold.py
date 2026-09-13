# Databricks notebook source
# Silver -> gold. Transform logic lives in transforms/gold.py so it can be unit tested without
# Databricks


from transforms.gold import (
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_dim_seller,
    build_fact_order,
    build_fact_order_item,
)

dbutils.widgets.text("catalog", "elio_dev")
CATALOG = dbutils.widgets.get("catalog")


def silver(table: str):
    return spark.table(f"{CATALOG}.silver.{table}")


def write_gold(df, table: str) -> int:
    target = f"{CATALOG}.gold.{table}"
    columns = [field.name for field in spark.table(target).schema.fields]
    df.select(*columns).write.insertInto(target, overwrite=True)
    return spark.table(target).count()


# COMMAND ----------

fact_order = build_fact_order(
    silver("orders"),
    silver("customers"),
    silver("order_items"),
    silver("order_payments"),
    silver("order_reviews"),
    silver("reviews"),
)

gold = {
    "dim_date": build_dim_date(silver("orders")),
    "dim_product": build_dim_product(silver("products"), silver("product_categories")),
    "dim_seller": build_dim_seller(silver("sellers"), silver("geography")),
    "dim_customer": build_dim_customer(
        silver("customers"), silver("orders"), silver("geography"), fact_order
    ),
    "fact_order": fact_order,
    "fact_order_item": build_fact_order_item(
        silver("order_items"), silver("orders"), silver("customers")
    ),
}

for table, df in gold.items():
    print(f"{CATALOG}.gold.{table}: {write_gold(df, table):,} rows")

# COMMAND ----------

# Reconciliation: gold should not have invented or lost revenue relative to silver.
checks = spark.sql(f"""
    SELECT 'item value: fact_order vs fact_order_item' AS check,
           (SELECT round(sum(item_value), 2) FROM {CATALOG}.gold.fact_order) AS gold_order,
           (SELECT round(sum(item_value), 2) FROM {CATALOG}.gold.fact_order_item) AS gold_item,
           (SELECT round(sum(price), 2) FROM {CATALOG}.silver.order_items) AS silver_source
    UNION ALL
    SELECT 'lifetime revenue: dim_customer vs fact_order',
           (SELECT round(sum(order_value), 2) FROM {CATALOG}.gold.fact_order WHERE is_fulfilled),
           (SELECT round(sum(lifetime_revenue), 2) FROM {CATALOG}.gold.dim_customer),
           NULL
""")
display(checks)
