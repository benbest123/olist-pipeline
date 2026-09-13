# Databricks notebook source
# MAGIC %md
# MAGIC # Profiling bronze layer
# MAGIC Exploring the raw data
# MAGIC Everything in bronze is STRING, this establishes facts needed to design silver
# MAGIC - Shape (table rows and cols)
# MAGIC - Column profile (nulls, distinct values, lengths)
# MAGIC - Keys (uniqueness, duplication)
# MAGIC - Castability (identify any potential issues with type conversion)
# MAGIC - Referential integrity (orphaned foreign keys)
# MAGIC - Domain checks (dataset-specific traps)
# MAGIC - Results documented in pipeline/DATA_PROFILE.md

# COMMAND ----------


from functools import reduce
 
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
 
dbutils.widgets.text("catalog", "elio_dev")
CATALOG = dbutils.widgets.get("catalog")
 
TABLES = [
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "product_category_translation",
]
 
bronze = {t: spark.table(f"{CATALOG}.bronze.{t}") for t in TABLES}

def source_columns(df: DataFrame) -> list[str]:
    """Source columns only — excludes the _-prefixed ingestion metadata."""
    return [c for c in df.columns if not c.startswith("_")]

# COMMAND ----------

# Shape
display(
    spark.createDataFrame(
        [(t, df.count(), len(source_columns(df)), ", ".join(source_columns(df)))
         for t, df in bronze.items()],
        "table string, rows long, source_columns int, columns string",
    )
)

# COMMAND ----------

# Column profiles
def profile_columns(table: str, df: DataFrame) -> DataFrame:
    """One aggregation pass over a table; returns one row per source column."""
    cols = source_columns(df)
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for c in cols:
        col = F.col(c)
        aggs += [
            F.count_if(col.isNull()).alias(f"{c}__nulls"),
            F.count_if(F.trim(col) == "").alias(f"{c}__blanks"),
            F.approx_count_distinct(col).alias(f"{c}__distinct"),
            F.min(F.length(col)).alias(f"{c}__min_len"),
            F.max(F.length(col)).alias(f"{c}__max_len"),
        ]
    r = df.agg(*aggs).first()
    rows = [
        (table, c, r["__rows"], r[f"{c}__nulls"], r[f"{c}__blanks"], r[f"{c}__distinct"],
         r[f"{c}__min_len"], r[f"{c}__max_len"])
        for c in cols
    ]
    return spark.createDataFrame(
        rows,
        "table string, column string, rows long, nulls long, blanks long, "
        "approx_distinct long, min_len int, max_len int",
    )
 
 
display(
    reduce(DataFrame.unionByName, [profile_columns(t, df) for t, df in bronze.items()])
)

# COMMAND ----------

# Candidate keys
KEY_CHECKS = [
    ("customers", ["customer_id"]),
    ("customers", ["customer_unique_id"]),  # expected dupes: one customer_id per order
    ("orders", ["order_id"]),
    ("order_items", ["order_id", "order_item_id"]),
    ("order_payments", ["order_id", "payment_sequential"]),
    ("order_reviews", ["review_id"]),
    ("order_reviews", ["order_id"]),  # expected dupes: multiple reviews per order
    ("products", ["product_id"]),
    ("sellers", ["seller_id"]),
    ("product_category_translation", ["product_category_name"]),
    ("geolocation", ["geolocation_zip_code_prefix"]),  # expected dupes: many coords per prefix
]
 
 
def key_check(table: str, cols: list[str]) -> tuple:
    df = bronze[table].select(source_columns(bronze[table]))
    total = df.count()
    distinct_keys = df.select(*cols).distinct().count()
    any_null = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols])
    return (
        table,
        ", ".join(cols),
        total,
        distinct_keys,
        total - distinct_keys,  # rows sharing a key with another row
        df.filter(any_null).count(),
        total - df.distinct().count(),  # fully identical rows
    )
 
 
display(
    spark.createDataFrame(
        [key_check(t, cols) for t, cols in KEY_CHECKS],
        "table string, key string, rows long, distinct_keys long, duplicate_key_rows long, "
        "null_key_rows long, exact_duplicate_rows long",
    )
)

# COMMAND ----------

# Castability
TS = "timestamp"
MONEY = "decimal(12,2)"
CASTS = {
    "orders": {
        "order_purchase_timestamp": TS,
        "order_approved_at": TS,
        "order_delivered_carrier_date": TS,
        "order_delivered_customer_date": TS,
        "order_estimated_delivery_date": TS,
    },
    "order_items": {
        "order_item_id": "int",
        "shipping_limit_date": TS,
        "price": MONEY,
        "freight_value": MONEY,
    },
    "order_payments": {
        "payment_sequential": "int",
        "payment_installments": "int",
        "payment_value": MONEY,
    },
    "order_reviews": {
        "review_score": "int",
        "review_creation_date": TS,
        "review_answer_timestamp": TS,
    },
    "products": {
        "product_name_lenght": "int",  # sic — misspelled in source
        "product_description_lenght": "int",
        "product_photos_qty": "int",
        "product_weight_g": "int",
        "product_length_cm": "int",
        "product_height_cm": "int",
        "product_width_cm": "int",
    },
    "geolocation": {
        "geolocation_lat": "double",
        "geolocation_lng": "double",
        "geolocation_zip_code_prefix": "int",
    },
    "customers": {"customer_zip_code_prefix": "int"},
    "sellers": {"seller_zip_code_prefix": "int"},
}
 
 
def cast_check(table: str, col_types: dict[str, str]) -> list[tuple]:
    df = bronze[table]
    aggs = []
    for c, t in col_types.items():
        failed = (
            F.col(c).isNotNull()
            & (F.trim(F.col(c)) != "")
            & F.expr(f"try_cast(`{c}` AS {t})").isNull()
        )
        aggs += [
            F.count_if(failed).alias(f"{c}__fails"),
            F.slice(F.collect_set(F.when(failed, F.col(c))), 1, 3).alias(f"{c}__samples"),
        ]
    r = df.agg(*aggs).first()
    return [
        (table, c, t, r[f"{c}__fails"], ", ".join(r[f"{c}__samples"]))
        for c, t in col_types.items()
    ]
 
 
display(
    spark.createDataFrame(
        [row for t, cols in CASTS.items() for row in cast_check(t, cols)],
        "table string, column string, target_type string, failures long, samples string",
    )
)

# COMMAND ----------

# Referential integrity - potential orphaned rows
FK_CHECKS = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
    ("products", "product_category_name", "product_category_translation", "product_category_name"),
    ("customers", "customer_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
    ("sellers", "seller_zip_code_prefix", "geolocation", "geolocation_zip_code_prefix"),
    ("orders", "order_id", "order_items", "order_id"),  # orders with no items
    ("orders", "order_id", "order_payments", "order_id"),  # orders with no payment
]
 
 
def fk_check(child: str, child_col: str, parent: str, parent_col: str) -> tuple:
    c = bronze[child].select(F.col(child_col).alias("k")).filter(F.col("k").isNotNull())
    p = bronze[parent].select(F.col(parent_col).alias("k")).distinct()
    orphans = c.join(p, "k", "left_anti")
    return (
        f"{child}.{child_col}",
        f"{parent}.{parent_col}",
        c.count(),
        orphans.count(),
        orphans.distinct().count(),
    )
 
 
display(
    spark.createDataFrame(
        [fk_check(*check) for check in FK_CHECKS],
        "child string, parent string, child_rows long, orphan_rows long, "
        "distinct_orphan_values long",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset-specific checks

# COMMAND ----------

# Order status vs missing timestamps
orders = bronze["orders"]
display(
    orders.groupBy("order_status")
    .agg(
        F.count(F.lit(1)).alias("orders"),
        F.count_if(F.col("order_approved_at").isNull()).alias("no_approved_at"),
        F.count_if(F.col("order_delivered_carrier_date").isNull()).alias("no_carrier_date"),
        F.count_if(F.col("order_delivered_customer_date").isNull()).alias("no_delivered_date"),
    )
    .orderBy(F.desc("orders"))
)

# COMMAND ----------

# Timeline sanity checks
def ts(c: str):
    return F.expr(f"try_cast(`{c}` AS timestamp)")
 
 
display(
    orders.agg(
        F.count_if(ts("order_approved_at") < ts("order_purchase_timestamp")).alias(
            "approved_before_purchase"
        ),
        F.count_if(ts("order_delivered_customer_date") < ts("order_purchase_timestamp")).alias(
            "delivered_before_purchase"
        ),
        F.count_if(
            ts("order_delivered_customer_date") < ts("order_delivered_carrier_date")
        ).alias("delivered_before_carrier"),
        F.min(ts("order_purchase_timestamp")).alias("first_purchase"),
        F.max(ts("order_purchase_timestamp")).alias("last_purchase"),
    )
)

# COMMAND ----------

# Monthly order volume
display(
    orders.withColumn("month", F.date_format(ts("order_purchase_timestamp"), "yyyy-MM"))
    .groupBy("month")
    .agg(F.count(F.lit(1)).alias("orders"))
    .orderBy("month")
)

# COMMAND ----------

# Customer id vs customer unique id
display(
    bronze["customers"]
    .groupBy("customer_unique_id")
    .agg(F.count(F.lit(1)).alias("orders_per_customer"))
    .groupBy("orders_per_customer")
    .agg(F.count(F.lit(1)).alias("customers"))
    .orderBy("orders_per_customer")
)

# COMMAND ----------

# Does a customer_unique_id always map to one location?
display(
    bronze["customers"]
    .groupBy("customer_unique_id")
    .agg(
        F.countDistinct("customer_zip_code_prefix").alias("zips"),
        F.countDistinct("customer_state").alias("states"),
    )
    .agg(
        F.count(F.lit(1)).alias("customers"),
        F.count_if(F.col("zips") > 1).alias("multiple_zips"),
        F.count_if(F.col("states") > 1).alias("multiple_states"),
    )
)

# COMMAND ----------

# Payments vs items - source of truth for order value
def money(c: str):
    return F.expr(f"try_cast(`{c}` AS {MONEY})")
 
 
items_total = bronze["order_items"].groupBy("order_id").agg(
    F.sum(money("price") + money("freight_value")).alias("items_total")
)
payments_total = bronze["order_payments"].groupBy("order_id").agg(
    F.sum(money("payment_value")).alias("payments_total")
)
recon = items_total.join(payments_total, "order_id", "full_outer").withColumn(
    "diff", F.col("payments_total") - F.col("items_total")
)
display(
    recon.agg(
        F.count(F.lit(1)).alias("orders"),
        F.count_if(F.abs(F.col("diff")) <= 0.01).alias("match"),
        F.count_if(F.col("diff") > 0.01).alias("paid_more_than_items"),
        F.count_if(F.col("diff") < -0.01).alias("paid_less_than_items"),
        F.count_if(F.col("items_total").isNull()).alias("payments_but_no_items"),
        F.count_if(F.col("payments_total").isNull()).alias("items_but_no_payments"),
        F.round(F.percentile_approx(F.abs(F.col("diff")), 0.99), 2).alias("p99_abs_diff"),
    )
)

# COMMAND ----------

display(
    bronze["order_payments"]
    .groupBy("payment_type")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum(money("payment_value")), 2).alias("total_value"),
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

# Geolocation - rows per ZIP prefix and coords outside Brazil
geo = bronze["geolocation"]
lat = F.expr("try_cast(geolocation_lat AS double)")
lng = F.expr("try_cast(geolocation_lng AS double)")
 
display(
    geo.groupBy("geolocation_zip_code_prefix")
    .agg(F.count(F.lit(1)).alias("rows_per_prefix"))
    .agg(
        F.count(F.lit(1)).alias("zip_prefixes"),
        F.min("rows_per_prefix").alias("min_rows"),
        F.percentile_approx("rows_per_prefix", 0.5).alias("median_rows"),
        F.max("rows_per_prefix").alias("max_rows"),
    )
)
 
display(
    geo.agg(
        F.count_if((lat < -34) | (lat > 6) | (lng < -74) | (lng > -34)).alias("outside_brazil"),
        F.countDistinct("geolocation_city").alias("distinct_city_raw"),
        F.countDistinct(F.lower(F.trim("geolocation_city"))).alias("distinct_city_normalised"),
        F.countDistinct("geolocation_state").alias("distinct_states"),
    )
)

# COMMAND ----------

# Do ZIP prefixes disagree on their state?
display(
    geo.groupBy("geolocation_zip_code_prefix")
    .agg(F.countDistinct("geolocation_state").alias("states"))
    .agg(
        F.count(F.lit(1)).alias("prefixes"),
        F.count_if(F.col("states") > 1).alias("prefixes_with_multiple_states"),
    )
)

# COMMAND ----------

# Products - missing categories and translation gaps
display(
    bronze["products"]
    .join(
        bronze["product_category_translation"].select(
            "product_category_name", "product_category_name_english"
        ),
        "product_category_name",
        "left",
    )
    .agg(
        F.count(F.lit(1)).alias("products"),
        F.count_if(F.col("product_category_name").isNull()).alias("no_category"),
        F.count_if(
            F.col("product_category_name").isNotNull()
            & F.col("product_category_name_english").isNull()
        ).alias("category_without_translation"),
        F.countDistinct("product_category_name").alias("distinct_categories"),
    )
)

# COMMAND ----------

# Reviews - duplicate review ids and orders with several reviews
reviews = bronze["order_reviews"]
display(
    reviews.groupBy("review_id")
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("review_score").alias("scores"),
    )
    .agg(
        F.count(F.lit(1)).alias("distinct_review_ids"),
        F.count_if(F.col("rows") > 1).alias("review_ids_with_multiple_rows"),
        F.count_if(F.col("orders") > 1).alias("review_ids_spanning_multiple_orders"),
        F.count_if(F.col("scores") > 1).alias("review_ids_with_conflicting_scores"),
    )
)
 
display(
    reviews.groupBy("order_id")
    .agg(F.count(F.lit(1)).alias("reviews_per_order"))
    .groupBy("reviews_per_order")
    .agg(F.count(F.lit(1)).alias("orders"))
    .orderBy("reviews_per_order")
)