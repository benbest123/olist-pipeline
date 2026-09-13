"""Silver -> gold transforms.

Denormalises the 3NF silver model into a star schema.

Two fact tables, because the data has two natural grains:
  fact_order_item  one row per line item - revenue attributable to a product, category and seller
  fact_order       one row per order     - delivery time, review score and payment, which are
                                           order-grain and would be non-additive on the item fact

Dimensions are SCD Type 1: rebuilt in full each run, no history. With a live source you would keep
Type 2 history on dim_customer and dim_product.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MONEY = "decimal(12,2)"

# Statuses that never represent fulfilled revenue. Kept in gold, not filtered out: the analytics
# queries decide, so the choice is visible in the SQL rather than buried in the pipeline.
UNFULFILLED_STATUSES = ["canceled", "unavailable"]


def surrogate_key(*columns: str) -> Column:
    """Deterministic hash key.

    sha2 of the natural key, so a rebuild reproduces identical keys and the facts can be rebuilt
    independently of the dimensions.
    """
    return F.sha2(F.concat_ws("||", *[F.col(c) for c in columns]), 256)


def date_key(column: str) -> Column:
    """yyyyMMdd integer, the conventional dim_date key: readable in raw fact rows and sortable."""
    return F.date_format(F.col(column), "yyyyMMdd").cast("int")


# --- dimensions --------------------------------------------------------------------------------


def build_dim_date(orders: DataFrame) -> DataFrame:
    """One row per calendar day spanning the order history, with no gaps.

    Generated rather than derived from distinct order dates, so a day with no orders still exists
    and time series do not silently skip it.
    """
    bounds = orders.agg(
        F.to_date(F.min("purchased_at")).alias("start"),
        F.to_date(F.max("purchased_at")).alias("end"),
    ).first()

    days = (
        F.sequence(F.lit(bounds["start"]), F.lit(bounds["end"]), F.expr("INTERVAL 1 DAY"))
        if bounds["start"] is not None
        else F.array()
    )

    return (
        orders.sparkSession.range(1)
        .select(F.explode(days).alias("date"))
        .select(
            date_key("date").alias("date_key"),
            F.col("date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.weekofyear("date").alias("week_of_year"),
            F.dayofweek("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )


def build_dim_product(products: DataFrame, product_categories: DataFrame) -> DataFrame:
    """Flattens products and their category into one dimension."""
    return products.join(product_categories, "category_name", "left").select(
        surrogate_key("product_id").alias("product_key"),
        F.col("product_id"),
        F.coalesce(F.col("category_name_english"), F.col("category_name"), F.lit("unknown")).alias(
            "category"
        ),
        F.col("category_name").alias("category_portuguese"),
        F.col("weight_g"),
        F.col("photos_qty"),
        (F.col("length_cm") * F.col("height_cm") * F.col("width_cm")).alias("volume_cm3"),
    )


def build_dim_seller(sellers: DataFrame, geography: DataFrame) -> DataFrame:
    """Seller with its location flattened in."""
    return sellers.join(geography, "zip_code_prefix", "left").select(
        surrogate_key("seller_id").alias("seller_key"),
        F.col("seller_id"),
        F.col("city").alias("seller_city"),
        F.col("state").alias("seller_state"),
        F.col("zip_code_prefix").alias("seller_zip_code_prefix"),
    )


def build_dim_customer(
    customers: DataFrame, orders: DataFrame, geography: DataFrame, fact_order: DataFrame
) -> DataFrame:
    """One row per real person, keyed on customer_unique_id."""
    per_order = orders.join(customers, "customer_id").select(
        "customer_unique_id", "customer_id", "order_id", "purchased_at", "zip_code_prefix"
    )

    latest = (
        per_order.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("customer_unique_id").orderBy(
                    F.col("purchased_at").desc(), F.col("order_id").asc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select("customer_unique_id", "zip_code_prefix")
    )

    metrics = (
        per_order.join(
            fact_order.select("order_id", "order_value", "is_fulfilled"), "order_id", "left"
        )
        .groupBy("customer_unique_id")
        .agg(
            F.count(F.lit(1)).alias("lifetime_orders"),
            F.count_if(F.col("is_fulfilled")).alias("lifetime_fulfilled_orders"),
            F.coalesce(F.sum(F.when(F.col("is_fulfilled"), F.col("order_value"))), F.lit(0))
            .cast(MONEY)
            .alias("lifetime_revenue"),
            F.min("purchased_at").alias("first_order_at"),
            F.max("purchased_at").alias("last_order_at"),
        )
    )

    return (
        metrics.join(latest, "customer_unique_id")
        .join(geography, "zip_code_prefix", "left")
        .select(
            surrogate_key("customer_unique_id").alias("customer_key"),
            F.col("customer_unique_id"),
            F.col("city").alias("customer_city"),
            F.col("state").alias("customer_state"),
            F.col("zip_code_prefix").alias("customer_zip_code_prefix"),
            F.col("latitude").alias("customer_latitude"),
            F.col("longitude").alias("customer_longitude"),
            F.col("first_order_at"),
            F.col("last_order_at"),
            F.col("lifetime_orders"),
            F.col("lifetime_fulfilled_orders"),
            F.col("lifetime_revenue"),
            (F.col("lifetime_orders") > 1).alias("is_repeat_customer"),
        )
    )


# --- facts -------------------------------------------------------------------------------------


def build_fact_order(
    orders: DataFrame,
    customers: DataFrame,
    order_items: DataFrame,
    order_payments: DataFrame,
    order_reviews: DataFrame,
    reviews: DataFrame,
) -> DataFrame:
    """One row per order."""
    items = order_items.groupBy("order_id").agg(
        F.sum("price").cast(MONEY).alias("item_value"),
        F.sum("freight_value").cast(MONEY).alias("freight_value"),
        F.count(F.lit(1)).alias("item_count"),
        F.countDistinct("product_id").alias("distinct_product_count"),
        F.countDistinct("seller_id").alias("distinct_seller_count"),
    )

    payments = order_payments.groupBy("order_id").agg(
        F.sum("payment_value").cast(MONEY).alias("amount_paid"),
        F.max("payment_installments").alias("max_installments"),
        # Payment type of the largest instrument used on the order.
        F.max(F.struct(F.col("payment_value"), F.col("payment_type")))["payment_type"].alias(
            "primary_payment_type"
        ),
    )

    # A review can cover several orders, so aggregate to order grain before joining or the join
    # would fan the order rows out.
    order_review = (
        order_reviews.join(reviews, "review_id")
        .groupBy("order_id")
        .agg(
            F.avg("review_score").cast("decimal(3,2)").alias("review_score"),
            F.count(F.lit(1)).alias("review_count"),
        )
    )

    return (
        orders.join(customers.select("customer_id", "customer_unique_id"), "customer_id")
        .join(items, "order_id", "left")
        .join(payments, "order_id", "left")
        .join(order_review, "order_id", "left")
        .select(
            F.col("order_id"),  # degenerate dimension: grouping and drill-back, no dim table
            surrogate_key("customer_unique_id").alias("customer_key"),
            date_key("purchased_at").alias("purchase_date_key"),
            F.col("purchased_at"),
            F.col("order_status"),
            (~F.col("order_status").isin(UNFULFILLED_STATUSES)).alias("is_fulfilled"),
            F.coalesce(F.col("item_value"), F.lit(0)).cast(MONEY).alias("item_value"),
            F.coalesce(F.col("freight_value"), F.lit(0)).cast(MONEY).alias("freight_value"),
            (
                F.coalesce(F.col("item_value"), F.lit(0))
                + F.coalesce(F.col("freight_value"), F.lit(0))
            )
            .cast(MONEY)
            .alias("order_value"),
            F.col("amount_paid").cast(MONEY),
            F.coalesce(F.col("item_count"), F.lit(0)).alias("item_count"),
            F.coalesce(F.col("distinct_product_count"), F.lit(0)).alias("distinct_product_count"),
            F.coalesce(F.col("distinct_seller_count"), F.lit(0)).alias("distinct_seller_count"),
            F.col("primary_payment_type"),
            F.col("max_installments"),
            F.col("review_score"),
            F.coalesce(F.col("review_count"), F.lit(0)).alias("review_count"),
            F.datediff("delivered_to_customer_at", "purchased_at").alias("delivery_days"),
            F.datediff("estimated_delivery_at", "delivered_to_customer_at").alias("days_early"),
            # Null rather than false where delivery never happened - a boolean would quietly
            # report undelivered orders as on time.
            F.when(
                F.col("delivered_to_customer_at").isNotNull(),
                F.col("delivered_to_customer_at") > F.col("estimated_delivery_at"),
            ).alias("is_late"),
        )
    )


def build_fact_order_item(
    order_items: DataFrame, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """One row per line item"""
    return (
        order_items.join(orders, "order_id")
        .join(customers.select("customer_id", "customer_unique_id"), "customer_id")
        .select(
            F.col("order_id"),
            F.col("order_item_id"),
            surrogate_key("customer_unique_id").alias("customer_key"),
            surrogate_key("product_id").alias("product_key"),
            surrogate_key("seller_id").alias("seller_key"),
            date_key("purchased_at").alias("purchase_date_key"),
            F.col("purchased_at"),
            F.col("order_status"),
            (~F.col("order_status").isin(UNFULFILLED_STATUSES)).alias("is_fulfilled"),
            F.col("price").cast(MONEY).alias("item_value"),
            F.col("freight_value").cast(MONEY),
            (F.col("price") + F.col("freight_value")).cast(MONEY).alias("gross_item_value"),
        )
    )
