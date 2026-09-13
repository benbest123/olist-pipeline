"""Bronze -> silver transforms.

One function per silver table. Each takes the bronze DataFrames it needs and returns a DataFrame
matching the corresponding table in 01_create_tables.sql.

"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Order statuses that never carry revenue, and the timestamp columns renamed on the way through.
ORDER_TIMESTAMPS = {
    "order_purchase_timestamp": "purchased_at",
    "order_approved_at": "approved_at",
    "order_delivered_carrier_date": "delivered_to_carrier_at",
    "order_delivered_customer_date": "delivered_to_customer_at",
    "order_estimated_delivery_date": "estimated_delivery_at",
}

MONEY = "decimal(10,2)"


def text(column: str) -> Column:
    """Free-text column.

    Spark's CSV reader already maps unquoted empty fields to null, and profiling found no blanks
    anywhere in bronze. A *quoted* empty field ("") would survive as an empty string, which is only
    plausible in the review comments, so those are normalised to null and nothing else is.
    """
    return F.nullif(F.trim(F.col(column)), F.lit(""))


def with_metadata(df: DataFrame, source: str) -> DataFrame:
    """Stamp the bronze lineage columns required on every silver table."""
    return df.withColumn("_source", F.lit(source))


def dedupe(df: DataFrame, keys: list[str], order_by: list[Column] | None = None) -> DataFrame:
    """Keep one row per key.

    Deterministic by design: ties are broken on `order_by` (defaulting to the remaining columns in
    name order), so a re-run picks the same row. This is what makes the RELY on the silver primary
    keys safe.
    """
    if order_by is None:
        order_by = [F.col(c).asc_nulls_last() for c in sorted(df.columns) if c not in keys]
    ranked = df.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy(*order_by)))
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def build_geography(customers: DataFrame, sellers: DataFrame, geolocation: DataFrame) -> DataFrame:
    """One row per zip prefix referenced by a customer or seller.

    City and state come from customers/sellers, whose values are already clean; geolocation_city
    holds 8,011 spellings for 5,938 real cities, mostly accent variants. Geolocation contributes
    coordinates only, median-averaged, and is left-joined so the 285 prefixes missing from it keep
    null coordinates rather than dropping out.
    """
    referenced = customers.select(
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
        F.col("_loaded_at"),
    ).unionByName(
        sellers.select(
            F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
            F.col("seller_city").alias("city"),
            F.col("seller_state").alias("state"),
            F.col("_loaded_at"),
        )
    )

    # 136 prefixes disagree on city and 33 on state - a five-digit prefix can straddle a
    # boundary. Resolve city and state as a pair so they stay consistent, most common wins,
    # alphabetical tie-break for determinism.
    counted = referenced.groupBy("zip_code_prefix", "city", "state").agg(
        F.count(F.lit(1)).alias("n"), F.max("_loaded_at").alias("_loaded_at")
    )
    resolved = dedupe(
        counted,
        ["zip_code_prefix"],
        [F.col("n").desc(), F.col("city").asc_nulls_last(), F.col("state").asc_nulls_last()],
    ).drop("n")

    coordinates = geolocation.groupBy(
        F.col("geolocation_zip_code_prefix").alias("zip_code_prefix")
    ).agg(
        F.median(F.col("geolocation_lat").cast("double")).alias("latitude"),
        F.median(F.col("geolocation_lng").cast("double")).alias("longitude"),
    )

    return with_metadata(
        resolved.join(coordinates, "zip_code_prefix", "left"),
        "bronze.customers, bronze.sellers, bronze.geolocation",
    ).select("zip_code_prefix", "city", "state", "latitude", "longitude", "_source", "_loaded_at")


def build_product_categories(products: DataFrame, translation: DataFrame) -> DataFrame:
    """One row per category.

    Driven by the categories actually used by products rather than by the translation file, so the
    13 products in 2 untranslated categories still satisfy the foreign key - they just get a null
    English name.
    """
    used = products.select(
        F.col("product_category_name").alias("category_name"), F.col("_loaded_at")
    ).filter(F.col("category_name").isNotNull())

    english = translation.select(
        F.col("product_category_name").alias("category_name"),
        F.col("product_category_name_english").alias("category_name_english"),
    )

    return with_metadata(
        dedupe(used, ["category_name"]).join(english, "category_name", "left"),
        "bronze.products, bronze.product_category_translation",
    ).select("category_name", "category_name_english", "_source", "_loaded_at")


def build_customers(customers: DataFrame) -> DataFrame:
    """One row per customer record per order. customer_unique_id identifies the real person."""
    renamed = customers.select(
        F.col("customer_id").alias("customer_id"),
        F.col("customer_unique_id").alias("customer_unique_id"),
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("_loaded_at"),
    )
    return with_metadata(dedupe(renamed, ["customer_id"]), "bronze.customers").select(
        "customer_id", "customer_unique_id", "zip_code_prefix", "_source", "_loaded_at"
    )


def build_sellers(sellers: DataFrame) -> DataFrame:
    renamed = sellers.select(
        F.col("seller_id").alias("seller_id"),
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("_loaded_at"),
    )
    return with_metadata(dedupe(renamed, ["seller_id"]), "bronze.sellers").select(
        "seller_id", "zip_code_prefix", "_source", "_loaded_at"
    )


def build_products(products: DataFrame) -> DataFrame:
    """Renames the misspelled source columns product_name_lenght / product_description_lenght."""
    renamed = products.select(
        F.col("product_id").alias("product_id"),
        F.col("product_category_name").alias("category_name"),
        F.col("product_name_lenght").cast("int").alias("name_length"),
        F.col("product_description_lenght").cast("int").alias("description_length"),
        F.col("product_photos_qty").cast("int").alias("photos_qty"),
        F.col("product_weight_g").cast("int").alias("weight_g"),
        F.col("product_length_cm").cast("int").alias("length_cm"),
        F.col("product_height_cm").cast("int").alias("height_cm"),
        F.col("product_width_cm").cast("int").alias("width_cm"),
        F.col("_loaded_at"),
    )
    return with_metadata(dedupe(renamed, ["product_id"]), "bronze.products").select(
        "product_id",
        "category_name",
        "name_length",
        "description_length",
        "photos_qty",
        "weight_g",
        "length_cm",
        "height_cm",
        "width_cm",
        "_source",
        "_loaded_at",
    )


def build_orders(orders: DataFrame) -> DataFrame:
    """Timestamps other than purchased_at stay nullable: they are legitimately absent for orders
    that were never approved, shipped or delivered."""
    renamed = orders.select(
        F.col("order_id").alias("order_id"),
        F.col("customer_id").alias("customer_id"),
        F.lower(F.trim(F.col("order_status"))).alias("order_status"),
        *[F.col(src).cast("timestamp").alias(dst) for src, dst in ORDER_TIMESTAMPS.items()],
        F.col("_loaded_at"),
    )
    return with_metadata(dedupe(renamed, ["order_id"]), "bronze.orders").select(
        "order_id",
        "customer_id",
        "order_status",
        *ORDER_TIMESTAMPS.values(),
        "_source",
        "_loaded_at",
    )


def build_order_items(order_items: DataFrame) -> DataFrame:
    """One row per line item. Olist apportions freight across the items of an order, so summing
    freight_value over an order gives its total freight rather than a multiple of it."""
    renamed = order_items.select(
        F.col("order_id").alias("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.col("product_id").alias("product_id"),
        F.col("seller_id").alias("seller_id"),
        F.col("shipping_limit_date").cast("timestamp").alias("shipping_limit_date"),
        F.col("price").cast(MONEY).alias("price"),
        F.col("freight_value").cast(MONEY).alias("freight_value"),
        F.col("_loaded_at"),
    )
    return with_metadata(
        dedupe(renamed, ["order_id", "order_item_id"]), "bronze.order_items"
    ).select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "_source",
        "_loaded_at",
    )


def build_order_payments(order_payments: DataFrame) -> DataFrame:
    """Payment method detail. Order value comes from order_items, which can be attributed to a
    product, category and seller; payment_value only exists at order grain."""
    renamed = order_payments.select(
        F.col("order_id").alias("order_id"),
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
        F.col("payment_installments").cast("int").alias("payment_installments"),
        F.col("payment_value").cast(MONEY).alias("payment_value"),
        F.col("_loaded_at"),
    )
    return with_metadata(
        dedupe(renamed, ["order_id", "payment_sequential"]), "bronze.order_payments"
    ).select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "_source",
        "_loaded_at",
    )


def build_reviews(order_reviews: DataFrame) -> DataFrame:
    """The review itself, one row per review_id.

    789 review_ids appear against more than one order with no conflicting scores, so the review
    attributes depend on review_id alone and the link to orders lives in build_order_reviews.
    """
    renamed = order_reviews.select(
        F.col("review_id").alias("review_id"),
        F.col("review_score").cast("int").alias("review_score"),
        text("review_comment_title").alias("review_comment_title"),
        text("review_comment_message").alias("review_comment_message"),
        F.col("review_creation_date").cast("timestamp").alias("created_at"),
        F.col("review_answer_timestamp").cast("timestamp").alias("answered_at"),
        F.col("_loaded_at"),
    )
    return with_metadata(dedupe(renamed, ["review_id"]), "bronze.order_reviews").select(
        "review_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "created_at",
        "answered_at",
        "_source",
        "_loaded_at",
    )


def build_order_reviews(order_reviews: DataFrame) -> DataFrame:
    """The review-to-order junction: keys only, so a review covering several orders is
    stored once."""
    links = order_reviews.select(
        F.col("review_id").alias("review_id"),
        F.col("order_id").alias("order_id"),
        F.col("_loaded_at"),
    )
    return with_metadata(dedupe(links, ["review_id", "order_id"]), "bronze.order_reviews").select(
        "review_id", "order_id", "_source", "_loaded_at"
    )
