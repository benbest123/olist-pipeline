"""Unit tests for the bronze -> silver transforms.

Each test builds a tiny bronze-shaped DataFrame (all STRING, as bronze is) and checks one
behaviour of the transform. No Databricks needed - these run in CI on a local SparkSession.
"""

import datetime as dt

import pytest
from pyspark.sql import functions as F

from pipeline.transforms.silver import (
    build_customers,
    build_geography,
    build_order_reviews,
    build_orders,
    build_product_categories,
    build_reviews,
    dedupe,
    text,
)

LOADED_AT = dt.datetime(2026, 9, 12, 8, 0, 0)


def rows(spark, data, schema):
    return spark.createDataFrame(data, schema)


# --- helpers ---------------------------------------------------------------------------------


def test_text_treats_quoted_blanks_as_missing(spark):
    df = rows(spark, [("a",), ("",), ("   ",), (None,)], "v string")
    got = df.select(text("v").alias("v")).collect()
    assert [r.v for r in got] == ["a", None, None, None]


def test_dedupe_is_deterministic(spark):
    df = rows(spark, [("k", "b"), ("k", "a"), ("j", "c")], "id string, v string")
    first = dedupe(df, ["id"]).orderBy("id").collect()
    second = dedupe(df, ["id"]).orderBy("id").collect()
    assert first == second
    assert [(r.id, r.v) for r in first] == [("j", "c"), ("k", "a")]


# --- geography -------------------------------------------------------------------------------


@pytest.fixture
def geo_inputs(spark):
    customers = rows(
        spark,
        [
            ("c1", "u1", "01001", "sao paulo", "SP", LOADED_AT),
            ("c2", "u2", "01001", "sao paulo", "SP", LOADED_AT),
            ("c3", "u3", "01001", "santana", "SP", LOADED_AT),  # minority spelling, loses
            ("c4", "u4", "99999", "nowhere", "XX", LOADED_AT),  # no coordinates available
        ],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string, _loaded_at timestamp",
    )
    sellers = rows(
        spark,
        [("s1", "02002", "campinas", "SP", LOADED_AT)],
        "seller_id string, seller_zip_code_prefix string, seller_city string, "
        "seller_state string, _loaded_at timestamp",
    )
    geolocation = rows(
        spark,
        [
            ("01001", "-23.5", "-46.6", LOADED_AT),
            ("01001", "-23.7", "-46.8", LOADED_AT),
            ("01001", "-23.9", "-47.0", LOADED_AT),
            ("02002", "-22.9", "-47.1", LOADED_AT),
            ("03003", "-20.0", "-40.0", LOADED_AT),  # never referenced, excluded
        ],
        "geolocation_zip_code_prefix string, geolocation_lat string, geolocation_lng string, "
        "_loaded_at timestamp",
    )
    return customers, sellers, geolocation


def test_geography_has_one_row_per_referenced_prefix(spark, geo_inputs):
    result = build_geography(*geo_inputs)
    assert sorted(r.zip_code_prefix for r in result.collect()) == ["01001", "02002", "99999"]


def test_geography_resolves_conflicting_cities_by_majority(spark, geo_inputs):
    result = build_geography(*geo_inputs)
    city = result.filter(F.col("zip_code_prefix") == "01001").first().city
    assert city == "sao paulo"


def test_geography_takes_median_coordinates(spark, geo_inputs):
    result = build_geography(*geo_inputs)
    row = result.filter(F.col("zip_code_prefix") == "01001").first()
    assert row.latitude == pytest.approx(-23.7)
    assert row.longitude == pytest.approx(-46.8)


def test_geography_keeps_prefixes_missing_from_geolocation(spark, geo_inputs):
    result = build_geography(*geo_inputs)
    row = result.filter(F.col("zip_code_prefix") == "99999").first()
    assert row is not None
    assert row.latitude is None


# --- reviews ---------------------------------------------------------------------------------


@pytest.fixture
def review_rows(spark):
    # r1 covers two orders -- one review, two links.
    return rows(
        spark,
        [
            ("r1", "o1", "5", "great", "arrived early", "2018-01-01", "2018-01-02", LOADED_AT),
            ("r1", "o2", "5", "great", "arrived early", "2018-01-01", "2018-01-02", LOADED_AT),
            ("r2", "o3", "1", None, None, "2018-02-01", "2018-02-03", LOADED_AT),
        ],
        "review_id string, order_id string, review_score string, review_comment_title string, "
        "review_comment_message string, review_creation_date string, "
        "review_answer_timestamp string, _loaded_at timestamp",
    )


def test_reviews_are_stored_once_per_review_id(spark, review_rows):
    result = build_reviews(review_rows)
    assert result.count() == 2
    assert result.filter(F.col("review_id") == "r1").first().review_score == 5


def test_order_reviews_keeps_every_link(spark, review_rows):
    result = build_order_reviews(review_rows)
    assert result.count() == 3
    links = {(r.review_id, r.order_id) for r in result.collect()}
    assert ("r1", "o1") in links and ("r1", "o2") in links


# --- orders and customers --------------------------------------------------------------------


def test_orders_keep_null_timestamps_for_undelivered(spark):
    df = rows(
        spark,
        [("o1", "c1", "canceled", "2018-01-01 10:00:00", None, None, None, None, LOADED_AT)],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp string, order_approved_at string, "
        "order_delivered_carrier_date string, order_delivered_customer_date string, "
        "order_estimated_delivery_date string, _loaded_at timestamp",
    )
    row = build_orders(df).first()
    assert row.purchased_at == dt.datetime(2018, 1, 1, 10, 0)
    assert row.delivered_to_customer_at is None


def test_customers_keep_one_row_per_order_customer(spark):
    df = rows(
        spark,
        [
            ("c1", "person_a", "01001", LOADED_AT),
            ("c2", "person_a", "01001", LOADED_AT),  # same person, second order
        ],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "_loaded_at timestamp",
    )
    result = build_customers(df)
    assert result.count() == 2
    assert result.select("customer_unique_id").distinct().count() == 1


# --- product categories ----------------------------------------------------------------------


def test_untranslated_categories_are_kept_with_null_english(spark):
    products = rows(
        spark,
        [
            ("p1", "cama_mesa_banho", LOADED_AT),
            ("p2", "sem_traducao", LOADED_AT),
            ("p3", None, LOADED_AT),
        ],
        "product_id string, product_category_name string, _loaded_at timestamp",
    )
    translation = rows(
        spark,
        [("cama_mesa_banho", "bed_bath_table")],
        "product_category_name string, product_category_name_english string",
    )
    result = {
        r.category_name: r.category_name_english
        for r in build_product_categories(products, translation).collect()
    }
    assert result == {"cama_mesa_banho": "bed_bath_table", "sem_traducao": None}
