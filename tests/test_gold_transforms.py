"""Unit tests for the silver -> gold transforms.

Focused on the traps: the customer regrain, non-additive measures kept off the item fact, orders
with no items surviving, and reviews that cover several orders not fanning the fact out.
"""

import datetime as dt
from decimal import Decimal as D

import pytest
from pyspark.sql import functions as F

from pipeline.transforms.gold import (
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_fact_order,
    build_fact_order_item,
    surrogate_key,
)


def ts(day, hour=10):
    return dt.datetime(2018, 1, day, hour, 0)


@pytest.fixture
def silver(spark):
    """Three people, four orders. o4 has no items (unavailable); o1 has two items; person_a
    ordered twice and moved; r1 covers o1 and o2."""
    customers = spark.createDataFrame(
        [
            ("c1", "person_a", "01001"),
            ("c2", "person_a", "02002"),  # same person, later order, different location
            ("c3", "person_b", "03003"),
            ("c4", "person_c", "03003"),
        ],
        "customer_id string, customer_unique_id string, zip_code_prefix string",
    )
    orders = spark.createDataFrame(
        [
            ("o1", "c1", "delivered", ts(1), ts(5), ts(8)),
            ("o2", "c2", "delivered", ts(10), ts(20), ts(15)),  # late
            ("o3", "c3", "delivered", ts(12), None, ts(20)),  # never delivered
            ("o4", "c4", "unavailable", ts(14), None, ts(22)),  # no items
        ],
        "order_id string, customer_id string, order_status string, purchased_at timestamp, "
        "delivered_to_customer_at timestamp, estimated_delivery_at timestamp",
    )
    order_items = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", D("100.00"), D("10.00")),
            ("o1", 2, "p2", "s1", D("50.00"), D("5.00")),
            ("o2", 1, "p1", "s2", D("200.00"), D("20.00")),
            ("o3", 1, "p2", "s1", D("30.00"), D("3.00")),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "price decimal(10,2), freight_value decimal(10,2)",
    )
    order_payments = spark.createDataFrame(
        [
            ("o1", 1, "credit_card", 3, D("160.00")),
            ("o2", 1, "voucher", 1, D("20.00")),
            ("o2", 2, "credit_card", 1, D("200.00")),
            ("o3", 1, "boleto", 1, D("33.00")),
            ("o4", 1, "credit_card", 1, D("99.00")),  # paid, then unavailable
        ],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value decimal(10,2)",
    )
    order_reviews = spark.createDataFrame(
        [("r1", "o1"), ("r1", "o2"), ("r2", "o3")], "review_id string, order_id string"
    )
    reviews = spark.createDataFrame([("r1", 5), ("r2", 1)], "review_id string, review_score int")
    geography = spark.createDataFrame(
        [
            ("01001", "sao paulo", "SP", -23.5, -46.6),
            ("02002", "campinas", "SP", -22.9, -47.1),
            ("03003", "rio de janeiro", "RJ", -22.9, -43.2),
        ],
        "zip_code_prefix string, city string, state string, latitude double, longitude double",
    )
    return dict(
        customers=customers,
        orders=orders,
        order_items=order_items,
        order_payments=order_payments,
        order_reviews=order_reviews,
        reviews=reviews,
        geography=geography,
    )


@pytest.fixture
def fact_order(silver):
    return build_fact_order(
        silver["orders"],
        silver["customers"],
        silver["order_items"],
        silver["order_payments"],
        silver["order_reviews"],
        silver["reviews"],
    )


# --- keys --------------------------------------------------------------------------------------


def test_surrogate_key_is_deterministic(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], "v string")
    keys = [r[0] for r in df.select(surrogate_key("v")).collect()]
    assert keys[0] == keys[1] != keys[2]


# --- fact_order --------------------------------------------------------------------------------


def test_every_order_appears_including_those_with_no_items(fact_order):
    assert fact_order.count() == 4
    row = fact_order.filter(F.col("order_id") == "o4").first()
    assert row.item_value == 0 and row.item_count == 0
    assert row.amount_paid == D("99.00")  # paid but never fulfilled
    assert row.is_fulfilled is False


def test_order_value_is_items_plus_freight(fact_order):
    row = fact_order.filter(F.col("order_id") == "o1").first()
    assert row.item_value == D("150.00")
    assert row.freight_value == D("15.00")
    assert row.order_value == D("165.00")


def test_review_covering_two_orders_does_not_fan_out(fact_order):
    assert fact_order.filter(F.col("order_id").isin("o1", "o2")).count() == 2
    assert fact_order.filter(F.col("order_id") == "o1").first().review_score == 5


def test_delivery_measures(fact_order):
    on_time = fact_order.filter(F.col("order_id") == "o1").first()
    assert on_time.delivery_days == 4
    assert on_time.is_late is False

    late = fact_order.filter(F.col("order_id") == "o2").first()
    assert late.is_late is True

    undelivered = fact_order.filter(F.col("order_id") == "o3").first()
    assert undelivered.delivery_days is None
    assert undelivered.is_late is None  # not False -- we don't know


def test_primary_payment_type_is_the_largest_instrument(fact_order):
    row = fact_order.filter(F.col("order_id") == "o2").first()
    assert row.primary_payment_type == "credit_card"  # 200.00 beats the 20.00 voucher
    assert row.amount_paid == D("220.00")


# --- fact_order_item ---------------------------------------------------------------------------


def test_item_fact_is_one_row_per_line(silver):
    fact = build_fact_order_item(silver["order_items"], silver["orders"], silver["customers"])
    assert fact.count() == 4
    assert fact.agg(F.sum("item_value")).first()[0] == D("380.00")


def test_item_and_order_facts_agree_on_total_item_value(silver, fact_order):
    item_fact = build_fact_order_item(silver["order_items"], silver["orders"], silver["customers"])
    assert (
        item_fact.agg(F.sum("item_value")).first()[0]
        == fact_order.agg(F.sum("item_value")).first()[0]
    )


# --- dim_customer ------------------------------------------------------------------------------


def test_dim_customer_is_one_row_per_person(silver, fact_order):
    dim = build_dim_customer(silver["customers"], silver["orders"], silver["geography"], fact_order)
    assert dim.count() == 3  # not 4 -- person_a placed two orders


def test_location_comes_from_the_most_recent_order(silver, fact_order):
    dim = build_dim_customer(silver["customers"], silver["orders"], silver["geography"], fact_order)
    row = dim.filter(F.col("customer_unique_id") == "person_a").first()
    assert row.customer_city == "campinas"  # o2 (day 10), not o1 (day 1)


def test_lifetime_measures_exclude_unfulfilled_orders(silver, fact_order):
    dim = build_dim_customer(silver["customers"], silver["orders"], silver["geography"], fact_order)
    person_a = dim.filter(F.col("customer_unique_id") == "person_a").first()
    assert person_a.lifetime_orders == 2
    assert person_a.is_repeat_customer is True
    assert person_a.lifetime_revenue == D("385.00")  # 165 + 220

    person_c = dim.filter(F.col("customer_unique_id") == "person_c").first()
    assert person_c.lifetime_orders == 1
    assert person_c.lifetime_fulfilled_orders == 0
    assert person_c.lifetime_revenue == 0  # paid 99.00, but the order was unavailable


# --- dim_date and dim_product ------------------------------------------------------------------


def test_dim_date_has_no_gaps(silver):
    dim = build_dim_date(silver["orders"])
    assert dim.count() == 14  # 1 Jan to 14 Jan inclusive
    assert dim.filter(F.col("date_key") == 20180107).count() == 1  # no orders that day


def test_dim_product_falls_back_when_translation_is_missing(spark):
    products = spark.createDataFrame(
        [
            ("p1", "cama_mesa_banho", 100, 2, 10, 10, 10),
            ("p2", "sem_traducao", 50, 1, 5, 5, 5),
            ("p3", None, 10, 0, 1, 1, 1),
        ],
        "product_id string, category_name string, weight_g int, photos_qty int, "
        "length_cm int, height_cm int, width_cm int",
    )
    categories = spark.createDataFrame(
        [("cama_mesa_banho", "bed_bath_table"), ("sem_traducao", None)],
        "category_name string, category_name_english string",
    )
    dim = {r.product_id: r.category for r in build_dim_product(products, categories).collect()}
    assert dim == {"p1": "bed_bath_table", "p2": "sem_traducao", "p3": "unknown"}
