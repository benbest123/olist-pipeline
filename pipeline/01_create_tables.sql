-- Databricks notebook source
CREATE WIDGET TEXT catalog DEFAULT 'elio_dev';

-- COMMAND ----------

-- Silver tables, implementing the 3NF model in data_model.md.
--
-- Keys: silver uses the source's natural keys (profiling confirmed all are unique and non-null).
--
-- Created parents-first so the FK references resolve. IF NOT EXISTS means schema changes need a
-- DROP SCHEMA silver CASCADE first.

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA silver;

-- COMMAND ----------

-- Built from the zip prefixes customers and sellers actually reference, so every FK resolves.
-- Coordinates are null for the 285 prefixes missing from the geolocation source.
CREATE TABLE IF NOT EXISTS geography (
  zip_code_prefix STRING NOT NULL COMMENT 'First five digits of the CEP; STRING to keep leading zeros',
  city            STRING COMMENT 'Most common city name for this prefix',
  state           STRING,
  latitude        DOUBLE COMMENT 'Median of the geolocation points for this prefix',
  longitude       DOUBLE,
  _source         STRING NOT NULL,
  _loaded_at      TIMESTAMP NOT NULL,
  CONSTRAINT geography_pk PRIMARY KEY (zip_code_prefix) RELY
)
COMMENT 'One row per zip-code prefix referenced by a customer or seller';

-- COMMAND ----------

-- Split out of products so the translation isn't repeated across 32k rows.
CREATE TABLE IF NOT EXISTS product_categories (
  category_name         STRING NOT NULL,
  category_name_english STRING COMMENT 'Null for 2 categories missing from the source translation table',
  _source               STRING NOT NULL,
  _loaded_at            TIMESTAMP NOT NULL,
  CONSTRAINT product_categories_pk PRIMARY KEY (category_name) RELY
)
COMMENT 'One row per product category';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS customers (
  customer_id        STRING NOT NULL COMMENT 'Issued per order -- identifies an order-customer, not a person',
  customer_unique_id STRING NOT NULL COMMENT 'The real person; group by this for customer-level analysis',
  zip_code_prefix    STRING NOT NULL,
  _source            STRING NOT NULL,
  _loaded_at         TIMESTAMP NOT NULL,
  CONSTRAINT customers_pk PRIMARY KEY (customer_id) RELY,
  CONSTRAINT customers_geography_fk FOREIGN KEY (zip_code_prefix) REFERENCES geography
)
COMMENT 'One row per customer record per order: 99,441 records, 96,096 distinct people';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sellers (
  seller_id       STRING NOT NULL,
  zip_code_prefix STRING NOT NULL,
  _source         STRING NOT NULL,
  _loaded_at      TIMESTAMP NOT NULL,
  CONSTRAINT sellers_pk PRIMARY KEY (seller_id) RELY,
  CONSTRAINT sellers_geography_fk FOREIGN KEY (zip_code_prefix) REFERENCES geography
)
COMMENT 'One row per marketplace seller';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS products (
  product_id         STRING NOT NULL,
  category_name      STRING COMMENT 'Null for 610 products with no category in the source',
  name_length        INT COMMENT 'Source column is misspelled product_name_lenght',
  description_length INT,
  photos_qty         INT,
  weight_g           INT,
  length_cm          INT,
  height_cm          INT,
  width_cm           INT,
  _source            STRING NOT NULL,
  _loaded_at         TIMESTAMP NOT NULL,
  CONSTRAINT products_pk PRIMARY KEY (product_id) RELY,
  CONSTRAINT products_category_fk FOREIGN KEY (category_name) REFERENCES product_categories
)
COMMENT 'One row per product';

-- COMMAND ----------

-- Only purchased_at is NOT NULL: the other timestamps are legitimately absent for orders that
-- were never approved, shipped or delivered. The status CHECK also catches source schema drift.
CREATE TABLE IF NOT EXISTS orders (
  order_id                 STRING NOT NULL,
  customer_id              STRING NOT NULL,
  order_status             STRING NOT NULL,
  purchased_at             TIMESTAMP NOT NULL,
  approved_at              TIMESTAMP COMMENT 'Null for 160 orders, including 14 delivered ones',
  delivered_to_carrier_at  TIMESTAMP,
  delivered_to_customer_at TIMESTAMP,
  estimated_delivery_at    TIMESTAMP COMMENT 'Delivery date promised at purchase',
  _source                  STRING NOT NULL,
  _loaded_at               TIMESTAMP NOT NULL,
  CONSTRAINT orders_pk PRIMARY KEY (order_id) RELY,
  CONSTRAINT orders_customers_fk FOREIGN KEY (customer_id) REFERENCES customers
)
CLUSTER BY (purchased_at)
COMMENT 'One row per order. Clustered on purchase date -- most queries filter or group by it';

-- COMMAND ----------

-- Junction resolving orders <-> products, with its own price and freight, so it's an entity.
CREATE TABLE IF NOT EXISTS order_items (
  order_id            STRING NOT NULL,
  order_item_id       INT NOT NULL COMMENT 'Line sequence within the order, from 1',
  product_id          STRING NOT NULL,
  seller_id           STRING NOT NULL,
  shipping_limit_date TIMESTAMP COMMENT 'Seller deadline to hand the item to the carrier',
  price               DECIMAL(10,2) NOT NULL COMMENT 'BRL, excluding freight',
  freight_value       DECIMAL(10,2) NOT NULL COMMENT 'BRL',
  _source             STRING NOT NULL,
  _loaded_at          TIMESTAMP NOT NULL,
  CONSTRAINT order_items_pk PRIMARY KEY (order_id, order_item_id) RELY,
  CONSTRAINT order_items_orders_fk FOREIGN KEY (order_id) REFERENCES orders,
  CONSTRAINT order_items_products_fk FOREIGN KEY (product_id) REFERENCES products,
  CONSTRAINT order_items_sellers_fk FOREIGN KEY (seller_id) REFERENCES sellers
)
CLUSTER BY (order_id)
COMMENT 'One row per line item';

-- COMMAND ----------

-- Payment method detail only -- order value comes from order_items (they reconcile for 98.9%
-- of orders; the rest are the 775 orders with no items).
CREATE TABLE IF NOT EXISTS order_payments (
  order_id             STRING NOT NULL,
  payment_sequential   INT NOT NULL COMMENT 'Sequence where an order is settled with several instruments',
  payment_type         STRING NOT NULL,
  payment_installments INT,
  payment_value        DECIMAL(10,2) NOT NULL COMMENT 'BRL',
  _source              STRING NOT NULL,
  _loaded_at           TIMESTAMP NOT NULL,
  CONSTRAINT order_payments_pk PRIMARY KEY (order_id, payment_sequential) RELY,
  CONSTRAINT order_payments_orders_fk FOREIGN KEY (order_id) REFERENCES orders
)
COMMENT 'One row per payment instrument used on an order';

-- COMMAND ----------

-- 789 reviews cover more than one order, with no conflicting scores, so score and comment depend
-- on review_id alone -- hence a separate entity plus the order_reviews junction below.
CREATE TABLE IF NOT EXISTS reviews (
  review_id              STRING NOT NULL,
  review_score           INT NOT NULL,
  review_comment_title   STRING COMMENT 'Free text, usually absent',
  review_comment_message STRING COMMENT 'Free text, may contain line breaks',
  created_at             TIMESTAMP COMMENT 'When the review survey was sent',
  answered_at            TIMESTAMP COMMENT 'When the customer submitted it',
  _source                STRING NOT NULL,
  _loaded_at             TIMESTAMP NOT NULL,
  CONSTRAINT reviews_pk PRIMARY KEY (review_id) RELY
)
COMMENT 'One row per review';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS order_reviews (
  review_id  STRING NOT NULL,
  order_id   STRING NOT NULL,
  _source    STRING NOT NULL,
  _loaded_at TIMESTAMP NOT NULL,
  CONSTRAINT order_reviews_pk PRIMARY KEY (review_id, order_id) RELY,
  CONSTRAINT order_reviews_reviews_fk FOREIGN KEY (review_id) REFERENCES reviews,
  CONSTRAINT order_reviews_orders_fk FOREIGN KEY (order_id) REFERENCES orders
)
COMMENT 'Links reviews to the orders they cover';

-- COMMAND ----------

-- CHECK constraints can only be added after creation\
-- DROP ... IF EXISTS first keeps this cell re-runnable.

ALTER TABLE products DROP CONSTRAINT IF EXISTS products_dimensions_non_negative;
ALTER TABLE products ADD CONSTRAINT products_dimensions_non_negative CHECK (
  coalesce(weight_g, 0) >= 0 AND coalesce(length_cm, 0) >= 0
  AND coalesce(height_cm, 0) >= 0 AND coalesce(width_cm, 0) >= 0
  AND coalesce(photos_qty, 0) >= 0
);

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_known;
ALTER TABLE orders ADD CONSTRAINT orders_status_known CHECK (
  order_status IN ('created', 'approved', 'invoiced', 'processing',
                   'shipped', 'delivered', 'unavailable', 'canceled')
);

ALTER TABLE order_items DROP CONSTRAINT IF EXISTS order_items_amounts_non_negative;
ALTER TABLE order_items ADD CONSTRAINT order_items_amounts_non_negative
  CHECK (price >= 0 AND freight_value >= 0);

ALTER TABLE order_items DROP CONSTRAINT IF EXISTS order_items_sequence_positive;
ALTER TABLE order_items ADD CONSTRAINT order_items_sequence_positive
  CHECK (order_item_id >= 1);

ALTER TABLE order_payments DROP CONSTRAINT IF EXISTS order_payments_amounts_non_negative;
ALTER TABLE order_payments ADD CONSTRAINT order_payments_amounts_non_negative
  CHECK (payment_value >= 0 AND coalesce(payment_installments, 0) >= 0);

ALTER TABLE order_payments DROP CONSTRAINT IF EXISTS order_payments_sequence_positive;
ALTER TABLE order_payments ADD CONSTRAINT order_payments_sequence_positive
  CHECK (payment_sequential >= 1);

ALTER TABLE reviews DROP CONSTRAINT IF EXISTS reviews_score_in_range;
ALTER TABLE reviews ADD CONSTRAINT reviews_score_in_range
  CHECK (review_score BETWEEN 1 AND 5);