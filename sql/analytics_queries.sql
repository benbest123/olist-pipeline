-- Analytics queries against the gold star schema.
--
-- Conventions applied throughout:
--   * Fulfilled orders only i.e. not 'cancelled' or 'unavailable' status
--   * "revenue" means order_value (item price plus freight). This matches how
--     dim_customer.lifetime_revenue is defined in gold.
--   * Time series are windowed to 2017-01..2018-08. The tails either side are nearly empty.

USE CATALOG elio_dev;   -- switch to elio_prod to run against production

-- COMMAND ----------

-- Q1a. Top 10 products by revenue.
WITH product_revenue AS (
    SELECT
        p.product_id,
        p.category,
        SUM(f.gross_item_value) AS revenue,
        SUM(f.item_value) AS revenue_excl_freight,
        COUNT(*) AS units_sold,
        COUNT(DISTINCT f.order_id) AS orders,
        COUNT(DISTINCT f.customer_key) AS customers,
        ROUND(AVG(f.item_value), 2) AS avg_unit_price
    FROM gold.fact_order_item f
    JOIN gold.dim_product p ON f.product_key = p.product_key
    WHERE f.is_fulfilled = true
    GROUP BY p.product_id, p.category
)
SELECT
    product_id,
    category,
    revenue,
    revenue_excl_freight,
    units_sold,
    orders,
    customers,
    avg_unit_price
FROM product_revenue
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

-- Q1b. Top 10 categories by revenue, with each category's share of the total.
WITH category_revenue AS (
    SELECT
        p.category,
        SUM(f.gross_item_value) AS revenue,
        SUM(f.item_value) AS revenue_excl_freight,
        COUNT(*) AS units_sold,
        COUNT(DISTINCT f.order_id) AS orders,
        COUNT(DISTINCT f.customer_key) AS customers,
        ROUND(AVG(f.item_value), 2) AS avg_unit_price
    FROM gold.fact_order_item f
    JOIN gold.dim_product p ON f.product_key = p.product_key
    WHERE f.is_fulfilled = true
    GROUP BY p.category
)
SELECT
    category,
    revenue,
    revenue_excl_freight,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS pct_of_revenue,
    ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER (), 1)
        AS cumulative_pct,
    units_sold,
    orders,
    customers,
    avg_unit_price
FROM category_revenue
ORDER BY revenue DESC
LIMIT 10;

-- COMMAND ----------

-- Q1c. Top 10 customers by lifetime revenue.
SELECT
    customer_unique_id,
    customer_state,
    customer_city,
    lifetime_revenue,
    lifetime_fulfilled_orders,
    ROUND(lifetime_revenue / NULLIF(lifetime_fulfilled_orders, 0), 2) AS avg_order_value,
    first_order_at,
    last_order_at
FROM gold.dim_customer
ORDER BY lifetime_revenue DESC
LIMIT 10;

-- COMMAND ----------

-- Q1d. Top 10 customers by order count.
SELECT
    customer_unique_id,
    customer_state,
    customer_city,
    lifetime_fulfilled_orders,
    lifetime_revenue,
    ROUND(lifetime_revenue / NULLIF(lifetime_fulfilled_orders, 0), 2) AS avg_order_value,
    DATEDIFF(last_order_at, first_order_at) AS days_between_first_and_last
FROM gold.dim_customer
ORDER BY lifetime_fulfilled_orders DESC, lifetime_revenue DESC
LIMIT 10;

-- COMMAND ----------

-- Q2. Monthly revenue and order volume, with month-on-month growth.
WITH monthly AS (
    SELECT
        d.year_month AS month,
        SUM(f.order_value) AS revenue,
        SUM(f.item_value) AS revenue_excl_freight,
        COUNT(*) AS order_count,
        ROUND(AVG(f.order_value), 2) AS avg_order_value
    FROM gold.fact_order f
    JOIN gold.dim_date d ON f.purchase_date_key = d.date_key
    WHERE f.is_fulfilled = true
      AND d.year_month BETWEEN '2017-01' AND '2018-08'
    GROUP BY d.year_month
),
with_prev AS (
    SELECT
        month,
        revenue,
        revenue_excl_freight,
        order_count,
        avg_order_value,
        LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
        LAG(order_count) OVER (ORDER BY month) AS prev_order_count
    FROM monthly
)
SELECT
    month,
    revenue,
    revenue_excl_freight,
    ROUND(100.0 * (revenue - prev_revenue) / prev_revenue, 1) AS revenue_growth_pct,
    order_count,
    ROUND(100.0 * (order_count - prev_order_count) / prev_order_count, 1) AS order_growth_pct,
    avg_order_value
FROM with_prev
ORDER BY month;

-- COMMAND ----------

-- Q3. Extreme delivery delays by state, and what they cost in customer satisfaction.
-- Note: avg_review_score here covers delivered orders only, since undelivered orders have no
-- delivery time to compare. Q4's version covers all fulfilled orders, so the two differ slightly.
WITH delivered AS (
    SELECT
        o.order_id,
        o.delivery_days,
        o.days_early,
        o.review_score,
        c.customer_state
    FROM gold.fact_order o
    JOIN gold.dim_customer c ON o.customer_key = c.customer_key
    WHERE o.is_fulfilled = true
      AND o.delivery_days IS NOT NULL
),
threshold AS (
    SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY delivery_days) AS p99_days
    FROM delivered
),
by_state AS (
    SELECT
        d.customer_state,
        COUNT(*) AS delivered_orders,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY d.delivery_days) AS median_days,
        COUNT_IF(d.delivery_days > t.p99_days) AS extreme_orders,
        MAX(d.delivery_days) AS worst_delivery_days,
        AVG(d.review_score) AS avg_review_score,
        AVG(CASE WHEN d.delivery_days > t.p99_days THEN d.review_score END)
            AS avg_review_score_of_extremes,
        COUNT_IF(d.days_early < 0) AS orders_past_promised_date
    FROM delivered d
    CROSS JOIN threshold t
    GROUP BY d.customer_state
    HAVING COUNT(*) >= 100      -- small states would otherwise top the rate on a handful of orders
)
SELECT
    customer_state,
    delivered_orders,
    ROUND(median_days) AS median_days,
    extreme_orders,
    ROUND(100.0 * extreme_orders / delivered_orders, 2) AS pct_extreme,
    worst_delivery_days,
    ROUND(100.0 * orders_past_promised_date / delivered_orders, 1) AS pct_past_promised_date,
    ROUND(avg_review_score, 2) AS avg_review_score,
    ROUND(avg_review_score_of_extremes, 2) AS avg_review_score_of_extremes,
    ROUND(avg_review_score - avg_review_score_of_extremes, 2) AS review_score_penalty
FROM by_state
ORDER BY pct_extreme DESC;

-- COMMAND ----------

-- Q4. Commercial performance by customer state, with delivery quality alongside.
WITH by_state AS (
    SELECT
        c.customer_state,
        COUNT(DISTINCT o.customer_key) AS customers,
        COUNT(*) AS orders,
        SUM(o.order_value) AS revenue,
        SUM(o.item_value) AS revenue_excl_freight,
        SUM(o.freight_value) AS freight,
        AVG(o.order_value) AS avg_order_value,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY o.delivery_days) AS median_delivery_days,
        AVG(o.review_score) AS avg_review_score
    FROM gold.fact_order o
    JOIN gold.dim_customer c ON o.customer_key = c.customer_key
    WHERE o.is_fulfilled = true
    GROUP BY c.customer_state
)
SELECT
    customer_state,
    customers,
    orders,
    revenue,
    revenue_excl_freight,
    ROUND(100.0 * revenue / SUM(revenue) OVER (), 2) AS pct_of_revenue,
    -- Running total down the ranking: shows how concentrated the market is.
    ROUND(100.0 * SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER (), 1)
        AS cumulative_pct,
    ROUND(revenue / customers, 2) AS revenue_per_customer,
    ROUND(avg_order_value, 2) AS avg_order_value,
    -- Freight as a share of what the customer pays. Rises with distance from the São Paulo
    -- seller base, so it is a proxy for how expensive a state is to serve.
    ROUND(100.0 * freight / revenue, 1) AS freight_pct_of_revenue,
    ROUND(median_delivery_days) AS median_delivery_days,
    ROUND(avg_review_score, 2) AS avg_review_score
FROM by_state
ORDER BY revenue DESC;

-- COMMAND ----------


-- Q5. Customer repeat purchase rate
WITH first_orders AS (
    SELECT
        o.purchase_date_key,
        o.order_value,
        DATEDIFF(
            LEAD(o.purchased_at) OVER (PARTITION BY o.customer_key ORDER BY o.purchased_at),
            o.purchased_at) AS gap_days,
        ROW_NUMBER() OVER (PARTITION BY o.customer_key ORDER BY o.purchased_at) AS order_seq
    FROM gold.fact_order o
    WHERE o.is_fulfilled = true
),
cohorts AS (
    SELECT
        d.year_month AS acquisition_month,
        f.order_value AS first_order_value,
        f.gap_days,
        f.gap_days BETWEEN 1 AND 90 AS returned_in_90d
    FROM first_orders f
    JOIN gold.dim_date d ON f.purchase_date_key = d.date_key
    WHERE f.order_seq = 1
      AND d.year_month BETWEEN '2017-01' AND '2018-05'
),
returners AS (
    SELECT acquisition_month,
           PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY gap_days) AS median_days
    FROM cohorts
    WHERE returned_in_90d
    GROUP BY acquisition_month
)
SELECT
    c.acquisition_month,
    COUNT(*) AS new_customers,
    ROUND(SUM(c.first_order_value), 2) AS first_order_revenue,
    ROUND(AVG(c.first_order_value), 2) AS avg_first_order_value,
    COUNT_IF(c.returned_in_90d) AS returned_within_90d,
    ROUND(100.0 * COUNT_IF(c.returned_in_90d) / COUNT(*), 2) AS repeat_rate_90d_pct,
    ROUND(MAX(r.median_days)) AS median_days_to_return
FROM cohorts c
LEFT JOIN returners r ON c.acquisition_month = r.acquisition_month
GROUP BY c.acquisition_month
ORDER BY c.acquisition_month;

-- COMMAND ----------

-- Q6. Retention, corrected: how much of the apparent repeat rate is real?

-- 6a. Days between a customer's first and second order. 
WITH gaps AS (
    SELECT
        DATEDIFF(next_at, purchased_at) AS gap_days,
        ROUND((BIGINT(next_at) - BIGINT(purchased_at)) / 60.0, 1) AS gap_minutes
    FROM (
        SELECT
            purchased_at,
            LEAD(purchased_at) OVER (PARTITION BY customer_key ORDER BY purchased_at) AS next_at,
            ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY purchased_at) AS order_seq
        FROM gold.fact_order
        WHERE is_fulfilled = true
    )
    WHERE order_seq = 1 AND next_at IS NOT NULL
)
SELECT
    gap_days,
    COUNT(*) AS customers,
    COUNT_IF(gap_days = 0 AND gap_minutes <= 5) AS within_5_minutes
FROM gaps
WHERE gap_days <= 10
GROUP BY gap_days
ORDER BY gap_days;

-- COMMAND ----------

-- 6b. Comparing variations of repeat rate
WITH firsts AS (
    SELECT
        DATEDIFF(next_at, purchased_at) AS gap_days,
        purchased_at < DATE'2018-08-31' - INTERVAL 90 DAYS AS in_cohort,
        review_score
    FROM (
        SELECT
            purchased_at,
            review_score,
            LEAD(purchased_at) OVER (PARTITION BY customer_key ORDER BY purchased_at) AS next_at,
            ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY purchased_at) AS order_seq
        FROM gold.fact_order
        WHERE is_fulfilled = true
    )
    WHERE order_seq = 1
)
SELECT
    COUNT(*) AS customers,
    COUNT_IF(gap_days IS NOT NULL) AS ever_returned_naive,
    COUNT_IF(gap_days = 0) AS same_day_split_checkouts,
    COUNT_IF(gap_days >= 1) AS ever_returned_genuine,
    ROUND(100.0 * COUNT_IF(gap_days >= 1) / COUNT(*), 2) AS lifetime_repeat_pct,
    COUNT_IF(gap_days > 90) AS genuine_returns_after_90_days,
    ROUND(100.0 * COUNT_IF(in_cohort AND gap_days BETWEEN 1 AND 90)
          / NULLIF(COUNT_IF(in_cohort), 0), 2) AS repeat_rate_90d_pct
FROM firsts;

-- COMMAND ----------

-- 6c. Does satisfaction predict a second order?
SELECT
    CAST(review_score AS INT) AS first_order_review_score,
    COUNT(*) AS customers,
    ROUND(100.0 * COUNT_IF(gap_days BETWEEN 1 AND 90) / COUNT(*), 2) AS repeat_rate_90d_pct
FROM (
    SELECT
        review_score,
        DATEDIFF(LEAD(purchased_at) OVER (PARTITION BY customer_key ORDER BY purchased_at),
                 purchased_at) AS gap_days,
        ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY purchased_at) AS order_seq,
        purchased_at
    FROM gold.fact_order
    WHERE is_fulfilled = true
)
WHERE order_seq = 1
  AND review_score IS NOT NULL
  AND purchased_at < DATE'2018-08-31' - INTERVAL 90 DAYS
GROUP BY CAST(review_score AS INT)
ORDER BY first_order_review_score;

-- COMMAND ----------

-- Q7. Why do remote customers spend more per order? Basket size or item price?

SELECT
    c.customer_state,
    COUNT(*) AS orders,
    ROUND(AVG(o.order_value), 2) AS avg_order_value,
    ROUND(AVG(o.item_count), 2) AS avg_items_per_order,
    ROUND(AVG(o.item_value / o.item_count), 2) AS avg_price_per_item,
    ROUND(AVG(o.freight_value / o.item_count), 2) AS avg_freight_per_item,
    ROUND(100.0 * SUM(o.freight_value) / SUM(o.order_value), 1) AS freight_pct_of_revenue
FROM gold.fact_order o
JOIN gold.dim_customer c ON o.customer_key = c.customer_key
WHERE o.is_fulfilled = true
  AND o.item_count > 0
GROUP BY c.customer_state
ORDER BY avg_price_per_item DESC;

-- COMMAND ----------

-- Q8. Sizing a retention campaign: who would it target, and what is a second order worth?

-- 8a. One-time buyers by recency and value.
WITH one_timers AS (
    SELECT
        lifetime_revenue,
        DATEDIFF(DATE'2018-08-31', last_order_at) AS days_since_order
    FROM gold.dim_customer
    WHERE lifetime_fulfilled_orders = 1
),
median_value AS (
    SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY lifetime_revenue) AS median_rev
    FROM one_timers
)
SELECT
    CASE WHEN o.days_since_order <= 90  THEN 'a. 0-90 days'
         WHEN o.days_since_order <= 180 THEN 'b. 91-180 days'
         WHEN o.days_since_order <= 365 THEN 'c. 181-365 days'
         ELSE                                'd. 365+ days' END AS recency,
    CASE WHEN o.lifetime_revenue >= m.median_rev THEN 'above median spend'
         ELSE 'below median spend' END AS value_tier,
    COUNT(*) AS customers,
    ROUND(AVG(o.lifetime_revenue), 2) AS avg_first_order_value
FROM one_timers o
CROSS JOIN median_value m
GROUP BY 1, 2
ORDER BY recency, value_tier;

-- COMMAND ----------

-- 8b. Order value by sequence
SELECT
    order_seq,
    COUNT(*) AS orders,
    ROUND(AVG(order_value), 2) AS avg_order_value
FROM (
    SELECT order_value,
           ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY purchased_at) AS order_seq
    FROM gold.fact_order
    WHERE is_fulfilled = true
)
WHERE order_seq <= 3
GROUP BY order_seq
ORDER BY order_seq;

-- COMMAND ----------

-- 8c. New customers per month in 2018, annualised for the impact estimate.
SELECT
    COUNT(*) AS months,
    ROUND(AVG(new_customers)) AS avg_new_customers_per_month,
    ROUND(AVG(new_customers) * 12) AS annualised_new_customers
FROM (
    SELECT d.year_month, COUNT(*) AS new_customers
    FROM (
        SELECT purchase_date_key,
               ROW_NUMBER() OVER (PARTITION BY customer_key ORDER BY purchased_at) AS order_seq
        FROM gold.fact_order
        WHERE is_fulfilled = true
    ) f
    JOIN gold.dim_date d ON f.purchase_date_key = d.date_key
    WHERE f.order_seq = 1
      AND d.year_month BETWEEN '2018-01' AND '2018-08'
    GROUP BY d.year_month
);