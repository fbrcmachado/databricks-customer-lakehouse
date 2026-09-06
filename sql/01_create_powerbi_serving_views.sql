-- Gold BI SQL serving views.
-- Power BI should consume the dimensional tables directly.
-- These views support SQL smoke tests and ad-hoc analytical access.

CREATE OR REPLACE VIEW customer_lakehouse.gold_bi.v_sales_enriched
COMMENT 'Enriched transaction-grain analytical view for SQL validation and ad-hoc analysis'
AS
SELECT
    f.transaction_id,
    d.full_date AS order_date,
    d.year,
    d.quarter,
    d.month_number,
    d.month_name,
    d.year_month,
    c.customer_key,
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    p.product_key,
    p.sku,
    p.product_name,
    p.category,
    f.quantity,
    f.unit_price,
    f.discount_pct,
    f.gross_amount,
    f.discount_amount,
    f.net_amount,
    f.payment_method,
    f.order_status,
    f.sales_channel,
    f.seller,
    f.coupon
FROM customer_lakehouse.gold_bi.fact_sales AS f
INNER JOIN customer_lakehouse.gold_bi.dim_date AS d
    ON f.date_key = d.date_key
INNER JOIN customer_lakehouse.gold_bi.dim_customer AS c
    ON f.customer_key = c.customer_key
INNER JOIN customer_lakehouse.gold_bi.dim_product AS p
    ON f.product_key = p.product_key

-- COMMAND ----------

CREATE OR REPLACE VIEW customer_lakehouse.gold_bi.v_sales_monthly
COMMENT 'Monthly sales summary for SQL smoke tests and lightweight analytical consumption'
AS
SELECT
    d.year,
    d.quarter,
    d.month_number,
    d.month_name,
    d.year_month,
    COUNT(DISTINCT f.transaction_id) AS total_orders,
    COUNT(DISTINCT f.customer_key) AS distinct_customers,
    SUM(f.quantity) AS units_sold,
    CAST(SUM(f.gross_amount) AS DECIMAL(24, 4)) AS gross_revenue,
    CAST(SUM(f.discount_amount) AS DECIMAL(24, 4)) AS discount_amount,
    CAST(SUM(f.net_amount) AS DECIMAL(24, 4)) AS net_revenue
FROM customer_lakehouse.gold_bi.fact_sales AS f
INNER JOIN customer_lakehouse.gold_bi.dim_date AS d
    ON f.date_key = d.date_key
GROUP BY
    d.year,
    d.quarter,
    d.month_number,
    d.month_name,
    d.year_month
