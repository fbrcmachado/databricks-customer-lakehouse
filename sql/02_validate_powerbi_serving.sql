WITH metrics AS (
    SELECT
        (SELECT COUNT(*) FROM customer_lakehouse.gold_bi.fact_sales)
            AS fact_rows,
        (
            SELECT COUNT(*)
            FROM customer_lakehouse.gold_bi.v_sales_enriched
        ) AS enriched_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT transaction_id
                FROM customer_lakehouse.gold_bi.fact_sales
                GROUP BY transaction_id
                HAVING COUNT(*) > 1
            )
        ) AS duplicate_transactions,
        (
            SELECT COUNT(*)
            FROM customer_lakehouse.gold_bi.fact_sales AS f
            LEFT ANTI JOIN customer_lakehouse.gold_bi.dim_date AS d
                ON f.date_key = d.date_key
        ) AS orphan_date_rows,
        (
            SELECT COUNT(*)
            FROM customer_lakehouse.gold_bi.fact_sales AS f
            LEFT ANTI JOIN customer_lakehouse.gold_bi.dim_customer AS c
                ON f.customer_key = c.customer_key
        ) AS orphan_customer_rows,
        (
            SELECT COUNT(*)
            FROM customer_lakehouse.gold_bi.fact_sales AS f
            LEFT ANTI JOIN customer_lakehouse.gold_bi.dim_product AS p
                ON f.product_key = p.product_key
        ) AS orphan_product_rows
)
SELECT
    CASE
        WHEN fact_rows <= 0 THEN
            raise_error('Power BI serving validation failed: fact_sales is empty.')
        WHEN enriched_rows <> fact_rows THEN
            raise_error('Power BI serving validation failed: enriched view row count differs from fact_sales.')
        WHEN duplicate_transactions <> 0 THEN
            raise_error('Power BI serving validation failed: duplicate transaction_id values.')
        WHEN orphan_date_rows <> 0 THEN
            raise_error('Power BI serving validation failed: orphan date keys.')
        WHEN orphan_customer_rows <> 0 THEN
            raise_error('Power BI serving validation failed: orphan customer keys.')
        WHEN orphan_product_rows <> 0 THEN
            raise_error('Power BI serving validation failed: orphan product keys.')
        ELSE 'PASS'
    END AS serving_status,
    fact_rows,
    enriched_rows,
    duplicate_transactions,
    orphan_date_rows,
    orphan_customer_rows,
    orphan_product_rows
FROM metrics

-- COMMAND ----------

SELECT
    year_month,
    total_orders,
    distinct_customers,
    units_sold,
    gross_revenue,
    discount_amount,
    net_revenue
FROM customer_lakehouse.gold_bi.v_sales_monthly
ORDER BY year_month DESC
LIMIT 12
