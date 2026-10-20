CREATE TABLE mart.f_customer_retention (
    new_customers_count int,
    returning_customers_count int,
    refunded_customer_count int,
    period_name varchar,
    period_id int,
    item_id int,
    new_customers_revenue numeric,
    returning_customers_revenue numeric,
    customers_refunded int
);


DELETE FROM mart.f_customer_retention
WHERE period_id = EXTRACT(WEEK FROM '{{ds}}'::date);


INSERT INTO mart.f_customer_retention (
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    period_name,
    period_id,
    item_id,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)

WITH user_week AS (
    SELECT
        fs.customer_id,
        fs.item_id,
        EXTRACT(WEEK FROM dc.date_actual) AS week_id,

        COUNT(*) FILTER (WHERE fs.status = 'shipped') AS orders_count,
        SUM(fs.payment_amount) FILTER (WHERE fs.status = 'shipped') AS revenue,
        COUNT(*) FILTER (WHERE fs.status = 'refunded') AS refunded_count

    FROM mart.f_sales fs
    JOIN mart.d_calendar dc 
        ON fs.date_id = dc.date_id

    WHERE dc.date_actual >= date_trunc('week', '{{ds}}'::date)
      AND dc.date_actual < date_trunc('week', '{{ds}}'::date) + interval '7 day'

    GROUP BY fs.customer_id, fs.item_id, week_id
)

SELECT
    COUNT(*) FILTER (WHERE orders_count = 1) AS new_customers_count,
    COUNT(*) FILTER (WHERE orders_count > 1) AS returning_customers_count,
    COUNT(*) FILTER (WHERE refunded_count > 0) AS refunded_customer_count,

    'weekly' AS period_name,
    week_id AS period_id,
    item_id,

    COALESCE(SUM(revenue) FILTER (WHERE orders_count = 1), 0) AS new_customers_revenue,
    COALESCE(SUM(revenue) FILTER (WHERE orders_count > 1), 0) AS returning_customers_revenue,
    COALESCE(SUM(refunded_count), 0) AS customers_refunded

FROM user_week
GROUP BY week_id, item_id;