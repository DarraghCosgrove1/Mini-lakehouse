-- Top 10 customers by revenue (last 365 days)
WITH recent AS (
  SELECT *
  FROM fact_orders
  WHERE date >= date '2025-02-03' - interval 365 day  -- adjust or parameterize
)
SELECT c.customer_id, c.customer_name, SUM(extended_amount) AS revenue
FROM recent f
JOIN dim_customers c USING (customer_id)
GROUP BY 1,2
ORDER BY revenue DESC
LIMIT 10;

-- Product category performance
SELECT p.category, COUNT(DISTINCT f.order_id) AS orders, SUM(f.extended_amount) AS revenue
FROM fact_orders f
JOIN dim_products p USING (product_id)
GROUP BY 1
ORDER BY revenue DESC;

-- Daily revenue time series
SELECT date, SUM(extended_amount) AS revenue
FROM fact_orders
GROUP BY 1
ORDER BY 1;
