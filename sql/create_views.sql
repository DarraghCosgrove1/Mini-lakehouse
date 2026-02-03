CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT date, SUM(extended_amount) AS revenue
FROM fact_orders
GROUP BY 1;

CREATE OR REPLACE VIEW v_customer_lifetime_value AS
SELECT c.customer_id, c.customer_name, SUM(f.extended_amount) AS lifetime_value
FROM fact_orders f
JOIN dim_customers c USING (customer_id)
GROUP BY 1,2;
