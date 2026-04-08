# Databricks notebook source
# MAGIC %md
# MAGIC Reference File for the Queries that I used in the Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dev_21927;
# MAGIC
# MAGIC -- Revenue by state
# MAGIC SELECT state, total_revenue, total_orders, total_customers
# MAGIC FROM gld_21927.agg_revenue_by_state
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- Top 10 products
# MAGIC SELECT product_name, category, units_sold, revenue
# MAGIC FROM gld_21927.agg_top_products
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC -- Daily trend
# MAGIC SELECT order_date_only AS sales_date, daily_revenue, daily_orders, daily_customers
# MAGIC FROM gld_21927.agg_daily_sales
# MAGIC ORDER BY sales_date;
# MAGIC
# MAGIC -- Monthly trend
# MAGIC SELECT order_year, order_month, monthly_revenue, monthly_orders, monthly_customers
# MAGIC FROM gld_21927.agg_monthly_sales
# MAGIC ORDER BY order_year, order_month;
# MAGIC
# MAGIC -- Category revenue
# MAGIC SELECT category, category_revenue, units_sold
# MAGIC FROM gld_21927.agg_category_sales
# MAGIC ORDER BY category_revenue DESC;
# MAGIC
# MAGIC -- Average order value by month
# MAGIC SELECT
# MAGIC     order_year,
# MAGIC     order_month,
# MAGIC     ROUND(SUM(sales_amount) / COUNT(DISTINCT order_id), 2) AS avg_order_value
# MAGIC FROM gld_21927.fact_sales
# MAGIC GROUP BY order_year, order_month
# MAGIC ORDER BY order_year, order_month;