# Databricks notebook source
# DBTITLE 1,Row Counts
# MAGIC %sql
# MAGIC -- Counts
# MAGIC SELECT 'brz_customers_raw' AS table_name, COUNT(1) AS cnt  FROM brz_21927.customers_raw
# MAGIC UNION ALL
# MAGIC SELECT 'brz_products_raw', COUNT(1) AS cnt FROM brz_21927.products_raw
# MAGIC UNION ALL
# MAGIC SELECT 'brz_orders_raw', COUNT(1) AS cnt FROM brz_21927.orders_raw
# MAGIC UNION ALL
# MAGIC SELECT 'brz_order_items_raw', COUNT(1) AS cnt FROM brz_21927.order_items_raw
# MAGIC UNION ALL
# MAGIC SELECT 'slv_customers', COUNT(1) AS cnt FROM slv_21927.customers
# MAGIC UNION ALL
# MAGIC SELECT 'slv_products', COUNT(1) AS cnt FROM slv_21927.products
# MAGIC UNION ALL
# MAGIC SELECT 'slv_orders', COUNT(1) AS cnt FROM slv_21927.orders
# MAGIC UNION ALL
# MAGIC SELECT 'slv_order_items', COUNT(1) AS cnt FROM slv_21927.order_items
# MAGIC UNION ALL
# MAGIC SELECT 'slv_sales_enriched', COUNT(1) AS cnt FROM slv_21927.sales_enriched
# MAGIC UNION ALL
# MAGIC SELECT 'gld_fact_sales', COUNT(1) AS cnt FROM gld_21927.fact_sales;

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Customers
# MAGIC %sql
# MAGIC SELECT customer_id, COUNT(1) AS cnt 
# MAGIC FROM slv_21927.customers
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(1) > 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1, Product Duplicate Check
# MAGIC %sql
# MAGIC SELECT product_id, COUNT(1) AS cnt 
# MAGIC FROM slv_21927.products
# MAGIC GROUP BY product_id
# MAGIC HAVING COUNT(1) > 1;

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Orders
# MAGIC %sql
# MAGIC SELECT order_id, COUNT(1) AS cnt 
# MAGIC FROM slv_21927.orders
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(1) > 1;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Duplicate Checks Order Items
# MAGIC %sql
# MAGIC SELECT order_item_id, COUNT(1) AS cnt 
# MAGIC FROM slv_21927.order_items
# MAGIC GROUP BY order_item_id
# MAGIC HAVING COUNT(1)  > 1;

# COMMAND ----------

# DBTITLE 1,Watermark check
# MAGIC %sql
# MAGIC -- Watermark check
# MAGIC SELECT 1 FROM brz_21927.pipeline_watermark;

# COMMAND ----------

# DBTITLE 1,Run logs
# MAGIC %sql
# MAGIC -- Run logs
# MAGIC SELECT 1 FROM brz_21927.pipeline_run_log ORDER BY run_start_ts DESC;