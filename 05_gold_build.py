# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# =========================================================
# Helper: build and log a gold table independently
# =========================================================

def write_gold_table(df, table_name, run_id, start_ts):
    """Writes a gold table and logs the result independently."""
    full_table = f"{CATALOG}.{GOLD_SCHEMA}.{table_name}"
    try:
        row_count = df.count()

        df_with_audit = (
            df.withColumn("gold_run_id", F.lit(run_id))
              .withColumn("gold_load_ts", F.current_timestamp())
        )

        df_with_audit.write.format("delta").mode("overwrite").saveAsTable(full_table)

        end_ts = datetime.now()
        log_run(run_id, "GOLD", table_name, "SUCCESS", row_count, f"{table_name} built successfully", start_ts, end_ts)
        print(f"  {table_name}: {row_count} rows written")

    except Exception as e:
        end_ts = datetime.now()
        log_run(run_id, "GOLD", table_name, "FAILED", 0, str(e), start_ts, end_ts)
        raise e

# COMMAND ----------

# =========================================================
# Read silver tables
# =========================================================

customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
products = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
sales_enriched = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.sales_enriched")

# sales_enriched already contains state, customer_name, product_name,
# category from the silver enriched join — no conditional joins needed.

# COMMAND ----------

# =========================================================
# DIM_CUSTOMERS
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

dim_customers = (
    customers.select(
        "customer_id", "name", "city", "state",
        "signup_date", "created_at", "updated_at"
    )
    .dropDuplicates(["customer_id"])
)

write_gold_table(dim_customers, "dim_customers", run_id, start_ts)

# COMMAND ----------

# =========================================================
# DIM_PRODUCTS
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

dim_products = (
    products.select(
        "product_id", "product_name", "category",
        "price", "created_at", "updated_at"
    )
    .dropDuplicates(["product_id"])
)

write_gold_table(dim_products, "dim_products", run_id, start_ts)

# COMMAND ----------

# =========================================================
# FACT_SALES (grain = one row per order_item_id)
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

fact_sales = (
    sales_enriched
    .withColumn("order_date_only", F.to_date(F.col("order_date")))
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .withColumn("order_day", F.dayofmonth(F.col("order_date")))
)

try:
    row_count = fact_sales.count()

    fact_sales_with_audit = (
        fact_sales
        .withColumn("gold_run_id", F.lit(run_id))
        .withColumn("gold_load_ts", F.current_timestamp())
    )

    (
        fact_sales_with_audit
        .repartition("order_year", "order_month")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")
    )

    end_ts = datetime.now()
    log_run(run_id, "GOLD", "fact_sales", "SUCCESS", row_count, "fact_sales built successfully", start_ts, end_ts)
    print(f"  fact_sales: {row_count} rows written")

except Exception as e:
    end_ts = datetime.now()
    log_run(run_id, "GOLD", "fact_sales", "FAILED", 0, str(e), start_ts, end_ts)
    raise e

# COMMAND ----------

# =========================================================
# AGG_REVENUE_BY_STATE
# =========================================================
# Uses sales_enriched.state directly — no conditional join needed.
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

agg_revenue_by_state = (
    sales_enriched.groupBy("state")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("total_customers"),
    )
)

write_gold_table(agg_revenue_by_state, "agg_revenue_by_state", run_id, start_ts)

# COMMAND ----------

# =========================================================
# AGG_TOP_PRODUCTS
# =========================================================
# Uses sales_enriched.product_name and .category directly.
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

agg_top_products = (
    sales_enriched.groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("sales_amount"), 2).alias("revenue"),
    )
)

write_gold_table(agg_top_products, "agg_top_products", run_id, start_ts)

# COMMAND ----------

# =========================================================
# AGG_DAILY_SALES
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

agg_daily_sales = (
    sales_enriched
    .withColumn("order_date_only", F.to_date(F.col("order_date")))
    .groupBy("order_date_only")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("daily_revenue"),
        F.countDistinct("order_id").alias("daily_orders"),
        F.countDistinct("customer_id").alias("daily_customers"),
    )
)

write_gold_table(agg_daily_sales, "agg_daily_sales", run_id, start_ts)

# COMMAND ----------

# =========================================================
# AGG_MONTHLY_SALES
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

agg_monthly_sales = (
    sales_enriched
    .withColumn("order_year", F.year(F.col("order_date")))
    .withColumn("order_month", F.month(F.col("order_date")))
    .groupBy("order_year", "order_month")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("monthly_revenue"),
        F.countDistinct("order_id").alias("monthly_orders"),
        F.countDistinct("customer_id").alias("monthly_customers"),
    )
)

write_gold_table(agg_monthly_sales, "agg_monthly_sales", run_id, start_ts)

# COMMAND ----------

# =========================================================
# AGG_CATEGORY_SALES
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

agg_category_sales = (
    sales_enriched.groupBy("category")
    .agg(
        F.round(F.sum("sales_amount"), 2).alias("category_revenue"),
        F.sum("quantity").alias("units_sold"),
    )
)

write_gold_table(agg_category_sales, "agg_category_sales", run_id, start_ts)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation: confirm all gold table runs
# MAGIC SELECT run_id, table_name, run_status, row_count, message,
# MAGIC        run_start_ts, run_end_ts
# MAGIC FROM dev_21927.brz_21927.pipeline_run_log
# MAGIC WHERE layer_name = 'GOLD'
# MAGIC ORDER BY run_start_ts DESC
# MAGIC LIMIT 20;