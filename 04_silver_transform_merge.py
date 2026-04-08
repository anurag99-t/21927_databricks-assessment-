# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

# =========================================================
# Watermark helpers (silver-layer specific)
# =========================================================

def get_silver_watermark(table_name):
    """Reads the last successful silver run timestamp from the watermark table."""
    wm_table = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_watermark"
    wm_key = f"silver_{table_name}"
    wm_df = spark.sql(f"""
        SELECT last_run_ts
        FROM {wm_table}
        WHERE table_name = '{wm_key}' AND status IN ('INIT', 'SUCCESS')
    """)
    if wm_df.count() == 0:
        return None
    return wm_df.collect()[0]["last_run_ts"]


def set_silver_watermark_running(table_name):
    """Marks the silver watermark as RUNNING before processing begins."""
    wm_table = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_watermark"
    wm_key = f"silver_{table_name}"
    spark.sql(f"""
        MERGE INTO {wm_table} t
        USING (SELECT '{wm_key}' AS table_name) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET status = 'RUNNING', updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, last_run_ts, last_run_id, status, updated_at)
            VALUES ('{wm_key}', TIMESTAMP('1900-01-01'), NULL, 'RUNNING', current_timestamp())
    """)


def update_silver_watermark(table_name, new_ts, run_id):
    """Updates the silver watermark after a successful load."""
    wm_table = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_watermark"
    wm_key = f"silver_{table_name}"
    spark.sql(f"""
        MERGE INTO {wm_table} t
        USING (
            SELECT
                '{wm_key}' AS table_name,
                TIMESTAMP('{new_ts}') AS last_run_ts,
                '{run_id}' AS last_run_id,
                'SUCCESS' AS status,
                current_timestamp() AS updated_at
        ) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            t.last_run_ts = s.last_run_ts,
            t.last_run_id = s.last_run_id,
            t.status = s.status,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)


def set_silver_watermark_failed(table_name):
    """Marks the silver watermark as FAILED if processing errors out."""
    wm_table = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_watermark"
    wm_key = f"silver_{table_name}"
    spark.sql(f"""
        UPDATE {wm_table}
        SET status = 'FAILED', updated_at = current_timestamp()
        WHERE table_name = '{wm_key}'
    """)

# COMMAND ----------

# =========================================================
# Cleaning functions
# =========================================================

def clean_customers(df):
    return (
        df.filter(F.col("customer_id").isNotNull())
          .withColumn("customer_id", F.col("customer_id").cast("long"))
          .withColumn("name", F.trim(F.col("name")))
          .withColumn("city", F.initcap(F.trim(F.col("city"))))
          .withColumn("state", F.upper(F.trim(F.col("state"))))
    )

def clean_products(df):
    return (
        df.filter(F.col("product_id").isNotNull())
          .withColumn("product_id", F.col("product_id").cast("long"))
          .withColumn("product_name", F.trim(F.col("product_name")))
          .withColumn("category", F.initcap(F.trim(F.col("category"))))
          .withColumn("price", F.col("price").cast("double"))
    )

def clean_orders(df):
    return (
        df.filter(F.col("order_id").isNotNull())
          .withColumn("order_id", F.col("order_id").cast("long"))
          .withColumn("customer_id", F.col("customer_id").cast("long"))
          .withColumn("total_amount", F.col("total_amount").cast("double"))
          .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
    )

def clean_order_items(df):
    return (
        df.filter(F.col("order_item_id").isNotNull())
          .withColumn("order_item_id", F.col("order_item_id").cast("long"))
          .withColumn("order_id", F.col("order_id").cast("long"))
          .withColumn("product_id", F.col("product_id").cast("long"))
          .withColumn("quantity", F.col("quantity").cast("int"))
          .withColumn("price", F.col("price").cast("double"))
    )

def apply_cleaning(table_name, df):
    cleaners = {
        "customers": clean_customers,
        "products": clean_products,
        "orders": clean_orders,
        "order_items": clean_order_items,
    }
    return cleaners.get(table_name, lambda d: d)(df)

# COMMAND ----------

# =========================================================
# Core silver merge
# =========================================================

def merge_to_silver(table_name):
    run_id = generate_run_id()
    start_ts = datetime.now()

    try:
        cfg = TABLE_CONFIG[table_name]
        bronze_table = cfg["bronze_table"]
        silver_table = cfg["silver_table"]
        pk = cfg["pk"]
        watermark_col = cfg.get("watermark_col", "updated_at")

        # Mark as running
        set_silver_watermark_running(table_name)

        # Read watermark
        last_run_ts = get_silver_watermark(table_name)
        print(f"Silver watermark for {table_name}: {last_run_ts}")

        # Read bronze and apply incremental filter using config-driven column
        bronze_df = spark.table(bronze_table)

        if last_run_ts is not None:
            incremental_df = bronze_df.filter(F.col(watermark_col) > F.lit(last_run_ts))
        else:
            incremental_df = bronze_df

        # Capture row count before write
        row_count = incremental_df.count()

        if row_count == 0:
            end_ts = datetime.now()
            update_silver_watermark(table_name, last_run_ts or datetime(1900, 1, 1), run_id)
            log_run(run_id, "SILVER", table_name, "SUCCESS", 0, "No incremental data found", start_ts, end_ts)
            print(f"No new data for {table_name}")
            return

        # Clean and deduplicate
        cleaned_df = apply_cleaning(table_name, incremental_df)
        dedup_df = deduplicate_latest(cleaned_df, pk)

        # Add silver audit columns
        dedup_df = (
            dedup_df.withColumn("silver_run_id", F.lit(run_id))
                    .withColumn("silver_load_ts", F.current_timestamp())
        )

        # Capture max watermark value before merge
        new_watermark = dedup_df.agg(F.max(F.col(watermark_col))).collect()[0][0]

        # Merge into silver
        if not table_exists(silver_table):
            dedup_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        else:
            delta_table = DeltaTable.forName(spark, silver_table)
            merge_condition = f"t.{pk} = s.{pk}"

            update_set = {col: F.col(f"s.{col}") for col in dedup_df.columns}
            insert_values = {col: F.col(f"s.{col}") for col in dedup_df.columns}

            (
                delta_table.alias("t")
                .merge(dedup_df.alias("s"), merge_condition)
                .whenMatchedUpdate(set=update_set)
                .whenNotMatchedInsert(values=insert_values)
                .execute()
            )

        # Update watermark and log success
        end_ts = datetime.now()
        update_silver_watermark(table_name, new_watermark, run_id)
        log_run(run_id, "SILVER", table_name, "SUCCESS", row_count, "Silver merge completed", start_ts, end_ts)

        print(f"Silver merge successful for {table_name}. Rows processed: {row_count}")

    except Exception as e:
        end_ts = datetime.now()
        set_silver_watermark_failed(table_name)
        log_run(run_id, "SILVER", table_name, "FAILED", 0, str(e), start_ts, end_ts)
        raise e

# COMMAND ----------

for table_name in ["customers", "products", "orders", "order_items"]:
    merge_to_silver(table_name)

# COMMAND ----------

# =========================================================
# Incremental sales_enriched build
# =========================================================
# Only reprocesses orders that changed since the last
# enriched build, then merges them into the enriched table.
# =========================================================

run_id = generate_run_id()
start_ts = datetime.now()

try:
    set_silver_watermark_running("sales_enriched")
    last_enriched_ts = get_silver_watermark("sales_enriched")

    customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
    products = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.products")
    orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    order_items = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.order_items")

    # Filter to only orders that changed since last enriched run
    if last_enriched_ts is not None:
        changed_orders = orders.filter(
            (F.col("updated_at") > F.lit(last_enriched_ts)) |
            (F.col("created_at") > F.lit(last_enriched_ts))
        )
    else:
        changed_orders = orders

    # Get order_items for only the changed orders
    changed_order_ids = changed_orders.select("order_id").distinct()
    changed_order_items = order_items.join(changed_order_ids, "order_id", "inner")

    sales_enriched = (
        changed_order_items.alias("oi")
        .join(changed_orders.alias("o"), F.col("oi.order_id") == F.col("o.order_id"), "inner")
        .join(customers.alias("c"), F.col("o.customer_id") == F.col("c.customer_id"), "left")
        .join(products.alias("p"), F.col("oi.product_id") == F.col("p.product_id"), "left")
        .select(
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("o.customer_id"),
            F.col("oi.product_id"),
            F.col("o.order_date"),
            F.col("o.order_status"),
            F.col("c.name").alias("customer_name"),
            F.col("c.city"),
            F.col("c.state"),
            F.col("p.product_name"),
            F.col("p.category"),
            F.col("oi.quantity"),
            F.col("oi.price").alias("unit_price"),
            (F.col("oi.quantity") * F.col("oi.price")).alias("sales_amount"),
            F.col("o.total_amount").alias("order_total_amount"),
            F.col("o.created_at").alias("order_created_at"),
            F.col("o.updated_at").alias("order_updated_at"),
            F.lit(run_id).alias("silver_run_id"),
            F.current_timestamp().alias("silver_load_ts"),
        )
    )

    row_count = sales_enriched.count()

    if row_count == 0:
        end_ts = datetime.now()
        update_silver_watermark("sales_enriched", last_enriched_ts or datetime(1900, 1, 1), run_id)
        log_run(run_id, "SILVER", "sales_enriched", "SUCCESS", 0, "No changed orders to enrich", start_ts, end_ts)
        print("No changed orders for sales_enriched. Skipping.")
    else:
        enriched_table = f"{CATALOG}.{SILVER_SCHEMA}.sales_enriched"

        if not table_exists(enriched_table):
            sales_enriched.write.format("delta").mode("overwrite").saveAsTable(enriched_table)
        else:
            delta_enriched = DeltaTable.forName(spark, enriched_table)
            (
                delta_enriched.alias("t")
                .merge(sales_enriched.alias("s"), "t.order_item_id = s.order_item_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        # New watermark = max of order timestamps in this batch
        new_enriched_ts = changed_orders.agg(
            F.greatest(F.max("created_at"), F.max("updated_at")).alias("max_ts")
        ).collect()[0]["max_ts"]

        end_ts = datetime.now()
        update_silver_watermark("sales_enriched", new_enriched_ts, run_id)
        log_run(run_id, "SILVER", "sales_enriched", "SUCCESS", row_count, "Silver enriched table built incrementally", start_ts, end_ts)

        print(f"Sales enriched built successfully. Rows processed: {row_count}")

except Exception as e:
    end_ts = datetime.now()
    set_silver_watermark_failed("sales_enriched")
    log_run(run_id, "SILVER", "sales_enriched", "FAILED", 0, str(e), start_ts, end_ts)
    raise e

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation: confirm silver watermark state after run
# MAGIC SELECT table_name, last_run_ts, last_run_id, status, updated_at
# MAGIC FROM dev_21927.brz_21927.pipeline_watermark
# MAGIC WHERE table_name LIKE 'silver_%'
# MAGIC ORDER BY table_name;