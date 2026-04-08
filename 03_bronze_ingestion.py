# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

def get_watermark(table_name):
    """Reads the last successful run timestamp from the watermark table."""
    wm_table = "dev_21927.brz_21927.pipeline_watermark"
    wm_df = spark.sql(f"""
        SELECT last_run_ts 
        FROM {wm_table} 
        WHERE table_name = '{table_name}' AND status IN ('INIT', 'SUCCESS')
    """)
    if wm_df.count() == 0:
        return None
    return wm_df.collect()[0]["last_run_ts"]


def update_watermark(table_name, new_ts, run_id):
    """Updates the watermark after a successful load."""
    wm_table = "dev_21927.brz_21927.pipeline_watermark"
    spark.sql(f"""
        MERGE INTO {wm_table} t
        USING (
            SELECT 
                '{table_name}' AS table_name, 
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


def set_watermark_running(table_name):
    """Marks the watermark as RUNNING before processing begins."""
    wm_table = "dev_21927.brz_21927.pipeline_watermark"
    spark.sql(f"""
        UPDATE {wm_table}
        SET status = 'RUNNING', updated_at = current_timestamp()
        WHERE table_name = '{table_name}'
    """)


def set_watermark_failed(table_name):
    """Marks the watermark as FAILED if processing errors out."""
    wm_table = "dev_21927.brz_21927.pipeline_watermark"
    spark.sql(f"""
        UPDATE {wm_table}
        SET status = 'FAILED', updated_at = current_timestamp()
        WHERE table_name = '{table_name}'
    """)

# COMMAND ----------

def load_bronze(table_name):
    run_id = generate_run_id()
    start_ts = datetime.now()

    try:
        cfg = TABLE_CONFIG[table_name]
        file_path = cfg["file_path"]
        bronze_table = cfg["bronze_table"]
        timestamp_cols = cfg["timestamp_cols"]
        watermark_col = cfg.get("watermark_col", timestamp_cols[0])

        # Mark as running
        set_watermark_running(table_name)

        # Read source file
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(file_path)
        )

        df = standardize_timestamps(df, timestamp_cols)

        # Apply watermark filter for incremental load
        last_watermark = get_watermark(table_name)
        if last_watermark is not None:
            df = df.filter(F.col(watermark_col) > F.lit(last_watermark))

        # Capture row count before write (avoids double scan)
        row_count = df.count()

        if row_count == 0:
            end_ts = datetime.now()
            update_watermark(table_name, last_watermark or datetime(1900, 1, 1), run_id)
            log_run(run_id, "BRONZE", table_name, "SUCCESS", 0, "No new rows to load", start_ts, end_ts)
            print(f"No new rows for {table_name}. Skipping.")
            return

        # Capture the max timestamp for the new watermark
        new_watermark = df.agg(F.max(F.col(watermark_col))).collect()[0][0]

        # Add audit columns
        df = (
            df.withColumn("ingestion_timestamp", F.current_timestamp())
              .withColumn("source_file_name", F.lit(file_path))
              .withColumn("pipeline_run_id", F.lit(run_id))
        )

        # Write to bronze
        if not table_exists(bronze_table):
            df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)
        else:
            df.write.format("delta").mode("append").saveAsTable(bronze_table)

        # Update watermark and log success
        end_ts = datetime.now()
        update_watermark(table_name, new_watermark, run_id)
        log_run(run_id, "BRONZE", table_name, "SUCCESS", row_count, "Bronze load completed", start_ts, end_ts)

        print(f"Bronze load successful for {table_name}. Rows loaded: {row_count}")

    except Exception as e:
        end_ts = datetime.now()
        set_watermark_failed(table_name)
        log_run(run_id, "BRONZE", table_name, "FAILED", 0, str(e), start_ts, end_ts)
        raise e

# COMMAND ----------

for table_name in ["customers", "products", "orders", "order_items"]:
    load_bronze(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation: confirm watermark state after run
# MAGIC SELECT table_name, last_run_ts, last_run_id, status, updated_at
# MAGIC FROM dev_21927.brz_21927.pipeline_watermark
# MAGIC ORDER BY table_name;