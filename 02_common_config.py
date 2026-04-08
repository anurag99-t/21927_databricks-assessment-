# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import uuid
from datetime import datetime

# =========================================================
# ENV CONFIG
# =========================================================
try:
    CATALOG = spark.conf.get("pipeline.catalog")
except Exception:
    CATALOG = "dev_21927"

BRONZE_SCHEMA = "brz_21927"
SILVER_SCHEMA = "slv_21927"
GOLD_SCHEMA = "gld_21927"

VOLUME_NAME = "manuallyuploadedfiles"

BASE_VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
WATERMARK_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_watermark"
RUN_LOG_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.pipeline_run_log"

spark.sql(f"USE CATALOG `{CATALOG}`")  # Fixed: wrap catalog name in backticks

TABLE_CONFIG = {
    "customers": {
        "file_path": f"{BASE_VOLUME_PATH}/customers.csv",
        "bronze_table": f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw",
        "silver_table": f"{CATALOG}.{SILVER_SCHEMA}.customers",
        "pk": "customer_id",
        "timestamp_cols": ["signup_date", "created_at", "updated_at"]
    },
    "products": {
        "file_path": f"{BASE_VOLUME_PATH}/products.csv",
        "bronze_table": f"{CATALOG}.{BRONZE_SCHEMA}.products_raw",
        "silver_table": f"{CATALOG}.{SILVER_SCHEMA}.products",
        "pk": "product_id",
        "timestamp_cols": ["created_at", "updated_at"]
    },
    "orders": {
        "file_path": f"{BASE_VOLUME_PATH}/orders.csv",
        "bronze_table": f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw",
        "silver_table": f"{CATALOG}.{SILVER_SCHEMA}.orders",
        "pk": "order_id",
        "timestamp_cols": ["order_date", "created_at", "updated_at"]
    },
    "order_items": {
        "file_path": f"{BASE_VOLUME_PATH}/order_items.csv",
        "bronze_table": f"{CATALOG}.{BRONZE_SCHEMA}.order_items_raw",
        "silver_table": f"{CATALOG}.{SILVER_SCHEMA}.order_items",
        "pk": "order_item_id",
        "timestamp_cols": ["created_at", "updated_at"]
    }
}

def generate_run_id():
    return str(uuid.uuid4())

def log_run(run_id, layer_name, table_name, run_status, row_count, message, run_start_ts, run_end_ts):
    log_df = spark.createDataFrame(
        [(run_id, layer_name, table_name, run_status, int(row_count), message, run_start_ts, run_end_ts)],
        ["run_id", "layer_name", "table_name", "run_status", "row_count", "message", "run_start_ts", "run_end_ts"]
    )
    log_df.write.format("delta").mode("append").saveAsTable(RUN_LOG_TABLE)

def get_last_run_ts(table_name):
    query = f"""
        SELECT COALESCE(MAX(last_run_ts), TIMESTAMP('1900-01-01 00:00:00')) AS last_run_ts
        FROM {WATERMARK_TABLE}
        WHERE table_name = '{table_name}'
          AND status IN ('SUCCESS', 'INIT')
    """
    return spark.sql(query).collect()[0]["last_run_ts"]

def update_watermark(table_name, last_run_ts):
    query = f"""
        MERGE INTO {WATERMARK_TABLE} t
        USING (
            SELECT
                '{table_name}' AS table_name,
                TIMESTAMP('{last_run_ts}') AS last_run_ts,
                'SUCCESS' AS status,
                current_timestamp() AS updated_at
        ) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET
            t.last_run_ts = s.last_run_ts,
            t.status = s.status,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN
            INSERT (table_name, last_run_ts, status, updated_at)
            VALUES (s.table_name, s.last_run_ts, s.status, s.updated_at)
    """
    spark.sql(query)

def standardize_timestamps(df, timestamp_cols):
    for c in timestamp_cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df

def deduplicate_latest(df, pk_col):
    w = Window.partitionBy(pk_col).orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("created_at").desc_nulls_last()
    )
    return (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

def table_exists(table_name):
    return spark.catalog.tableExists(table_name)