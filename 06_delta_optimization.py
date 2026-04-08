# Databricks notebook source
# MAGIC %run ./02_common_config

# COMMAND ----------

from datetime import datetime

# =========================================================
# Optimization config: (schema.table, zorder_columns)
# =========================================================

OPTIMIZE_CONFIG = [
    # Bronze
    ("brz_21927.customers_raw",     "customer_id"),
    ("brz_21927.products_raw",      "product_id"),
    ("brz_21927.orders_raw",        "order_id, customer_id"),
    ("brz_21927.order_items_raw",   "order_id, product_id"),

    # Silver
    ("slv_21927.customers",         "customer_id, state"),
    ("slv_21927.products",          "product_id, category"),
    ("slv_21927.orders",            "order_id, customer_id, order_date"),
    ("slv_21927.order_items",       "order_id, product_id"),
    ("slv_21927.sales_enriched",    "order_date, customer_id, product_id"),

    # Gold
    ("gld_21927.fact_sales",        "order_date_only, state, product_id"),
    ("gld_21927.dim_customers",     "customer_id, state"),
    ("gld_21927.dim_products",      "product_id, category"),
]

VACUUM_RETENTION_HOURS = 168

# COMMAND ----------

# =========================================================
# Run OPTIMIZE + ZORDER
# =========================================================

spark.sql(f"USE CATALOG {CATALOG}")

print("=" * 60)
print("OPTIMIZE + ZORDER")
print("=" * 60)

for table, zorder_cols in OPTIMIZE_CONFIG:
    run_id = generate_run_id()
    start_ts = datetime.now()
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")
        end_ts = datetime.now()
        log_run(run_id, "OPTIMIZE", table, "SUCCESS", 0, f"ZORDER BY ({zorder_cols})", start_ts, end_ts)
        print(f"  OPTIMIZE {table} ZORDER BY ({zorder_cols}) — done")
    except Exception as e:
        end_ts = datetime.now()
        log_run(run_id, "OPTIMIZE", table, "FAILED", 0, str(e), start_ts, end_ts)
        print(f"  OPTIMIZE {table} — FAILED: {e}")

# COMMAND ----------

# =========================================================
# Run VACUUM
# =========================================================

print("=" * 60)
print(f"VACUUM (RETAIN {VACUUM_RETENTION_HOURS} HOURS)")
print("=" * 60)

for table, _ in OPTIMIZE_CONFIG:
    run_id = generate_run_id()
    start_ts = datetime.now()
    try:
        spark.sql(f"VACUUM {table} RETAIN {VACUUM_RETENTION_HOURS} HOURS")
        end_ts = datetime.now()
        log_run(run_id, "VACUUM", table, "SUCCESS", 0, f"RETAIN {VACUUM_RETENTION_HOURS} HOURS", start_ts, end_ts)
        print(f"  VACUUM {table} — done")
    except Exception as e:
        end_ts = datetime.now()
        log_run(run_id, "VACUUM", table, "FAILED", 0, str(e), start_ts, end_ts)
        print(f"  VACUUM {table} — FAILED: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Validation: confirm optimization runs
# MAGIC SELECT run_id, layer_name, table_name, run_status, message,
# MAGIC        run_start_ts, run_end_ts
# MAGIC FROM dev_21927.brz_21927.pipeline_run_log
# MAGIC WHERE layer_name IN ('OPTIMIZE', 'VACUUM')
# MAGIC ORDER BY run_start_ts DESC
# MAGIC LIMIT 30;