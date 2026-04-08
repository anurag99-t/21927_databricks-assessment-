# Databricks notebook source
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- 01_setup_dev.sql (Improved)
# MAGIC -- ============================================
# MAGIC
# MAGIC -- ============================================
# MAGIC -- SECTION 1: Catalog & Schemas
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS dev_21927;
# MAGIC USE CATALOG dev_21927;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS brz_21927 COMMENT 'Bronze layer - raw/landing zone for ingested source data';
# MAGIC CREATE SCHEMA IF NOT EXISTS slv_21927 COMMENT 'Silver layer - cleansed, conformed, and deduplicated data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gld_21927 COMMENT 'Gold layer - business-ready aggregates and reporting tables';
# MAGIC
# MAGIC -- ============================================
# MAGIC -- SECTION 2: Volume for source files
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS dev_21927.brz_21927.manuallyuploadedfiles
# MAGIC COMMENT 'Landing zone for manually uploaded source files (CSV, Excel, etc.)';
# MAGIC
# MAGIC -- ============================================
# MAGIC -- SECTION 3: Watermark table
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev_21927.brz_21927.pipeline_watermark (
# MAGIC     table_name  STRING       COMMENT 'Name of the source table being tracked (e.g. customers, orders)',
# MAGIC     last_run_ts TIMESTAMP    COMMENT 'Timestamp of the last successfully processed record for this table',
# MAGIC     last_run_id STRING       COMMENT 'Run ID of the pipeline run that last updated this watermark (links to pipeline_run_log.run_id)',
# MAGIC     status      STRING       COMMENT 'Current pipeline status for this table. Allowed values: INIT, RUNNING, SUCCESS, FAILED',
# MAGIC     updated_at  TIMESTAMP    COMMENT 'Timestamp when this watermark row was last modified'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Tracks incremental processing state per source table. Used by bronze ingestion to determine which records to pick up on the next run.'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Enforce allowed status values
# MAGIC ALTER TABLE dev_21927.brz_21927.pipeline_watermark
# MAGIC ADD CONSTRAINT valid_status CHECK (status IN ('INIT', 'RUNNING', 'SUCCESS', 'FAILED'));
# MAGIC
# MAGIC -- ============================================
# MAGIC -- SECTION 4: Run log table
# MAGIC -- ============================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev_21927.brz_21927.pipeline_run_log (
# MAGIC     run_id       STRING      COMMENT 'Unique identifier for the pipeline run',
# MAGIC     layer_name   STRING      COMMENT 'Medallion layer being processed (brz, slv, gld)',
# MAGIC     table_name   STRING      COMMENT 'Target table name being written to',
# MAGIC     run_status   STRING      COMMENT 'Outcome of the run: SUCCESS or FAILED',
# MAGIC     row_count    BIGINT      COMMENT 'Number of rows written during this run',
# MAGIC     message      STRING      COMMENT 'Human-readable summary or error message',
# MAGIC     run_start_ts TIMESTAMP   COMMENT 'Timestamp when the pipeline run started',
# MAGIC     run_end_ts   TIMESTAMP   COMMENT 'Timestamp when the pipeline run completed'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Audit log capturing execution details for every pipeline run across all layers.'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- ============================================
# MAGIC -- SECTION 5: Seed watermark (idempotent)
# MAGIC -- ============================================
# MAGIC -- Safe to re-run: MERGE ensures existing
# MAGIC -- watermarks are not overwritten.
# MAGIC -- CREATE TABLE IF NOT EXISTS above guarantees
# MAGIC -- the target table exists even if this block
# MAGIC -- is executed independently.
# MAGIC -- ============================================
# MAGIC
# MAGIC MERGE INTO dev_21927.brz_21927.pipeline_watermark t
# MAGIC USING (
# MAGIC     SELECT 'customers'   AS table_name, TIMESTAMP('1900-01-01 00:00:00') AS last_run_ts, CAST(NULL AS STRING) AS last_run_id, 'INIT' AS status, current_timestamp() AS updated_at
# MAGIC     UNION ALL
# MAGIC     SELECT 'products',   TIMESTAMP('1900-01-01 00:00:00'), NULL, 'INIT', current_timestamp()
# MAGIC     UNION ALL
# MAGIC     SELECT 'orders',     TIMESTAMP('1900-01-01 00:00:00'), NULL, 'INIT', current_timestamp()
# MAGIC     UNION ALL
# MAGIC     SELECT 'order_items', TIMESTAMP('1900-01-01 00:00:00'), NULL, 'INIT', current_timestamp()
# MAGIC ) s
# MAGIC ON t.table_name = s.table_name
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (table_name, last_run_ts, last_run_id, status, updated_at)
# MAGIC VALUES (s.table_name, s.last_run_ts, s.last_run_id, s.status, s.updated_at);
# MAGIC
# MAGIC -- ============================================
# MAGIC -- SECTION 6: Validation
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT 'SCHEMAS' AS check_type, schema_name AS object_name, 'EXISTS' AS status
# MAGIC FROM dev_21927.information_schema.schemata
# MAGIC WHERE schema_name IN ('brz_21927', 'slv_21927', 'gld_21927')
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'TABLE', table_name, 'EXISTS'
# MAGIC FROM dev_21927.information_schema.tables
# MAGIC WHERE table_schema = 'brz_21927'
# MAGIC   AND table_name IN ('pipeline_watermark', 'pipeline_run_log')
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'WATERMARK_SEED', table_name, status
# MAGIC FROM dev_21927.brz_21927.pipeline_watermark
# MAGIC ORDER BY check_type, object_name;