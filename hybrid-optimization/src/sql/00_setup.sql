-- =============================================================
-- 00 SETUP: ops schema, checkpoint volume, append-only stores.
-- Idempotent. Creates objects only; never drops or rewrites.
-- History and scorecard tables are created in 20/21 from their views.
-- =============================================================
CREATE SCHEMA IF NOT EXISTS ${catalog}.ops
COMMENT 'Hybrid job optimization pilot: system tables + Spark/job logs';

-- Streaming checkpoints for the Spark event-log ingest (Auto Loader).
CREATE VOLUME IF NOT EXISTS ${catalog}.ops.checkpoints
COMMENT 'Auto Loader checkpoints (hybrid-optimization)';

-- Evidence: which workspaces and review period were analyzed (acceptance criterion 1).
CREATE TABLE IF NOT EXISTS ${catalog}.ops.scope_history (
  recorded_at      TIMESTAMP,
  workspace_id     STRING,
  workspace_name   STRING,
  review_label     STRING  COMMENT 'Fixed dates or rolling 30 days',
  review_start     DATE    COMMENT 'Resolved start on the day recorded',
  review_end       DATE,
  price_multiplier DOUBLE,
  recorded_by      STRING
)
COMMENT 'Append-only record of pilot scope (workspaces, review period)'
TBLPROPERTIES ('delta.appendOnly' = 'true');

-- Evidence: access to every source was checked (acceptance criterion 2).
CREATE TABLE IF NOT EXISTS ${catalog}.ops.access_checks (
  checked_at  TIMESTAMP,
  source      STRING  COMMENT 'system tables | spark logs | job logs | lineage | query history',
  object_name STRING,
  required    BOOLEAN,
  ok          BOOLEAN,
  detail      STRING,
  checked_by  STRING
)
COMMENT 'Append-only access checks, one row per source per run'
TBLPROPERTIES ('delta.appendOnly' = 'true');

-- Spark event logs summarized per stage per file version (shuffle, spill, skew).
CREATE TABLE IF NOT EXISTS ${catalog}.ops.spark_stage_metrics (
  ingested_at          TIMESTAMP,
  cluster_id           STRING,
  file_path            STRING,
  file_modified_at     TIMESTAMP COMMENT 'Active event logs are re-delivered; views keep the newest version',
  stage_id             BIGINT,
  stage_attempt        BIGINT,
  tasks                BIGINT,
  failed_tasks         BIGINT,
  executor_run_ms      BIGINT,
  max_task_ms          BIGINT,
  p50_task_ms          BIGINT,
  input_bytes          BIGINT,
  shuffle_read_bytes   BIGINT,
  shuffle_write_bytes  BIGINT,
  memory_spilled_bytes BIGINT,
  disk_spilled_bytes   BIGINT,
  first_task_end       TIMESTAMP,
  last_task_end        TIMESTAMP
)
COMMENT 'Append-only per-stage summaries parsed from Spark event logs'
TBLPROPERTIES ('delta.appendOnly' = 'true');

-- File sizing and maintenance of tables written by in-scope jobs.
CREATE TABLE IF NOT EXISTS ${catalog}.ops.table_layout_snapshot (
  snapshot_date       DATE,
  table_name          STRING,
  table_format        STRING,
  num_files           BIGINT,
  size_bytes          BIGINT,
  avg_file_mb         DOUBLE,
  partition_columns   STRING,
  clustering_columns  STRING,
  last_optimize_at    TIMESTAMP,
  detail              STRING,
  inserted_at         TIMESTAMP
)
COMMENT 'Append-only daily DESCRIBE DETAIL / HISTORY summary per written table'
TBLPROPERTIES ('delta.appendOnly' = 'true');

-- Validation: automated source-vs-ops checks and manual profiling-tool checks.
CREATE TABLE IF NOT EXISTS ${catalog}.ops.validation_results (
  validated_at   TIMESTAMP,
  kind           STRING  COMMENT 'auto | manual | genie_benchmark',
  workspace_id   STRING,
  check_name     STRING,
  source_value   STRING  COMMENT 'Answer from raw system tables / logs / tool',
  ops_value      STRING  COMMENT 'Answer from the ops layer (what Genie and the dashboard use)',
  passed         BOOLEAN,
  detail         STRING,
  validated_by   STRING
)
COMMENT 'Append-only validation evidence'
TBLPROPERTIES ('delta.appendOnly' = 'true');
