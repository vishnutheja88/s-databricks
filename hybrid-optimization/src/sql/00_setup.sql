-- =============================================================
-- 00 SETUP: ops schema + append-only history table
-- Idempotent. Creates objects only; never drops or rewrites.
-- =============================================================
CREATE SCHEMA IF NOT EXISTS ${catalog}.ops
COMMENT 'Hybrid job optimization: system tables + Watchtower logs';

-- One row per job per day. delta.appendOnly rejects DELETE, UPDATE and overwrite.
CREATE TABLE IF NOT EXISTS ${catalog}.ops.job_daily_history (
  snapshot_date        DATE    COMMENT 'Usage date the row describes',
  job_id               STRING,
  job_name             STRING,
  dbus                 DOUBLE,
  list_cost_usd        DOUBLE  COMMENT 'List price, not negotiated rate',
  photon               BOOLEAN,
  avg_cpu              DOUBLE  COMMENT 'Worker nodes, user+system %',
  p95_cpu              DOUBLE,
  avg_mem              DOUBLE,
  max_swap             DOUBLE,
  oom_events           BIGINT  COMMENT 'From Watchtower driver logs',
  spill_events         BIGINT,
  lost_executor_events BIGINT,
  error_lines          BIGINT,
  runs                 BIGINT,
  failed_runs          BIGINT,
  avg_minutes          DOUBLE,
  inserted_at          TIMESTAMP
)
COMMENT 'Append-only daily snapshot per job (no-delete policy)'
TBLPROPERTIES ('delta.appendOnly' = 'true');
