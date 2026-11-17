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

-- Daily saved copy of v_job_scorecard_30d. Dashboards and Genie read the latest
-- snapshot (ops.v_job_scorecard_latest) instead of recomputing from system tables.
CREATE TABLE IF NOT EXISTS ${catalog}.ops.job_scorecard_daily (
  snapshot_date           DATE    COMMENT 'Day the 30-day scorecard was computed',
  job_id                  STRING,
  job_name                STRING,
  cost_30d_usd            DOUBLE  COMMENT 'List price',
  dbus_30d                DOUBLE,
  photon                  BOOLEAN,
  runs                    BIGINT,
  failed_runs             BIGINT,
  failed_run_cost_usd     DOUBLE,
  avg_minutes             DOUBLE,
  clusters_per_run        DOUBLE,
  avg_cpu                 DOUBLE,
  p95_cpu                 DOUBLE,
  avg_mem                 DOUBLE,
  max_swap                DOUBLE,
  oom_events              BIGINT,
  spill_events            BIGINT,
  lost_executor_events    BIGINT,
  error_lines             BIGINT,
  category                STRING  COMMENT 'Fix type',
  top_recommendation      STRING,
  est_monthly_savings_usd DOUBLE  COMMENT 'Estimate, list price',
  fix_effort              STRING,
  est_annual_savings_usd  DOUBLE,
  priority_rank           INT     COMMENT '1 = fix first; NULL when category = OK',
  inserted_at             TIMESTAMP
)
COMMENT 'Append-only daily snapshot of the job scorecard (no-delete policy)'
TBLPROPERTIES ('delta.appendOnly' = 'true');
