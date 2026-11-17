-- =============================================================
-- 20 DAILY HISTORY: append one row per job for one day.
-- Default day = 2 days ago (billing and node data have settled).
-- Backfill: run the job with snapshot_date=YYYY-MM-DD (tools/backfill.py).
-- Re-running for a day that is already written inserts nothing.
-- The table takes its columns from v_job_daily_metrics on first run.
-- =============================================================
CREATE TABLE IF NOT EXISTS ${catalog}.ops.job_daily_history
COMMENT 'Append-only daily metrics per job (no-delete policy)'
TBLPROPERTIES ('delta.appendOnly' = 'true')
AS SELECT d.*, current_timestamp() AS inserted_at
FROM ${catalog}.ops.v_job_daily_metrics d
WHERE 1 = 0;

INSERT INTO ${catalog}.ops.job_daily_history
SELECT d.*, current_timestamp() AS inserted_at
FROM ${catalog}.ops.v_job_daily_metrics d
WHERE d.metric_date = ${snapshot_date_expr}
  AND NOT EXISTS (
    SELECT 1 FROM ${catalog}.ops.job_daily_history x WHERE x.metric_date = ${snapshot_date_expr}
  );
