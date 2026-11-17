-- =============================================================
-- 90 VERIFY (run manually in the SQL editor; not part of the job)
-- Replace watchtower with your catalog if different.
-- =============================================================

-- Expect delta.appendOnly = true
SHOW TBLPROPERTIES watchtower.ops.job_daily_history;
SHOW TBLPROPERTIES watchtower.ops.job_scorecard_daily;

-- Expect pipelines.reset.allowed = false (after applying watchtower/no-delete.patch)
SHOW TBLPROPERTIES watchtower.bronze.logs;
SHOW TBLPROPERTIES watchtower.silver.logs;

-- Expect only CREATE TABLE and WRITE operations, never DELETE / UPDATE / TRUNCATE
DESCRIBE HISTORY watchtower.ops.job_daily_history;

-- Raw logs are still in the volume (one folder per cluster ID)
LIST '/Volumes/watchtower/default/cluster_logs/cluster-logs/';

-- Smoke test: the scorecard returns your prod jobs
SELECT * FROM watchtower.ops.v_job_scorecard_30d ORDER BY cost_30d_usd DESC LIMIT 20;
