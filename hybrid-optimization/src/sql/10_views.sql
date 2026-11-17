-- =============================================================
-- 10 VIEWS: read-only hybrid layer (system tables + Watchtower)
-- Every object here is a VIEW; re-creating a view never touches data.
-- ${catalog}.silver.logs is created by the Watchtower pipeline.
-- =============================================================

-- 1. Cluster -> job run map: the key that ties log lines to jobs.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_cluster_map
COMMENT 'Which cluster ran which job run (last 90 days)'
AS
SELECT DISTINCT workspace_id, job_id, job_run_id, cid AS cluster_id
FROM system.lakeflow.job_task_run_timeline
LATERAL VIEW explode(compute_ids) t AS cid
WHERE workspace_id = '${workspace_id}'
  AND period_start_time >= current_date() - INTERVAL 90 DAYS;

-- 2. Daily utilization per job (worker nodes only; single-node jobs have none).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_utilization_daily
COMMENT 'Daily CPU / memory / swap per job from node_timeline'
AS
SELECT m.job_id,
  DATE(n.start_time) AS usage_date,
  COUNT(DISTINCT n.cluster_id) AS clusters,
  ROUND(AVG(n.cpu_user_percent + n.cpu_system_percent), 1) AS avg_cpu,
  ROUND(PERCENTILE(n.cpu_user_percent + n.cpu_system_percent, 0.95), 1) AS p95_cpu,
  ROUND(AVG(n.mem_used_percent), 1) AS avg_mem,
  ROUND(MAX(n.mem_swap_percent), 1) AS max_swap
FROM system.compute.node_timeline n
JOIN (SELECT DISTINCT job_id, cluster_id FROM ${catalog}.ops.v_job_cluster_map) m
  ON n.cluster_id = m.cluster_id
WHERE n.workspace_id = '${workspace_id}'
  AND n.driver = false
GROUP BY ALL;

-- 3. Daily cost per job (list price, USD).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_cost_daily
COMMENT 'Daily DBUs and list cost per job'
AS
SELECT u.usage_metadata.job_id AS job_id,
  u.usage_date,
  ROUND(SUM(u.usage_quantity), 2) AS dbus,
  ROUND(SUM(u.usage_quantity * p.pricing.default), 2) AS list_cost_usd,
  BOOL_OR(u.product_features.is_photon) AS photon
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.cloud = p.cloud
 AND u.usage_unit = p.usage_unit
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.workspace_id = '${workspace_id}'
  AND u.usage_metadata.job_id IS NOT NULL
GROUP BY ALL;

-- 4. Daily log signals per job (Watchtower silver.logs, driver logs only).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_log_signals_daily
COMMENT 'OOM, spill, lost-executor and error counts per job per day'
AS
SELECT m.job_id,
  COALESCE(DATE(l.log_ts), DATE(l.file_modification_time)) AS log_date,
  COUNT_IF(l.log_msg RLIKE '(?i)OutOfMemoryError|GC overhead limit|exit code 137|Container killed.*memory') AS oom_events,
  COUNT_IF(l.log_msg RLIKE '(?i)spill(ing|ed)? .*to disk') AS spill_events,
  COUNT_IF(l.log_msg RLIKE '(?i)Lost executor|ExecutorLostFailure|decommission') AS lost_executor_events,
  COUNT_IF(l.log_level IN ('ERROR', 'FATAL', 'CRITICAL')) AS error_lines,
  COUNT(DISTINCT m.job_run_id) AS runs_with_logs
FROM ${catalog}.silver.logs l
JOIN ${catalog}.ops.v_job_cluster_map m
  ON l.cluster_id = m.cluster_id
GROUP BY ALL;

-- 5. Daily run health per job (one row per run, last state wins).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_run_health_daily
COMMENT 'Runs, failed runs and average duration per job per day'
AS
WITH runs AS (
  SELECT job_id, run_id,
    MIN(period_start_time) AS start_time,
    MAX(period_end_time) AS end_time,
    MAX_BY(result_state, period_end_time) AS result_state
  FROM system.lakeflow.job_run_timeline
  WHERE workspace_id = '${workspace_id}'
  GROUP BY ALL
)
SELECT job_id,
  DATE(start_time) AS run_date,
  COUNT(*) AS runs,
  COUNT_IF(result_state IN ('FAILED', 'TIMED_OUT', 'ERROR')) AS failed_runs,
  ROUND(AVG(timestampdiff(SECOND, start_time, end_time)) / 60, 1) AS avg_minutes
FROM runs
GROUP BY ALL;

-- 6. 30-day scorecard: one row per job with one prioritized recommendation.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_scorecard_30d
COMMENT 'Per-job cost, utilization, log signals and top recommendation (30 days)'
AS
WITH c AS (
  SELECT job_id, SUM(list_cost_usd) AS cost, SUM(dbus) AS dbus, BOOL_OR(photon) AS photon
  FROM ${catalog}.ops.v_job_cost_daily
  WHERE usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY job_id
),
u AS (
  SELECT job_id,
    ROUND(AVG(avg_cpu), 1) AS avg_cpu,
    ROUND(AVG(p95_cpu), 1) AS p95_cpu,
    ROUND(AVG(avg_mem), 1) AS avg_mem,
    MAX(max_swap) AS max_swap
  FROM ${catalog}.ops.v_job_utilization_daily
  WHERE usage_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY job_id
),
l AS (
  SELECT job_id,
    SUM(oom_events) AS oom,
    SUM(spill_events) AS spill,
    SUM(lost_executor_events) AS lost_exec,
    SUM(error_lines) AS errors
  FROM ${catalog}.ops.v_job_log_signals_daily
  WHERE log_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY job_id
),
h AS (
  SELECT job_id,
    SUM(runs) AS runs,
    SUM(failed_runs) AS failed_runs,
    ROUND(AVG(avg_minutes), 1) AS avg_minutes
  FROM ${catalog}.ops.v_job_run_health_daily
  WHERE run_date >= current_date() - INTERVAL 30 DAYS
  GROUP BY job_id
),
k AS (
  SELECT job_id, ROUND(AVG(n), 1) AS clusters_per_run
  FROM (
    SELECT job_id, job_run_id, COUNT(DISTINCT cluster_id) AS n
    FROM ${catalog}.ops.v_job_cluster_map
    GROUP BY ALL
  ) r
  GROUP BY job_id
),
j AS (
  SELECT job_id, name
  FROM system.lakeflow.jobs
  WHERE workspace_id = '${workspace_id}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
)
SELECT c.job_id,
  j.name AS job_name,
  ROUND(c.cost) AS cost_30d_usd,
  c.dbus AS dbus_30d,
  c.photon,
  h.runs,
  h.failed_runs,
  h.avg_minutes,
  k.clusters_per_run,
  u.avg_cpu,
  u.p95_cpu,
  u.avg_mem,
  u.max_swap,
  COALESCE(l.oom, 0) AS oom_events,
  COALESCE(l.spill, 0) AS spill_events,
  COALESCE(l.lost_exec, 0) AS lost_executor_events,
  COALESCE(l.errors, 0) AS error_lines,
  CASE
    WHEN COALESCE(l.oom, 0) > 0 OR u.max_swap > 5
      THEN 'Memory failure: larger or r-family nodes; check skew and partition size'
    WHEN COALESCE(l.spill, 0) > 0 AND u.avg_mem > 70
      THEN 'Spilling: more memory per core or more shuffle partitions'
    WHEN COALESCE(l.lost_exec, 0) > 0
      THEN 'Lost executors: check spot reclaims; SPOT_WITH_FALLBACK, first_on_demand >= 1'
    WHEN h.failed_runs > 0
      THEN 'Failing runs: fix the failure before tuning'
    WHEN k.clusters_per_run > 1
      THEN 'Several clusters per run: share one job cluster across tasks'
    WHEN u.p95_cpu < 40 AND u.avg_mem < 50
      THEN 'Oversized: lower max workers or use smaller nodes'
    WHEN u.avg_cpu > 85
      THEN 'CPU-bound: c-family nodes or more workers'
    ELSE 'OK'
  END AS top_recommendation
FROM c
LEFT JOIN j USING (job_id)
LEFT JOIN u USING (job_id)
LEFT JOIN l USING (job_id)
LEFT JOIN h USING (job_id)
LEFT JOIN k USING (job_id);
