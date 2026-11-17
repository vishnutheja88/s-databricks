-- =============================================================
-- 10 VIEWS: read-only hybrid layer (system tables + Spark/job logs)
-- Every object here is a VIEW: re-creating one never touches data.
-- Keys are (workspace_id, job_id): job IDs are only unique within a workspace.
-- Scope: workspaces IN (${workspace_ids}), review period ${review_label}.
-- Sources that may not be enabled get a fallback view first, then an
-- optional real definition that replaces it when the source exists.
-- =============================================================

-- 0. Scope: workspaces and review period in this pilot.
CREATE OR REPLACE VIEW ${catalog}.ops.v_scope
COMMENT 'Pilot scope: workspaces and the review period (${review_label})'
AS
SELECT workspace_id, workspace_name,
  ${review_start_expr} AS review_start,
  ${review_end_expr} AS review_end,
  '${review_label}' AS review_label,
  datediff(${review_end_expr}, ${review_start_expr}) + 1 AS review_days
FROM VALUES ${scope_values} AS s(workspace_id, workspace_name);

-- 1. Cluster -> job run map: ties node metrics, logs and event logs to jobs.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_cluster_map
COMMENT 'Which cluster ran which job run and task (last 90 days)'
AS
SELECT DISTINCT workspace_id, job_id, job_run_id, task_key, cid AS cluster_id
FROM system.lakeflow.job_task_run_timeline
LATERAL VIEW explode(compute_ids) t AS cid
WHERE workspace_id IN (${workspace_ids})
  AND period_start_time >= current_date() - INTERVAL 90 DAYS;

-- 2. Daily cost per job. cost_usd = usage x effective list x ${price_multiplier} (reported).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_cost_daily
COMMENT 'Daily DBUs and cost per job; cost_usd = usage x effective list x ${price_multiplier}'
AS
SELECT u.workspace_id,
  u.usage_metadata.job_id AS job_id,
  u.usage_date,
  ROUND(SUM(u.usage_quantity), 2) AS dbus,
  ROUND(SUM(u.usage_quantity * p.pricing.effective_list.default * ${price_multiplier}), 2) AS cost_usd,
  ROUND(SUM(u.usage_quantity * p.pricing.effective_list.default), 2) AS list_cost_usd,
  BOOL_OR(u.product_features.is_photon) AS photon
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.cloud = p.cloud
 AND u.usage_unit = p.usage_unit
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.workspace_id IN (${workspace_ids})
  AND u.usage_metadata.job_id IS NOT NULL
  AND u.usage_date >= current_date() - INTERVAL 120 DAYS
GROUP BY ALL;

-- 3. Daily utilization and idle time per job (worker nodes only).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_utilization_daily
COMMENT 'Daily CPU, memory, swap and idle node-hours per job (idle = CPU below ${idle_cpu_pct} percent)'
AS
SELECT m.workspace_id, m.job_id,
  DATE(n.start_time) AS usage_date,
  COUNT(DISTINCT n.cluster_id) AS clusters,
  ROUND(SUM(timestampdiff(SECOND, n.start_time, n.end_time)) / 3600.0, 2) AS node_hours,
  ROUND(SUM(CASE WHEN n.cpu_user_percent + n.cpu_system_percent < ${idle_cpu_pct}
                 THEN timestampdiff(SECOND, n.start_time, n.end_time) ELSE 0 END) / 3600.0, 2) AS idle_node_hours,
  ROUND(AVG(n.cpu_user_percent + n.cpu_system_percent), 1) AS avg_cpu,
  ROUND(PERCENTILE(n.cpu_user_percent + n.cpu_system_percent, 0.95), 1) AS p95_cpu,
  ROUND(AVG(n.mem_used_percent), 1) AS avg_mem,
  ROUND(MAX(n.mem_swap_percent), 1) AS max_swap
FROM system.compute.node_timeline n
JOIN (SELECT DISTINCT workspace_id, job_id, cluster_id FROM ${catalog}.ops.v_job_cluster_map) m
  ON n.workspace_id = m.workspace_id AND n.cluster_id = m.cluster_id
WHERE n.workspace_id IN (${workspace_ids})
  AND n.driver = false
GROUP BY ALL;

-- 4. Idle time per cluster over the review period (Genie: clusters with the highest idle time).
CREATE OR REPLACE VIEW ${catalog}.ops.v_cluster_idle
COMMENT 'Per-cluster node-hours, idle node-hours and estimated idle cost in the review period'
AS
WITH sc AS (SELECT DISTINCT review_start, review_end FROM ${catalog}.ops.v_scope),
n AS (
  SELECT n.workspace_id, n.cluster_id,
    SUM(timestampdiff(SECOND, n.start_time, n.end_time)) / 3600.0 AS node_hours,
    SUM(CASE WHEN n.cpu_user_percent + n.cpu_system_percent < ${idle_cpu_pct}
             THEN timestampdiff(SECOND, n.start_time, n.end_time) ELSE 0 END) / 3600.0 AS idle_node_hours
  FROM system.compute.node_timeline n CROSS JOIN sc
  WHERE n.workspace_id IN (${workspace_ids})
    AND n.driver = false
    AND DATE(n.start_time) BETWEEN sc.review_start AND sc.review_end
  GROUP BY ALL
),
c AS (
  SELECT u.workspace_id, u.usage_metadata.cluster_id AS cluster_id,
    SUM(u.usage_quantity * p.pricing.effective_list.default * ${price_multiplier}) AS cost_usd
  FROM system.billing.usage u
  JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
   AND u.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
  CROSS JOIN sc
  WHERE u.workspace_id IN (${workspace_ids})
    AND u.usage_metadata.cluster_id IS NOT NULL
    AND u.usage_date BETWEEN sc.review_start AND sc.review_end
  GROUP BY ALL
),
cl AS (
  SELECT workspace_id, cluster_id, cluster_name, cluster_source
  FROM system.compute.clusters
  WHERE workspace_id IN (${workspace_ids})
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1
),
m AS (SELECT workspace_id, cluster_id, MAX(job_id) AS job_id FROM ${catalog}.ops.v_job_cluster_map GROUP BY ALL)
SELECT n.workspace_id, s.workspace_name, n.cluster_id, cl.cluster_name, cl.cluster_source, m.job_id,
  ROUND(n.node_hours, 1) AS node_hours,
  ROUND(n.idle_node_hours, 1) AS idle_node_hours,
  ROUND(100 * n.idle_node_hours / NULLIF(n.node_hours, 0), 1) AS idle_pct,
  ROUND(c.cost_usd, 2) AS cost_usd,
  ROUND(c.cost_usd * n.idle_node_hours / NULLIF(n.node_hours, 0), 2) AS est_idle_cost_usd
FROM n
JOIN ${catalog}.ops.v_scope s ON s.workspace_id = n.workspace_id
LEFT JOIN c ON c.workspace_id = n.workspace_id AND c.cluster_id = n.cluster_id
LEFT JOIN cl ON cl.workspace_id = n.workspace_id AND cl.cluster_id = n.cluster_id
LEFT JOIN m ON m.workspace_id = n.workspace_id AND m.cluster_id = n.cluster_id;

-- 5. Daily log signals per job (Watchtower silver.logs, driver logs).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_log_signals_daily
COMMENT 'OOM, spill, lost-executor and error counts per job per day from driver logs'
AS
SELECT m.workspace_id, m.job_id,
  COALESCE(DATE(l.log_ts), DATE(l.file_modification_time)) AS log_date,
  COUNT_IF(l.log_msg RLIKE '(?i)OutOfMemoryError|GC overhead limit|exit code 137|Container killed.*memory') AS oom_events,
  COUNT_IF(l.log_msg RLIKE '(?i)spill(ing|ed)? .*to disk') AS spill_events,
  COUNT_IF(l.log_msg RLIKE '(?i)Lost executor|ExecutorLostFailure') AS lost_executor_events,
  COUNT_IF(l.log_level IN ('ERROR', 'FATAL', 'CRITICAL')) AS error_lines,
  COUNT(DISTINCT m.job_run_id) AS runs_with_logs
FROM ${catalog}.silver.logs l
JOIN (SELECT DISTINCT workspace_id, job_id, job_run_id, cluster_id FROM ${catalog}.ops.v_job_cluster_map) m
  ON l.cluster_id = m.cluster_id
GROUP BY ALL;

-- 6. Daily run health per job (one row per run, last state wins).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_run_health_daily
COMMENT 'Runs, failed runs, average and longest duration per job per day'
AS
WITH runs AS (
  SELECT workspace_id, job_id, run_id,
    MIN(period_start_time) AS start_time,
    MAX(period_end_time) AS end_time,
    MAX_BY(result_state, period_end_time) AS result_state
  FROM system.lakeflow.job_run_timeline
  WHERE workspace_id IN (${workspace_ids})
    AND period_start_time >= current_date() - INTERVAL 120 DAYS
  GROUP BY ALL
)
SELECT workspace_id, job_id,
  DATE(start_time) AS run_date,
  COUNT(*) AS runs,
  COUNT_IF(result_state IN ('FAILED', 'TIMED_OUT', 'ERROR')) AS failed_runs,
  ROUND(AVG(timestampdiff(SECOND, start_time, end_time)) / 60, 1) AS avg_minutes,
  ROUND(MAX(timestampdiff(SECOND, start_time, end_time)) / 60, 1) AS max_minutes
FROM runs
GROUP BY ALL;

-- 7. Daily task retries per job (a task with more than one attempt in a run).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_retries_daily
COMMENT 'Task retries and repairs per job per day'
AS
WITH t AS (
  SELECT workspace_id, job_id, job_run_id, task_key,
    COUNT(DISTINCT run_id) AS attempts,
    MIN(period_start_time) AS first_start
  FROM system.lakeflow.job_task_run_timeline
  WHERE workspace_id IN (${workspace_ids})
    AND period_start_time >= current_date() - INTERVAL 120 DAYS
  GROUP BY ALL
)
SELECT workspace_id, job_id,
  DATE(first_start) AS run_date,
  SUM(attempts - 1) AS retries,
  COUNT(DISTINCT CASE WHEN attempts > 1 THEN job_run_id END) AS runs_with_retries
FROM t
GROUP BY ALL;

-- 8. Daily Spark metrics per job from event logs: shuffle, spill, skew.
--    Keeps only the newest delivered version of each event-log file.
--    Skew = longest task / median task, for stages with 20+ tasks and a 1+ minute longest task.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_spark_metrics_daily
COMMENT 'Shuffle read/write, disk spill and worst stage skew per job per day (Spark event logs)'
AS
WITH latest AS (
  SELECT *
  FROM ${catalog}.ops.spark_stage_metrics
  QUALIFY file_modified_at = MAX(file_modified_at) OVER (PARTITION BY file_path)
),
s AS (
  SELECT cluster_id, DATE(first_task_end) AS metric_date,
    SUM(shuffle_read_bytes) AS shuffle_read_bytes,
    SUM(shuffle_write_bytes) AS shuffle_write_bytes,
    SUM(disk_spilled_bytes) AS disk_spilled_bytes,
    SUM(memory_spilled_bytes) AS memory_spilled_bytes,
    MAX(CASE WHEN tasks >= 20 AND max_task_ms >= 60000
             THEN max_task_ms / GREATEST(p50_task_ms, 1) END) AS max_skew_ratio
  FROM latest
  GROUP BY ALL
)
SELECT m.workspace_id, m.job_id, s.metric_date,
  ROUND(SUM(s.shuffle_read_bytes) / 1e9, 2) AS shuffle_read_gb,
  ROUND(SUM(s.shuffle_write_bytes) / 1e9, 2) AS shuffle_write_gb,
  ROUND(SUM(s.disk_spilled_bytes) / 1e9, 2) AS disk_spill_gb,
  ROUND(SUM(s.memory_spilled_bytes) / 1e9, 2) AS memory_spill_gb,
  ROUND(MAX(s.max_skew_ratio), 1) AS max_skew_ratio
FROM s
JOIN (SELECT DISTINCT workspace_id, job_id, cluster_id FROM ${catalog}.ops.v_job_cluster_map) m
  ON s.cluster_id = m.cluster_id
GROUP BY ALL;

-- 9. Daily SQL query signals per job (system.query.history: SQL warehouse and serverless only).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_query_daily
COMMENT 'Slow and spilling SQL queries per job per day (empty if query history is not enabled)'
AS
SELECT CAST(NULL AS STRING) AS workspace_id, CAST(NULL AS STRING) AS job_id, CAST(NULL AS DATE) AS metric_date,
  CAST(0 AS BIGINT) AS queries, CAST(0 AS BIGINT) AS slow_queries, CAST(0 AS BIGINT) AS spilled_queries,
  CAST(0 AS DOUBLE) AS query_shuffle_read_gb
WHERE 1 = 0;

-- @optional: replaces the fallback above when system.query.history is enabled.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_query_daily
COMMENT 'Slow and spilling SQL queries per job per day (system.query.history)'
AS
SELECT workspace_id,
  query_source.job_info.job_id AS job_id,
  DATE(start_time) AS metric_date,
  COUNT(*) AS queries,
  COUNT_IF(total_duration_ms > 300000) AS slow_queries,
  COUNT_IF(spilled_local_bytes > 0) AS spilled_queries,
  ROUND(SUM(shuffle_read_bytes) / 1e9, 2) AS query_shuffle_read_gb
FROM system.query.history
WHERE workspace_id IN (${workspace_ids})
  AND query_source.job_info.job_id IS NOT NULL
  AND start_time >= current_date() - INTERVAL 120 DAYS
GROUP BY ALL;

-- 10. Slowest SQL queries in the review period, all sources (query optimization).
CREATE OR REPLACE VIEW ${catalog}.ops.v_query_hotspots
COMMENT 'Top 100 slowest queries per workspace in the review period (empty if query history is not enabled)'
AS
SELECT CAST(NULL AS STRING) AS workspace_id, CAST(NULL AS STRING) AS statement_id,
  CAST(NULL AS STRING) AS source_type, CAST(NULL AS STRING) AS job_id, CAST(NULL AS STRING) AS warehouse_id,
  CAST(NULL AS TIMESTAMP) AS start_time, CAST(NULL AS DOUBLE) AS duration_min, CAST(NULL AS DOUBLE) AS read_gb,
  CAST(NULL AS DOUBLE) AS spilled_gb, CAST(NULL AS DOUBLE) AS shuffle_read_gb, CAST(NULL AS STRING) AS statement_preview
WHERE 1 = 0;

-- @optional: replaces the fallback above when system.query.history is enabled.
CREATE OR REPLACE VIEW ${catalog}.ops.v_query_hotspots
COMMENT 'Top 100 slowest queries per workspace in the review period (system.query.history)'
AS
SELECT q.workspace_id, q.statement_id,
  CASE WHEN q.query_source.job_info.job_id IS NOT NULL THEN 'job'
       WHEN q.query_source.dashboard_id IS NOT NULL THEN 'dashboard'
       ELSE 'other' END AS source_type,
  q.query_source.job_info.job_id AS job_id,
  q.compute.warehouse_id AS warehouse_id,
  q.start_time,
  ROUND(q.total_duration_ms / 60000.0, 1) AS duration_min,
  ROUND(q.read_bytes / 1e9, 2) AS read_gb,
  ROUND(q.spilled_local_bytes / 1e9, 2) AS spilled_gb,
  ROUND(q.shuffle_read_bytes / 1e9, 2) AS shuffle_read_gb,
  LEFT(q.statement_text, 300) AS statement_preview
FROM system.query.history q
CROSS JOIN (SELECT DISTINCT review_start, review_end FROM ${catalog}.ops.v_scope) sc
WHERE q.workspace_id IN (${workspace_ids})
  AND DATE(q.start_time) BETWEEN sc.review_start AND sc.review_end
QUALIFY ROW_NUMBER() OVER (PARTITION BY q.workspace_id ORDER BY q.total_duration_ms DESC) <= 100;

-- 11. Tables each job writes (lineage), used for file sizing and maintenance checks.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_written_tables
COMMENT 'Tables written by in-scope jobs, last 90 days (empty if lineage is not enabled)'
AS
SELECT CAST(NULL AS STRING) AS workspace_id, CAST(NULL AS STRING) AS job_id,
  CAST(NULL AS STRING) AS table_name, CAST(NULL AS DATE) AS last_written, CAST(0 AS BIGINT) AS write_events
WHERE 1 = 0;

-- @optional: replaces the fallback above when system.access.table_lineage is enabled.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_written_tables
COMMENT 'Tables written by in-scope jobs, last 90 days (system.access.table_lineage)'
AS
SELECT workspace_id, entity_id AS job_id, target_table_full_name AS table_name,
  MAX(event_date) AS last_written, COUNT(*) AS write_events
FROM system.access.table_lineage
WHERE workspace_id IN (${workspace_ids})
  AND entity_type = 'JOB'
  AND target_table_full_name IS NOT NULL
  AND event_date >= current_date() - INTERVAL 90 DAYS
GROUP BY ALL;

-- 12. Data layout per job: small files and unmaintained tables among the tables it writes.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_layout
COMMENT 'Small-file and unmaintained tables written by each job (latest layout snapshot)'
AS
WITH lt AS (
  SELECT *
  FROM ${catalog}.ops.table_layout_snapshot
  QUALIFY ROW_NUMBER() OVER (PARTITION BY table_name ORDER BY snapshot_date DESC, inserted_at DESC) = 1
),
x AS (
  SELECT w.workspace_id, w.job_id, lt.table_name,
    (lt.avg_file_mb < 32 AND lt.num_files > 1000) AS small_files,
    (lt.num_files > 1000 AND COALESCE(lt.clustering_columns, '') = ''
     AND (lt.last_optimize_at IS NULL OR lt.last_optimize_at < current_date() - INTERVAL 30 DAYS)) AS unmaintained
  FROM ${catalog}.ops.v_job_written_tables w
  JOIN lt ON lt.table_name = w.table_name
)
SELECT workspace_id, job_id,
  COUNT(*) AS tables_checked,
  COUNT_IF(small_files) AS small_file_tables,
  COUNT_IF(unmaintained) AS unmaintained_tables,
  CONCAT_WS(', ', SLICE(COLLECT_LIST(CASE WHEN small_files OR unmaintained THEN table_name END), 1, 5)) AS layout_tables
FROM x
GROUP BY ALL;

-- 13. Latest cluster configuration per job: runtime version, policy, node type.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_config
COMMENT 'Latest runtime, node type, max workers and policy use per job (empty if columns differ)'
AS
SELECT CAST(NULL AS STRING) AS workspace_id, CAST(NULL AS STRING) AS job_id, CAST(NULL AS STRING) AS dbr_version,
  CAST(NULL AS INT) AS min_dbr_code, CAST(NULL AS STRING) AS worker_node_type, CAST(NULL AS INT) AS max_workers,
  CAST(NULL AS BOOLEAN) AS policy_missing
WHERE 1 = 0;

-- @optional: replaces the fallback above (system.compute.clusters).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_config
COMMENT 'Latest runtime, node type, max workers and policy use per job (system.compute.clusters)'
AS
WITH cl AS (
  SELECT *
  FROM system.compute.clusters
  WHERE workspace_id IN (${workspace_ids})
  QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1
)
SELECT m.workspace_id, m.job_id,
  MAX_BY(cl.dbr_version, cl.change_time) AS dbr_version,
  MIN(CAST(regexp_extract(cl.dbr_version, '^([0-9]+)\\.([0-9]+)', 1) AS INT) * 100
      + CAST(regexp_extract(cl.dbr_version, '^([0-9]+)\\.([0-9]+)', 2) AS INT)) AS min_dbr_code,
  MAX_BY(cl.worker_node_type, cl.change_time) AS worker_node_type,
  MAX_BY(CAST(COALESCE(cl.max_autoscale_workers, cl.worker_count) AS INT), cl.change_time) AS max_workers,
  BOOL_OR(cl.policy_id IS NULL) AS policy_missing
FROM (SELECT DISTINCT workspace_id, job_id, cluster_id FROM ${catalog}.ops.v_job_cluster_map) m
JOIN cl ON cl.workspace_id = m.workspace_id AND cl.cluster_id = m.cluster_id
GROUP BY m.workspace_id, m.job_id;

-- 14. Schedule hotspots: run starts per workspace and hour of day (UTC) in the review period.
CREATE OR REPLACE VIEW ${catalog}.ops.v_schedule_hotspots
COMMENT 'Job runs started per workspace per UTC hour in the review period; busy_rank 1 = busiest hour'
AS
WITH sc AS (SELECT DISTINCT review_start, review_end, review_days FROM ${catalog}.ops.v_scope),
r AS (
  SELECT t.workspace_id, t.job_id, t.run_id, MIN(t.period_start_time) AS start_time
  FROM system.lakeflow.job_run_timeline t CROSS JOIN sc
  WHERE t.workspace_id IN (${workspace_ids})
    AND DATE(t.period_start_time) BETWEEN sc.review_start AND sc.review_end
  GROUP BY ALL
)
SELECT r.workspace_id, s.workspace_name,
  HOUR(r.start_time) AS start_hour_utc,
  COUNT(*) AS runs_started,
  COUNT(DISTINCT r.job_id) AS jobs,
  ROUND(COUNT(*) / MAX(sc.review_days), 1) AS avg_runs_per_day,
  DENSE_RANK() OVER (PARTITION BY r.workspace_id ORDER BY COUNT(*) DESC) AS busy_rank
FROM r
CROSS JOIN sc
JOIN ${catalog}.ops.v_scope s ON s.workspace_id = r.workspace_id
GROUP BY r.workspace_id, s.workspace_name, HOUR(r.start_time);

-- 15. Jobs that mostly start in their workspace's two busiest hours.
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_schedule_peak
COMMENT 'Share of runs per job that start in the two busiest hours of the workspace'
AS
WITH sc AS (SELECT DISTINCT review_start, review_end FROM ${catalog}.ops.v_scope),
r AS (
  SELECT t.workspace_id, t.job_id, t.run_id, HOUR(MIN(t.period_start_time)) AS start_hour_utc
  FROM system.lakeflow.job_run_timeline t CROSS JOIN sc
  WHERE t.workspace_id IN (${workspace_ids})
    AND DATE(t.period_start_time) BETWEEN sc.review_start AND sc.review_end
  GROUP BY t.workspace_id, t.job_id, t.run_id
)
SELECT r.workspace_id, r.job_id,
  COUNT(*) AS runs,
  COUNT_IF(h.busy_rank <= 2 AND h.avg_runs_per_day >= 5) AS peak_runs,
  ROUND(COUNT_IF(h.busy_rank <= 2 AND h.avg_runs_per_day >= 5) / COUNT(*), 2) AS peak_share
FROM r
JOIN ${catalog}.ops.v_schedule_hotspots h
  ON h.workspace_id = r.workspace_id AND h.start_hour_utc = r.start_hour_utc
GROUP BY ALL;

-- 16. Latest job name and owner (owner tag, else run-as, else creator).
CREATE OR REPLACE VIEW ${catalog}.ops.v_jobs_latest
COMMENT 'Latest job name and owner per job'
AS
SELECT workspace_id, job_id, name AS job_name, CAST(NULL AS STRING) AS owner
FROM system.lakeflow.jobs
WHERE workspace_id IN (${workspace_ids})
QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1;

-- @optional: adds run-as / creator as owner when those columns exist.
CREATE OR REPLACE VIEW ${catalog}.ops.v_jobs_latest
COMMENT 'Latest job name and owner per job (run-as, else creator)'
AS
SELECT workspace_id, job_id, name AS job_name, COALESCE(run_as, creator_id) AS owner
FROM system.lakeflow.jobs
WHERE workspace_id IN (${workspace_ids})
QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1;

-- @optional: prefers an "owner" job tag when tags is a map.
CREATE OR REPLACE VIEW ${catalog}.ops.v_jobs_latest
COMMENT 'Latest job name and owner per job (owner tag, else run-as, else creator)'
AS
SELECT workspace_id, job_id, name AS job_name,
  COALESCE(element_at(tags, 'owner'), run_as, creator_id) AS owner
FROM system.lakeflow.jobs
WHERE workspace_id IN (${workspace_ids})
QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1;

-- 17. One row per job per day with every daily metric (feeds history, trends and the scorecard).
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_daily_metrics
COMMENT 'Daily cost, utilization, idle, run health, retries, log and Spark signals per job'
AS
SELECT c.usage_date AS metric_date,
  c.workspace_id, s.workspace_name, c.job_id,
  COALESCE(j.job_name, c.job_id) AS job_name, j.owner,
  c.dbus, c.cost_usd, c.list_cost_usd, c.photon,
  u.node_hours, u.idle_node_hours, u.avg_cpu, u.p95_cpu, u.avg_mem, u.max_swap,
  h.runs, h.failed_runs, h.avg_minutes, h.max_minutes,
  r.retries, r.runs_with_retries,
  l.oom_events, l.spill_events, l.lost_executor_events, l.error_lines,
  k.shuffle_read_gb, k.shuffle_write_gb, k.disk_spill_gb, k.memory_spill_gb, k.max_skew_ratio,
  q.queries, q.slow_queries, q.spilled_queries, q.query_shuffle_read_gb
FROM ${catalog}.ops.v_job_cost_daily c
JOIN ${catalog}.ops.v_scope s ON s.workspace_id = c.workspace_id
LEFT JOIN ${catalog}.ops.v_jobs_latest j ON j.workspace_id = c.workspace_id AND j.job_id = c.job_id
LEFT JOIN ${catalog}.ops.v_job_utilization_daily u
  ON u.workspace_id = c.workspace_id AND u.job_id = c.job_id AND u.usage_date = c.usage_date
LEFT JOIN ${catalog}.ops.v_job_run_health_daily h
  ON h.workspace_id = c.workspace_id AND h.job_id = c.job_id AND h.run_date = c.usage_date
LEFT JOIN ${catalog}.ops.v_job_retries_daily r
  ON r.workspace_id = c.workspace_id AND r.job_id = c.job_id AND r.run_date = c.usage_date
LEFT JOIN ${catalog}.ops.v_job_log_signals_daily l
  ON l.workspace_id = c.workspace_id AND l.job_id = c.job_id AND l.log_date = c.usage_date
LEFT JOIN ${catalog}.ops.v_job_spark_metrics_daily k
  ON k.workspace_id = c.workspace_id AND k.job_id = c.job_id AND k.metric_date = c.usage_date
LEFT JOIN ${catalog}.ops.v_job_query_daily q
  ON q.workspace_id = c.workspace_id AND q.job_id = c.job_id AND q.metric_date = c.usage_date;

-- 18. Scorecard over the review period: one row per job, one top action, secondary findings,
--     estimated monthly saving (contract cost), effort, risk, owner and priority ranks.
--     Savings (normalized to 30 days):
--       FAILING / MEMORY / LOST_EXECUTORS -> spend on failed runs
--       MULTI_CLUSTER -> 10% of job cost
--       OVERSIZED -> job cost x (1 - max(p95 CPU, avg mem) / 70), capped at 50%
--       SPILL / CPU_BOUND -> 0 (runtime gains)
CREATE OR REPLACE VIEW ${catalog}.ops.v_job_scorecard_30d
COMMENT 'Per-job scorecard for the review period (${review_label}): cost, utilization, performance, findings, savings, risk, owner'
AS
WITH sc AS (
  SELECT DISTINCT review_start, review_end, review_days, review_label,
    date_add(review_start, CAST(FLOOR((review_days - 1) / 2) AS INT)) AS mid_date
  FROM ${catalog}.ops.v_scope
),
dm AS (
  SELECT d.*, sc.mid_date
  FROM ${catalog}.ops.v_job_daily_metrics d CROSS JOIN sc
  WHERE d.metric_date BETWEEN sc.review_start AND sc.review_end
),
a AS (
  SELECT workspace_id, job_id,
    MAX(workspace_name) AS workspace_name, MAX(job_name) AS job_name, MAX(owner) AS owner,
    SUM(cost_usd) AS period_cost, SUM(list_cost_usd) AS period_list_cost, SUM(dbus) AS dbus,
    BOOL_OR(photon) AS photon,
    SUM(runs) AS runs, SUM(failed_runs) AS failed_runs,
    SUM(retries) AS retries, SUM(runs_with_retries) AS runs_with_retries,
    SUM(COALESCE(avg_minutes, 0) * COALESCE(runs, 0)) / NULLIF(SUM(CASE WHEN avg_minutes IS NOT NULL THEN runs END), 0) AS avg_minutes,
    MAX(max_minutes) AS max_minutes,
    SUM(node_hours) AS node_hours, SUM(idle_node_hours) AS idle_node_hours,
    AVG(avg_cpu) AS avg_cpu, AVG(p95_cpu) AS p95_cpu, AVG(avg_mem) AS avg_mem, MAX(max_swap) AS max_swap,
    SUM(oom_events) AS oom, SUM(spill_events) AS spill, SUM(lost_executor_events) AS lost_exec, SUM(error_lines) AS errors,
    SUM(shuffle_read_gb) AS shuffle_read_gb, SUM(shuffle_write_gb) AS shuffle_write_gb,
    SUM(disk_spill_gb) AS disk_spill_gb, MAX(max_skew_ratio) AS max_skew_ratio,
    SUM(slow_queries) AS slow_queries, SUM(spilled_queries) AS spilled_queries,
    SUM(CASE WHEN metric_date <= mid_date THEN cost_usd END) AS cost_h1,
    SUM(CASE WHEN metric_date > mid_date THEN cost_usd END) AS cost_h2,
    SUM(CASE WHEN metric_date <= mid_date THEN avg_minutes * runs END)
      / NULLIF(SUM(CASE WHEN metric_date <= mid_date AND avg_minutes IS NOT NULL THEN runs END), 0) AS minutes_h1,
    SUM(CASE WHEN metric_date > mid_date THEN avg_minutes * runs END)
      / NULLIF(SUM(CASE WHEN metric_date > mid_date AND avg_minutes IS NOT NULL THEN runs END), 0) AS minutes_h2,
    AVG(CASE WHEN metric_date <= mid_date THEN p95_cpu END) AS cpu_h1,
    AVG(CASE WHEN metric_date > mid_date THEN p95_cpu END) AS cpu_h2
  FROM dm
  GROUP BY workspace_id, job_id
),
k AS (
  SELECT m.workspace_id, m.job_id, AVG(m.n) AS clusters_per_run
  FROM (
    SELECT cm.workspace_id, cm.job_id, cm.job_run_id, COUNT(DISTINCT cm.cluster_id) AS n
    FROM ${catalog}.ops.v_job_cluster_map cm
    GROUP BY ALL
  ) m
  GROUP BY m.workspace_id, m.job_id
),
fr AS (
  SELECT t.workspace_id, t.job_id, t.run_id
  FROM system.lakeflow.job_run_timeline t CROSS JOIN sc
  WHERE t.workspace_id IN (${workspace_ids})
    AND DATE(t.period_start_time) BETWEEN sc.review_start AND sc.review_end
  GROUP BY t.workspace_id, t.job_id, t.run_id
  HAVING MAX_BY(t.result_state, t.period_end_time) IN ('FAILED', 'TIMED_OUT', 'ERROR')
),
f AS (
  SELECT fr.workspace_id, fr.job_id,
    SUM(bu.usage_quantity * p.pricing.effective_list.default * ${price_multiplier}) AS failed_cost
  FROM fr
  JOIN system.billing.usage bu
    ON bu.workspace_id = fr.workspace_id
   AND bu.usage_metadata.job_run_id = fr.run_id
  JOIN system.billing.list_prices p
    ON bu.sku_name = p.sku_name AND bu.cloud = p.cloud AND bu.usage_unit = p.usage_unit
   AND bu.usage_start_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR bu.usage_start_time < p.price_end_time)
  GROUP BY fr.workspace_id, fr.job_id
),
base AS (
  SELECT a.workspace_id, a.workspace_name, a.job_id, a.job_name, a.owner,
    sc.review_start, sc.review_end, sc.review_label, sc.review_days,
    ROUND(a.period_cost, 2) AS period_cost_usd,
    ROUND(a.period_cost * 30.0 / sc.review_days, 2) AS monthly_cost_usd,
    ROUND(a.period_list_cost, 2) AS period_list_cost_usd,
    ROUND(a.dbus, 1) AS dbus,
    a.photon,
    a.runs, a.failed_runs,
    ROUND(COALESCE(f.failed_cost, 0), 2) AS failed_run_cost_usd,
    COALESCE(a.retries, 0) AS retries, COALESCE(a.runs_with_retries, 0) AS runs_with_retries,
    ROUND(a.avg_minutes, 1) AS avg_minutes, a.max_minutes,
    ROUND(100 * (a.minutes_h2 - a.minutes_h1) / NULLIF(a.minutes_h1, 0), 1) AS runtime_change_pct,
    ROUND(100 * (a.cost_h2 - a.cost_h1) / NULLIF(a.cost_h1, 0), 1) AS cost_change_pct,
    ROUND(a.cpu_h2 - a.cpu_h1, 1) AS p95_cpu_change,
    ROUND(k.clusters_per_run, 1) AS clusters_per_run,
    ROUND(a.avg_cpu, 1) AS avg_cpu, ROUND(a.p95_cpu, 1) AS p95_cpu, ROUND(a.avg_mem, 1) AS avg_mem, a.max_swap,
    ROUND(a.node_hours, 1) AS node_hours, ROUND(a.idle_node_hours, 1) AS idle_node_hours,
    ROUND(100 * a.idle_node_hours / NULLIF(a.node_hours, 0), 1) AS idle_pct,
    COALESCE(a.oom, 0) AS oom_events, COALESCE(a.spill, 0) AS spill_events,
    COALESCE(a.lost_exec, 0) AS lost_executor_events, COALESCE(a.errors, 0) AS error_lines,
    ROUND(COALESCE(a.shuffle_read_gb, 0), 1) AS shuffle_read_gb,
    ROUND(COALESCE(a.shuffle_write_gb, 0), 1) AS shuffle_write_gb,
    ROUND(COALESCE(a.disk_spill_gb, 0), 1) AS disk_spill_gb,
    a.max_skew_ratio,
    COALESCE(a.slow_queries, 0) AS slow_queries, COALESCE(a.spilled_queries, 0) AS spilled_queries,
    cf.dbr_version, cf.worker_node_type, cf.max_workers,
    (cf.min_dbr_code < ${lts_code}) AS runtime_outdated,
    COALESCE(cf.policy_missing, false) AS policy_missing,
    COALESCE(ly.small_file_tables, 0) AS small_file_tables,
    COALESCE(ly.unmaintained_tables, 0) AS unmaintained_tables,
    ly.layout_tables,
    COALESCE(sp.peak_share >= 0.5, false) AS schedule_peak,
    CASE
      WHEN COALESCE(a.oom, 0) > 0 OR a.max_swap > 5 THEN 'MEMORY'
      WHEN (COALESCE(a.spill, 0) > 0 AND a.avg_mem > 70) OR COALESCE(a.disk_spill_gb, 0) >= 10 THEN 'SPILL'
      WHEN COALESCE(a.lost_exec, 0) > 0 THEN 'LOST_EXECUTORS'
      WHEN a.failed_runs > 0 THEN 'FAILING'
      WHEN k.clusters_per_run > 1 THEN 'MULTI_CLUSTER'
      WHEN a.p95_cpu < 40 AND a.avg_mem < 50 THEN 'OVERSIZED'
      WHEN a.avg_cpu > 85 THEN 'CPU_BOUND'
      ELSE 'OK'
    END AS category
  FROM a
  CROSS JOIN sc
  LEFT JOIN k  ON k.workspace_id = a.workspace_id AND k.job_id = a.job_id
  LEFT JOIN f  ON f.workspace_id = a.workspace_id AND f.job_id = a.job_id
  LEFT JOIN ${catalog}.ops.v_job_config cf ON cf.workspace_id = a.workspace_id AND cf.job_id = a.job_id
  LEFT JOIN ${catalog}.ops.v_job_layout ly ON ly.workspace_id = a.workspace_id AND ly.job_id = a.job_id
  LEFT JOIN ${catalog}.ops.v_job_schedule_peak sp ON sp.workspace_id = a.workspace_id AND sp.job_id = a.job_id
),
flagged AS (
  SELECT base.*,
    filter(array(
      IF(retries > 0, 'RETRIES', NULL),
      IF(max_skew_ratio >= 5, 'SKEW', NULL),
      IF(shuffle_read_gb >= 100, 'HIGH_SHUFFLE', NULL),
      IF(slow_queries > 0 OR spilled_queries > 0, 'SLOW_QUERIES', NULL),
      IF(small_file_tables > 0 OR unmaintained_tables > 0, 'DATA_LAYOUT', NULL),
      IF(runtime_outdated, 'RUNTIME_OUTDATED', NULL),
      IF(NOT COALESCE(photon, false) AND monthly_cost_usd >= 500 AND (avg_cpu > 60 OR category = 'CPU_BOUND'), 'PHOTON_CANDIDATE', NULL),
      IF(policy_missing, 'NO_POLICY', NULL),
      IF(schedule_peak, 'SCHEDULE_PEAK', NULL),
      IF(idle_pct >= 50 AND category <> 'OVERSIZED', 'IDLE', NULL),
      IF(cost_change_pct >= 20 AND p95_cpu_change <= -10, 'COST_UP_UTIL_DOWN', NULL),
      IF(runtime_change_pct >= 25, 'RUNTIME_GROWING', NULL)
    ), x -> x IS NOT NULL) AS finding_codes
  FROM base
),
scored AS (
  SELECT flagged.*,
    array_join(transform(finding_codes, c -> CASE c
      WHEN 'RETRIES'           THEN 'Tasks retried: fix flaky tasks, review max_retries'
      WHEN 'SKEW'              THEN 'Data skew: enable AQE skew join, salt hot keys'
      WHEN 'HIGH_SHUFFLE'      THEN 'Large shuffle: broadcast small tables, filter early, avoid wide repartitions'
      WHEN 'SLOW_QUERIES'      THEN 'Slow or spilling SQL: check query profile, prune columns and partitions'
      WHEN 'DATA_LAYOUT'       THEN 'Small files or unmaintained tables: liquid clustering, OPTIMIZE, predictive optimization'
      WHEN 'RUNTIME_OUTDATED'  THEN 'Old runtime: upgrade to the current LTS'
      WHEN 'PHOTON_CANDIDATE'  THEN 'Test Photon on this SQL/DataFrame-heavy job'
      WHEN 'NO_POLICY'         THEN 'No cluster policy: attach the standard job policy'
      WHEN 'SCHEDULE_PEAK'     THEN 'Starts in the busiest hours: stagger the schedule'
      WHEN 'IDLE'              THEN 'Over half of node time idle: lower min workers, tighten autoscaling'
      WHEN 'COST_UP_UTIL_DOWN' THEN 'Cost rising while utilization falls: right-size'
      WHEN 'RUNTIME_GROWING'   THEN 'Runtime growing: check data growth and query plans'
    END), '; ') AS secondary_findings,
    CASE category
      WHEN 'MEMORY'         THEN 'Memory failure: larger or r-family nodes; check skew and partition size'
      WHEN 'SPILL'          THEN 'Spilling: more memory per core or more shuffle partitions'
      WHEN 'LOST_EXECUTORS' THEN 'Lost executors: check spot reclaims; SPOT_WITH_FALLBACK, first_on_demand >= 1'
      WHEN 'FAILING'        THEN 'Failing runs: fix the failure before tuning'
      WHEN 'MULTI_CLUSTER'  THEN 'Several clusters per run: share one job cluster across tasks'
      WHEN 'OVERSIZED'      THEN 'Oversized: lower max workers or use smaller nodes'
      WHEN 'CPU_BOUND'      THEN 'CPU-bound: c-family nodes or more workers'
      ELSE 'OK'
    END AS top_recommendation,
    ROUND(CASE category
      WHEN 'MEMORY'         THEN failed_run_cost_usd * 30.0 / review_days
      WHEN 'LOST_EXECUTORS' THEN failed_run_cost_usd * 30.0 / review_days
      WHEN 'FAILING'        THEN failed_run_cost_usd * 30.0 / review_days
      WHEN 'MULTI_CLUSTER'  THEN monthly_cost_usd * 0.10
      WHEN 'OVERSIZED'      THEN monthly_cost_usd * LEAST(0.5, GREATEST(0, 1 - GREATEST(p95_cpu, avg_mem) / 70.0))
      ELSE 0
    END) AS est_monthly_savings_usd,
    CASE category
      WHEN 'OVERSIZED'      THEN 'Low: cluster config change'
      WHEN 'LOST_EXECUTORS' THEN 'Low: cluster config change'
      WHEN 'CPU_BOUND'      THEN 'Low: cluster config change'
      WHEN 'MULTI_CLUSTER'  THEN 'Medium: job definition change'
      WHEN 'SPILL'          THEN 'Medium: Spark config or node change'
      WHEN 'MEMORY'         THEN 'Medium: node change, possibly code'
      WHEN 'FAILING'        THEN 'High: code or data fix'
      ELSE '-'
    END AS fix_effort,
    CASE category
      WHEN 'OVERSIZED'      THEN 'Low'
      WHEN 'LOST_EXECUTORS' THEN 'Low'
      WHEN 'CPU_BOUND'      THEN 'Low'
      WHEN 'MULTI_CLUSTER'  THEN 'Medium'
      WHEN 'SPILL'          THEN 'Medium'
      WHEN 'MEMORY'         THEN 'Medium'
      WHEN 'FAILING'        THEN 'High'
      ELSE '-'
    END AS risk_level,
    CASE category
      WHEN 'OVERSIZED'      THEN 'Runtime can grow if cut too far: reduce in steps, watch the SLA'
      WHEN 'LOST_EXECUTORS' THEN 'On-demand driver costs slightly more per hour'
      WHEN 'CPU_BOUND'      THEN 'Faster nodes cost more per hour: confirm runtime falls'
      WHEN 'MULTI_CLUSTER'  THEN 'Tasks share one cluster: size it for the largest task'
      WHEN 'SPILL'          THEN 'Partition changes can move skew elsewhere: test in dev'
      WHEN 'MEMORY'         THEN 'Bigger nodes raise hourly cost: confirm failures stop'
      WHEN 'FAILING'        THEN 'Code or data change: test in dev, deploy through review'
      ELSE '-'
    END AS risk
  FROM flagged
)
SELECT scored.*,
  ROUND(est_monthly_savings_usd * 12) AS est_annual_savings_usd,
  CASE WHEN category = 'OK' THEN NULL
       ELSE ROW_NUMBER() OVER (ORDER BY CASE WHEN category = 'OK' THEN 1 ELSE 0 END,
                                        est_monthly_savings_usd DESC, period_cost_usd DESC)
  END AS priority_rank,
  ROW_NUMBER() OVER (PARTITION BY workspace_id
                     ORDER BY CASE WHEN category = 'OK' THEN 1 ELSE 0 END,
                              est_monthly_savings_usd DESC, size(finding_codes) DESC, period_cost_usd DESC) AS workspace_rank
FROM scored;

-- 19. One row per workspace: spend, savings and its top opportunity (at least one per workspace).
CREATE OR REPLACE VIEW ${catalog}.ops.v_workspace_summary
COMMENT 'Per-workspace spend, estimated savings, jobs needing action and top opportunity'
AS
SELECT s.workspace_id, s.workspace_name, s.review_label,
  COUNT(j.job_id) AS jobs_analyzed,
  COUNT_IF(j.category <> 'OK') AS jobs_needing_action,
  COUNT_IF(size(j.finding_codes) > 0) AS jobs_with_findings,
  ROUND(SUM(j.period_cost_usd), 2) AS period_cost_usd,
  ROUND(SUM(j.est_monthly_savings_usd)) AS est_monthly_savings_usd,
  ROUND(SUM(j.failed_run_cost_usd), 2) AS failed_run_cost_usd,
  MAX(CASE WHEN j.workspace_rank = 1 THEN j.job_name END) AS top_opportunity_job,
  MAX(CASE WHEN j.workspace_rank = 1 THEN
        CASE WHEN j.category <> 'OK' THEN j.top_recommendation ELSE j.secondary_findings END END) AS top_opportunity,
  MAX(CASE WHEN j.workspace_rank = 1 THEN j.est_monthly_savings_usd END) AS top_opportunity_monthly_savings_usd
FROM ${catalog}.ops.v_scope s
LEFT JOIN ${catalog}.ops.v_job_scorecard_30d j ON j.workspace_id = s.workspace_id
GROUP BY s.workspace_id, s.workspace_name, s.review_label;
