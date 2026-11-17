-- =============================================================
-- 20 DAILY SNAPSHOT: append one row per job for one day.
-- Default day = 2 days ago (billing and node data have settled).
-- Backfill: run the job with snapshot_date=YYYY-MM-DD.
-- Re-running for a day that is already written inserts nothing.
-- =============================================================
INSERT INTO ${catalog}.ops.job_daily_history (
  snapshot_date, job_id, job_name, dbus, list_cost_usd, cost_usd, photon,
  avg_cpu, p95_cpu, avg_mem, max_swap,
  oom_events, spill_events, lost_executor_events, error_lines,
  runs, failed_runs, avg_minutes, inserted_at
)
SELECT d.dt AS snapshot_date,
  c.job_id,
  j.name AS job_name,
  c.dbus,
  c.list_cost_usd,
  c.cost_usd,
  c.photon,
  u.avg_cpu,
  u.p95_cpu,
  u.avg_mem,
  u.max_swap,
  l.oom_events,
  l.spill_events,
  l.lost_executor_events,
  l.error_lines,
  h.runs,
  h.failed_runs,
  h.avg_minutes,
  current_timestamp() AS inserted_at
FROM (SELECT ${snapshot_date_expr} AS dt) d
JOIN ${catalog}.ops.v_job_cost_daily c
  ON c.usage_date = d.dt
LEFT JOIN ${catalog}.ops.v_job_utilization_daily u
  ON u.job_id = c.job_id AND u.usage_date = d.dt
LEFT JOIN ${catalog}.ops.v_job_log_signals_daily l
  ON l.job_id = c.job_id AND l.log_date = d.dt
LEFT JOIN ${catalog}.ops.v_job_run_health_daily h
  ON h.job_id = c.job_id AND h.run_date = d.dt
LEFT JOIN (
  SELECT job_id, name
  FROM system.lakeflow.jobs
  WHERE workspace_id = '${workspace_id}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
) j
  ON j.job_id = c.job_id
WHERE NOT EXISTS (
  SELECT 1 FROM ${catalog}.ops.job_daily_history x WHERE x.snapshot_date = d.dt
);
