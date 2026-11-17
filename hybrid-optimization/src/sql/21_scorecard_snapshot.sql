-- =============================================================
-- 21 SCORECARD SNAPSHOT: save today's 30-day scorecard once per day.
-- Re-running on the same day inserts nothing.
-- =============================================================
INSERT INTO ${catalog}.ops.job_scorecard_daily (
  snapshot_date, job_id, job_name, cost_30d_usd, dbus_30d, photon, runs, failed_runs,
  failed_run_cost_usd, avg_minutes, clusters_per_run, avg_cpu, p95_cpu, avg_mem, max_swap,
  oom_events, spill_events, lost_executor_events, error_lines, category, top_recommendation,
  est_monthly_savings_usd, fix_effort, est_annual_savings_usd, priority_rank, inserted_at
)
SELECT current_date(), job_id, job_name, cost_30d_usd, dbus_30d, photon, runs, failed_runs,
  failed_run_cost_usd, avg_minutes, clusters_per_run, avg_cpu, p95_cpu, avg_mem, max_swap,
  oom_events, spill_events, lost_executor_events, error_lines, category, top_recommendation,
  est_monthly_savings_usd, fix_effort, est_annual_savings_usd, priority_rank, current_timestamp()
FROM ${catalog}.ops.v_job_scorecard_30d
WHERE NOT EXISTS (
  SELECT 1 FROM ${catalog}.ops.job_scorecard_daily x WHERE x.snapshot_date = current_date()
);
