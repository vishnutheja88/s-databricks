-- =============================================================
-- AI/BI dashboard datasets. Create a dashboard in the UI, add each
-- query below as a dataset, then build the listed visual.
-- Replace watchtower with your catalog if different.
-- Keep Watchtower's own "Logs Dashboard" for log search / drill-down.
-- =============================================================

-- [Overview] Top 20 jobs by 30-day cost, colored by top_recommendation (bar)
SELECT job_name, cost_30d_usd, top_recommendation
FROM watchtower.ops.v_job_scorecard_30d
ORDER BY cost_30d_usd DESC
LIMIT 20;

-- [Overview] Jobs and cost per recommendation (bar or counter row)
SELECT top_recommendation, COUNT(*) AS jobs, ROUND(SUM(cost_30d_usd)) AS cost_30d_usd
FROM watchtower.ops.v_job_scorecard_30d
GROUP BY top_recommendation
ORDER BY cost_30d_usd DESC;

-- [Utilization] p95 CPU vs cost per job (scatter; bottom-right = biggest downsizing wins)
SELECT job_name, p95_cpu, avg_mem, cost_30d_usd
FROM watchtower.ops.v_job_scorecard_30d
WHERE p95_cpu IS NOT NULL;

-- [Utilization] Oversized jobs (table)
SELECT job_name, cost_30d_usd, avg_cpu, p95_cpu, avg_mem, clusters_per_run, photon
FROM watchtower.ops.v_job_scorecard_30d
WHERE top_recommendation LIKE 'Oversized%'
ORDER BY cost_30d_usd DESC;

-- [Root cause] Jobs with memory / spill / executor problems (table)
SELECT job_name, cost_30d_usd, oom_events, spill_events, lost_executor_events, error_lines,
       failed_runs, max_swap, top_recommendation
FROM watchtower.ops.v_job_scorecard_30d
WHERE oom_events + spill_events + lost_executor_events > 0 OR failed_runs > 0
ORDER BY cost_30d_usd DESC;

-- [Trends] Daily cost and runtime per job (line; add a job_name filter)
SELECT snapshot_date, job_name, list_cost_usd, avg_minutes, p95_cpu, oom_events
FROM watchtower.ops.job_daily_history
ORDER BY snapshot_date;
