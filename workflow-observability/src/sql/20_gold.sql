-- =============================================================
-- 20 GOLD: KPI views, 30-day job summary, optimization findings
-- =============================================================

-- Daily cost by workspace / product / SKU (the app filters this)
CREATE OR REPLACE VIEW ${catalog}.gold.v_daily_cost AS
SELECT u.usage_date, u.workspace_id,
       COALESCE(w.workspace_name, u.workspace_id) AS workspace_name,
       u.billing_origin_product, u.sku_name, u.is_serverless,
       SUM(u.dbus) AS dbus, SUM(u.list_cost) AS list_cost, SUM(u.effective_cost) AS effective_cost
FROM ${catalog}.silver.fact_usage_cost u
LEFT JOIN ${catalog}.config.workspaces w USING (workspace_id)
GROUP BY ALL;

-- One row per job with everything the rules need (last 30 days)
CREATE OR REPLACE TABLE ${catalog}.gold.job_summary_30d AS
WITH runs AS (
  SELECT workspace_id, job_id,
         COUNT(*)                                                    AS runs,
         COUNT_IF(result_state = 'SUCCEEDED')                        AS succeeded,
         COUNT_IF(result_state IN ('FAILED','ERROR','TIMED_OUT'))    AS failed,
         SUM(effective_cost)                                         AS cost_30d,
         SUM(CASE WHEN result_state IN ('FAILED','ERROR','TIMED_OUT') THEN effective_cost ELSE 0 END) AS failed_cost,
         AVG(duration_min)                                           AS avg_duration_min,
         SUM(repairs)                                                AS repairs,
         MAX(run_as)                                                 AS run_as,
         MAX(start_time)                                             AS last_run
  FROM ${catalog}.silver.fact_job_run
  WHERE start_time >= date_sub(current_date(), 30)
  GROUP BY workspace_id, job_id
),
usage AS (
  SELECT workspace_id, job_id,
         SUM(CASE WHEN usage_date >= date_sub(current_date(), 7) THEN effective_cost ELSE 0 END) AS last7,
         SUM(CASE WHEN usage_date <  date_sub(current_date(), 7)
                   AND usage_date >= date_sub(current_date(), 14) THEN effective_cost ELSE 0 END) AS prev7,
         SUM(CASE WHEN NOT has_tags THEN effective_cost ELSE 0 END)     AS untagged_cost,
         SUM(CASE WHEN is_serverless THEN effective_cost ELSE 0 END) / NULLIF(SUM(effective_cost), 0) AS serverless_share
  FROM ${catalog}.silver.fact_usage_cost
  WHERE billing_origin_product = 'JOBS' AND job_id IS NOT NULL
    AND usage_date >= date_sub(current_date(), 30)
  GROUP BY workspace_id, job_id
),
tasks AS (
  SELECT workspace_id, job_id,
         100 * SUM(setup_s) / NULLIF(SUM(setup_s + exec_s), 0) AS setup_pct,
         SUM(queue_s) / 60.0                                   AS queue_min
  FROM ${catalog}.silver.fact_task_run
  WHERE start_time >= date_sub(current_date(), 30)
  GROUP BY workspace_id, job_id
),
util AS (
  SELECT jc.workspace_id, jc.job_id,
         AVG(cu.avg_cpu_pct)  AS avg_cpu_pct,
         MAX(cu.peak_cpu_pct) AS peak_cpu_pct,
         AVG(cu.avg_mem_pct)  AS avg_mem_pct
  FROM (SELECT DISTINCT workspace_id, job_id, cluster_id
        FROM ${catalog}.silver.fact_usage_cost
        WHERE job_id IS NOT NULL AND cluster_id IS NOT NULL
          AND usage_date >= date_sub(current_date(), 30)) jc
  JOIN ${catalog}.silver.fact_cluster_util cu
    ON cu.workspace_id = jc.workspace_id AND cu.cluster_id = jc.cluster_id
   AND cu.util_date >= date_sub(current_date(), 30)
  GROUP BY jc.workspace_id, jc.job_id
),
all_purpose AS (
  SELECT t.workspace_id, t.job_id, COUNT(DISTINCT t.job_run_id) AS ap_runs
  FROM ${catalog}.silver.fact_task_run t
  LATERAL VIEW explode(t.compute_ids) x AS compute_id
  JOIN ${catalog}.silver.dim_cluster c
    ON c.workspace_id = t.workspace_id AND c.cluster_id = x.compute_id
   AND c.cluster_source IN ('UI', 'API')
  WHERE t.start_time >= date_sub(current_date(), 30)
  GROUP BY t.workspace_id, t.job_id
)
SELECT
  r.workspace_id,
  COALESCE(w.workspace_name, r.workspace_id)            AS workspace_name,
  w.environment, w.owner_team,
  r.job_id,
  COALESCE(j.job_name, concat('job ', r.job_id))        AS job_name,
  r.run_as, r.runs, r.succeeded, r.failed, r.repairs, r.last_run,
  ROUND(100.0 * r.succeeded / NULLIF(r.runs, 0), 1)     AS success_pct,
  ROUND(r.cost_30d, 2)                                  AS cost_30d,
  ROUND(r.cost_30d / NULLIF(r.runs, 0), 2)              AS cost_per_run,
  ROUND(r.failed_cost, 2)                               AS failed_cost,
  ROUND(r.avg_duration_min, 1)                          AS avg_duration_min,
  ROUND(u.last7, 2) AS last7_cost, ROUND(u.prev7, 2) AS prev7_cost,
  ROUND(100.0 * (u.last7 - u.prev7) / NULLIF(u.prev7, 0), 1) AS growth_pct,
  ROUND(u.untagged_cost, 2)                             AS untagged_cost,
  ROUND(COALESCE(u.serverless_share, 0), 2)             AS serverless_share,
  ROUND(t.setup_pct, 1)                                 AS setup_pct,
  ROUND(t.queue_min, 1)                                 AS queue_min,
  ROUND(ut.avg_cpu_pct, 1)  AS avg_cpu_pct,
  ROUND(ut.peak_cpu_pct, 1) AS peak_cpu_pct,
  ROUND(ut.avg_mem_pct, 1)  AS avg_mem_pct,
  COALESCE(ap.ap_runs, 0)                               AS all_purpose_runs
FROM runs r
LEFT JOIN usage u        USING (workspace_id, job_id)
LEFT JOIN tasks t        USING (workspace_id, job_id)
LEFT JOIN util ut        USING (workspace_id, job_id)
LEFT JOIN all_purpose ap USING (workspace_id, job_id)
LEFT JOIN ${catalog}.silver.dim_job j USING (workspace_id, job_id)
LEFT JOIN ${catalog}.config.workspaces w USING (workspace_id);

-- Findings: one row per (job, rule) breach. Savings are rough estimates.
CREATE OR REPLACE TABLE ${catalog}.gold.findings AS
WITH m AS (
  -- one candidate row per (job, rule) with the metric the rule checks
  SELECT s.*, f.rid, f.metric_value, f.est
  FROM ${catalog}.gold.job_summary_30d s
  LATERAL VIEW inline(array(
    named_struct('rid','LOW_CPU',             'metric_value', CAST(s.avg_cpu_pct AS DOUBLE),
                 'est', CAST(s.cost_30d * (1 - s.serverless_share) * 0.3 AS DOUBLE)),
    named_struct('rid','HIGH_FAILURE_COST',   'metric_value', CAST(s.failed_cost AS DOUBLE),
                 'est', CAST(s.failed_cost AS DOUBLE)),
    named_struct('rid','HIGH_SETUP_OVERHEAD', 'metric_value', CAST(s.setup_pct AS DOUBLE),
                 'est', CAST(s.cost_30d * s.setup_pct / 100 * 0.5 AS DOUBLE)),
    named_struct('rid','ALL_PURPOSE_COMPUTE', 'metric_value', CAST(s.all_purpose_runs AS DOUBLE),
                 'est', CAST(NULL AS DOUBLE)),
    named_struct('rid','COST_SPIKE',          'metric_value', CAST(s.growth_pct AS DOUBLE),
                 'est', CAST(NULL AS DOUBLE)),
    named_struct('rid','UNTAGGED_SPEND',      'metric_value', CAST(s.untagged_cost AS DOUBLE),
                 'est', CAST(NULL AS DOUBLE)),
    named_struct('rid','LOW_SUCCESS_RATE',    'metric_value', CAST(s.success_pct AS DOUBLE),
                 'est', CAST(NULL AS DOUBLE))
  )) f AS rid, metric_value, est
)
SELECT m.workspace_id, m.workspace_name, m.owner_team, m.job_id, m.job_name,
       r.rule_id, r.title, r.severity,
       ROUND(m.metric_value, 1) AS metric_value, r.threshold,
       ROUND(m.est, 2)          AS est_monthly_savings,
       r.recommendation, current_timestamp() AS detected_at
FROM m
JOIN ${catalog}.config.rules r ON r.rule_id = m.rid AND r.enabled
WHERE m.metric_value IS NOT NULL
  AND CASE r.rule_id
        WHEN 'LOW_CPU'          THEN m.metric_value < r.threshold AND m.cost_30d > 0
        WHEN 'LOW_SUCCESS_RATE' THEN m.metric_value < r.threshold AND m.runs >= 5
        WHEN 'COST_SPIKE'       THEN m.metric_value > r.threshold AND m.prev7_cost > 10
        ELSE m.metric_value > r.threshold
      END;
