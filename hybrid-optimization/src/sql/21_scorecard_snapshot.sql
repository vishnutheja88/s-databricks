-- =============================================================
-- 21 SCORECARD SNAPSHOT: save today's scorecard once per day, then point
-- the "latest" views (dashboard, Genie, notebook) at it.
-- Re-running on the same day inserts nothing.
-- =============================================================
CREATE TABLE IF NOT EXISTS ${catalog}.ops.job_scorecard_daily
COMMENT 'Append-only daily snapshot of the job scorecard (no-delete policy)'
TBLPROPERTIES ('delta.appendOnly' = 'true')
AS SELECT current_date() AS snapshot_date, s.*, current_timestamp() AS inserted_at
FROM ${catalog}.ops.v_job_scorecard_30d s
WHERE 1 = 0;

INSERT INTO ${catalog}.ops.job_scorecard_daily
SELECT current_date() AS snapshot_date, s.*, current_timestamp() AS inserted_at
FROM ${catalog}.ops.v_job_scorecard_30d s
WHERE NOT EXISTS (
  SELECT 1 FROM ${catalog}.ops.job_scorecard_daily x WHERE x.snapshot_date = current_date()
);

CREATE OR REPLACE VIEW ${catalog}.ops.v_job_scorecard_latest
COMMENT 'Most recent daily snapshot of the job scorecard'
AS
SELECT *
FROM ${catalog}.ops.job_scorecard_daily
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ${catalog}.ops.job_scorecard_daily);

CREATE OR REPLACE VIEW ${catalog}.ops.v_workspace_summary_latest
COMMENT 'Per-workspace summary from the latest scorecard snapshot'
AS
SELECT s.workspace_id, s.workspace_name, MAX(j.review_label) AS review_label,
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
LEFT JOIN ${catalog}.ops.v_job_scorecard_latest j ON j.workspace_id = s.workspace_id
GROUP BY s.workspace_id, s.workspace_name;
