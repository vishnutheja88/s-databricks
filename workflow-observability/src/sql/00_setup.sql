-- =============================================================
-- 00 SETUP: schemas, config tables, workspace auto-discovery
-- =============================================================
CREATE SCHEMA IF NOT EXISTS ${catalog}.config;
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver;
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold;

-- Workspace registry: every workspace seen in billing lands here.
-- Admins edit name / environment / owner / enabled from the app.
CREATE TABLE IF NOT EXISTS ${catalog}.config.workspaces (
  workspace_id   STRING,
  workspace_name STRING,
  environment    STRING,
  owner_team     STRING,
  enabled        BOOLEAN,
  first_seen     DATE,
  updated_at     TIMESTAMP
);

-- Optimization rules: thresholds are data, not code.
CREATE TABLE IF NOT EXISTS ${catalog}.config.rules (
  rule_id        STRING,
  title          STRING,
  threshold      DOUBLE,
  severity       STRING,
  enabled        BOOLEAN,
  recommendation STRING
);

-- Key/value settings (e.g. negotiated discount vs list price)
CREATE TABLE IF NOT EXISTS ${catalog}.config.settings (
  key   STRING,
  value STRING
);

MERGE INTO ${catalog}.config.settings t
USING (SELECT * FROM VALUES ('discount_pct', '0') AS v(key, value)) s
ON t.key = s.key
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO ${catalog}.config.rules t
USING (
  SELECT * FROM VALUES
    ('LOW_CPU',            'Over-provisioned job cluster (avg CPU %)',      30.0,  'HIGH',   true, 'Downsize node type / workers, enable autoscaling, or move to serverless.'),
    ('HIGH_FAILURE_COST',  'Spend on failed runs (USD, 30d)',               50.0,  'HIGH',   true, 'Fix top termination codes; add retries only for transient errors.'),
    ('HIGH_SETUP_OVERHEAD','Cluster startup share of task time (%)',        25.0,  'MEDIUM', true, 'Use serverless, instance pools, or share one job cluster across tasks.'),
    ('ALL_PURPOSE_COMPUTE','Task runs on all-purpose clusters (30d)',        0.0,  'HIGH',   true, 'Switch tasks to job clusters or serverless - all-purpose DBUs cost more.'),
    ('COST_SPIKE',         'Week-over-week cost growth (%)',                50.0,  'MEDIUM', true, 'Check for data volume change, config drift or added schedules.'),
    ('UNTAGGED_SPEND',     'Spend without custom tags (USD, 30d)',         100.0,  'LOW',    true, 'Add team / cost_center tags for chargeback.'),
    ('LOW_SUCCESS_RATE',   'Run success rate below (%)',                    90.0,  'MEDIUM', true, 'Review failing tasks and upstream dependencies.')
  AS v(rule_id, title, threshold, severity, enabled, recommendation)
) s
ON t.rule_id = s.rule_id
WHEN NOT MATCHED THEN INSERT *;

-- Auto-discover workspaces from billing (account-wide, this region)
MERGE INTO ${catalog}.config.workspaces t
USING (
  SELECT workspace_id, MIN(usage_date) AS first_seen
  FROM system.billing.usage
  WHERE usage_date >= date_sub(current_date(), 30)
  GROUP BY workspace_id
) s
ON t.workspace_id = s.workspace_id
WHEN NOT MATCHED THEN INSERT
  (workspace_id, workspace_name, environment, owner_team, enabled, first_seen, updated_at)
  VALUES (s.workspace_id, NULL, NULL, NULL, ${auto_enable}, s.first_seen, current_timestamp());

-- Fill in workspace names where the account exposes them  -- @optional
MERGE INTO ${catalog}.config.workspaces t
USING (SELECT CAST(workspace_id AS STRING) AS workspace_id, workspace_name FROM system.access.workspaces_latest) s
ON t.workspace_id = s.workspace_id
WHEN MATCHED AND t.workspace_name IS NULL THEN UPDATE SET workspace_name = s.workspace_name, updated_at = current_timestamp();
