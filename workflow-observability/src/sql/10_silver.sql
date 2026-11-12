-- =============================================================
-- 10 SILVER: curated, incremental facts for enabled workspaces
-- Each run re-merges the last ${lookback_days} days so late
-- billing corrections and long-running jobs are picked up.
-- First deploy: run once with --lookback_days=90 to backfill.
-- =============================================================

-- ---------- usage + cost (one row per billing record) ----------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.fact_usage_cost (
  record_id              STRING,
  workspace_id           STRING,
  usage_date             DATE,
  usage_start_time       TIMESTAMP,
  usage_end_time         TIMESTAMP,
  sku_name               STRING,
  billing_origin_product STRING,
  job_id                 STRING,
  run_id                 STRING,
  cluster_id             STRING,
  warehouse_id           STRING,
  run_as                 STRING,
  is_serverless          BOOLEAN,
  is_photon              BOOLEAN,
  has_tags               BOOLEAN,
  dbus                   DOUBLE,
  list_cost              DOUBLE,
  effective_cost         DOUBLE
) CLUSTER BY (workspace_id, usage_date)
COMMENT 'One row per billing record for monitored workspaces, priced at list and effective (discounted) cost.';

MERGE INTO ${catalog}.silver.fact_usage_cost t
USING (
  SELECT
    u.record_id,
    u.workspace_id,
    u.usage_date,
    u.usage_start_time,
    u.usage_end_time,
    u.sku_name,
    u.billing_origin_product,
    u.usage_metadata.job_id,
    u.usage_metadata.job_run_id                         AS run_id,
    u.usage_metadata.cluster_id,
    u.usage_metadata.warehouse_id,
    u.identity_metadata.run_as,
    COALESCE(u.product_features.is_serverless, false)   AS is_serverless,
    COALESCE(u.product_features.is_photon, false)       AS is_photon,
    (u.custom_tags IS NOT NULL AND cardinality(u.custom_tags) > 0) AS has_tags,
    CAST(u.usage_quantity AS DOUBLE)                    AS dbus,
    CAST(u.usage_quantity * lp.pricing.default AS DOUBLE)                         AS list_cost,
    CAST(u.usage_quantity * lp.pricing.default AS DOUBLE) * (1 - d.disc / 100.0)  AS effective_cost
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON  u.sku_name = lp.sku_name
    AND u.cloud = lp.cloud
    AND u.usage_unit = lp.usage_unit
    AND u.usage_end_time >= lp.price_start_time
    AND (lp.price_end_time IS NULL OR u.usage_end_time < lp.price_end_time)
  CROSS JOIN (
    SELECT COALESCE(MAX(CAST(value AS DOUBLE)), 0) AS disc
    FROM ${catalog}.config.settings WHERE key = 'discount_pct'
  ) d
  WHERE u.usage_date >= date_sub(current_date(), ${lookback_days})
    AND u.workspace_id IN (SELECT workspace_id FROM ${catalog}.config.workspaces WHERE enabled)
) s
ON t.record_id = s.record_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ---------- latest job definitions ----------
CREATE OR REPLACE TABLE ${catalog}.silver.dim_job
COMMENT 'Latest definition of each workflow (job_id, job_name) per workspace.'
AS
SELECT workspace_id, job_id, name AS job_name, creator_id, tags, change_time, delete_time
FROM system.lakeflow.jobs
WHERE workspace_id IN (SELECT workspace_id FROM ${catalog}.config.workspaces WHERE enabled)
QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1;

-- ---------- latest cluster definitions (last 90d) ----------
CREATE OR REPLACE TABLE ${catalog}.silver.dim_cluster
COMMENT 'Latest definition of each cluster seen in the last 90 days.'
AS
SELECT workspace_id, cluster_id, cluster_name, cluster_source, owned_by, dbr_version,
       driver_node_type, worker_node_type, worker_count,
       min_autoscale_workers, max_autoscale_workers, change_time
FROM system.compute.clusters
WHERE change_time >= date_sub(current_date(), 90)
  AND workspace_id IN (SELECT workspace_id FROM ${catalog}.config.workspaces WHERE enabled)
QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) = 1;

-- ---------- one row per job run ----------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.fact_job_run (
  workspace_id     STRING,
  job_id           STRING,
  run_id           STRING,
  run_name         STRING,
  trigger_type     STRING,
  run_type         STRING,
  start_time       TIMESTAMP,
  end_time         TIMESTAMP,
  duration_min     DOUBLE,
  result_state     STRING,
  termination_code STRING,
  repairs          INT,
  run_as           STRING,
  list_cost        DOUBLE,
  effective_cost   DOUBLE
) CLUSTER BY (workspace_id, start_time)
COMMENT 'One row per workflow run: timing, duration_min, result_state, termination_code, repairs and cost.';

MERGE INTO ${catalog}.silver.fact_job_run t
USING (
  WITH touched AS (            -- runs with any activity in the window
    SELECT DISTINCT workspace_id, run_id
    FROM system.lakeflow.job_run_timeline
    WHERE period_end_time >= date_sub(current_date(), ${lookback_days})
      AND workspace_id IN (SELECT workspace_id FROM ${catalog}.config.workspaces WHERE enabled)
  ),
  r AS (
    SELECT
      t.workspace_id, t.job_id, t.run_id,
      MAX(t.run_name)      AS run_name,
      MAX(t.trigger_type)  AS trigger_type,
      MAX(t.run_type)      AS run_type,
      MIN(t.period_start_time) AS start_time,
      MAX(t.period_end_time)   AS end_time,
      MAX_BY(t.result_state, t.period_end_time)     AS result_state,
      MAX_BY(t.termination_code, t.period_end_time) AS termination_code,
      CAST(GREATEST(COUNT_IF(t.result_state IS NOT NULL) - 1, 0) AS INT) AS repairs
    FROM system.lakeflow.job_run_timeline t
    JOIN touched USING (workspace_id, run_id)
    GROUP BY t.workspace_id, t.job_id, t.run_id
  ),
  c AS (
    SELECT workspace_id, run_id,
           FIRST(run_as, true) AS run_as,
           SUM(list_cost)      AS list_cost,
           SUM(effective_cost) AS effective_cost
    FROM ${catalog}.silver.fact_usage_cost
    WHERE run_id IS NOT NULL
      AND usage_date >= date_sub(current_date(), ${lookback_days} + 7)
    GROUP BY workspace_id, run_id
  )
  SELECT r.workspace_id, r.job_id, r.run_id, r.run_name, r.trigger_type, r.run_type,
         r.start_time, r.end_time,
         (unix_timestamp(r.end_time) - unix_timestamp(r.start_time)) / 60.0 AS duration_min,
         r.result_state, r.termination_code, r.repairs,
         c.run_as, COALESCE(c.list_cost, 0) AS list_cost, COALESCE(c.effective_cost, 0) AS effective_cost
  FROM r LEFT JOIN c USING (workspace_id, run_id)
) s
ON t.workspace_id = s.workspace_id AND t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ---------- one row per task run ----------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.fact_task_run (
  workspace_id     STRING,
  job_id           STRING,
  job_run_id       STRING,
  task_run_id      STRING,
  task_key         STRING,
  start_time       TIMESTAMP,
  end_time         TIMESTAMP,
  result_state     STRING,
  termination_code STRING,
  setup_s          DOUBLE,
  queue_s          DOUBLE,
  exec_s           DOUBLE,
  compute_ids      ARRAY<STRING>
) CLUSTER BY (workspace_id, start_time)
COMMENT 'One row per task run inside a workflow run: setup, queue and execution seconds.';

MERGE INTO ${catalog}.silver.fact_task_run t
USING (
  WITH touched AS (
    SELECT DISTINCT workspace_id, run_id
    FROM system.lakeflow.job_task_run_timeline
    WHERE period_end_time >= date_sub(current_date(), ${lookback_days})
      AND workspace_id IN (SELECT workspace_id FROM ${catalog}.config.workspaces WHERE enabled)
  )
  SELECT
    t.workspace_id, t.job_id,
    MAX(t.job_run_id)                AS job_run_id,
    t.run_id                         AS task_run_id,
    MAX(t.task_key)                  AS task_key,
    MIN(t.period_start_time)         AS start_time,
    MAX(t.period_end_time)           AS end_time,
    MAX_BY(t.result_state, t.period_end_time)     AS result_state,
    MAX_BY(t.termination_code, t.period_end_time) AS termination_code,
    CAST(SUM(COALESCE(t.setup_duration_seconds, 0))     AS DOUBLE) AS setup_s,
    CAST(SUM(COALESCE(t.queue_duration_seconds, 0))     AS DOUBLE) AS queue_s,
    CAST(SUM(COALESCE(t.execution_duration_seconds, 0)) AS DOUBLE) AS exec_s,
    array_distinct(flatten(collect_list(t.compute_ids))) AS compute_ids
  FROM system.lakeflow.job_task_run_timeline t
  JOIN touched USING (workspace_id, run_id)
  GROUP BY t.workspace_id, t.job_id, t.run_id
) s
ON t.workspace_id = s.workspace_id AND t.task_run_id = s.task_run_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- ---------- daily worker utilization per cluster ----------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.fact_cluster_util (
  workspace_id    STRING,
  cluster_id      STRING,
  util_date       DATE,
  avg_cpu_pct     DOUBLE,
  peak_cpu_pct    DOUBLE,
  avg_mem_pct     DOUBLE,
  avg_io_wait_pct DOUBLE
) CLUSTER BY (workspace_id, util_date)
COMMENT 'Daily average/peak CPU and memory utilization of worker nodes per cluster.';

MERGE INTO ${catalog}.silver.fact_cluster_util t
USING (
  SELECT workspace_id, cluster_id, to_date(start_time) AS util_date,
         AVG(cpu_user_percent + cpu_system_percent) AS avg_cpu_pct,
         MAX(cpu_user_percent + cpu_system_percent) AS peak_cpu_pct,
         AVG(mem_used_percent)                      AS avg_mem_pct,
         AVG(cpu_wait_percent)                      AS avg_io_wait_pct
  FROM system.compute.node_timeline
  WHERE start_time >= date_sub(current_date(), ${lookback_days})
    AND driver = false
    AND workspace_id IN (SELECT workspace_id FROM ${catalog}.config.workspaces WHERE enabled)
  GROUP BY workspace_id, cluster_id, to_date(start_time)
) s
ON t.workspace_id = s.workspace_id AND t.cluster_id = s.cluster_id AND t.util_date = s.util_date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
