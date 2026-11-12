# Workflow Observability for Databricks

Cost, reliability and optimization insights for Lakeflow Jobs (workflows) across
any number of workspaces, delivered as a Databricks App plus a scheduled collector
job, packaged as one Databricks Asset Bundle.

**New here? Follow [STEP_BY_STEP.md](STEP_BY_STEP.md).** No clone access? Use `bootstrap.sh` at the repo root to recreate this folder.

## How it scales to N workspaces

System tables are account-wide **per cloud region**. One "hub" deployment per region
sees every workspace in that region:

1. The collector auto-discovers workspaces from `system.billing.usage` into
   `config.workspaces`.
2. New workspaces are monitored immediately (`auto_enable_workspaces=true`) or
   wait for an admin to enable them on the app's **Workspaces** page.
3. More regions = add a bundle target (`prod_<region>`) and a matrix entry in CI.

No per-workspace deployment is needed.

## Layout

```
databricks.yml              bundle + targets (one per regional hub)
resources/collector_job.yml serverless job: setup -> silver -> gold (every 4h)
resources/app.yml           Databricks App + its warehouse/job resources
src/run_sql.py              runs the SQL files with ${catalog} substitution
src/sql/00_setup.sql        schemas, config tables, rules seed, workspace discovery
src/sql/10_silver.sql       incremental facts: usage cost, job runs, task runs, utilization
src/sql/20_gold.sql         daily cost view, job_summary_30d, findings
app/app.py                  Streamlit app: Overview, Workflows, Findings, Workspaces, Rules
.github/workflows/deploy.yml validate on PR, deploy hubs on main
```

## Data model (catalog `platform_obs`)

| Schema | Object | Grain |
|---|---|---|
| config | workspaces, rules, settings | editable from the app |
| silver | fact_usage_cost | billing record |
| silver | fact_job_run | job run |
| silver | fact_task_run | task run |
| silver | fact_cluster_util | cluster × day |
| silver | dim_job, dim_cluster | latest definition |
| gold | v_daily_cost | day × workspace × SKU |
| gold | job_summary_30d | job |
| gold | findings | job × rule breach |

## Deploy

1. **Fill in `databricks.yml`**: hub host, `warehouse_id`, `job_sp`, `admin_emails`.
2. **Grants** (run once as a metastore/account admin):
   ```sql
   -- collector service principal
   GRANT USE CATALOG ON CATALOG system TO `<job-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.billing  TO `<job-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow TO `<job-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.compute  TO `<job-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA system.access   TO `<job-sp>`;  -- optional: workspace names
   CREATE CATALOG IF NOT EXISTS platform_obs;
   GRANT ALL PRIVILEGES ON CATALOG platform_obs TO `<job-sp>`;

   -- app service principal (shown on the app's page after first deploy)
   GRANT USE CATALOG ON CATALOG platform_obs TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs.silver TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs.gold   TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA platform_obs.config TO `<app-sp>`;
   ```
3. **Deploy and backfill**:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle run collector -t dev --params lookback_days=90
   databricks bundle run workflow_obs_app -t dev
   ```
   After the backfill, the schedule keeps a 3-day rolling window.

## Notes

- Cost = list price × (1 − `discount_pct`). Set the discount on the **Rules** page.
- Job cost attribution covers jobs compute and serverless jobs. Tasks on all-purpose
  clusters or SQL warehouses are billed there; the `ALL_PURPOSE_COMPUTE` rule flags them.
- Estimated savings are heuristics (e.g. 30% of classic cost for low-CPU clusters).
  Treat them as a way to rank work, not as a commitment.
- Admin pages are restricted to the `admin_emails` bundle variable (read from the job parameters); with it empty, every app user is an admin.
- Grant `CAN_USE` on the app to the teams who should see it.
