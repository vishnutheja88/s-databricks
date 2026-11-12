# Workflow Observability for Databricks

Cost, reliability and optimization insights for Lakeflow Jobs (workflows) across
any number of workspaces, built on system tables and packaged as one Databricks
Asset Bundle.

**New here? Follow [STEP_BY_STEP.md](STEP_BY_STEP.md).** No clone access? Use
`bootstrap.sh` at the repo root to recreate this folder.

## What you get

| Component | Status | What it's for |
|---|---|---|
| Collector job | core | builds silver/gold tables from `system.billing`, `system.lakeflow`, `system.compute` every 4h |
| AI/BI dashboard | core | Overview · Workflows · Findings · Reliability pages |
| SQL alerts | core | spend spike, failed runs, high-severity findings, stale data |
| Genie space | core | plain-English questions over the gold/silver tables |
| Databricks App | **optional** | interactive admin UI (edit workspaces/rules, run collector) |

## How it scales to N workspaces

System tables are account-wide **per cloud region**. One "hub" deployment per
region sees every workspace in that region:

1. The collector auto-discovers workspaces from `system.billing.usage` into
   `config.workspaces`.
2. New workspaces are monitored immediately (`auto_enable_workspaces=true`) or
   wait for an admin to set `enabled = true`.
3. More regions = add a bundle target (`prod_<region>`) and a CI matrix entry.

No per-workspace deployment is needed.

## Layout

```
databricks.yml                       bundle, variables, targets (one per regional hub)
resources/collector_job.yml          serverless job: setup -> silver -> gold
resources/dashboard.yml              AI/BI dashboard resource
resources/alerts.yml                 4 SQL alerts (v2)
optional/app.yml                     Databricks App (enable via include in databricks.yml)
src/run_sql.py                       runs the SQL files with ${catalog} substitution
src/sql/00_setup.sql                 schemas, config tables, rules seed, workspace discovery
src/sql/10_silver.sql                incremental facts: usage cost, job runs, task runs, utilization
src/sql/20_gold.sql                  daily cost view, job_summary_30d, findings
src/dashboards/*.lvdash.json         dashboard definition
src/genie/genie_space.json           Genie tables, instructions, sample questions
src/genie/deploy_genie.py            create/update the Genie space (idempotent)
app/                                 optional Streamlit app
.github/workflows/deploy.yml         validate on PR, deploy hubs on main
```

## Data model (catalog `platform_obs`)

| Schema | Object | Grain |
|---|---|---|
| config | workspaces, rules, settings | editable (SQL or the optional app) |
| silver | fact_usage_cost | billing record |
| silver | fact_job_run | job run |
| silver | fact_task_run | task run |
| silver | fact_cluster_util | cluster × day |
| silver | dim_job, dim_cluster | latest definition |
| gold | v_daily_cost | day × workspace × SKU |
| gold | job_summary_30d | job |
| gold | findings | job × rule breach |

## Quick deploy (details in STEP_BY_STEP.md)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev                                  # job + dashboard + alerts
databricks bundle run collector -t dev --params lookback_days=90   # first-time backfill
python src/genie/deploy_genie.py --catalog platform_obs_dev --warehouse-id <id> --title-suffix " (dev)"
```

Optional app: uncomment `- optional/app.yml` in `databricks.yml`, then
`databricks bundle deploy -t dev && databricks bundle run workflow_obs_app -t dev`.

## CI secrets (if you use `.github/workflows/deploy.yml`)

- dev validation: `DEV_DATABRICKS_HOST`, `DEV_DATABRICKS_CLIENT_ID`, `DEV_DATABRICKS_CLIENT_SECRET`
- per prod environment: `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`

## Notes

- Cost = list price × (1 − `discount_pct` in `config.settings`).
- Job cost attribution covers jobs compute and serverless jobs. Tasks on all-purpose
  clusters or SQL warehouses are billed there; the `ALL_PURPOSE_COMPUTE` rule flags them.
- Estimated savings are heuristics (e.g. 30% of classic cost for low-CPU clusters).
  Use them to rank work, not as a commitment.
- The dashboard runs with each viewer's own permissions (`embed_credentials: false`).
  Viewers need SELECT on the gold/silver/config schemas.
- A Genie space must have a human owner, so `deploy_genie.py` runs as you, not in CI.
- Needs Databricks CLI v0.281+ (alerts v2 and dashboard `dataset_catalog`).
