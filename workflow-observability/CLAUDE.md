# CLAUDE.md — Workflow Observability for Databricks

Context for Claude Code. Read this first, then README.md and STEP_BY_STEP.md.

## What this project is

A reusable monitoring layer on top of Databricks that tracks **workflow (Lakeflow
Jobs) cost, reliability and optimization opportunities** across any number of
workspaces, using **system tables** only. Packaged as one Databricks Asset Bundle.

Requirement from the team: monitor workflows via system tables, review billing,
find process-optimization opportunities, deliver insights as a **dashboard,
SQL alerts and a Genie space** (all required). The **Databricks App is optional**.

## Architecture (data flow)

```
system.billing.usage / list_prices ┐
system.lakeflow.jobs / job_run_timeline / job_task_run_timeline ├─> collector job (every 4h, serverless)
system.compute.clusters / node_timeline ┘        │
                                                 ├─ 00_setup.sql : config schema, rules seed, workspace auto-discovery
                                                 ├─ 10_silver.sql: incremental MERGE facts (3-day rolling reprocess)
                                                 └─ 20_gold.sql  : v_daily_cost, job_summary_30d, findings
                                                          │
            ┌──────────────── AI/BI dashboard ────────────┤
            ├──────────────── SQL alerts (4) ─────────────┤
            ├──────────────── Genie space ────────────────┤
            └──────────────── Databricks App (optional) ──┘
```

Catalog per target (`platform_obs_dev`, `platform_obs`), schemas `config`, `silver`, `gold`.

## Key design decisions (keep these unless asked to change)

1. **One hub deployment per cloud region.** System tables are account-wide but
   regional; workspaces are auto-discovered from billing into `config.workspaces`.
   Never deploy per workspace.
2. **Rules are data, not code:** thresholds live in `config.rules`. A new rule
   needs a seed row in `00_setup.sql` + a `named_struct` candidate in the findings
   query in `20_gold.sql` (+ a `WHEN` branch for "less than" rules).
3. **Cost = list price × (1 − discount_pct)** from `config.settings`.
4. **Incremental silver:** MERGE on natural keys, reprocess `lookback_days` (default 3)
   because billing corrections and long runs land late. First deploy backfills with
   `--params lookback_days=90`.
5. **Job parameters are the single source of truth** for catalog/admins; the optional
   app reads them via the SDK (the bundle `config:` block for apps is unreliable).
6. **Genie space is deployed by `src/genie/deploy_genie.py`**, not the bundle: Genie
   needs a human owner and bundle-native `genie_spaces` requires CLI ≥1.3 + `engine: direct`.
   The script is idempotent (matches by title, PATCHes on re-run).
7. **Dashboard JSON** uses `schema.table` names; `dataset_catalog` in
   `resources/dashboard.yml` sets the catalog per target. Edit in the UI, then pull back
   with `databricks bundle generate dashboard --resource workflow_observability --watch`.
8. **App is optional:** `optional/app.yml` is only deployed when uncommented under
   `include` in `databricks.yml`.
9. **Offline distribution:** `../bootstrap.sh` recreates this folder from one paste;
   regenerate it with `python3 tools/make_bootstrap.py` from the repo root after any change.

## Layout

```
databricks.yml                 bundle, variables (catalog, warehouse_id, alert thresholds…), targets
resources/collector_job.yml    3 tasks: setup_and_discover -> build_silver -> build_gold_and_findings
resources/dashboard.yml        AI/BI dashboard resource
resources/alerts.yml           spend spike, failed runs 24h, HIGH findings, stale data
optional/app.yml               Databricks App resource (optional)
src/run_sql.py                 runs SQL files; substitutes ${catalog} ${lookback_days} ${auto_enable};
                               splits on lines ending ';'; '-- @optional' statements may fail
src/sql/*.sql                  pipeline SQL
src/dashboards/*.lvdash.json   dashboard (Overview, Workflows, Findings, Reliability)
src/genie/genie_space.json     Genie tables, instructions, sample questions
src/genie/deploy_genie.py      create/update Genie space (--dry-run supported)
app/                           Streamlit app (optional)
```

## Commands

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run collector -t dev --params lookback_days=90   # backfill
databricks bundle run collector -t dev                             # normal refresh
python src/genie/deploy_genie.py --catalog platform_obs_dev --warehouse-id <id> --title-suffix " (dev)"
python src/genie/deploy_genie.py --catalog x --warehouse-id x --dry-run   # preview payload
# optional app
databricks bundle run workflow_obs_app -t dev
```

Requires Databricks CLI **v0.281+** (alerts v2, dashboard dataset_catalog).

## Status / not yet verified against a live workspace

Static checks passed (YAML/JSON/Python compile, SQL parsed with sqlglot databricks
dialect, dashboard field refs, alert columns, Genie payload). Still to confirm on the
first real deploy:
- `databricks bundle validate` (could not run the CLI when this was written).
- `job_task_run_timeline` duration columns (`setup/queue/execution_duration_seconds`)
  exist and are summed correctly across hourly-split rows.
- `system.access.workspaces_latest` availability (statement is `@optional`).
- Dashboard widget specs render (hand-authored JSON; fix in UI + `generate --watch`).
- Alert threshold variables are numeric (`failed_runs_threshold`, `cost_spike_pct`, `stale_hours`).
- Genie serialized_space accepted (version 1 format; tables sorted by identifier).

## Conventions

- SQL: Databricks SQL, `${catalog}` placeholder in pipeline files, `${var.catalog}` in bundle YAML.
- Keep all workspace filters on `config.workspaces WHERE enabled`.
- Never hardcode workspace IDs, catalogs or emails; use bundle variables.
- After changing any file in this folder, regenerate `bootstrap.sh`.
