# CLAUDE.md — Hybrid Job Optimization

Context for Claude Code. Read README.md next.

## What this is

Per-job optimization layer for ONE Databricks workspace (prod, all workflows on job
clusters): system tables say which jobs waste capacity/money, Watchtower's parsed
driver logs (`${catalog}.silver.logs`) say why. Packaged as a Databricks Asset Bundle.

## Hard rule: nothing deletes data

- No DROP / DELETE / TRUNCATE / INSERT OVERWRITE / MERGE...DELETE / VACUUM / RESTORE /
  CREATE OR REPLACE TABLE in `src/sql/`. `src/run_sql.py` enforces this and fails the
  task before running anything if a file contains one.
- `ops.job_daily_history` is `delta.appendOnly`; the daily insert is idempotent
  (`WHERE NOT EXISTS` on `snapshot_date`).
- Watchtower must run with `watchtower/no-delete.patch` applied (retention OFF,
  `pipelines.reset.allowed=false`, Terraform `prevent_destroy`).
- Never suggest `bundle destroy`, `terraform destroy` or a pipeline full refresh for prod.

## Conventions

- `${catalog}`, `${workspace_id}`, `${snapshot_date_expr}` placeholders in `src/sql`;
  `${var.*}` in bundle YAML. Never hardcode workspace IDs or emails.
- Statements end with `;` at end of line (runner splits on that). No comment line may end in `;`.
- New recommendation: add the signal to a daily view, roll it up in `v_job_scorecard_30d`,
  add a `WHEN` branch in priority order, and a column to `job_daily_history` via
  `ALTER TABLE ... ADD COLUMNS` (never recreate the table).
- Genie space: `src/genie/deploy_genie.py` (copied from workflow-observability), run by a human.

## Not yet verified live

Column names in `system.compute.node_timeline`, `system.lakeflow.job_task_run_timeline`
(`compute_ids`, `job_run_id`) and the log regexes in `v_job_log_signals_daily`.
