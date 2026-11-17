# CLAUDE.md — Hybrid Job Optimization Pilot

Context for Claude Code. Read README.md and docs/ARCHITECTURE.md next.

## What this is

Genie-assisted optimization pilot for SELECTED workspaces over a REVIEW PERIOD: system tables,
Spark event logs and Watchtower-parsed driver logs -> one scorecard per (workspace, job) with a top
action, secondary findings, savings, effort, risk, owner and ranks. Databricks Asset Bundle.
Acceptance criteria map: docs/REQUIREMENTS_TRACEABILITY.md (keep it in sync).

## Hard rules

- Nothing deletes data: no DROP / DELETE / TRUNCATE / INSERT OVERWRITE / MERGE...DELETE / VACUUM /
  RESTORE / CREATE OR REPLACE TABLE in `src/sql/`; `src/run_sql.py` enforces it. Stores are
  `delta.appendOnly`; daily inserts are idempotent (`WHERE NOT EXISTS`).
- Nothing changes a job or cluster. The notebook proposes; humans apply after review and approval.
- Watchtower runs with `watchtower/no-delete.patch`. Never suggest `bundle destroy`,
  `terraform destroy` or a pipeline full refresh for prod.
- Access is engineers only (logs can hold sensitive values).

## Conventions

- Keys are always (workspace_id, job_id); job IDs repeat across workspaces.
- Scope/params come from `src/common.py`: placeholders `${catalog}`, `${workspace_ids}`,
  `${scope_values}`, `${review_start_expr}`, `${review_end_expr}`, `${review_label}`,
  `${price_multiplier}`, `${idle_cpu_pct}`, `${lts_code}`, `${snapshot_date_expr}`.
- Cost = `usage_quantity x pricing.effective_list.default x ${price_multiplier}` (1.093). Report cost_usd.
- Statements end with `;` at end of line; no comment line may end in `;`; avoid apostrophes in SQL strings.
- Optional sources (query history, lineage, cluster config, job owner columns): fallback view first,
  then `-- @optional` real definition.
- `job_daily_history` / `job_scorecard_daily` are CTAS from their views on first run; adding a column
  to the view needs `ALTER TABLE ... ADD COLUMNS` on the table (never recreate it).
- New signal: daily view -> `v_job_daily_metrics` -> scorecard `a` CTE -> category (top action, keep
  priority order) or `finding_codes` + message (secondary) -> dashboard/Genie/docs.
- Dashboard JSON is generated: edit `tools/build_dashboard.py`, re-run, commit both.
- Run `python3 tools/check.py` before committing (CI runs it; it never deploys).

## Not yet verified live

Column names in `system.compute.node_timeline`, `system.compute.clusters` (policy_id),
`system.lakeflow.job_task_run_timeline` (compute_ids, job_run_id, task_key, run_id),
`system.query.history` (query_source, spilled_local_bytes), `system.access.table_lineage`
(entity_type/entity_id), Spark event-log JSON field names, and the log regexes.
