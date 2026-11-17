# Handoff: turn the observability prototype into a Databricks App

This file is written for Claude (Claude Code) working in this repo on the engineer's work laptop.
Read it fully, then read `../CLAUDE.md`, `../README.md` and `../docs/ARCHITECTURE.md` before changing code.

## What exists

- `app-prototype/index.html` is a single-file, working UI prototype ("Lakehouse Job Observability").
  It runs on **demo data** generated in the page (search for `/* ---------- Demo data`), shaped exactly
  like `watchtower.ops.v_job_scorecard_latest`, and scored with the same rules as `v_job_scorecard_30d`.
- Layout (keep it): **filters on the left**, **insights and reports in the middle** (6 tabs), **assistant on the right**.
  - Filters: search, review period, workspace, top fix type, effort, risk, owner, secondary findings,
    minimum monthly cost, "only needs action", reset. One shared filtered dataset feeds every chart,
    table and assistant answer.
  - Tabs: Overview (KPIs, daily spend by workspace, savings by fix type, top opportunity per workspace,
    prioritized action plan), Cost & utilization, Performance (runtime trend, shuffle, run-start heatmap),
    Reliability, Recommendations (coverage of the 9 recommendation areas), Scope & validation.
  - Job detail dialog: proposed change, effort, risk, owner, metrics, daily cost sparkline, findings.
  - Assistant: suggested pilot questions; answers only from the filtered jobs; can apply filters and open a
    job. In the prototype it uses `claude.use("sample")` (claude.ai artifact runtime) with a built-in
    rule-based fallback (`localAnswer`).
- Accessibility already in place: skip link, landmarks, keyboard tabs (arrow keys), Enter on rows,
  `/` focuses the assistant, `aria-live` chat log, icons + text for effort/risk/status, light/dark themes,
  reduced motion, responsive side panels. Keep all of it.

## Goal

Ship the same UI as a **Databricks App** that reads **live data** from the `watchtower.ops` views built by
the `hybrid-optimization` bundle, with the assistant backed by the pilot's **Genie space**.

## Hard rules (same as the rest of the repo)

- **Read-only.** The app only SELECTs from `watchtower.ops` (and `watchtower.silver.logs` if you add log
  drill-down). No INSERT/UPDATE/DELETE/DDL, no job or cluster changes. The fix plan is a proposal;
  changes go through review and approval.
- **Engineers only.** App permission `CAN_USE` for the engineering group only; the app's identity gets
  SELECT on `watchtower.ops` (+ `watchtower.silver` if used). Log lines can hold sensitive values.
- **Contract cost.** Money columns are contract cost (usage × effective list price × 1.093). Never
  recompute cost in the app; display what the views return.
- No secrets in code or in the repo; use app resources / environment variables.
- Do not change the scoring rules in the UI; they live in `src/sql/10_views.sql`. The app displays
  `category`, `est_monthly_savings_usd`, `priority_rank`, etc. as returned.

## Suggested architecture

```
app-prototype/            (keep as the design reference)
app/
  app.yaml                Databricks App config: command + env (warehouse id, catalog, genie space id)
  requirements.txt        fastapi, uvicorn, databricks-sql-connector, databricks-sdk
  main.py                 FastAPI: serves static/ and read-only JSON endpoints
  queries.py              parameterized SQL (named parameters only, never string-built filters)
  genie.py                Genie conversation wrapper for the assistant
  static/index.html       the prototype, with demo data replaced by fetch() calls
resources/app.yml         bundle resource for the app (optional; deploy with the bundle)
```

Python (FastAPI) keeps it consistent with the bundle's Python tasks. Streamlit/Dash would force a
rewrite of the UI; avoid that, because the prototype layout is the agreed design.

### Endpoints (all GET, read-only)

| Endpoint | Source | Notes |
| --- | --- | --- |
| `/api/scorecard` | `ops.v_job_scorecard_latest` | all columns the UI uses (see mapping); filter in the browser (data is small) |
| `/api/workspaces` | `ops.v_workspace_summary_latest` | top opportunity per workspace |
| `/api/trend?days=30\|60\|90` | `ops.v_job_cost_daily` joined to scorecard for names | daily cost per workspace/job |
| `/api/runtime?days=` | `ops.v_job_run_health_daily` | runtime and failures per day |
| `/api/hotspots` | `ops.v_schedule_hotspots` | heatmap |
| `/api/idle` | `ops.v_cluster_idle` | idle per cluster |
| `/api/scope` | `ops.v_scope` + latest `ops.scope_history` | scope tab |
| `/api/access` | latest row per object in `ops.access_checks` | scope tab |
| `/api/validation` | latest row per check in `ops.validation_results` | scope tab + header badge |
| `/api/job/{workspace_id}/{job_id}/daily` | `ops.v_job_daily_metrics` | sparkline in the job dialog |
| `POST /api/ask` | Genie space | assistant (the only POST; it writes nothing to our tables) |

Use the Databricks SQL connector with the app's credentials (or on-behalf-of-user authorization if the
workspace has it enabled and you want per-user Unity Catalog permissions). Cache scorecard responses for a
few minutes: the snapshot changes once a day.

### Review period in the app

The views cover the bundle's configured review period. The prototype's 30/60/90 selector re-scores demo
data; in the app either (a) show the configured period from `ops.v_scope` as read-only text, or
(b) keep the selector only for the trend charts (`/api/trend?days=`). Do not re-score jobs in the browser.

### Assistant

- Replace `claude.use("sample")` with `POST /api/ask` -> Genie (Databricks SDK `w.genie` conversation
  methods: start a conversation, send follow-up messages, read the generated SQL and the query result;
  verify the exact method names against the installed SDK version).
- Send the user's question plus the active filters as plain text context (for example
  "Filters: workspaces=prod-etl; fix type=OVERSIZED. Question: ..."), so answers match what is on screen.
- Show Genie's text answer, its result table, and a "Show SQL" disclosure (transparency for validation).
- Keep `localAnswer` as the fallback when Genie errors or times out, and label the answer source
  ("Genie" vs "Built-in"), as the prototype does.
- The prototype's assistant tools (`apply_filters`, `open_job`) can stay as UI actions on buttons the
  backend suggests; Genie does not call page functions.

## Field mapping (UI <- `v_job_scorecard_latest`)

The demo generator builds these keys; the live endpoint should return the same names so the rendering code
changes as little as possible.

| UI key | View column |
| --- | --- |
| workspace_id, workspace_name, job_id, job_name, owner | same |
| period_cost_usd, monthly_cost_usd, est_monthly_savings_usd, est_annual_savings_usd | same |
| dbus, runs, failed_runs, failed_run_cost_usd, retries | same |
| avg_minutes, runtime_change_pct, cost_change_pct, p95_cpu_change | same |
| avg_cpu, p95_cpu, avg_mem, max_swap, idle_pct, idle_node_hours, node_hours, clusters_per_run | same |
| oom_events, spill_events, lost_executor_events, error_lines | same |
| shuffle_read_gb, disk_spill_gb, max_skew_ratio, slow_queries | same |
| dbr_version, runtime_outdated, photon, policy_missing, small_file_tables, schedule_peak | same |
| category, top_recommendation, finding_codes, secondary_findings, fix_effort, risk_level, risk | same |
| priority_rank, workspace_rank, review_days, review_label | same |
| node, max_workers, autoscale (job dialog) | `worker_node_type`, `max_workers`; autoscale is not in the view: show "max workers" only, or add it to `v_job_config` |
| daily (sparkline/trend) | from `/api/trend` or `/api/job/.../daily` |
| hours (heatmap) | from `/api/hotspots` |

The prototype's `proposal(j)` builds the "proposed change" text from category + node + max_workers. Keep it
in the browser (it mirrors `notebooks/optimization_insights.py`) or move it to the backend; either way it is a
suggestion only.

## Step-by-step plan

1. Create `app/` with FastAPI serving `static/index.html` (copy of the prototype) and `/api/health`.
2. Implement `/api/scorecard` and switch the page from `BASE`/`score()` demo data to the fetched rows
   (keep the demo generator behind a `?demo` flag for offline UI work).
3. Implement the remaining endpoints and wire each chart/table; remove the "Demo data" pill when live.
4. Implement `/api/ask` with Genie; keep the built-in fallback.
5. Add `app.yaml` (and optionally `resources/app.yml` in the bundle) with the warehouse, catalog and Genie
   space as configuration, not hardcoded values.
6. Permissions: app `CAN_USE` -> engineering group; app identity -> `USE CATALOG watchtower`,
   `USE SCHEMA watchtower.ops`, `SELECT` on `watchtower.ops`; `CAN_USE` on the SQL warehouse; access to
   the Genie space.
7. Test in `dev` (one workspace), then prod through change review.

## Acceptance checks

- With live data, the Overview totals equal `SELECT SUM(period_cost_usd), SUM(est_monthly_savings_usd)
  FROM watchtower.ops.v_job_scorecard_latest` (and the dashboard's Executive summary).
- Every filter changes charts, tables and the assistant context together.
- The 8 pilot questions return answers in the assistant that match `src/genie/benchmarks.json` SQL.
- The Scope & validation tab shows the latest `access_checks` and `validation_results`.
- Keyboard-only walkthrough works (tabs, rows, dialog, assistant); no console errors; light and dark readable.
- `grep -rniE "insert |update |delete |merge |drop |create |alter " app/` finds nothing in SQL strings.
- Run `python3 tools/check.py` (bundle) before committing; it must still pass.

## Known placeholders to fill on the work laptop

SQL warehouse id, Genie space id, catalog (default `watchtower`), engineering group name, app name.
