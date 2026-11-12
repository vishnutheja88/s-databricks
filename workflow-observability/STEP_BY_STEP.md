# Step-by-step: build and deploy Workflow Observability

What you get:

| Component | Status | Deployed by |
|---|---|---|
| Collector job (system tables → silver/gold) | **core** | bundle |
| AI/BI dashboard (Overview, Workflows, Findings, Reliability) | **core** | bundle |
| SQL alerts (spend spike, failed runs, high findings, stale data) | **core** | bundle |
| Genie space (ask questions in plain English) | **core** | `src/genie/deploy_genie.py` |
| Databricks App (interactive admin UI) | optional | bundle, when enabled |

Follow the steps in order. Each step ends with a **Check** so you know it
worked before moving on.

---

## Step 0 — What you need

| Need | Why | Who usually has it |
|---|---|---|
| Databricks CLI **v0.281+** (`databricks -v`) | deploys job, dashboard and alerts | you |
| Python 3.10+ with `pip install databricks-sdk` | creates the Genie space | you |
| A "hub" workspace in the region you monitor | hosts everything | platform team |
| A SQL warehouse ID (serverless preferred) | dashboard, alerts, Genie | workspace admin |
| A service principal for the collector job | reads system tables | account admin |
| Metastore admin (or someone who is) | grants on `system.*` schemas | account admin |
| Bash: macOS/Linux terminal, WSL, or **Git Bash** on Windows | runs `bootstrap.sh` | you |

> No CLI on the laptop? See **Appendix A**.

---

## Step 1 — Recreate the project from `bootstrap.sh`

You can't clone the repo, so the whole project is packed into one script.

1. Open `bootstrap.sh` from the repo in your browser. Click **Raw** (or the
   copy icon on the file page) and copy **all** of it.
2. On the laptop, create an empty file named `bootstrap.sh` in the folder where
   you keep projects, paste, and save.
3. Run:
   ```bash
   bash bootstrap.sh
   ```

**Check:** it ends with `Done. N files created.` (N matches the number printed
at the top of `bootstrap.sh`), and you have:
```
workflow-observability/
├── databricks.yml
├── README.md, STEP_BY_STEP.md
├── resources/   collector_job.yml, dashboard.yml, alerts.yml
├── optional/    app.yml                      (only used if you enable the app)
├── src/
│   ├── run_sql.py, sql/00_setup.sql, 10_silver.sql, 20_gold.sql
│   ├── dashboards/workflow_observability.lvdash.json
│   └── genie/genie_space.json, deploy_genie.py
├── app/         app.py, app.yaml, requirements.txt   (optional app)
└── .github/     workflows/deploy.yml                 (optional CI)
```

If the count is low, the copy was cut off. Copy again and make sure the last
line of the script is the one about `.github/`.

On Windows, if you see `$'\r': command not found`, your editor saved Windows line
endings. Fix it with `sed -i 's/\r$//' bootstrap.sh` and run it again.

From here on, work inside `workflow-observability/` and commit it to your
project repo as usual.

---

## Step 2 — Fill in your values in `databricks.yml`

Replace every `<...>` placeholder:

| Placeholder | Example | Where to find it |
|---|---|---|
| `<dev-hub-workspace>` | `dbc-1234abcd-5678` | workspace URL |
| `<dev-warehouse-id>` | `a1b2c3d4e5f67890` | SQL Warehouses → your warehouse → Connection details |
| `<you@company.com>` | your email | receives dev alerts |
| `<us-east-1-hub>`, `<prod-warehouse-id>`, `<platform-oncall@company.com>` | prod values | |
| `job_sp` | `6f1c...-...` | Account console → Service principals → Application ID |

Alert thresholds are variables too (`failed_runs_threshold`, `cost_spike_pct`,
`stale_hours`). Override them per target if needed.

On Azure or GCP, change `.cloud.databricks.com` to your workspace host
(`adb-xxxx.azuredatabricks.net`, `xxxx.gcp.databricks.com`).

**Check:** `grep -n "<" databricks.yml` returns nothing.

---

## Step 3 — Log in with the CLI

```bash
databricks auth login --host https://<dev-hub-workspace>.cloud.databricks.com
databricks current-user me
```

**Check:** the second command prints your user.

---

## Step 4 — Grants (one time, metastore admin)

Run in a SQL editor of the hub workspace. Replace `<job-sp>` with the service
principal's application ID.

```sql
GRANT USE CATALOG ON CATALOG system TO `<job-sp>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing  TO `<job-sp>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.lakeflow TO `<job-sp>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.compute  TO `<job-sp>`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.access   TO `<job-sp>`;   -- optional

CREATE CATALOG IF NOT EXISTS platform_obs_dev;   -- and platform_obs for prod
GRANT ALL PRIVILEGES ON CATALOG platform_obs_dev TO `<job-sp>`;

-- Who may view the dashboard / use Genie / receive alert data
GRANT USE CATALOG ON CATALOG platform_obs_dev TO `<viewers-group>`;
GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.gold   TO `<viewers-group>`;
GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.silver TO `<viewers-group>`;
GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.config TO `<viewers-group>`;
```

In **dev**, the job, dashboard and alerts run as **you**, so you need the
`system.*` grants yourself too.

**Check:** this returns rows when run by you or the service principal:
```sql
SELECT COUNT(*) FROM system.billing.usage WHERE usage_date >= current_date() - 7;
SELECT COUNT(*) FROM system.lakeflow.job_run_timeline WHERE period_start_time >= current_date() - 7;
```
If a system schema is missing, an account admin must enable it first.

---

## Step 5 — Validate and deploy to dev

```bash
cd workflow-observability
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

This deploys the **collector job**, the **dashboard** and the **4 SQL alerts**.

**Check:** in the workspace you see
- Jobs: `[dev <you>] workflow-obs-collector`
- Dashboards: `Workflow Observability (dev)`
- Alerts: four alerts starting with `[wfobs]`

---

## Step 6 — Backfill 90 days (first run only)

```bash
databricks bundle run collector -t dev --params lookback_days=90
```

Usually 5–20 minutes. Later scheduled runs (every 4 hours) reprocess only the
last 3 days.

**Check:**
```sql
SELECT COUNT(*) FROM platform_obs_dev.config.workspaces;      -- discovered workspaces
SELECT COUNT(*) FROM platform_obs_dev.silver.fact_job_run;    -- runs
SELECT * FROM platform_obs_dev.gold.findings LIMIT 20;        -- optimization findings
```

---

## Step 7 — Dashboard

1. Open **Dashboards → Workflow Observability (dev)**.
2. Look at each page: **Overview** (KPIs, daily spend), **Workflows** (top
   jobs, growth, full table), **Findings** (by rule, savings, to-do table),
   **Reliability** (outcomes, termination codes, failed-run spend).
3. Click **Publish**, then **Share** with your viewers group.
   (Viewers query with their own permissions, so they need the Step 4 viewer grants.)
4. Optional: **Schedule** an emailed PDF snapshot (e.g. Monday 9:00).

**Check:** the KPI counters show numbers and the charts render.

If you change the dashboard in the UI, pull the change back into code:
```bash
databricks bundle generate dashboard --resource workflow_observability --watch
```

---

## Step 8 — SQL alerts

| Alert | Fires when | Schedule |
|---|---|---|
| `[wfobs] Workflow spend spike` | yesterday's JOBS spend > 7-day avg by `cost_spike_pct`% | daily 09:30 |
| `[wfobs] Failed workflow runs (24h)` | failed runs in 24h > `failed_runs_threshold` | every 4h |
| `[wfobs] High-severity optimization findings` | any HIGH finding open | Mondays 10:00 |
| `[wfobs] Observability data is stale` | newest data older than `stale_hours` | every 2h |

1. Open each alert and click **Run now** (or **Evaluate**) once.
2. To send to Slack/Teams instead of email, create a notification destination
   (Settings → Notifications), then in `resources/alerts.yml` replace
   `user_email: ...` with `destination_id: <destination-id>` and redeploy.

**Check:** each alert shows a state of **OK** or **TRIGGERED** (not "Unknown"),
and a triggered alert sends you an email.

---

## Step 9 — Genie space

A service principal can't own a Genie space, so you create it as yourself with
a small script. It is idempotent: re-run it after editing `src/genie/genie_space.json`.

```bash
pip install databricks-sdk
python src/genie/deploy_genie.py --catalog platform_obs_dev \
    --warehouse-id <dev-warehouse-id> --title-suffix " (dev)"
# add --profile <name> if you use a non-default CLI profile
# add --dry-run to preview the payload without creating anything
```

1. Open the printed link.
2. Try the sample questions, e.g. *"Which 10 workflows cost the most in the last 30 days?"*
3. **Share** the space with your viewers group (CAN_RUN).
4. Improve answers over time: add instructions or questions to
   `genie_space.json` and re-run, or add **benchmarks** and trusted SQL in the UI.

**Check:** Genie answers *"Which workflows wasted the most money on failed runs?"*
with a table that matches the dashboard's Findings page.

---

## Step 10 — (Optional) Databricks App

Skip this unless you want the interactive admin UI (edit workspaces/rules from
a web page, run the collector on demand). Everything else works without it;
rules and workspace settings can also be edited with SQL:
```sql
UPDATE platform_obs_dev.config.rules SET threshold = 20 WHERE rule_id = 'LOW_CPU';
UPDATE platform_obs_dev.config.workspaces SET workspace_name = 'Finance prod', environment = 'prod' WHERE workspace_id = '123';
UPDATE platform_obs_dev.config.settings SET value = '25' WHERE key = 'discount_pct';
```

To enable the app:
1. In `databricks.yml`, uncomment `# - optional/app.yml` under `include`.
2. Set `admin_emails` for the target.
3. Deploy and start it:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle run workflow_obs_app -t dev
   ```
4. Grant the app's service principal (Compute → Apps → workflow-obs-dev):
   ```sql
   GRANT USE CATALOG ON CATALOG platform_obs_dev TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.silver TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.gold   TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA platform_obs_dev.config TO `<app-sp>`;
   ```

**Check:** the app's Overview page shows spend numbers.

---

## Step 11 — Promote to prod and add regions

```bash
databricks auth login --host https://<us-east-1-hub>.cloud.databricks.com
databricks bundle deploy -t prod_us_east_1
databricks bundle run collector -t prod_us_east_1 --params lookback_days=90
python src/genie/deploy_genie.py --catalog platform_obs --warehouse-id <prod-warehouse-id>
```
Repeat the Step 4 grants for the prod catalog, then share the prod dashboard and
Genie space.

**Another region:** copy the `prod_us_east_1` block in `databricks.yml` and
rename it (e.g. `prod_us_west_2`). Use a hub workspace in that region and
repeat this step. New workspaces in a region are picked up automatically; you
never deploy per workspace.

**CI (optional):** move `.github/` to your repo root and add the secrets listed
in README. Every merge to `main` deploys the job, dashboard and alerts. Run the
Genie script yourself, because it needs a human owner.

---

## Developing further

| You want to… | Change | Then |
|---|---|---|
| Change a rule threshold | `UPDATE ... config.rules` (or the app) | run collector |
| Add a new rule | 1) add a row to the `config.rules` seed in `src/sql/00_setup.sql` 2) add a `named_struct(...)` line in the findings query in `src/sql/20_gold.sql` 3) add a `WHEN` branch if it's a "less than" rule | deploy, run collector |
| Add a metric per job | CTE + column in `gold.job_summary_30d` (`20_gold.sql`) | deploy, run collector |
| Add a dashboard chart | edit in the UI → `bundle generate dashboard ... --watch` | commit the JSON |
| Add an alert | copy a block in `resources/alerts.yml` | `bundle deploy` |
| Teach Genie something | `instructions` / `sample_questions` in `src/genie/genie_space.json` | re-run `deploy_genie.py` |
| Change the schedule | `quartz_cron_expression` in `resources/collector_job.yml` | `bundle deploy` |

Test SQL changes quickly by pasting the statement into a SQL editor, with
`${catalog}` replaced by `platform_obs_dev` and `${lookback_days}` by `3`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `bundle validate`: unknown field `dataset_catalog` / `alerts` | CLI too old | upgrade to v0.281+ |
| `TABLE_OR_VIEW_NOT_FOUND system.lakeflow...` | system schema not enabled | account admin enables it |
| `PERMISSION_DENIED` in a job task | run-as identity lacks grants | Step 4 grants |
| Column not found `setup_duration_seconds` | older task timeline schema | in `10_silver.sql` replace the three duration lines with `0`; disable rule `HIGH_SETUP_OVERHEAD` |
| Dashboard widget error "no fields to visualize" | widget field mismatch after an edit | fix in the UI, then `bundle generate dashboard --watch` |
| Dashboard empty for a viewer | viewer lacks SELECT | Step 4 viewer grants |
| Alert state "Unknown" | never evaluated, or the query errors | open the alert → Run now; read the error |
| Genie script: 403 / not found | Genie not enabled, or no CAN_USE on the warehouse | workspace admin enables Genie; grant warehouse CAN_USE |
| Genie gives wrong joins | needs guidance | add an instruction or sample question; re-run the script |
| Costs look ~20–40% high | list price vs negotiated | `UPDATE config.settings SET value='<pct>' WHERE key='discount_pct'` |
| Only some workspaces appear | system tables are per region | deploy a hub per region |

---

## Appendix A — No CLI on the laptop

1. In the workspace, create a Git folder (or a plain workspace folder) and
   create each file with the same paths. Paste the contents from
   `bootstrap.sh`; each file sits between a `write '<path>'` line and the
   end-of-file marker line that follows it.
2. Open `databricks.yml` in the workspace file editor and use the
   **Deployments** (bundle) panel to validate, deploy and run (Steps 5–6).
3. For the Genie space without local Python, run `src/genie/deploy_genie.py`
   in a notebook cell:
   `%pip install databricks-sdk` then
   `!python deploy_genie.py --catalog platform_obs_dev --warehouse-id <id>`.
   Or create the space in the UI: add the tables listed in
   `genie_space.json`, then paste its instructions and sample questions.
