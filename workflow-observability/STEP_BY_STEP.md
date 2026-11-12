# Step-by-step: build and deploy Workflow Observability

Follow these steps in order on your project laptop. Each step ends with a
**Check** so you know it worked before moving on.

---

## Step 0 — What you need

| Need | Why | Who usually has it |
|---|---|---|
| Databricks CLI **v0.250+** (`databricks -v`) | deploys the bundle | you (install from Databricks docs) |
| A "hub" workspace in the region you monitor | runs the job and the app | platform team |
| A SQL warehouse ID (serverless preferred) | the app queries through it | workspace admin |
| A service principal for the collector job | reads system tables | account admin |
| Metastore admin (or someone who is) | grants on `system.*` schemas | account admin |
| Bash: macOS/Linux terminal, WSL, or **Git Bash** on Windows | runs `bootstrap.sh` | you |

> If you can't install the CLI, see **Appendix A: no CLI on the laptop**.

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

**Check:** it prints `Done. 13 files created.` and you now have:
```
workflow-observability/
├── databricks.yml
├── README.md
├── STEP_BY_STEP.md
├── app/            app.py, app.yaml, requirements.txt
├── resources/      app.yml, collector_job.yml
├── src/            run_sql.py, sql/00_setup.sql, 10_silver.sql, 20_gold.sql
└── .github/        workflows/deploy.yml   (optional CI)
```

If the count is lower, the copy was cut off. Copy again and make sure the last
line of the script is the one about `.github/`.

On Windows, if you see `$'\r': command not found`, your editor saved Windows line
endings. Fix it with `sed -i 's/\r$//' bootstrap.sh` and run it again (or set
your editor to save with LF line endings).

From here on, work inside `workflow-observability/` and commit it to your
project repo as you normally would.

---

## Step 2 — Fill in your values in `databricks.yml`

Replace every `<...>` placeholder:

| Placeholder | Example | Where to find it |
|---|---|---|
| `<dev-hub-workspace>` | `dbc-1234abcd-5678` | workspace URL |
| `<dev-warehouse-id>` | `a1b2c3d4e5f67890` | SQL Warehouses → your warehouse → Connection details |
| `<us-east-1-hub>` / `<prod-warehouse-id>` | same, for prod | |
| `job_sp` | `6f1c...-...` | Account console → Service principals → Application ID |
| `admin_emails` | `you@company.com` | who may edit workspaces/rules in the app |

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
```

In **dev** the job runs as **you**, so you need the same `system.*` grants yourself
(or run dev as the service principal too).

**Check:** this returns rows when run by you or the service principal:
```sql
SELECT COUNT(*) FROM system.billing.usage WHERE usage_date >= current_date() - 7;
SELECT COUNT(*) FROM system.lakeflow.job_run_timeline WHERE period_start_time >= current_date() - 7;
```
If a system schema is missing, an account admin must enable it first
(Account console, or `databricks system-schemas enable`).

---

## Step 5 — Validate and deploy to dev

```bash
cd workflow-observability
databricks bundle validate -t dev
databricks bundle deploy   -t dev
```

**Check:** in the workspace you can see the job `[dev <you>] workflow-obs-collector`
and the app `workflow-obs-dev` (Compute → Apps).

---

## Step 6 — Backfill 90 days (first run only)

```bash
databricks bundle run collector -t dev --params lookback_days=90
```

Usually 5–20 minutes. Later scheduled runs reprocess only the last 3 days.

**Check:**
```sql
SELECT COUNT(*) FROM platform_obs_dev.config.workspaces;      -- discovered workspaces
SELECT COUNT(*) FROM platform_obs_dev.silver.fact_job_run;    -- runs
SELECT * FROM platform_obs_dev.gold.findings LIMIT 20;        -- optimization findings
```

If a task fails, open the run, read the task output, and see **Troubleshooting**.

---

## Step 7 — Start the app and give it access

```bash
databricks bundle run workflow_obs_app -t dev
```

1. Open Compute → Apps → `workflow-obs-dev` and copy the app's **service principal** name.
2. Grant it access to the data:
   ```sql
   GRANT USE CATALOG ON CATALOG platform_obs_dev TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.silver TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA platform_obs_dev.gold   TO `<app-sp>`;
   GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA platform_obs_dev.config TO `<app-sp>`;
   ```
3. Open the app URL.

**Check:** the Overview page shows spend numbers and charts.

---

## Step 8 — Configure from the app

1. **Workspaces:** give each workspace a name, environment and owner team, and
   turn off any you don't want monitored. Click **Save changes**.
2. **Rules:** adjust thresholds, and set your negotiated **discount %** so cost
   matches your invoice instead of list price.
3. Click **Run collector now** (on Workspaces) so findings use the new settings.

**Check:** the Findings page lists items with estimated savings.

---

## Step 9 — Share it

App → Permissions → give `CAN_USE` to the groups that should see it.
Only people in `admin_emails` see the Workspaces and Rules pages.

---

## Step 10 — Promote to prod and add regions

```bash
databricks auth login --host https://<us-east-1-hub>.cloud.databricks.com
databricks bundle deploy -t prod_us_east_1
databricks bundle run collector -t prod_us_east_1 --params lookback_days=90
databricks bundle run workflow_obs_app -t prod_us_east_1
```
Repeat Step 7's grants for the prod catalog and prod app service principal.

**Another region:** copy the `prod_us_east_1` block in `databricks.yml` and
rename it (e.g. `prod_us_west_2`). Use a hub workspace in that region and
repeat this step. New workspaces in a region are picked up automatically; you
never deploy per workspace.

**CI (optional):** move `.github/` to your repo root, add the secrets listed in
README, and every merge to `main` deploys.

---

## Developing further

| You want to… | Change | Then |
|---|---|---|
| Change a threshold | Rules page in the app | Run collector |
| Add a new rule | 1) add a row to the `config.rules` seed in `src/sql/00_setup.sql` 2) add a `named_struct(...)` line in the findings query in `src/sql/20_gold.sql` 3) add a `WHEN` branch if it's a "less than" rule | `bundle deploy`, run collector |
| Add a metric per job | add a CTE + column in `gold.job_summary_30d` (`20_gold.sql`) | deploy, run collector |
| Add a new app page | add an option to `page = st.sidebar.radio(...)` and an `elif page == ...` block in `app/app.py` | `bundle deploy` + `bundle run workflow_obs_app` |
| Change the schedule | `quartz_cron_expression` in `resources/collector_job.yml` | `bundle deploy` |
| Change catalog / admins | `variables` per target in `databricks.yml` | `bundle deploy`, restart app |

Test SQL changes quickly by pasting the statement into a SQL editor, with
`${catalog}` replaced by `platform_obs_dev` and `${lookback_days}` by `3`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `TABLE_OR_VIEW_NOT_FOUND system.lakeflow...` | system schema not enabled | account admin enables `lakeflow` / `compute` schemas |
| `PERMISSION_DENIED` in a job task | the run-as identity lacks grants | Step 4 grants for that identity |
| Column not found `setup_duration_seconds` | older task timeline schema in your account | in `10_silver.sql` replace the three duration lines with `0` and remove the `HIGH_SETUP_OVERHEAD` rule |
| App: "No usage yet" | collector hasn't run, or no workspaces enabled | run Step 6; check `config.workspaces.enabled` |
| App error about the warehouse | app has no `CAN_USE`, or warehouse stopped | redeploy (the bundle grants it); start the warehouse |
| Costs look ~20–40% high | list price vs negotiated | set discount % on the Rules page |
| Workspace names are IDs | `system.access.workspaces_latest` not available | type names on the Workspaces page |
| Only some workspaces appear | system tables are per region | deploy a hub in each region (Step 10) |

---

## Appendix A — No CLI on the laptop

If you can only use the Databricks web UI:

1. In the workspace, create a Git folder for your project repo (or a plain
   workspace folder), then create each file there with the same paths. Paste
   the contents from `bootstrap.sh`; each file sits between a `write '<path>'`
   line and the end-of-file marker line that follows it.
2. Open `databricks.yml` in the workspace file editor. The **Deployments**
   (bundle) panel lets you validate, deploy and run the bundle from the browser,
   with the same targets as Steps 5–7.
3. Everything after that (grants, backfill, app setup) is the same.
