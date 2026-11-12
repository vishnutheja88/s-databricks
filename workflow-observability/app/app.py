"""Workflow Observability – Databricks App (Streamlit).

Runs as the app's service principal; reads the obs catalog through a SQL
warehouse. Pages: Overview, Workflows, Findings, Workspaces (admin), Rules (admin).
"""
import os
import re

import altair as alt
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(page_title="Workflow Observability", layout="wide")

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")
JOB_ID = os.getenv("COLLECTOR_JOB_ID")
cfg = Config()


@st.cache_resource
def job_params() -> dict:
    """Catalog and admins come from the collector job's parameters (one source of truth)."""
    params = {}
    if JOB_ID:
        job = WorkspaceClient().jobs.get(job_id=int(JOB_ID))
        params = {p.name: p.default for p in (job.settings.parameters or [])}
    return params


_p = job_params()
CATALOG = os.getenv("OBS_CATALOG") or _p.get("catalog", "platform_obs")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    st.error(f"Catalog name '{CATALOG}' is not a valid identifier")
    st.stop()
ADMINS = {e.strip().lower() for e in (_p.get("admin_emails") or "").split(",") if e.strip()}


# ------------------------------------------------------------------ data access
@st.cache_resource
def _conn():
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=600, show_spinner=False)
def q(statement: str, params: dict | None = None) -> pd.DataFrame:
    with _conn().cursor() as cur:
        cur.execute(statement, params or {})
        return cur.fetchall_arrow().to_pandas()


def execute(statement: str, params: dict | None = None) -> None:
    with _conn().cursor() as cur:
        cur.execute(statement, params or {})


def t(name: str) -> str:
    return f"{CATALOG}.{name}"


def ws_filter(ids: list[str], col: str = "workspace_id") -> str:
    """Workspace IDs are numeric; validate, then inline as an IN list."""
    safe = [i for i in ids if re.fullmatch(r"\d+", str(i))]
    if not safe:
        return "1 = 0"
    return f"{col} IN ({', '.join(repr(str(i)) for i in safe)})"


def money(v) -> str:
    return "—" if pd.isna(v) else f"${v:,.0f}"


# ------------------------------------------------------------------ identity
user_email = (st.context.headers.get("X-Forwarded-Email") or "").lower()
is_admin = not ADMINS or user_email in ADMINS

# ------------------------------------------------------------------ sidebar
st.sidebar.title("Workflow Observability")
page = st.sidebar.radio(
    "Page", ["Overview", "Workflows", "Findings"] + (["Workspaces", "Rules"] if is_admin else [])
)

workspaces = q(f"SELECT workspace_id, COALESCE(workspace_name, workspace_id) AS name, environment, "
               f"owner_team, enabled FROM {t('config.workspaces')} ORDER BY name")
enabled = workspaces[workspaces["enabled"] == True]  # noqa: E712
labels = {r.workspace_id: f"{r.name} ({r.environment or 'n/a'})" for r in enabled.itertuples()}

envs = sorted([e for e in enabled["environment"].dropna().unique()])
env_pick = st.sidebar.multiselect("Environment", envs)
ws_options = [w for w in labels if not env_pick or
              enabled.set_index("workspace_id").loc[w, "environment"] in env_pick]
ws_pick = st.sidebar.multiselect("Workspaces", ws_options, default=ws_options,
                                 format_func=lambda w: labels.get(w, w))
days = st.sidebar.selectbox("Window", [7, 14, 30, 90], index=2, format_func=lambda d: f"Last {d} days")
st.sidebar.caption(f"{len(enabled)} of {len(workspaces)} discovered workspaces monitored")
WS = ws_filter(ws_pick)

# ------------------------------------------------------------------ pages
if page == "Overview":
    st.header("Overview")
    daily = q(f"""SELECT usage_date, workspace_name, billing_origin_product AS product,
                         SUM(effective_cost) AS cost
                  FROM {t('gold.v_daily_cost')}
                  WHERE {WS} AND usage_date >= date_sub(current_date(), :d)
                  GROUP BY ALL""", {"d": days})
    findings = q(f"SELECT COUNT(*) AS n, SUM(est_monthly_savings) AS savings "
                 f"FROM {t('gold.findings')} WHERE {WS}")
    failed = q(f"SELECT SUM(effective_cost) AS c FROM {t('silver.fact_job_run')} "
               f"WHERE {WS} AND start_time >= date_sub(current_date(), :d) "
               f"AND result_state IN ('FAILED','ERROR','TIMED_OUT')", {"d": days})

    total = daily["cost"].sum() if not daily.empty else 0
    jobs_cost = daily.loc[daily["product"] == "JOBS", "cost"].sum() if not daily.empty else 0
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total spend", money(total))
    c2.metric("Workflow (JOBS) spend", money(jobs_cost))
    c3.metric("Spend on failed runs", money(failed["c"].iloc[0]))
    c4.metric("Open findings · est. savings/mo",
              f"{int(findings['n'].iloc[0] or 0)} · {money(findings['savings'].iloc[0])}")

    if daily.empty:
        st.info("No usage yet for this selection. Has the collector run?")
    else:
        st.subheader("Daily spend by product")
        st.altair_chart(
            alt.Chart(daily.groupby(["usage_date", "product"], as_index=False)["cost"].sum())
            .mark_area().encode(x="usage_date:T", y=alt.Y("cost:Q", title="USD"),
                                color="product:N", tooltip=["usage_date:T", "product", alt.Tooltip("cost:Q", format="$,.0f")]),
            use_container_width=True)
        st.subheader("Spend by workspace")
        by_ws = daily.groupby("workspace_name", as_index=False)["cost"].sum().sort_values("cost", ascending=False)
        st.altair_chart(
            alt.Chart(by_ws).mark_bar().encode(
                x=alt.X("cost:Q", title="USD"), y=alt.Y("workspace_name:N", sort="-x", title=None),
                tooltip=["workspace_name", alt.Tooltip("cost:Q", format="$,.0f")]),
            use_container_width=True)

elif page == "Workflows":
    st.header("Workflows · last 30 days")
    jobs = q(f"""SELECT workspace_name, job_name, job_id, workspace_id, run_as, runs, success_pct,
                        cost_30d, cost_per_run, failed_cost, growth_pct, avg_duration_min,
                        setup_pct, avg_cpu_pct, all_purpose_runs, serverless_share
                 FROM {t('gold.job_summary_30d')} WHERE {WS} ORDER BY cost_30d DESC""")
    search = st.text_input("Filter by job name")
    if search:
        jobs = jobs[jobs["job_name"].str.contains(search, case=False, na=False)]
    sel = st.dataframe(
        jobs, use_container_width=True, hide_index=True, on_select="rerun", selection_mode="single-row",
        column_config={
            "cost_30d": st.column_config.NumberColumn("Cost 30d", format="$%.0f"),
            "cost_per_run": st.column_config.NumberColumn("Cost/run", format="$%.2f"),
            "failed_cost": st.column_config.NumberColumn("Failed cost", format="$%.0f"),
            "success_pct": st.column_config.ProgressColumn("Success %", min_value=0, max_value=100),
            "avg_cpu_pct": st.column_config.ProgressColumn("Avg CPU %", min_value=0, max_value=100),
            "growth_pct": st.column_config.NumberColumn("WoW %", format="%.0f%%"),
        })
    rows = sel.selection.rows if sel and sel.selection else []
    if rows:
        job = jobs.iloc[rows[0]]
        st.subheader(f"Run history · {job.job_name}")
        hist = q(f"""SELECT start_time, duration_min, effective_cost AS cost, result_state, termination_code
                     FROM {t('silver.fact_job_run')}
                     WHERE workspace_id = :w AND job_id = :j AND start_time >= date_sub(current_date(), :d)
                     ORDER BY start_time""", {"w": job.workspace_id, "j": job.job_id, "d": days})
        a, b = st.columns(2)
        base = alt.Chart(hist).encode(x="start_time:T", color="result_state:N",
                                      tooltip=["start_time:T", "result_state", "termination_code",
                                               alt.Tooltip("cost:Q", format="$,.2f"), "duration_min"])
        a.altair_chart(base.mark_circle(size=60).encode(y=alt.Y("cost:Q", title="Cost per run (USD)")),
                       use_container_width=True)
        b.altair_chart(base.mark_circle(size=60).encode(y=alt.Y("duration_min:Q", title="Duration (min)")),
                       use_container_width=True)
        tasks = q(f"""SELECT task_key, COUNT(*) AS runs, ROUND(AVG(exec_s)/60,1) AS avg_exec_min,
                             ROUND(AVG(setup_s)/60,1) AS avg_setup_min, ROUND(AVG(queue_s)/60,1) AS avg_queue_min,
                             COUNT_IF(result_state <> 'SUCCEEDED') AS not_succeeded
                      FROM {t('silver.fact_task_run')}
                      WHERE workspace_id = :w AND job_id = :j AND start_time >= date_sub(current_date(), :d)
                      GROUP BY task_key ORDER BY avg_exec_min DESC""",
                  {"w": job.workspace_id, "j": job.job_id, "d": days})
        st.subheader("Tasks")
        st.dataframe(tasks, use_container_width=True, hide_index=True)
    else:
        st.caption("Select a row to see run history and task breakdown.")

elif page == "Findings":
    st.header("Optimization findings")
    f = q(f"""SELECT severity, title, workspace_name, job_name, owner_team, metric_value, threshold,
                     est_monthly_savings, recommendation, rule_id, job_id, workspace_id, detected_at
              FROM {t('gold.findings')} WHERE {WS}
              ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
                       est_monthly_savings DESC NULLS LAST""")
    if f.empty:
        st.success("No findings for this selection.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Findings", len(f))
        c2.metric("High severity", int((f["severity"] == "HIGH").sum()))
        c3.metric("Est. savings / month", money(f["est_monthly_savings"].sum()))
        rule_pick = st.multiselect("Rule", sorted(f["title"].unique()))
        if rule_pick:
            f = f[f["title"].isin(rule_pick)]
        by_rule = f.groupby("title", as_index=False).agg(findings=("job_id", "count"),
                                                         savings=("est_monthly_savings", "sum"))
        st.altair_chart(alt.Chart(by_rule).mark_bar().encode(
            x=alt.X("findings:Q"), y=alt.Y("title:N", sort="-x", title=None),
            tooltip=["title", "findings", alt.Tooltip("savings:Q", format="$,.0f")]), use_container_width=True)
        st.dataframe(f, use_container_width=True, hide_index=True, column_config={
            "est_monthly_savings": st.column_config.NumberColumn("Est. savings/mo", format="$%.0f")})
        st.download_button("Download CSV", f.to_csv(index=False), "findings.csv", "text/csv")
        st.caption("Savings are rough estimates from list/discounted cost; validate before committing.")

elif page == "Workspaces" and is_admin:
    st.header("Workspaces")
    st.write("Every workspace in this region is discovered from billing. Enable it to start monitoring; "
             "the next collector run picks it up.")
    edited = st.data_editor(
        workspaces.rename(columns={"name": "workspace_name"}), hide_index=True, use_container_width=True,
        disabled=["workspace_id"],
        column_config={"environment": st.column_config.SelectboxColumn(options=["prod", "staging", "dev", "sandbox"])})
    c1, c2 = st.columns(2)
    if c1.button("Save changes", type="primary"):
        for r in edited.itertuples():
            execute(f"""UPDATE {t('config.workspaces')}
                        SET workspace_name = :n, environment = :e, owner_team = :o,
                            enabled = :en, updated_at = current_timestamp()
                        WHERE workspace_id = :w""",
                    {"n": r.workspace_name, "e": r.environment, "o": r.owner_team,
                     "en": bool(r.enabled), "w": r.workspace_id})
        q.clear()
        st.success("Saved.")
    if JOB_ID and c2.button("Run collector now"):
        run = WorkspaceClient().jobs.run_now(job_id=int(JOB_ID))
        st.info(f"Collector started (run {run.run_id}). Data refreshes when it finishes.")

elif page == "Rules" and is_admin:
    st.header("Rules")
    rules = q(f"SELECT rule_id, title, threshold, severity, enabled, recommendation "
              f"FROM {t('config.rules')} ORDER BY rule_id")
    edited = st.data_editor(rules, hide_index=True, use_container_width=True, disabled=["rule_id", "title"],
                            column_config={"severity": st.column_config.SelectboxColumn(options=["HIGH", "MEDIUM", "LOW"])})
    disc = q(f"SELECT CAST(value AS DOUBLE) AS v FROM {t('config.settings')} WHERE key = 'discount_pct'")
    discount = st.number_input("Negotiated discount vs list price (%)", 0.0, 90.0,
                               float(disc["v"].iloc[0]) if not disc.empty else 0.0, step=1.0)
    if st.button("Save rules", type="primary"):
        for r in edited.itertuples():
            execute(f"""UPDATE {t('config.rules')} SET threshold = :th, severity = :s, enabled = :e,
                        recommendation = :rec WHERE rule_id = :id""",
                    {"th": float(r.threshold), "s": r.severity, "e": bool(r.enabled),
                     "rec": r.recommendation, "id": r.rule_id})
        execute(f"UPDATE {t('config.settings')} SET value = :v WHERE key = 'discount_pct'", {"v": str(discount)})
        q.clear()
        st.success("Saved. Findings and discounted cost update on the next collector run.")
