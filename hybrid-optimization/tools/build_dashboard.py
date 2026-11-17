"""Generate src/dashboards/job_optimization.lvdash.json (AI/BI dashboard).

Edit the datasets / pages here and re-run:
    python3 tools/build_dashboard.py

Queries use schema.table (ops.*); resources/dashboard.yml sets dataset_catalog
per target, so the same file works in dev and prod.
"""
import json
import os

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "dashboards", "job_optimization.lvdash.json")

USD = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact",
       "decimalPlaces": {"type": "max", "places": 0}}

DATASETS = {
    "ds_kpi": ("Executive KPIs (review period)", """
SELECT
  ROUND(SUM(period_cost_usd), 0)                                AS period_spend,
  ROUND(SUM(est_monthly_savings_usd), 0)                        AS est_monthly_savings,
  ROUND(SUM(est_annual_savings_usd), 0)                         AS est_annual_savings,
  ROUND(SUM(failed_run_cost_usd), 0)                            AS failed_run_spend,
  COUNT_IF(category <> 'OK')                                    AS jobs_needing_action,
  COUNT_IF(size(finding_codes) > 0)                             AS jobs_with_findings,
  COUNT(*)                                                      AS jobs_total,
  COUNT(DISTINCT workspace_id)                                  AS workspaces
FROM ops.v_job_scorecard_latest"""),
    "ds_scorecard": ("Job scorecard (review period)", """
SELECT priority_rank, workspace_rank, workspace_name, job_name, job_id, owner, category, top_recommendation,
       fix_effort, risk_level, risk, secondary_findings, size(finding_codes) AS findings,
       period_cost_usd, monthly_cost_usd, est_monthly_savings_usd, est_annual_savings_usd, dbus,
       runs, failed_runs, failed_run_cost_usd, retries, runs_with_retries,
       avg_minutes, max_minutes, runtime_change_pct, cost_change_pct, p95_cpu_change,
       clusters_per_run, avg_cpu, p95_cpu, avg_mem, max_swap, node_hours, idle_node_hours, idle_pct,
       oom_events, spill_events, lost_executor_events, error_lines,
       shuffle_read_gb, shuffle_write_gb, disk_spill_gb, max_skew_ratio, slow_queries, spilled_queries,
       dbr_version, runtime_outdated, photon, policy_missing, small_file_tables, unmaintained_tables,
       layout_tables, schedule_peak, review_label
FROM ops.v_job_scorecard_latest"""),
    "ds_workspace": ("Workspace summary", """
SELECT workspace_name, review_label, jobs_analyzed, jobs_needing_action, jobs_with_findings,
       period_cost_usd, est_monthly_savings_usd, failed_run_cost_usd,
       top_opportunity_job, top_opportunity, top_opportunity_monthly_savings_usd
FROM ops.v_workspace_summary_latest"""),
    "ds_idle": ("Cluster idle time (review period)", """
SELECT workspace_name, cluster_name, cluster_id, cluster_source, job_id,
       node_hours, idle_node_hours, idle_pct, cost_usd, est_idle_cost_usd
FROM ops.v_cluster_idle"""),
    "ds_hotspots": ("Run starts by hour (UTC)", """
SELECT workspace_name, start_hour_utc, runs_started, jobs, avg_runs_per_day, busy_rank
FROM ops.v_schedule_hotspots"""),
    "ds_queries": ("Slowest SQL queries (review period)", """
SELECT workspace_id, source_type, job_id, warehouse_id, start_time, duration_min, read_gb,
       spilled_gb, shuffle_read_gb, statement_preview
FROM ops.v_query_hotspots"""),
    "ds_cost_trend": ("Daily job spend (90 days)", """
SELECT c.usage_date, s.workspace_name, COALESCE(s.job_name, c.job_id) AS job_name, s.category, c.cost_usd
FROM ops.v_job_cost_daily c
LEFT JOIN ops.v_job_scorecard_latest s ON s.workspace_id = c.workspace_id AND s.job_id = c.job_id
WHERE c.usage_date >= current_date() - INTERVAL 90 DAYS"""),
    "ds_runtime_trend": ("Daily runs and runtime (90 days)", """
SELECT h.run_date, s.workspace_name, COALESCE(s.job_name, h.job_id) AS job_name,
       h.runs, h.failed_runs, h.avg_minutes, h.max_minutes
FROM ops.v_job_run_health_daily h
LEFT JOIN ops.v_job_scorecard_latest s ON s.workspace_id = h.workspace_id AND s.job_id = h.job_id
WHERE h.run_date >= current_date() - INTERVAL 90 DAYS"""),
    "ds_signals_trend": ("Daily log signals (90 days)", """
SELECT g.log_date, s.workspace_name, COALESCE(s.job_name, g.job_id) AS job_name,
       g.oom_events, g.spill_events, g.lost_executor_events, g.error_lines
FROM ops.v_job_log_signals_daily g
LEFT JOIN ops.v_job_scorecard_latest s ON s.workspace_id = g.workspace_id AND s.job_id = g.job_id
WHERE g.log_date >= current_date() - INTERVAL 90 DAYS"""),
    "ds_scope": ("Pilot scope", """
SELECT s.workspace_id, s.workspace_name, s.review_label, s.review_start, s.review_end, s.review_days,
       h.recorded_at AS scope_recorded_at, h.recorded_by
FROM ops.v_scope s
LEFT JOIN (SELECT * FROM ops.scope_history
           QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id ORDER BY recorded_at DESC) = 1) h
  ON h.workspace_id = s.workspace_id"""),
    "ds_access": ("Access checks (latest)", """
SELECT checked_at, source, object_name, required, ok, detail, checked_by
FROM ops.access_checks
QUALIFY ROW_NUMBER() OVER (PARTITION BY object_name ORDER BY checked_at DESC) = 1"""),
    "ds_validation": ("Validation results (latest)", """
SELECT validated_at, kind, workspace_id, check_name, source_value, ops_value, passed, detail, validated_by
FROM ops.validation_results
QUALIFY ROW_NUMBER() OVER (PARTITION BY kind, workspace_id, check_name ORDER BY validated_at DESC) = 1"""),
}

def dataset(name, display, sql):
    lines = sql.strip("\n").split("\n")
    return {"name": name, "displayName": display, "queryLines": [l + "\n" for l in lines[:-1]] + [lines[-1]]}


def fields(cols):
    return [{"name": n, "expression": e} for n, e in cols]


def counter(name, ds, col, title, fmt=None, x=0, y=0, w=1, h=2):
    enc = {"fieldName": col, "displayName": title}
    if fmt:
        enc["format"] = fmt
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields([(col, f"`{col}`")]), "disaggregated": True}}],
                       "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": enc},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def bar(name, ds, xcol, xexpr, ycol, yexpr, title, x, y, w, h, color=None):
    cols = [(xcol, xexpr), (ycol, yexpr)] + ([(color, f"`{color}`")] if color else [])
    enc = {"x": {"fieldName": xcol, "scale": {"type": "categorical", "sort": {"by": "y-reversed"}}, "displayName": xcol},
           "y": {"fieldName": ycol, "scale": {"type": "quantitative"}, "displayName": ycol}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields(cols), "disaggregated": False}}],
                       "spec": {"version": 3, "widgetType": "bar", "encodings": enc,
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def line(name, ds, xcol, xexpr, ycol, yexpr, title, x, y, w, h, color=None):
    cols = [(xcol, xexpr), (ycol, yexpr)] + ([(color, f"`{color}`")] if color else [])
    enc = {"x": {"fieldName": xcol, "scale": {"type": "temporal"}, "displayName": "date"},
           "y": {"fieldName": ycol, "scale": {"type": "quantitative"}, "displayName": ycol}}
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields(cols), "disaggregated": False}}],
                       "spec": {"version": 3, "widgetType": "line", "encodings": enc,
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def table(name, ds, cols, title, x, y, w, h):
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields([(c, f"`{c}`") for c in cols]), "disaggregated": True}}],
                       "spec": {"version": 2, "widgetType": "table",
                                "encodings": {"columns": [{"fieldName": c, "displayName": c} for c in cols]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def filt(name, ds, col, title, x, y, w=6, h=1):
    q = f"f_{ds}_{col}"
    return {"widget": {"name": name,
                       "queries": [{"name": q, "query": {"datasetName": ds, "fields": fields([(col, f"`{col}`")]), "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "filter-multi-select",
                                "encodings": {"fields": [{"fieldName": col, "displayName": title, "queryName": q}]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


ACTION_COLS = ["priority_rank", "workspace_name", "job_name", "owner", "top_recommendation", "fix_effort",
               "risk_level", "est_monthly_savings_usd", "est_annual_savings_usd", "period_cost_usd"]

PAGES = [
    ("executive", "Executive summary", [
        counter("kpi_spend", "ds_kpi", "period_spend", "Job spend (review period, contract)", USD, x=0),
        counter("kpi_save_m", "ds_kpi", "est_monthly_savings", "Est. savings / month", USD, x=1),
        counter("kpi_save_y", "ds_kpi", "est_annual_savings", "Est. savings / year", USD, x=2),
        counter("kpi_failed", "ds_kpi", "failed_run_spend", "Spend on failed runs", USD, x=3),
        counter("kpi_action", "ds_kpi", "jobs_needing_action", "Jobs needing action", x=4),
        counter("kpi_total", "ds_kpi", "jobs_total", "Jobs analyzed", x=5),
        bar("save_by_ws", "ds_scorecard", "workspace_name", "`workspace_name`", "savings", "SUM(`est_monthly_savings_usd`)",
            "Est. monthly savings by workspace", 0, 2, 3, 6),
        bar("save_by_cat", "ds_scorecard", "category", "`category`", "savings", "SUM(`est_monthly_savings_usd`)",
            "Est. monthly savings by fix type", 3, 2, 3, 6),
        table("top_actions", "ds_scorecard", ACTION_COLS, "Prioritized action plan (rank 1 = do first)", 0, 8, 6, 8),
    ]),
    ("workspaces", "Workspaces", [
        table("ws_table", "ds_workspace",
              ["workspace_name", "review_label", "jobs_analyzed", "jobs_needing_action", "jobs_with_findings",
               "period_cost_usd", "est_monthly_savings_usd", "failed_run_cost_usd",
               "top_opportunity_job", "top_opportunity", "top_opportunity_monthly_savings_usd"],
              "Top opportunity per workspace", 0, 0, 6, 6),
        bar("spend_by_ws", "ds_workspace", "workspace_name", "`workspace_name`", "spend", "SUM(`period_cost_usd`)",
            "Spend by workspace (review period)", 0, 6, 3, 6),
        bar("cost_top_jobs", "ds_scorecard", "job_name", "`job_name`", "cost", "SUM(`period_cost_usd`)",
            "Top jobs by spend (color = workspace)", 3, 6, 3, 6, color="workspace_name"),
    ]),
    ("utilization", "Cost & utilization", [
        filt("flt_ws_util", "ds_scorecard", "workspace_name", "Workspace", 0, 0),
        bar("p95_by_job", "ds_scorecard", "job_name", "`job_name`", "p95_cpu", "AVG(`p95_cpu`)",
            "p95 worker CPU % by job (below 40 = oversized)", 0, 1, 3, 6),
        bar("idle_by_cluster", "ds_idle", "cluster_name", "`cluster_name`", "idle_hours", "SUM(`idle_node_hours`)",
            "Idle node-hours by cluster", 3, 1, 3, 6),
        table("util_table", "ds_scorecard",
              ["workspace_name", "job_name", "category", "period_cost_usd", "dbus", "avg_cpu", "p95_cpu", "avg_mem",
               "max_swap", "idle_pct", "clusters_per_run", "cost_change_pct", "p95_cpu_change",
               "est_monthly_savings_usd"],
              "Utilization detail", 0, 7, 6, 7),
        table("idle_table", "ds_idle",
              ["workspace_name", "cluster_name", "cluster_source", "job_id", "node_hours", "idle_node_hours",
               "idle_pct", "cost_usd", "est_idle_cost_usd"],
              "Clusters with the most idle time", 0, 14, 6, 6),
    ]),
    ("performance", "Job performance", [
        filt("flt_ws_perf", "ds_scorecard", "workspace_name", "Workspace", 0, 0),
        bar("runtime_by_job", "ds_scorecard", "job_name", "`job_name`", "avg_minutes", "AVG(`avg_minutes`)",
            "Average run minutes by job", 0, 1, 3, 6),
        bar("shuffle_by_job", "ds_scorecard", "job_name", "`job_name`", "shuffle_gb", "SUM(`shuffle_read_gb`)",
            "Shuffle read GB by job (Spark event logs)", 3, 1, 3, 6),
        table("perf_table", "ds_scorecard",
              ["workspace_name", "job_name", "avg_minutes", "max_minutes", "runtime_change_pct", "shuffle_read_gb",
               "shuffle_write_gb", "disk_spill_gb", "max_skew_ratio", "spill_events", "slow_queries",
               "spilled_queries", "top_recommendation"],
              "Slow jobs and Spark bottlenecks", 0, 7, 6, 7),
        bar("hotspots", "ds_hotspots", "start_hour_utc", "`start_hour_utc`", "runs", "SUM(`runs_started`)",
            "Run starts by hour of day (UTC)", 0, 14, 3, 6, color="workspace_name"),
        table("query_table", "ds_queries",
              ["workspace_id", "source_type", "job_id", "duration_min", "read_gb", "spilled_gb", "shuffle_read_gb",
               "statement_preview"],
              "Slowest SQL queries (query history)", 3, 14, 3, 6),
    ]),
    ("reliability", "Reliability & root cause", [
        filt("flt_ws_rel", "ds_scorecard", "workspace_name", "Workspace", 0, 0),
        bar("failed_cost_by_job", "ds_scorecard", "job_name", "`job_name`", "failed_cost", "SUM(`failed_run_cost_usd`)",
            "Spend on failed runs by job", 0, 1, 3, 6),
        bar("retries_by_job", "ds_scorecard", "job_name", "`job_name`", "retries", "SUM(`retries`)",
            "Task retries by job", 3, 1, 3, 6),
        line("oom_trend", "ds_signals_trend", "day", "DATE_TRUNC(\"DAY\", `log_date`)", "oom", "SUM(`oom_events`)",
             "Out-of-memory events per day", 0, 7, 6, 5),
        table("rc_table", "ds_scorecard",
              ["workspace_name", "job_name", "category", "failed_runs", "failed_run_cost_usd", "retries",
               "oom_events", "spill_events", "lost_executor_events", "error_lines", "max_swap", "top_recommendation"],
              "Jobs with failures, retries or log signals (Watchtower Logs Dashboard has the lines)", 0, 12, 6, 7),
    ]),
    ("findings", "Recommendations", [
        filt("flt_ws_find", "ds_scorecard", "workspace_name", "Workspace", 0, 0),
        table("findings_table", "ds_scorecard",
              ["workspace_rank", "workspace_name", "job_name", "owner", "top_recommendation", "secondary_findings",
               "fix_effort", "risk_level", "risk", "est_monthly_savings_usd"],
              "Top action and secondary findings per job", 0, 1, 6, 8),
        table("config_table", "ds_scorecard",
              ["workspace_name", "job_name", "dbr_version", "runtime_outdated", "photon", "policy_missing",
               "schedule_peak", "small_file_tables", "unmaintained_tables", "layout_tables"],
              "Runtime, Photon, policy, schedule and data-layout signals", 0, 9, 6, 7),
    ]),
    ("trends", "Trends", [
        filt("flt_job_cost", "ds_cost_trend", "job_name", "Job", 0, 0, 3),
        filt("flt_job_rt", "ds_runtime_trend", "job_name", "Job", 3, 0, 3),
        line("cost_trend", "ds_cost_trend", "day", "DATE_TRUNC(\"DAY\", `usage_date`)", "cost", "SUM(`cost_usd`)",
             "Daily job spend by workspace (90d)", 0, 1, 6, 6, color="workspace_name"),
        line("runtime_trend", "ds_runtime_trend", "day", "DATE_TRUNC(\"DAY\", `run_date`)", "avg_minutes", "AVG(`avg_minutes`)",
             "Average run minutes (90d)", 0, 7, 3, 6),
        line("fail_trend", "ds_runtime_trend", "day", "DATE_TRUNC(\"DAY\", `run_date`)", "failed_runs", "SUM(`failed_runs`)",
             "Failed runs per day (90d)", 3, 7, 3, 6),
    ]),
    ("scope", "Scope & validation", [
        table("scope_table", "ds_scope",
              ["workspace_id", "workspace_name", "review_label", "review_start", "review_end", "review_days",
               "scope_recorded_at", "recorded_by"],
              "Pilot scope (workspaces and review period)", 0, 0, 6, 4),
        table("access_table", "ds_access",
              ["source", "object_name", "required", "ok", "detail", "checked_at"],
              "Access to system tables, Spark logs and job logs (latest check)", 0, 4, 6, 6),
        table("validation_table", "ds_validation",
              ["kind", "workspace_id", "check_name", "source_value", "ops_value", "passed", "detail", "validated_at",
               "validated_by"],
              "Validation: ops results vs raw sources, Genie benchmarks, profiling-tool checks", 0, 10, 6, 8),
    ]),
]

def main():
    doc = {
        "datasets": [dataset(n, d, q) for n, (d, q) in DATASETS.items()],
        "pages": [{"name": n, "displayName": d, "layout": layout} for n, d, layout in PAGES],
        "uiSettings": {"theme": {"widgetHeaderAlignment": "LEFT"}},
    }
    names = [w["widget"]["name"] for p in doc["pages"] for w in p["layout"]]
    assert len(names) == len(set(names)), "widget names must be unique"
    ds = set(DATASETS)
    for p in doc["pages"]:
        for w in p["layout"]:
            for q in w["widget"]["queries"]:
                assert q["query"]["datasetName"] in ds
    with open(OUT, "w") as f:
        json.dump(doc, f, indent=2)
        f.write("\n")
    print(f"wrote {os.path.normpath(OUT)}: {len(doc['datasets'])} datasets, {len(names)} widgets")


if __name__ == "__main__":
    main()
