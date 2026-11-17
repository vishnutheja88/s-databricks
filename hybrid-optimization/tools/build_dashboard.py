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
    "ds_kpi": ("Executive KPIs (30 days)", """
SELECT
  ROUND(SUM(cost_30d_usd), 0)                                   AS job_spend_30d,
  ROUND(SUM(est_monthly_savings_usd), 0)                        AS est_monthly_savings,
  ROUND(SUM(est_annual_savings_usd), 0)                         AS est_annual_savings,
  ROUND(SUM(failed_run_cost_usd), 0)                            AS failed_run_spend_30d,
  COUNT_IF(category <> 'OK')                                    AS jobs_needing_action,
  COUNT(*)                                                      AS jobs_total
FROM ops.v_job_scorecard_30d"""),
    "ds_scorecard": ("Job scorecard (30 days)", """
SELECT priority_rank, job_name, job_id, category, top_recommendation, fix_effort,
       cost_30d_usd, est_monthly_savings_usd, est_annual_savings_usd,
       runs, failed_runs, failed_run_cost_usd, avg_minutes, clusters_per_run,
       avg_cpu, p95_cpu, avg_mem, max_swap,
       oom_events, spill_events, lost_executor_events, error_lines, photon
FROM ops.v_job_scorecard_30d"""),
    "ds_cost_trend": ("Daily job spend (90 days)", """
SELECT c.usage_date, COALESCE(s.job_name, c.job_id) AS job_name, s.category, c.list_cost_usd
FROM ops.v_job_cost_daily c
LEFT JOIN ops.v_job_scorecard_30d s USING (job_id)
WHERE c.usage_date >= current_date() - INTERVAL 90 DAYS"""),
    "ds_runtime_trend": ("Daily runs and runtime (90 days)", """
SELECT h.run_date, COALESCE(s.job_name, h.job_id) AS job_name, h.runs, h.failed_runs, h.avg_minutes
FROM ops.v_job_run_health_daily h
LEFT JOIN ops.v_job_scorecard_30d s USING (job_id)
WHERE h.run_date >= current_date() - INTERVAL 90 DAYS"""),
    "ds_signals_trend": ("Daily log signals (30 days)", """
SELECT g.log_date, COALESCE(s.job_name, g.job_id) AS job_name,
       g.oom_events, g.spill_events, g.lost_executor_events, g.error_lines
FROM ops.v_job_log_signals_daily g
LEFT JOIN ops.v_job_scorecard_30d s USING (job_id)
WHERE g.log_date >= current_date() - INTERVAL 30 DAYS"""),
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


ACTION_COLS = ["priority_rank", "job_name", "top_recommendation", "fix_effort",
               "est_monthly_savings_usd", "est_annual_savings_usd", "cost_30d_usd"]

PAGES = [
    ("executive", "Executive summary", [
        counter("kpi_spend", "ds_kpi", "job_spend_30d", "Job spend (30d, list)", USD, x=0),
        counter("kpi_save_m", "ds_kpi", "est_monthly_savings", "Est. savings / month", USD, x=1),
        counter("kpi_save_y", "ds_kpi", "est_annual_savings", "Est. savings / year", USD, x=2),
        counter("kpi_failed", "ds_kpi", "failed_run_spend_30d", "Spend on failed runs (30d)", USD, x=3),
        counter("kpi_action", "ds_kpi", "jobs_needing_action", "Jobs needing action", x=4),
        counter("kpi_total", "ds_kpi", "jobs_total", "Jobs analyzed", x=5),
        bar("save_by_cat", "ds_scorecard", "category", "`category`", "savings", "SUM(`est_monthly_savings_usd`)",
            "Est. monthly savings by fix type", 0, 2, 3, 6),
        bar("cost_top_jobs", "ds_scorecard", "job_name", "`job_name`", "cost", "SUM(`cost_30d_usd`)",
            "Top jobs by 30-day spend (color = fix type)", 3, 2, 3, 6, color="category"),
        table("top_actions", "ds_scorecard", ACTION_COLS, "Prioritized action list (rank 1 = do first)", 0, 8, 6, 8),
    ]),
    ("utilization", "Cluster utilization", [
        filt("flt_cat_util", "ds_scorecard", "category", "Fix type", 0, 0),
        bar("p95_by_job", "ds_scorecard", "job_name", "`job_name`", "p95_cpu", "AVG(`p95_cpu`)",
            "p95 worker CPU % by job (below 40 = oversized)", 0, 1, 3, 6),
        bar("mem_by_job", "ds_scorecard", "job_name", "`job_name`", "avg_mem", "AVG(`avg_mem`)",
            "Avg worker memory % by job", 3, 1, 3, 6),
        table("util_table", "ds_scorecard",
              ["job_name", "category", "cost_30d_usd", "avg_cpu", "p95_cpu", "avg_mem", "max_swap",
               "clusters_per_run", "photon", "est_monthly_savings_usd"],
              "Utilization detail", 0, 7, 6, 8),
    ]),
    ("reliability", "Reliability & root cause", [
        bar("failed_cost_by_job", "ds_scorecard", "job_name", "`job_name`", "failed_cost", "SUM(`failed_run_cost_usd`)",
            "Spend on failed runs by job (30d)", 0, 0, 3, 6),
        line("oom_trend", "ds_signals_trend", "day", "DATE_TRUNC(\"DAY\", `log_date`)", "oom", "SUM(`oom_events`)",
             "Out-of-memory events per day", 3, 0, 3, 6),
        table("rc_table", "ds_scorecard",
              ["job_name", "category", "failed_runs", "failed_run_cost_usd", "oom_events", "spill_events",
               "lost_executor_events", "error_lines", "max_swap", "top_recommendation"],
              "Jobs with failures or log signals (open Watchtower Logs Dashboard for the lines)", 0, 6, 6, 8),
    ]),
    ("trends", "Trends", [
        filt("flt_job_cost", "ds_cost_trend", "job_name", "Job", 0, 0, 3),
        filt("flt_job_rt", "ds_runtime_trend", "job_name", "Job", 3, 0, 3),
        line("cost_trend", "ds_cost_trend", "day", "DATE_TRUNC(\"DAY\", `usage_date`)", "cost", "SUM(`list_cost_usd`)",
             "Daily job spend by fix type (90d)", 0, 1, 6, 6, color="category"),
        line("runtime_trend", "ds_runtime_trend", "day", "DATE_TRUNC(\"DAY\", `run_date`)", "avg_minutes", "AVG(`avg_minutes`)",
             "Average run minutes (90d)", 0, 7, 3, 6),
        line("fail_trend", "ds_runtime_trend", "day", "DATE_TRUNC(\"DAY\", `run_date`)", "failed_runs", "SUM(`failed_runs`)",
             "Failed runs per day (90d)", 3, 7, 3, 6),
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
