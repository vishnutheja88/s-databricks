"""Validate the ops layer (what Genie and the dashboard read) against raw sources.

Each check answers the same question twice for every workspace in scope:
  source_value: straight from system tables / logs, written independently of the ops views
  ops_value:    from ops.v_job_scorecard_latest (the snapshot Genie and the dashboard use)
and appends pass/fail to ops.validation_results (kind = 'auto'). It never fails
the job: a failed check is evidence to investigate, shown on the dashboard.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402


def checks(cat, s):
    """(name, source_sql, ops_sql, compare) with {ws} as the workspace id placeholder."""
    win_usage = f"u.usage_date BETWEEN {s['review_start_expr']} AND {s['review_end_expr']}"
    win_ts = lambda col: f"DATE({col}) BETWEEN {s['review_start_expr']} AND {s['review_end_expr']}"  # noqa: E731
    sc = f"{cat}.ops.v_job_scorecard_latest"
    price = f"""JOIN system.billing.list_prices p
      ON u.sku_name = p.sku_name AND u.cloud = p.cloud AND u.usage_unit = p.usage_unit
     AND u.usage_start_time >= p.price_start_time
     AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)"""
    runs = f"""SELECT job_id, run_id, MAX_BY(result_state, period_end_time) AS state
      FROM system.lakeflow.job_run_timeline
      WHERE workspace_id = '{{ws}}' AND {win_ts('period_start_time')}
      GROUP BY job_id, run_id"""
    return [
        ("Total job DBUs in review period",
         f"SELECT ROUND(SUM(usage_quantity), 1) FROM system.billing.usage u WHERE u.workspace_id = '{{ws}}' "
         f"AND u.usage_metadata.job_id IS NOT NULL AND {win_usage}",
         f"SELECT ROUND(SUM(dbus), 1) FROM {sc} WHERE workspace_id = '{{ws}}'", ("pct", 0.5)),
        ("Total job cost (contract) in review period",
         f"SELECT ROUND(SUM(u.usage_quantity * p.pricing.effective_list.default * {s['price_multiplier']}), 2) "
         f"FROM system.billing.usage u {price} WHERE u.workspace_id = '{{ws}}' "
         f"AND u.usage_metadata.job_id IS NOT NULL AND {win_usage}",
         f"SELECT ROUND(SUM(period_cost_usd), 2) FROM {sc} WHERE workspace_id = '{{ws}}'", ("pct", 0.5)),
        ("Top 10 jobs by DBUs (overlap of 10)",
         f"SELECT CONCAT_WS(',', COLLECT_LIST(job_id)) FROM (SELECT u.usage_metadata.job_id AS job_id, "
         f"SUM(usage_quantity) AS d FROM system.billing.usage u WHERE u.workspace_id = '{{ws}}' "
         f"AND u.usage_metadata.job_id IS NOT NULL AND {win_usage} GROUP BY 1 ORDER BY d DESC LIMIT 10)",
         f"SELECT CONCAT_WS(',', COLLECT_LIST(job_id)) FROM (SELECT job_id FROM {sc} "
         f"WHERE workspace_id = '{{ws}}' ORDER BY dbus DESC LIMIT 10)", ("overlap", 8)),
        ("Job runs in review period",
         f"SELECT COUNT(*) FROM ({runs})",
         f"SELECT SUM(runs) FROM {sc} WHERE workspace_id = '{{ws}}'", ("pct", 2.0)),
        ("Failed runs in review period",
         f"SELECT COUNT_IF(state IN ('FAILED', 'TIMED_OUT', 'ERROR')) FROM ({runs})",
         f"SELECT SUM(failed_runs) FROM {sc} WHERE workspace_id = '{{ws}}'", ("pct", 2.0)),
        ("Task retries in review period",
         f"SELECT COALESCE(SUM(a - 1), 0) FROM (SELECT job_run_id, task_key, COUNT(DISTINCT run_id) AS a "
         f"FROM system.lakeflow.job_task_run_timeline WHERE workspace_id = '{{ws}}' "
         f"AND {win_ts('period_start_time')} GROUP BY ALL)",
         f"SELECT SUM(retries) FROM {sc} WHERE workspace_id = '{{ws}}'", ("pct", 5.0)),
        ("Out-of-memory log lines in review period",
         f"SELECT COUNT(*) FROM {cat}.silver.logs l JOIN (SELECT DISTINCT cid FROM system.lakeflow.job_task_run_timeline "
         f"LATERAL VIEW explode(compute_ids) t AS cid WHERE workspace_id = '{{ws}}' "
         f"AND period_start_time >= current_date() - INTERVAL 120 DAYS) c ON l.cluster_id = c.cid "
         f"WHERE {win_ts('COALESCE(l.log_ts, l.file_modification_time)')} "
         f"AND l.log_msg RLIKE '(?i)OutOfMemoryError|GC overhead limit|exit code 137|Container killed.*memory'",
         f"SELECT SUM(oom_events) FROM {sc} WHERE workspace_id = '{{ws}}'", ("pct", 5.0)),
        ("Avg worker CPU % of the most expensive job",
         f"SELECT ROUND(AVG(n.cpu_user_percent + n.cpu_system_percent), 1) FROM system.compute.node_timeline n "
         f"JOIN (SELECT DISTINCT cid FROM system.lakeflow.job_task_run_timeline LATERAL VIEW explode(compute_ids) t AS cid "
         f"WHERE workspace_id = '{{ws}}' AND job_id = (SELECT job_id FROM {sc} WHERE workspace_id = '{{ws}}' "
         f"ORDER BY period_cost_usd DESC LIMIT 1)) c ON n.cluster_id = c.cid "
         f"WHERE n.workspace_id = '{{ws}}' AND n.driver = false AND {win_ts('n.start_time')}",
         f"SELECT avg_cpu FROM {sc} WHERE workspace_id = '{{ws}}' ORDER BY period_cost_usd DESC LIMIT 1",
         ("abs", 5.0)),
    ]


def compare(src, ops, rule):
    kind, tol = rule
    if src is None and ops is None:
        return True, "both empty"
    if kind == "overlap":
        a = set((src or "").split(",")) - {""}
        b = set((ops or "").split(",")) - {""}
        n = len(a & b)
        return n >= min(tol, len(a)), f"{n} of {len(a)} match"
    try:
        s, o = float(src or 0), float(ops or 0)
    except (TypeError, ValueError):
        return False, "non-numeric"
    if kind == "abs":
        return abs(s - o) <= tol, f"difference {abs(s - o):.1f} points (tolerance {tol})"
    base = max(abs(s), 1e-9)
    diff = 100 * abs(s - o) / base if s else (0.0 if o == 0 else 100.0)
    return diff <= tol, f"difference {diff:.2f}% (tolerance {tol}%)"


def main(argv=None):
    a = common.parser("Validate ops layer against raw sources").parse_args(argv)
    s = common.substitutions(a)
    cat = a.catalog

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    me = spark.sql("SELECT current_user() AS u").first().u
    rows = []
    for wid, name in common.parse_workspaces(a.workspaces):
        for check, src_sql, ops_sql, rule in checks(cat, s):
            try:
                src = spark.sql(src_sql.replace("{ws}", wid)).first()[0]
                ops = spark.sql(ops_sql.replace("{ws}", wid)).first()[0]
                ok, detail = compare(src, ops, rule)
            except Exception as e:  # noqa: BLE001
                src, ops, ok, detail = None, None, False, "error: " + str(e).splitlines()[0][:300]
            rows.append((wid, check, None if src is None else str(src), None if ops is None else str(ops), ok, detail))
            print(f"{'PASS' if ok else 'FAIL'} [{name}] {check}: source={src} ops={ops} ({detail})")

    df = spark.createDataFrame(rows, "workspace_id STRING, check_name STRING, source_value STRING, "
                                     "ops_value STRING, passed BOOLEAN, detail STRING")
    df.createOrReplaceTempView("vr")
    spark.sql(f"""
        INSERT INTO {cat}.ops.validation_results
        SELECT current_timestamp(), 'auto', workspace_id, check_name, source_value, ops_value, passed,
               detail, '{me}' FROM vr""")
    print(f"{sum(r[4] for r in rows)} of {len(rows)} checks passed")


if __name__ == "__main__":
    main()
