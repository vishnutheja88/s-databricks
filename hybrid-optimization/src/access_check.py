"""Record pilot scope and confirm access to every source (acceptance criteria 1 and 2).

Appends to (never updates):
  ops.scope_history  one row per workspace when scope or review period changes
  ops.access_checks  one row per source per run (ok / failed + reason)

Fails the task after recording if a REQUIRED source is unreadable, so the
run stops with a clear reason instead of failing later in a view.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

# (source group, object, required)
SOURCES = [
    ("system tables", "system.billing.usage", True),
    ("system tables", "system.billing.list_prices", True),
    ("system tables", "system.compute.node_timeline", True),
    ("system tables", "system.compute.clusters", False),
    ("system tables", "system.lakeflow.jobs", True),
    ("system tables", "system.lakeflow.job_run_timeline", True),
    ("system tables", "system.lakeflow.job_task_run_timeline", True),
    ("lineage", "system.access.table_lineage", False),
    ("query history", "system.query.history", False),
    ("job logs", "{catalog}.silver.logs", True),
]


def main(argv=None):
    p = common.parser("Access check and scope record")
    p.add_argument("--eventlog_path", required=True, help="cluster log root, e.g. /Volumes/watchtower/default/cluster_logs/cluster-logs")
    a = p.parse_args(argv)
    subs = common.substitutions(a)
    ws = common.parse_workspaces(a.workspaces)
    cat = a.catalog

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    me = spark.sql("SELECT current_user() AS u").first().u

    # Scope evidence: one row per workspace when anything about the scope changes.
    # Resolve the review dates first: VALUES lists only accept literals.
    period = spark.sql(f"SELECT {subs['review_start_expr']} AS s, {subs['review_end_expr']} AS e").first()
    rows = ", ".join(
        f"('{wid}', '{name}', '{subs['review_label']}', DATE'{period.s.isoformat()}', DATE'{period.e.isoformat()}', "
        f"CAST({subs['price_multiplier']} AS DOUBLE))" for wid, name in ws)
    spark.sql(f"""
        INSERT INTO {cat}.ops.scope_history
        SELECT current_timestamp(), v.workspace_id, v.workspace_name, v.review_label, v.review_start, v.review_end,
               v.price_multiplier, '{me}'
        FROM VALUES {rows} AS v(workspace_id, workspace_name, review_label, review_start, review_end, price_multiplier)
        WHERE NOT EXISTS (
          SELECT 1 FROM {cat}.ops.scope_history h
          WHERE h.workspace_id = v.workspace_id AND h.workspace_name = v.workspace_name
            AND h.review_label = v.review_label AND h.price_multiplier = v.price_multiplier
            AND (h.review_label = 'rolling 30 days' OR (h.review_start = v.review_start AND h.review_end = v.review_end)))
    """)

    results = []
    for group, obj, required in SOURCES:
        name = obj.format(catalog=cat)
        try:
            spark.sql(f"SELECT 1 FROM {name} LIMIT 1").collect()
            results.append((group, name, required, True, "readable"))
        except Exception as e:  # noqa: BLE001
            results.append((group, name, required, False, str(e).splitlines()[0][:400]))

    # Spark logs: the cluster log root must be listable; event logs are optional per cluster.
    try:
        listing = spark.sql(f"LIST '{a.eventlog_path}' LIMIT 5").collect()
        results.append(("spark logs", a.eventlog_path, True, True, f"listable, {len(listing)}+ entries"))
    except Exception as e:  # noqa: BLE001
        results.append(("spark logs", a.eventlog_path, True, False, str(e).splitlines()[0][:400]))

    df = spark.createDataFrame(results, "source STRING, object_name STRING, required BOOLEAN, ok BOOLEAN, detail STRING")
    df.createOrReplaceTempView("checks")
    spark.sql(f"""
        INSERT INTO {cat}.ops.access_checks
        SELECT current_timestamp(), source, object_name, required, ok, detail, '{me}' FROM checks
    """)

    for r in results:
        print(f"{'OK  ' if r[3] else 'FAIL'} {'required' if r[2] else 'optional'} {r[0]:<14} {r[1]}: {r[4]}")
    missing = [r[1] for r in results if r[2] and not r[3]]
    if missing:
        raise RuntimeError(f"required sources not readable (recorded in ops.access_checks): {missing}")


if __name__ == "__main__":
    main()
