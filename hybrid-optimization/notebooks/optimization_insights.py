# Databricks notebook source
# MAGIC %md
# MAGIC # Job Optimization Insights & Fix Plan
# MAGIC
# MAGIC Run top to bottom (any SQL-capable compute or serverless). It answers four questions:
# MAGIC
# MAGIC 1. **How much are prod jobs costing, and how much can we save?** (executive summary)
# MAGIC 2. **What should we fix first?** (prioritized action list, ranked by estimated saving)
# MAGIC 3. **Exactly what do we change?** (fix plan: current cluster config -> proposed config, per job)
# MAGIC 4. **Why is it failing?** (root-cause log lines from Watchtower) and **did the fix work?** (before/after)
# MAGIC
# MAGIC **Read-only.** It changes no job, cluster or table. The only write is optional: saving the fix plan to
# MAGIC the append-only table `ops.fix_plan_history` (widget `save_plan = yes`), so you can track what was proposed.
# MAGIC
# MAGIC Cost is **contract cost**: usage x effective list price x contract multiplier (1.093), set in the bundle.
# MAGIC Savings are conservative 30-day estimates.

# COMMAND ----------

dbutils.widgets.text("catalog", "watchtower", "Catalog")
dbutils.widgets.text("top_n", "10", "Jobs in fix plan")
dbutils.widgets.dropdown("save_plan", "no", ["no", "yes"], "Save fix plan to ops.fix_plan_history")
dbutils.widgets.text("check_job_id", "", "Before/after: job_id")
dbutils.widgets.text("check_change_date", "", "Before/after: change date (YYYY-MM-DD)")

import math
import re

CATALOG = dbutils.widgets.get("catalog").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"invalid catalog: {CATALOG}")
TOP_N = max(1, int(dbutils.widgets.get("top_n") or 10))
OPS = f"{CATALOG}.ops"

# Read the saved daily scorecard (fast). Before the first daily snapshot exists, fall back to the live view.
SCORECARD = f"{OPS}.v_job_scorecard_latest"
if spark.sql(f"SELECT COUNT(*) AS n FROM {SCORECARD}").first().n == 0:
    SCORECARD = f"{OPS}.v_job_scorecard_30d"
print(f"Scorecard source: {SCORECARD}")

# COMMAND ----------

# MAGIC %md ## 1. Executive summary

# COMMAND ----------

kpi = spark.sql(f"""
SELECT ROUND(SUM(cost_30d_usd))            AS job_spend_30d_usd,
       ROUND(SUM(est_monthly_savings_usd)) AS est_monthly_savings_usd,
       ROUND(SUM(est_annual_savings_usd))  AS est_annual_savings_usd,
       ROUND(SUM(failed_run_cost_usd))     AS failed_run_spend_30d_usd,
       COUNT_IF(category <> 'OK')          AS jobs_needing_action,
       COUNT(*)                            AS jobs_analyzed
FROM {SCORECARD}
""").first()

spend = kpi.job_spend_30d_usd or 0
save = kpi.est_monthly_savings_usd or 0
pct = (100 * save / spend) if spend else 0
print(
    f"Prod jobs cost ${spend:,.0f} in the last 30 days (contract cost).\n"
    f"{kpi.jobs_needing_action} of {kpi.jobs_analyzed} jobs need action. "
    f"Estimated savings: ${save:,.0f}/month ({pct:.0f}% of spend), about ${kpi.est_annual_savings_usd or 0:,.0f}/year.\n"
    f"${kpi.failed_run_spend_30d_usd or 0:,.0f} was spent on runs that failed."
)

display(spark.sql(f"""
SELECT category AS fix_type, fix_effort,
       COUNT(*) AS jobs,
       ROUND(SUM(cost_30d_usd)) AS spend_30d_usd,
       ROUND(SUM(est_monthly_savings_usd)) AS est_monthly_savings_usd
FROM {SCORECARD}
WHERE category <> 'OK'
GROUP BY ALL
ORDER BY est_monthly_savings_usd DESC
"""))

# COMMAND ----------

# MAGIC %md ## 2. Prioritized action list (rank 1 = do first)

# COMMAND ----------

actions = spark.sql(f"""
SELECT priority_rank, job_name, job_id, category, top_recommendation, fix_effort,
       est_monthly_savings_usd, cost_30d_usd, failed_runs, failed_run_cost_usd,
       avg_cpu, p95_cpu, avg_mem, max_swap, clusters_per_run,
       oom_events, spill_events, lost_executor_events
FROM {SCORECARD}
WHERE category <> 'OK'
ORDER BY priority_rank
LIMIT {TOP_N}
""")
display(actions)
action_rows = actions.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fix plan: current config -> proposed change
# MAGIC Reads each job's definition through the Jobs API (read-only) and proposes the concrete change.
# MAGIC Apply it in the job's source of truth (bundle / Terraform / UI), one change per job, then use section 5 to check it.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

FAMILY_SWAP = {"r": {"m": "r", "c": "r", "i": "r", "z": "r"}, "c": {"m": "c", "r": "c", "i": "c", "z": "c"}}


def swap_family(node, target):
    """m5d.xlarge -> r5d.xlarge (target 'r') or c5d.xlarge (target 'c'). Verify availability in your region."""
    m = re.match(r"^([a-z]+?)(\d+)([a-z]*)\.(.+)$", node or "")
    if not m:
        return None
    fam, gen, suffix, size = m.groups()
    new_fam = FAMILY_SWAP[target].get(fam[0])
    if not new_fam:
        return None
    if fam[0] == "i":  # storage-optimized -> keep local disk
        gen, suffix = "5", "d"
    candidate = f"{new_fam}{gen}{suffix}.{size}"
    return candidate if candidate != node else None


def job_clusters(job_id):
    """[(scope, ClusterSpec)] for shared job clusters and per-task clusters."""
    s = w.jobs.get(job_id=int(job_id)).settings
    out = [(f"job_cluster:{jc.job_cluster_key}", jc.new_cluster) for jc in (s.job_clusters or []) if jc.new_cluster]
    out += [(f"task:{t.task_key}", t.new_cluster) for t in (s.tasks or []) if t.new_cluster]
    return s, out


def describe(nc):
    if nc.autoscale:
        workers = f"autoscale {nc.autoscale.min_workers}-{nc.autoscale.max_workers}"
    else:
        workers = f"{nc.num_workers} workers"
    aws = nc.aws_attributes
    avail = f"{aws.availability.value if aws and aws.availability else 'default'}" + (
        f", first_on_demand={aws.first_on_demand}" if aws and aws.first_on_demand is not None else "")
    return f"{nc.node_type_id} x {workers}; driver {nc.driver_node_type_id or nc.node_type_id}; {avail}; DBR {nc.spark_version}"


def propose(row, nc):
    cat = row.category
    if cat == "OVERSIZED":
        cut = (row.est_monthly_savings_usd or 0) / row.cost_30d_usd if row.cost_30d_usd else 0.25
        cut = min(0.5, max(0.25, cut))
        if nc.autoscale and nc.autoscale.max_workers:
            new_max = max(nc.autoscale.min_workers or 1, math.ceil(nc.autoscale.max_workers * (1 - cut)))
            return f"autoscale.max_workers: {nc.autoscale.max_workers} -> {new_max}"
        if nc.num_workers:
            return f"num_workers: {nc.num_workers} -> {max(1, math.ceil(nc.num_workers * (1 - cut)))}"
        return "single node: try one instance size smaller"
    if cat in ("MEMORY", "SPILL"):
        alt = swap_family(nc.node_type_id, "r")
        step = f"node_type_id: {nc.node_type_id} -> {alt}" if alt else "move to a memory-optimized (r-family) node"
        extra = "; check data skew and partition sizes" if cat == "MEMORY" else "; or raise spark.sql.shuffle.partitions"
        return step + extra
    if cat == "CPU_BOUND":
        alt = swap_family(nc.node_type_id, "c")
        return f"node_type_id: {nc.node_type_id} -> {alt}" if alt else "move to a compute-optimized (c-family) node"
    if cat == "LOST_EXECUTORS":
        return "aws_attributes: availability -> SPOT_WITH_FALLBACK, first_on_demand -> 1, zone_id -> auto"
    if cat == "MULTI_CLUSTER":
        return "define one job_clusters entry and point every task at it with job_cluster_key"
    if cat == "FAILING":
        return "fix the failing task first (see section 4); no cluster change"
    return "-"


plan = []
for r in action_rows:
    try:
        settings, clusters = job_clusters(r.job_id)
    except Exception as e:  # noqa: BLE001  (job deleted, no permission, ...)
        plan.append((r.priority_rank, r.job_name, r.job_id, r.category, "-", f"could not read job: {e}", "-",
                     float(r.est_monthly_savings_usd or 0), r.fix_effort))
        continue
    if not clusters:
        plan.append((r.priority_rank, r.job_name, r.job_id, r.category, "-", "no classic job cluster (serverless?)",
                     propose(r, None) if r.category in ("FAILING", "MULTI_CLUSTER") else "-",
                     float(r.est_monthly_savings_usd or 0), r.fix_effort))
        continue
    if r.category == "MULTI_CLUSTER":
        scopes = ", ".join(s for s, _ in clusters)
        plan.append((r.priority_rank, r.job_name, r.job_id, r.category, "all", f"{len(clusters)} cluster specs: {scopes}",
                     propose(r, clusters[0][1]), float(r.est_monthly_savings_usd or 0), r.fix_effort))
        continue
    for scope, nc in clusters:
        plan.append((r.priority_rank, r.job_name, r.job_id, r.category, scope, describe(nc), propose(r, nc),
                     float(r.est_monthly_savings_usd or 0), r.fix_effort))

plan_df = spark.createDataFrame(
    plan,
    "priority_rank INT, job_name STRING, job_id STRING, category STRING, cluster_scope STRING, "
    "current_config STRING, proposed_change STRING, est_monthly_savings_usd DOUBLE, fix_effort STRING",
)
display(plan_df.orderBy("priority_rank"))

# COMMAND ----------

# MAGIC %md ## 4. Root cause: latest error / memory / executor log lines (Watchtower)

# COMMAND ----------

rc_jobs = [r for r in action_rows if r.category in ("MEMORY", "SPILL", "LOST_EXECUTORS", "FAILING")]
if not rc_jobs:
    print("No jobs in the action list need log investigation.")
for r in rc_jobs:
    print(f"#{r.priority_rank} {r.job_name} ({r.category})")
    display(spark.sql(f"""
        SELECT m.job_run_id, l.log_ts, l.log_source, l.log_level, LEFT(l.log_msg, 400) AS message
        FROM {CATALOG}.silver.logs l
        JOIN {OPS}.v_job_cluster_map m ON l.cluster_id = m.cluster_id
        WHERE m.job_id = :job_id
          AND (l.log_level IN ('ERROR', 'FATAL', 'CRITICAL')
               OR l.log_msg RLIKE '(?i)OutOfMemoryError|ExecutorLostFailure|Lost executor|spilling|exit code 137')
        ORDER BY l.log_ts DESC
        LIMIT 20
    """, args={"job_id": r.job_id}))

# COMMAND ----------

# MAGIC %md ## 5. Did the fix work? 7 days before vs. 7 days after a change

# COMMAND ----------

check_job = dbutils.widgets.get("check_job_id").strip()
check_date = dbutils.widgets.get("check_change_date").strip()
if not (check_job and check_date):
    print("Set the 'Before/after' widgets (job_id and change date) to compare a job before and after a fix.")
else:
    display(spark.sql(f"""
        WITH cost AS (
          SELECT IF(usage_date < DATE(:d), 'before', 'after') AS period,
                 ROUND(AVG(cost_usd), 2) AS avg_daily_cost_usd
          FROM {OPS}.v_job_cost_daily
          WHERE job_id = :j AND usage_date BETWEEN date_sub(DATE(:d), 7) AND date_add(DATE(:d), 6)
          GROUP BY 1),
        runs AS (
          SELECT IF(run_date < DATE(:d), 'before', 'after') AS period,
                 SUM(runs) AS runs, SUM(failed_runs) AS failed_runs, ROUND(AVG(avg_minutes), 1) AS avg_minutes
          FROM {OPS}.v_job_run_health_daily
          WHERE job_id = :j AND run_date BETWEEN date_sub(DATE(:d), 7) AND date_add(DATE(:d), 6)
          GROUP BY 1),
        util AS (
          SELECT IF(usage_date < DATE(:d), 'before', 'after') AS period,
                 ROUND(AVG(p95_cpu), 1) AS p95_cpu, ROUND(AVG(avg_mem), 1) AS avg_mem
          FROM {OPS}.v_job_utilization_daily
          WHERE job_id = :j AND usage_date BETWEEN date_sub(DATE(:d), 7) AND date_add(DATE(:d), 6)
          GROUP BY 1),
        sig AS (
          SELECT IF(log_date < DATE(:d), 'before', 'after') AS period,
                 SUM(oom_events) AS oom_events, SUM(spill_events) AS spill_events
          FROM {OPS}.v_job_log_signals_daily
          WHERE job_id = :j AND log_date BETWEEN date_sub(DATE(:d), 7) AND date_add(DATE(:d), 6)
          GROUP BY 1)
        SELECT * FROM cost
        FULL JOIN runs USING (period) FULL JOIN util USING (period) FULL JOIN sig USING (period)
        ORDER BY period DESC
    """, args={"j": check_job, "d": check_date}))

# COMMAND ----------

# MAGIC %md ## 6. (Optional) Save the fix plan — append-only, nothing is overwritten

# COMMAND ----------

if dbutils.widgets.get("save_plan") == "yes":
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {OPS}.fix_plan_history (
          proposed_at TIMESTAMP, priority_rank INT, job_name STRING, job_id STRING, category STRING,
          cluster_scope STRING, current_config STRING, proposed_change STRING,
          est_monthly_savings_usd DOUBLE, fix_effort STRING)
        TBLPROPERTIES ('delta.appendOnly' = 'true')
    """)
    from pyspark.sql import functions as F

    (plan_df.withColumn("proposed_at", F.current_timestamp())
        .select("proposed_at", "priority_rank", "job_name", "job_id", "category", "cluster_scope",
                "current_config", "proposed_change", "est_monthly_savings_usd", "fix_effort")
        .write.mode("append").saveAsTable(f"{OPS}.fix_plan_history"))
    print(f"Appended {plan_df.count()} rows to {OPS}.fix_plan_history")
else:
    print("Fix plan not saved (set widget save_plan = yes to append it to ops.fix_plan_history).")
