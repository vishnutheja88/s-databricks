# Databricks notebook source
# MAGIC %md
# MAGIC # Job Optimization Insights & Fix Plan
# MAGIC
# MAGIC Run top to bottom (any SQL-capable compute or serverless). For the pilot workspaces and review period it answers:
# MAGIC
# MAGIC 1. **What do jobs cost and how much can we save?** (summary per workspace)
# MAGIC 2. **What should we fix first?** (prioritized action plan with owner, effort, risk and estimated saving)
# MAGIC 3. **Exactly what do we change?** (fix plan: current cluster config -> proposed change, plus secondary findings)
# MAGIC 4. **Why is it slow or failing?** (root-cause log lines and Spark stage signals)
# MAGIC 5. **Did the fix work?** (7 days before vs after a change)
# MAGIC
# MAGIC **Read-only.** It changes no job, cluster or table. Changes go through your normal review and approval.
# MAGIC The only write is optional: appending the plan to `ops.fix_plan_history` (widget `save_plan = yes`).
# MAGIC Cost is **contract cost** (usage x effective list price x 1.093). Savings are conservative estimates per 30 days.

# COMMAND ----------

dbutils.widgets.text("catalog", "watchtower", "Catalog")
dbutils.widgets.text("top_n", "10", "Jobs in fix plan")
dbutils.widgets.text("workspace_name", "", "Workspace (empty = all)")
dbutils.widgets.dropdown("save_plan", "no", ["no", "yes"], "Save fix plan to ops.fix_plan_history")
dbutils.widgets.text("check_workspace_id", "", "Before/after: workspace_id")
dbutils.widgets.text("check_job_id", "", "Before/after: job_id")
dbutils.widgets.text("check_change_date", "", "Before/after: change date (YYYY-MM-DD)")

import math
import re

CATALOG = dbutils.widgets.get("catalog").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"invalid catalog: {CATALOG}")
TOP_N = max(1, int(dbutils.widgets.get("top_n") or 10))
WS_NAME = dbutils.widgets.get("workspace_name").strip()
OPS = f"{CATALOG}.ops"

# Read the saved daily scorecard (fast). Before the first daily snapshot exists, fall back to the live view.
SCORECARD = f"{OPS}.v_job_scorecard_latest"
try:
    if spark.sql(f"SELECT COUNT(*) AS n FROM {SCORECARD}").first().n == 0:
        SCORECARD = f"{OPS}.v_job_scorecard_30d"
except Exception:  # noqa: BLE001  (snapshot table not created yet)
    SCORECARD = f"{OPS}.v_job_scorecard_30d"
WS_FILTER = "workspace_name = :ws" if WS_NAME else "1 = 1"
ARGS = {"ws": WS_NAME} if WS_NAME else {}
print(f"Scorecard source: {SCORECARD}; workspace filter: {WS_NAME or 'all'}")

# COMMAND ----------

# MAGIC %md ## 1. Summary per workspace

# COMMAND ----------

display(spark.sql(f"""
SELECT workspace_name, MAX(review_label) AS review_period,
       COUNT(*) AS jobs_analyzed, COUNT_IF(category <> 'OK') AS jobs_needing_action,
       ROUND(SUM(period_cost_usd)) AS period_cost_usd,
       ROUND(SUM(est_monthly_savings_usd)) AS est_monthly_savings_usd,
       ROUND(SUM(est_annual_savings_usd)) AS est_annual_savings_usd,
       ROUND(SUM(failed_run_cost_usd)) AS failed_run_cost_usd
FROM {SCORECARD}
WHERE {WS_FILTER}
GROUP BY workspace_name
ORDER BY est_monthly_savings_usd DESC
""", args=ARGS))

display(spark.sql(f"""
SELECT category AS fix_type, fix_effort, risk_level, COUNT(*) AS jobs,
       ROUND(SUM(period_cost_usd)) AS period_cost_usd, ROUND(SUM(est_monthly_savings_usd)) AS est_monthly_savings_usd
FROM {SCORECARD}
WHERE category <> 'OK' AND {WS_FILTER}
GROUP BY ALL ORDER BY est_monthly_savings_usd DESC
""", args=ARGS))

# COMMAND ----------

# MAGIC %md ## 2. Prioritized action plan (rank 1 = do first)

# COMMAND ----------

actions = spark.sql(f"""
SELECT priority_rank, workspace_id, workspace_name, job_name, job_id, owner, category, top_recommendation,
       secondary_findings, fix_effort, risk_level, risk, est_monthly_savings_usd, period_cost_usd, cost_30d,
       failed_runs, retries, avg_cpu, p95_cpu, avg_mem, max_swap, clusters_per_run, idle_pct,
       shuffle_read_gb, disk_spill_gb, max_skew_ratio, oom_events, spill_events, lost_executor_events,
       dbr_version, runtime_outdated, policy_missing
FROM (SELECT *, monthly_cost_usd AS cost_30d FROM {SCORECARD})
WHERE (category <> 'OK' OR size(finding_codes) > 0) AND {WS_FILTER}
ORDER BY priority_rank NULLS LAST, size(finding_codes) DESC, period_cost_usd DESC
LIMIT {TOP_N}
""", args=ARGS)
display(actions)
action_rows = actions.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fix plan: current config -> proposed change
# MAGIC Reads job definitions through the Jobs API (read-only). The API only sees jobs in **this** workspace;
# MAGIC jobs in other pilot workspaces show the proposed change without the current config (run this notebook there to see it).

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
try:
    THIS_WS = str(w.get_workspace_id())
except Exception:  # noqa: BLE001
    THIS_WS = ""

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
    if fam[0] == "i":
        gen, suffix = "5", "d"
    candidate = f"{new_fam}{gen}{suffix}.{size}"
    return candidate if candidate != node else None


def job_clusters(job_id):
    s = w.jobs.get(job_id=int(job_id)).settings
    out = [(f"job_cluster:{jc.job_cluster_key}", jc.new_cluster) for jc in (s.job_clusters or []) if jc.new_cluster]
    out += [(f"task:{t.task_key}", t.new_cluster) for t in (s.tasks or []) if t.new_cluster]
    return out


def describe(nc):
    workers = (f"autoscale {nc.autoscale.min_workers}-{nc.autoscale.max_workers}" if nc.autoscale
               else f"{nc.num_workers} workers")
    aws = nc.aws_attributes
    avail = (aws.availability.value if aws and aws.availability else "default") + (
        f", first_on_demand={aws.first_on_demand}" if aws and aws.first_on_demand is not None else "")
    return (f"{nc.node_type_id} x {workers}; driver {nc.driver_node_type_id or nc.node_type_id}; {avail}; "
            f"DBR {nc.spark_version}; policy {nc.policy_id or 'none'}")


def propose(row, nc):
    cat = row.category
    if cat == "OVERSIZED":
        cut = (row.est_monthly_savings_usd or 0) / row.cost_30d if row.cost_30d else 0.25
        cut = min(0.5, max(0.25, cut))
        if nc and nc.autoscale and nc.autoscale.max_workers:
            new_max = max(nc.autoscale.min_workers or 1, math.ceil(nc.autoscale.max_workers * (1 - cut)))
            return f"autoscale.max_workers: {nc.autoscale.max_workers} -> {new_max}"
        if nc and nc.num_workers:
            return f"num_workers: {nc.num_workers} -> {max(1, math.ceil(nc.num_workers * (1 - cut)))}"
        return f"cut workers by about {int(cut * 100)}% or one instance size smaller"
    if cat in ("MEMORY", "SPILL"):
        alt = swap_family(nc.node_type_id, "r") if nc else None
        step = f"node_type_id: {nc.node_type_id} -> {alt}" if alt else "move to a memory-optimized (r-family) node"
        return step + ("; check data skew and partition sizes" if cat == "MEMORY" else "; or raise spark.sql.shuffle.partitions")
    if cat == "CPU_BOUND":
        alt = swap_family(nc.node_type_id, "c") if nc else None
        return f"node_type_id: {nc.node_type_id} -> {alt}" if alt else "move to a compute-optimized (c-family) node"
    if cat == "LOST_EXECUTORS":
        return "aws_attributes: availability -> SPOT_WITH_FALLBACK, first_on_demand -> 1, zone_id -> auto"
    if cat == "MULTI_CLUSTER":
        return "define one job_clusters entry and point every task at it with job_cluster_key"
    if cat == "FAILING":
        return "fix the failing task first (section 4); no cluster change"
    return "no primary change; see secondary findings"


plan = []
for r in action_rows:
    base = (r.priority_rank, r.workspace_name, r.job_name, r.job_id, r.owner, r.category)
    tail = (r.secondary_findings or "", float(r.est_monthly_savings_usd or 0), r.fix_effort, r.risk_level, r.risk)
    if r.workspace_id != THIS_WS:
        plan.append(base + ("-", "job is in another workspace: run this notebook there for current config",
                            propose(r, None)) + tail)
        continue
    try:
        clusters = job_clusters(r.job_id)
    except Exception as e:  # noqa: BLE001
        plan.append(base + ("-", f"could not read job: {str(e)[:200]}", propose(r, None)) + tail)
        continue
    if not clusters or r.category == "MULTI_CLUSTER":
        scopes = ", ".join(s for s, _ in clusters) or "no classic job cluster"
        plan.append(base + ("all", scopes, propose(r, clusters[0][1] if clusters else None)) + tail)
        continue
    for scope, nc in clusters:
        plan.append(base + (scope, describe(nc), propose(r, nc)) + tail)

plan_df = spark.createDataFrame(
    plan,
    "priority_rank INT, workspace_name STRING, job_name STRING, job_id STRING, owner STRING, category STRING, "
    "cluster_scope STRING, current_config STRING, proposed_change STRING, secondary_findings STRING, "
    "est_monthly_savings_usd DOUBLE, fix_effort STRING, risk_level STRING, risk STRING",
)
display(plan_df.orderBy("priority_rank"))

# COMMAND ----------

# MAGIC %md ## 4. Root cause: log lines (Watchtower) and worst Spark stages (event logs)

# COMMAND ----------

rc_jobs = [r for r in action_rows if r.category in ("MEMORY", "SPILL", "LOST_EXECUTORS", "FAILING")
           or (r.max_skew_ratio or 0) >= 5 or (r.disk_spill_gb or 0) > 0]
if not rc_jobs:
    print("No jobs in the action list need log or stage investigation.")
for r in rc_jobs:
    print(f"#{r.priority_rank} [{r.workspace_name}] {r.job_name} ({r.category})")
    display(spark.sql(f"""
        SELECT m.job_run_id, l.log_ts, l.log_source, l.log_level, LEFT(l.log_msg, 400) AS message
        FROM {CATALOG}.silver.logs l
        JOIN {OPS}.v_job_cluster_map m ON l.cluster_id = m.cluster_id
        WHERE m.workspace_id = :ws_id AND m.job_id = :job_id
          AND (l.log_level IN ('ERROR', 'FATAL', 'CRITICAL')
               OR l.log_msg RLIKE '(?i)OutOfMemoryError|ExecutorLostFailure|Lost executor|spilling|exit code 137')
        ORDER BY l.log_ts DESC
        LIMIT 20
    """, args={"ws_id": r.workspace_id, "job_id": r.job_id}))
    display(spark.sql(f"""
        WITH latest AS (
          SELECT * FROM {OPS}.spark_stage_metrics
          QUALIFY file_modified_at = MAX(file_modified_at) OVER (PARTITION BY file_path))
        SELECT s.cluster_id, s.stage_id, s.tasks, ROUND(s.max_task_ms / 1000, 1) AS max_task_s,
               ROUND(s.p50_task_ms / 1000, 1) AS median_task_s,
               ROUND(s.max_task_ms / GREATEST(s.p50_task_ms, 1), 1) AS skew_ratio,
               ROUND(s.shuffle_read_bytes / 1e9, 2) AS shuffle_read_gb, ROUND(s.disk_spilled_bytes / 1e9, 2) AS disk_spill_gb
        FROM latest s
        JOIN (SELECT DISTINCT cluster_id FROM {OPS}.v_job_cluster_map WHERE workspace_id = :ws_id AND job_id = :job_id) c
          ON s.cluster_id = c.cluster_id
        ORDER BY s.disk_spilled_bytes + s.shuffle_read_bytes DESC
        LIMIT 10
    """, args={"ws_id": r.workspace_id, "job_id": r.job_id}))
    print("Confirm in the Spark UI (stages tab) and compute metrics before changing anything; log it in validate_findings.")

# COMMAND ----------

# MAGIC %md ## 5. Did the fix work? 7 days before vs 7 days after a change

# COMMAND ----------

cw, cj, cd = (dbutils.widgets.get(k).strip() for k in ("check_workspace_id", "check_job_id", "check_change_date"))
if not (cw and cj and cd):
    print("Set the 'Before/after' widgets (workspace_id, job_id, change date) to compare a job before and after a fix.")
else:
    display(spark.sql(f"""
        SELECT IF(metric_date < DATE(:d), 'before', 'after') AS period,
               ROUND(AVG(cost_usd), 2) AS avg_daily_cost_usd,
               SUM(runs) AS runs, SUM(failed_runs) AS failed_runs, SUM(retries) AS retries,
               ROUND(SUM(avg_minutes * runs) / NULLIF(SUM(runs), 0), 1) AS avg_minutes,
               ROUND(AVG(p95_cpu), 1) AS p95_cpu, ROUND(AVG(avg_mem), 1) AS avg_mem,
               ROUND(SUM(idle_node_hours), 1) AS idle_node_hours,
               SUM(oom_events) AS oom_events, ROUND(SUM(disk_spill_gb), 1) AS disk_spill_gb
        FROM {OPS}.v_job_daily_metrics
        WHERE workspace_id = :w AND job_id = :j
          AND metric_date BETWEEN date_sub(DATE(:d), 7) AND date_add(DATE(:d), 6)
        GROUP BY 1 ORDER BY period DESC
    """, args={"w": cw, "j": cj, "d": cd}))

# COMMAND ----------

# MAGIC %md ## 6. (Optional) Save the fix plan, append-only

# COMMAND ----------

if dbutils.widgets.get("save_plan") == "yes":
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {OPS}.fix_plan_history (
          proposed_at TIMESTAMP, priority_rank INT, workspace_name STRING, job_name STRING, job_id STRING,
          owner STRING, category STRING, cluster_scope STRING, current_config STRING, proposed_change STRING,
          secondary_findings STRING, est_monthly_savings_usd DOUBLE, fix_effort STRING, risk_level STRING,
          risk STRING, approval_status STRING)
        TBLPROPERTIES ('delta.appendOnly' = 'true')
    """)
    from pyspark.sql import functions as F

    (plan_df.withColumn("proposed_at", F.current_timestamp())
        .withColumn("approval_status", F.lit("proposed: needs review and approval"))
        .select("proposed_at", "priority_rank", "workspace_name", "job_name", "job_id", "owner", "category",
                "cluster_scope", "current_config", "proposed_change", "secondary_findings",
                "est_monthly_savings_usd", "fix_effort", "risk_level", "risk", "approval_status")
        .write.mode("append").saveAsTable(f"{OPS}.fix_plan_history"))
    print(f"Appended {plan_df.count()} rows to {OPS}.fix_plan_history")
else:
    print("Fix plan not saved (set widget save_plan = yes to append it to ops.fix_plan_history).")
