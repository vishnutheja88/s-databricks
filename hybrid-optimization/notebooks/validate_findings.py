# Databricks notebook source
# MAGIC %md
# MAGIC # Validate findings
# MAGIC
# MAGIC Three layers of validation, all recorded append-only in `ops.validation_results`:
# MAGIC
# MAGIC | Kind | What it proves | How |
# MAGIC | --- | --- | --- |
# MAGIC | `auto` | The ops layer (what Genie and the dashboard read) matches raw system tables and logs | The daily job's `validate` task re-computes key numbers straight from the sources |
# MAGIC | `genie_benchmark` | Genie answers the pilot questions correctly on governed data | Genie space Benchmarks tab, using `src/genie/benchmarks.json`; record each result below |
# MAGIC | `manual` | A finding holds up in standard Databricks tools | Spark UI, compute metrics, query profile; record each check below |
# MAGIC
# MAGIC Nothing here changes jobs, clusters or data. See `docs/VALIDATION.md` for what to look at per finding.

# COMMAND ----------

dbutils.widgets.text("catalog", "watchtower", "Catalog")
dbutils.widgets.dropdown("kind", "manual", ["manual", "genie_benchmark"], "Record kind")
dbutils.widgets.text("workspace_id", "", "Workspace id")
dbutils.widgets.text("check_name", "", "Check (job + finding, or benchmark question)")
dbutils.widgets.text("tool", "Spark UI", "Tool (Spark UI / Compute metrics / Query profile / Genie)")
dbutils.widgets.text("source_value", "", "What the tool / ground truth shows")
dbutils.widgets.text("ops_value", "", "What the scorecard / Genie said")
dbutils.widgets.dropdown("passed", "yes", ["yes", "no"], "Confirmed?")
dbutils.widgets.text("notes", "", "Notes")
dbutils.widgets.dropdown("record", "no", ["no", "yes"], "Record this entry now")

import re

CATALOG = dbutils.widgets.get("catalog").strip()
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", CATALOG):
    raise ValueError(f"invalid catalog: {CATALOG}")
OPS = f"{CATALOG}.ops"

# COMMAND ----------

# MAGIC %md ## 1. Latest automated checks (ops layer vs raw sources)

# COMMAND ----------

display(spark.sql(f"""
SELECT v.validated_at, s.workspace_name, v.check_name, v.source_value, v.ops_value, v.passed, v.detail
FROM {OPS}.validation_results v
LEFT JOIN {OPS}.v_scope s ON s.workspace_id = v.workspace_id
WHERE v.kind = 'auto'
QUALIFY ROW_NUMBER() OVER (PARTITION BY v.workspace_id, v.check_name ORDER BY v.validated_at DESC) = 1
ORDER BY v.passed, s.workspace_name, v.check_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Findings to confirm in Databricks tools
# MAGIC Top actions per workspace that have not been confirmed manually yet.

# COMMAND ----------

display(spark.sql(f"""
SELECT j.workspace_name, j.workspace_rank, j.job_name, j.job_id, j.category, j.top_recommendation,
       j.secondary_findings,
       CASE j.category
         WHEN 'OVERSIZED' THEN 'Compute metrics: CPU and memory stay low through the run'
         WHEN 'MEMORY' THEN 'Spark UI executors tab: OOM / failed tasks; compute metrics: memory near 100%'
         WHEN 'SPILL' THEN 'Spark UI stages: spill (disk) column on the largest stages'
         WHEN 'LOST_EXECUTORS' THEN 'Cluster event log: node lost / spot reclaimed events'
         WHEN 'FAILING' THEN 'Job run page: failed task error and stack trace'
         WHEN 'MULTI_CLUSTER' THEN 'Job definition: tasks each with their own new_cluster'
         WHEN 'CPU_BOUND' THEN 'Compute metrics: CPU above 85% for most of the run'
         ELSE 'Spark UI stages / query profile for the secondary finding'
       END AS where_to_confirm
FROM {OPS}.v_job_scorecard_latest j
WHERE j.workspace_rank <= 3
  AND NOT EXISTS (SELECT 1 FROM {OPS}.validation_results v
                  WHERE v.kind = 'manual' AND v.workspace_id = j.workspace_id
                    AND v.check_name LIKE CONCAT('%', j.job_id, '%'))
ORDER BY j.workspace_name, j.workspace_rank
"""))

# COMMAND ----------

# MAGIC %md ## 3. Genie benchmark questions (paste into the Genie space Benchmarks tab)

# COMMAND ----------

import json
import os

path = os.path.join(os.path.dirname(os.getcwd()), "src", "genie", "benchmarks.json")
try:
    bench = json.load(open(path))["benchmarks"]
    display(spark.createDataFrame([(b["question"], b["sql"].replace("${catalog}", CATALOG)) for b in bench],
                                  "question STRING, ground_truth_sql STRING"))
except Exception as e:  # noqa: BLE001
    print(f"Open src/genie/benchmarks.json in the repo ({e})")

# COMMAND ----------

# MAGIC %md ## 4. Record a manual or Genie-benchmark result (append-only)

# COMMAND ----------

if dbutils.widgets.get("record") == "yes":
    vals = {k: dbutils.widgets.get(k).strip() for k in
            ("kind", "workspace_id", "check_name", "tool", "source_value", "ops_value", "passed", "notes")}
    if not vals["check_name"]:
        raise ValueError("Fill in 'Check' before recording")
    spark.sql(f"""
        INSERT INTO {OPS}.validation_results
        SELECT current_timestamp(), :kind, NULLIF(:ws, ''), :check, :src, :ops, :passed = 'yes',
               CONCAT(:tool, ': ', :notes), current_user()
    """, args={"kind": vals["kind"], "ws": vals["workspace_id"], "check": vals["check_name"],
               "src": vals["source_value"], "ops": vals["ops_value"], "passed": vals["passed"],
               "tool": vals["tool"], "notes": vals["notes"]})
    print("Recorded. Set 'Record this entry now' back to 'no' before the next entry.")
else:
    print("Fill the widgets and set 'Record this entry now' = yes to append an entry.")

display(spark.sql(f"""
SELECT validated_at, kind, workspace_id, check_name, source_value, ops_value, passed, detail, validated_by
FROM {OPS}.validation_results WHERE kind <> 'auto' ORDER BY validated_at DESC LIMIT 50
"""))
