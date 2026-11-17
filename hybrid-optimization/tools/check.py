"""Offline static checks for hybrid-optimization (used by CI and locally).

    pip install pyyaml sqlglot
    python3 hybrid-optimization/tools/check.py

Checks: YAML/JSON parse, Python compiles, every SQL statement renders and parses
(sqlglot databricks dialect), the no-delete guard blocks forbidden statements,
the dashboard JSON is up to date with tools/build_dashboard.py, and every
dashboard field references a column its dataset returns. No workspace access.
"""
import glob
import json
import os
import re
import subprocess
import sys

import sqlglot
import yaml

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))
import run_sql  # noqa: E402

FAIL = []


def check(cond, msg):
    if not cond:
        FAIL.append(msg)


def main():
    os.chdir(ROOT)

    for f in ["databricks.yml"] + glob.glob("resources/*.yml") + glob.glob("watchtower/*.yml"):
        yaml.safe_load(open(f))
    tw = yaml.safe_load(open("watchtower/prod-target.yml"))
    check(tw["prod"]["variables"]["raw_log_retention_action"] == "OFF", "watchtower retention must be the string OFF")

    for f in glob.glob("**/*.json", recursive=True):
        json.load(open(f))
    for f in glob.glob("**/*.py", recursive=True):
        compile(open(f).read(), f, "exec")  # syntax check without writing .pyc files

    subs = run_sql.substitutions(run_sql.parse_args(["--catalog=watchtower", "--workspace_id=1", "--files=x"]))
    n = 0
    for f in sorted(os.path.basename(p) for p in glob.glob("src/sql/[0-8]*.sql")):
        for stmt, _ in run_sql.statements(run_sql.render(f, subs)):
            try:
                run_sql.check_no_delete(stmt)
                sqlglot.parse_one(stmt, read="databricks")
                n += 1
            except Exception as e:  # noqa: BLE001
                FAIL.append(f"{f}: {e}")
        for i, line in enumerate(open(os.path.join("src/sql", f)), 1):
            check(not (line.strip().startswith("--") and line.rstrip().endswith(";")), f"{f}:{i} comment line ends with ';'")
    print(f"SQL statements parsed: {n}")

    for bad in ["DROP TABLE x", "DELETE FROM x", "TRUNCATE TABLE x", "INSERT OVERWRITE x SELECT 1",
                "MERGE INTO t USING s ON a=b WHEN MATCHED THEN DELETE", "VACUUM x",
                "CREATE OR REPLACE TABLE x AS SELECT 1", "RESTORE TABLE x TO VERSION AS OF 1"]:
        try:
            run_sql.check_no_delete(bad)
            FAIL.append(f"guard did not block: {bad}")
        except ValueError:
            pass

    path = "src/dashboards/job_optimization.lvdash.json"
    before = open(path).read()
    subprocess.run([sys.executable, "tools/build_dashboard.py"], check=True, capture_output=True)
    check(open(path).read() == before, "dashboard JSON is stale: run tools/build_dashboard.py and commit")

    d = json.load(open(path))
    cols = {}
    for ds in d["datasets"]:
        q = "".join(ds["queryLines"])
        cols[ds["name"]] = {s.alias_or_name for s in sqlglot.parse_one(q, read="databricks").selects}
    for pg in d["pages"]:
        for w in pg["layout"]:
            for q in w["widget"]["queries"]:
                for fld in q["query"]["fields"]:
                    for ref in re.findall(r"`([^`]+)`", fld["expression"]):
                        check(ref in cols[q["query"]["datasetName"]], f"dashboard {w['widget']['name']}: unknown column {ref}")

    if FAIL:
        print("FAILED:\n- " + "\n- ".join(FAIL))
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
