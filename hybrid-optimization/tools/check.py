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

    args = ["--catalog=watchtower", "--workspaces=1111111111111111:prod,2222222222222222:analytics",
            "--review_start=2026-10-01", "--review_end=2026-10-31", "--files=x"]
    subs = run_sql.substitutions(run_sql.parse_args(args))
    rolling = run_sql.substitutions(run_sql.parse_args(["--catalog=watchtower", "--workspaces=1:a", "--files=x"]))
    check(rolling["review_label"] == "rolling 30 days", "rolling review label")
    for bad in (["--workspaces=abc:x"], ["--workspaces=1:a,1:b"], ["--workspaces=1:a", "--review_start=2026-02-01",
                                                                  "--review_end=2026-01-01"]):
        try:
            run_sql.substitutions(run_sql.parse_args(["--catalog=watchtower", "--files=x"] + bad))
            FAIL.append(f"bad scope accepted: {bad}")
        except ValueError:
            pass
    n = 0
    view_text = run_sql.render("10_views.sql", subs)
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

    # Every column the dashboard reads from a view must be produced somewhere in the SQL layer.
    sql_text = "".join(open(f).read() for f in glob.glob("src/sql/*.sql"))
    for ds in d["datasets"]:
        q = sqlglot.parse_one("".join(ds["queryLines"]), read="databricks")
        for e in q.selects:
            if isinstance(e, sqlglot.exp.Alias):
                continue  # computed in the dataset itself
            check(re.search(rf"\b{re.escape(e.alias_or_name)}\b", sql_text),
                  f"dashboard dataset {ds['name']}: column {e.alias_or_name} not found in src/sql")

    # Genie benchmark and space reference only ops objects that exist.
    objs = set(re.findall(r"CREATE (?:OR REPLACE VIEW|TABLE IF NOT EXISTS|VOLUME IF NOT EXISTS) \S+\.ops\.([a-z0-9_]+)",
                          sql_text))
    for f in ("src/genie/genie_space.json", "src/genie/benchmarks.json"):
        for ref in re.findall(r"ops\.([a-z0-9_]+)", open(f).read()):
            check(ref in objs, f"{f}: unknown ops object {ref}")
    check(len(json.load(open("src/genie/genie_space.json"))["sample_questions"]) >= 8, "Genie sample questions")

    if FAIL:
        print("FAILED:\n- " + "\n- ".join(FAIL))
        sys.exit(1)
    print("All checks passed.")


if __name__ == "__main__":
    main()
