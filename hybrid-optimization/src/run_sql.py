"""Run the hybrid layer's SQL files on serverless jobs compute.

Placeholders substituted in every file:
  ${catalog}            Unity Catalog catalog that holds Watchtower + the ops schema
  ${workspace_id}       numeric ID of the workspace being optimized (prod)
  ${snapshot_date_expr} DATE literal for --snapshot_date, else date_sub(current_date(), 2)
  ${price_multiplier}   contract multiplier on effective list price (e.g. 1.093)

Statements are separated by a line ending in ';'.
A statement containing '-- @optional' may fail without failing the job.

No-delete policy: this runner refuses any statement that deletes or
overwrites data (DROP, DELETE, TRUNCATE, INSERT OVERWRITE, MERGE ... DELETE,
VACUUM, RESTORE). CREATE OR REPLACE is allowed for VIEWS only.
"""
import argparse
import datetime
import os
import re

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")

FORBIDDEN = [
    r"\bDROP\s+(TABLE|SCHEMA|DATABASE|CATALOG|VOLUME|VIEW)\b",
    r"\bDELETE\s+FROM\b",
    r"\bTRUNCATE\b",
    r"\bINSERT\s+OVERWRITE\b",
    r"\bWHEN\s+MATCHED\b[^;]*\bDELETE\b",
    r"\bVACUUM\b",
    r"\bRESTORE\s+TABLE\b",
    r"\bCREATE\s+OR\s+REPLACE\s+TABLE\b",
    r"\bREPLACE\s+TABLE\b",
]


def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--workspace_id", required=True)
    p.add_argument("--price_multiplier", default="1.093",
                   help="cost = usage_quantity x pricing.effective_list.default x this")
    p.add_argument("--snapshot_date", default="", help="YYYY-MM-DD for backfill; empty = 2 days ago")
    p.add_argument("--files", required=True, help="comma-separated file names in src/sql")
    return p.parse_args(argv)


def substitutions(a):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", a.catalog):
        raise ValueError(f"invalid catalog name: {a.catalog}")
    if not re.fullmatch(r"[0-9]+", a.workspace_id):
        raise ValueError(f"workspace_id must be numeric, got: {a.workspace_id}")
    mult = float(a.price_multiplier)
    if not 0 < mult < 5:
        raise ValueError(f"price_multiplier out of range: {a.price_multiplier}")
    if a.snapshot_date.strip():
        d = datetime.date.fromisoformat(a.snapshot_date.strip())
        snap = f"DATE'{d.isoformat()}'"
    else:
        snap = "date_sub(current_date(), 2)"
    return {"catalog": a.catalog, "workspace_id": a.workspace_id, "snapshot_date_expr": snap,
            "price_multiplier": repr(mult)}


def strip_comments(sql):
    return "\n".join(l for l in sql.splitlines() if not l.strip().startswith("--"))


def statements(text):
    for chunk in re.split(r";[ \t]*$", text, flags=re.MULTILINE):
        if strip_comments(chunk).strip():
            yield chunk.strip(), "@optional" in chunk


def check_no_delete(stmt):
    body = strip_comments(stmt).upper()
    for pattern in FORBIDDEN:
        if re.search(pattern, body, flags=re.DOTALL):
            raise ValueError(f"statement blocked by no-delete policy ({pattern}):\n{stmt[:300]}")


def render(name, subs):
    with open(os.path.join(SQL_DIR, name.strip())) as f:
        text = f.read()
    for k, v in subs.items():
        text = text.replace("${" + k + "}", v)
    left = re.findall(r"\$\{[a-z_]+\}", text)
    if left:
        raise ValueError(f"{name}: unresolved placeholders {sorted(set(left))}")
    return text


def main(argv=None):
    a = parse_args(argv)
    subs = substitutions(a)
    files = [f for f in a.files.split(",") if f.strip()]

    # Render and policy-check everything before running anything.
    plan = []
    for name in files:
        for i, (stmt, optional) in enumerate(statements(render(name, subs)), 1):
            check_no_delete(stmt)
            plan.append((name, i, stmt, optional))

    from pyspark.sql import SparkSession  # imported late so --help / tests work offline

    spark = SparkSession.builder.getOrCreate()
    for name, i, stmt, optional in plan:
        print(f"[{name} #{i}] running")
        try:
            spark.sql(stmt)
        except Exception as e:  # noqa: BLE001
            if optional:
                print(f"[{name} #{i}] optional statement skipped: {e}")
            else:
                raise


if __name__ == "__main__":
    main()
