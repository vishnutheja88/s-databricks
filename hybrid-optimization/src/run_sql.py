"""Run the hybrid layer's SQL files on serverless jobs compute.

Placeholders come from common.substitutions() (workspace scope, review period,
price multiplier, thresholds). Statements are separated by a line ending in ';'.
A statement containing '-- @optional' may fail without failing the job; use it
for sources that may not be enabled (query history, lineage) right after a
fallback statement that always succeeds.

No-delete policy: this runner refuses any statement that deletes or
overwrites data (DROP, DELETE, TRUNCATE, INSERT OVERWRITE, MERGE ... DELETE,
VACUUM, RESTORE, CREATE OR REPLACE TABLE). CREATE OR REPLACE is allowed for VIEWS only.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

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
    p = common.parser("Run hybrid-optimization SQL files")
    p.add_argument("--files", required=True, help="comma-separated file names in src/sql")
    return p.parse_args(argv)


def substitutions(a):
    return common.substitutions(a)


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
