"""Run the layer's SQL files on serverless jobs compute.

Placeholders ${catalog}, ${lookback_days}, ${auto_enable} are substituted.
Statements are separated by a line ending in ';'.
A statement containing '-- @optional' is allowed to fail (e.g. a system
table not enabled in this account) without failing the job.
"""
import argparse
import os
import re

from pyspark.sql import SparkSession

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--lookback_days", type=int, default=3)
    p.add_argument("--auto_enable", default="true")
    p.add_argument("--files", required=True, help="comma-separated file names in src/sql")
    return p.parse_args()


def statements(text):
    for chunk in re.split(r";[ \t]*$", text, flags=re.MULTILINE):
        body = "\n".join(l for l in chunk.splitlines() if not l.strip().startswith("--") or "@optional" in l)
        if body.strip():
            yield chunk.strip(), "@optional" in chunk


def main():
    a = parse_args()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", a.catalog):
        raise ValueError(f"invalid catalog name: {a.catalog}")
    auto_enable = "true" if a.auto_enable.lower() == "true" else "false"
    subs = {"catalog": a.catalog, "lookback_days": str(max(1, a.lookback_days)), "auto_enable": auto_enable}

    spark = SparkSession.builder.getOrCreate()
    for name in a.files.split(","):
        with open(os.path.join(SQL_DIR, name.strip())) as f:
            text = f.read()
        for k, v in subs.items():
            text = text.replace("${" + k + "}", v)
        for i, (stmt, optional) in enumerate(statements(text), 1):
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
