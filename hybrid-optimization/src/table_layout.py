"""Snapshot file sizing and maintenance for tables written by in-scope jobs.

For up to --limit tables from ops.v_job_written_tables (lineage), reads
DESCRIBE DETAIL (files, size, partitioning, clustering) and DESCRIBE HISTORY
(last OPTIMIZE) and appends one row per table per day to
ops.table_layout_snapshot. Read-only on the tables themselves.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402


def main(argv=None):
    p = common.parser("Table layout snapshot")
    p.add_argument("--limit", type=int, default=50)
    a = p.parse_args(argv)
    common.substitutions(a)
    cat = a.catalog

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    if spark.sql(f"SELECT COUNT(*) AS n FROM {cat}.ops.table_layout_snapshot WHERE snapshot_date = current_date()").first().n:
        print("layout already captured today")
        return
    tables = [r.table_name for r in spark.sql(f"""
        SELECT table_name, SUM(write_events) AS w
        FROM {cat}.ops.v_job_written_tables
        GROUP BY table_name ORDER BY w DESC LIMIT {int(a.limit)}""").collect()]
    if not tables:
        print("no written tables found (lineage disabled or no writes in scope)")
        return

    rows = []
    for t in tables:
        quoted = ".".join(f"`{part}`" for part in t.split("."))
        try:
            d = spark.sql(f"DESCRIBE DETAIL {quoted}").first().asDict()
            fmt = (d.get("format") or "").lower()
            files, size = d.get("numFiles"), d.get("sizeInBytes")
            last_opt = None
            if fmt == "delta":
                h = (spark.sql(f"DESCRIBE HISTORY {quoted} LIMIT 200")
                     .where("operation = 'OPTIMIZE'").agg(F.max("timestamp").alias("t")).first())
                last_opt = h.t if h else None
            rows.append((t, fmt, files, size,
                         round(size / files / 1e6, 2) if files and size else None,
                         ", ".join(d.get("partitionColumns") or []),
                         ", ".join(d.get("clusteringColumns") or []),
                         last_opt, "ok"))
        except Exception as e:  # noqa: BLE001
            rows.append((t, None, None, None, None, None, None, None, str(e).splitlines()[0][:300]))

    df = spark.createDataFrame(rows, "table_name STRING, table_format STRING, num_files BIGINT, size_bytes BIGINT, "
                                     "avg_file_mb DOUBLE, partition_columns STRING, clustering_columns STRING, "
                                     "last_optimize_at TIMESTAMP, detail STRING")
    df.createOrReplaceTempView("layout")
    spark.sql(f"""
        INSERT INTO {cat}.ops.table_layout_snapshot
        SELECT current_date(), table_name, table_format, num_files, size_bytes, avg_file_mb,
               partition_columns, clustering_columns, last_optimize_at, detail, current_timestamp()
        FROM layout""")
    print(f"captured layout for {len(rows)} tables")


if __name__ == "__main__":
    main()
