"""Summarize Spark event logs per stage (shuffle, spill, task-time skew).

Reads new event-log files incrementally with Auto Loader (availableNow) from the
cluster log delivery root (<root>/<cluster_id>/eventlog/...), keeps only
SparkListenerTaskEnd events, and appends one summary row per
(cluster, file version, stage) to ops.spark_stage_metrics.

- Append-only: never updates or deletes; Auto Loader cleanSource stays OFF.
- Active event logs are re-delivered while a cluster runs, so modified files are
  re-read (allowOverwrites) and the views keep only the newest version per file.
- Watchtower reads only <cluster_id>/driver/, so the two never overlap.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import common  # noqa: E402

TASK_END_SCHEMA = """
  `Event` STRING,
  `Stage ID` BIGINT,
  `Stage Attempt ID` BIGINT,
  `Task Info` STRUCT<`Launch Time`: BIGINT, `Finish Time`: BIGINT, `Failed`: BOOLEAN>,
  `Task Metrics` STRUCT<
    `Executor Run Time`: BIGINT,
    `Memory Bytes Spilled`: BIGINT,
    `Disk Bytes Spilled`: BIGINT,
    `Input Metrics`: STRUCT<`Bytes Read`: BIGINT>,
    `Shuffle Read Metrics`: STRUCT<`Remote Bytes Read`: BIGINT, `Local Bytes Read`: BIGINT>,
    `Shuffle Write Metrics`: STRUCT<`Shuffle Bytes Written`: BIGINT>
  >
"""


def main(argv=None):
    p = common.parser("Spark event log ingest")
    p.add_argument("--eventlog_path", required=True)
    a = p.parse_args(argv)
    common.substitutions(a)  # validates catalog and scope
    cat = a.catalog

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()
    try:
        spark.sql(f"LIST '{a.eventlog_path}' LIMIT 1").collect()
    except Exception as e:  # noqa: BLE001
        print(f"Cluster log root not readable, skipping event-log ingest: {e}")
        return

    raw = (spark.readStream.format("cloudFiles")
           .option("cloudFiles.format", "text")
           .option("cloudFiles.allowOverwrites", "true")
           .option("cloudFiles.includeExistingFiles", "true")
           .option("recursiveFileLookup", "true")
           .option("pathGlobFilter", "eventlog*")
           .load(a.eventlog_path)
           .select("value",
                   F.col("_metadata.file_path").alias("file_path"),
                   F.col("_metadata.file_modification_time").alias("file_modified_at"))
           .where(F.col("value").contains('"Event":"SparkListenerTaskEnd"'))
           .where(F.col("file_path").contains("/eventlog/")))

    def summarize(batch, batch_id):
        e = batch.select(
            "file_path", "file_modified_at",
            F.regexp_extract("file_path", r"/([^/]+)/eventlog/", 1).alias("cluster_id"),
            F.from_json("value", TASK_END_SCHEMA).alias("j"))
        t = e.select(
            "cluster_id", "file_path", "file_modified_at",
            F.col("j.`Stage ID`").alias("stage_id"),
            F.col("j.`Stage Attempt ID`").alias("stage_attempt"),
            F.col("j.`Task Info`.`Failed`").alias("failed"),
            (F.col("j.`Task Info`.`Finish Time`") - F.col("j.`Task Info`.`Launch Time`")).alias("task_ms"),
            F.col("j.`Task Info`.`Finish Time`").alias("finish_ms"),
            F.col("j.`Task Metrics`.`Executor Run Time`").alias("run_ms"),
            F.col("j.`Task Metrics`.`Memory Bytes Spilled`").alias("mem_spill"),
            F.col("j.`Task Metrics`.`Disk Bytes Spilled`").alias("disk_spill"),
            F.col("j.`Task Metrics`.`Input Metrics`.`Bytes Read`").alias("input_bytes"),
            (F.coalesce(F.col("j.`Task Metrics`.`Shuffle Read Metrics`.`Remote Bytes Read`"), F.lit(0))
             + F.coalesce(F.col("j.`Task Metrics`.`Shuffle Read Metrics`.`Local Bytes Read`"), F.lit(0))).alias("shuffle_read"),
            F.col("j.`Task Metrics`.`Shuffle Write Metrics`.`Shuffle Bytes Written`").alias("shuffle_write"),
        ).where(F.col("stage_id").isNotNull())
        out = (t.groupBy("cluster_id", "file_path", "file_modified_at", "stage_id", "stage_attempt")
               .agg(F.count("*").alias("tasks"),
                    F.sum(F.col("failed").cast("int")).alias("failed_tasks"),
                    F.sum("run_ms").alias("executor_run_ms"),
                    F.max("task_ms").alias("max_task_ms"),
                    F.percentile_approx("task_ms", 0.5).alias("p50_task_ms"),
                    F.sum("input_bytes").alias("input_bytes"),
                    F.sum("shuffle_read").alias("shuffle_read_bytes"),
                    F.sum("shuffle_write").alias("shuffle_write_bytes"),
                    F.sum("mem_spill").alias("memory_spilled_bytes"),
                    F.sum("disk_spill").alias("disk_spilled_bytes"),
                    F.min("finish_ms").alias("first_ms"),
                    F.max("finish_ms").alias("last_ms"))
               .select(F.current_timestamp().alias("ingested_at"), "cluster_id", "file_path", "file_modified_at",
                       "stage_id", "stage_attempt", "tasks", F.col("failed_tasks").cast("bigint").alias("failed_tasks"),
                       "executor_run_ms", "max_task_ms", F.col("p50_task_ms").cast("bigint").alias("p50_task_ms"),
                       "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
                       "memory_spilled_bytes", "disk_spilled_bytes",
                       F.timestamp_millis("first_ms").alias("first_task_end"),
                       F.timestamp_millis("last_ms").alias("last_task_end")))
        out.write.mode("append").saveAsTable(f"{cat}.ops.spark_stage_metrics")

    (raw.writeStream
        .foreachBatch(summarize)
        .option("checkpointLocation", f"/Volumes/{cat}/ops/checkpoints/eventlog_ingest")
        .trigger(availableNow=True)
        .start()
        .awaitTermination())
    print("event-log ingest finished")


if __name__ == "__main__":
    main()
