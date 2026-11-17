# Watchtower: no-delete setup

[Watchtower](https://github.com/databricks-industry-solutions/watchtower) ingests
Databricks driver logs (stdout, stderr, log4j) into `bronze.logs` and `silver.logs`
and ships a Logs Dashboard with OOM / spill / lost-executor detectors.

Out of the box it **deletes raw logs 30 days after ingestion** (Auto Loader
`cleanSource = DELETE`). This folder turns that off and hardens the pipeline.

## What `no-delete.patch` changes

Built against Watchtower commit `1c8e60e8339a802e5582dfe6d6f9d4eef74bf268`.

| File | Change |
| --- | --- |
| `src/watchtower/log_ingest_pipeline.py` | `cleanSource` defaults to `OFF`; `DELETE` raises an error; an unquoted YAML `OFF` (arrives as `false`) is treated as `OFF`; `pipelines.reset.allowed = false` on `bronze.logs` and `silver.logs` so a full refresh can't truncate them |
| `databricks.yml` | `raw_log_retention_action` default `DELETE` -> `"OFF"` |
| `terraform/main.tf` | `lifecycle { prevent_destroy = true }` on both volumes |

## Apply

```bash
git clone https://github.com/databricks-industry-solutions/watchtower
cd watchtower
git checkout 1c8e60e8339a802e5582dfe6d6f9d4eef74bf268
git apply /path/to/s-databricks/hybrid-optimization/watchtower/no-delete.patch
# then add prod-target.yml under `targets:` in databricks.yml
databricks bundle validate -t prod     # confirm raw_log_retention_action resolves to OFF
databricks bundle deploy   -t prod
databricks bundle run watchtower_pipeline -t prod
```

On a newer Watchtower commit, use `git apply --3way` and re-check the three edits by hand.

## Never run in prod

- `databricks bundle destroy` (drops the pipeline and its tables)
- `terraform destroy` (blocked by `prevent_destroy`, keep it that way)
- A pipeline **full refresh** (blocked by `pipelines.reset.allowed = false`)
- The `integration_tests` job or the `staging` target
- Removing a resource from the bundle and redeploying (deletes that resource)

Also confirm the S3 bucket behind the catalog's storage location has no lifecycle
Expiration rule.

## Storage tiering (keeps every file)

With retention OFF the raw logs grow forever. `s3-lifecycle-transition.json` moves objects
to S3 Standard-IA after 30 days and Glacier Instant Retrieval after 90 days. It has no
Expiration, so nothing is deleted, and Glacier IR is still readable in milliseconds.

1. Find the volume's S3 prefix: `DESCRIBE VOLUME watchtower.default.cluster_logs` (storage_location).
2. Put that prefix in the rule's `Filter.Prefix`.
3. `put-bucket-lifecycle-configuration` **replaces the bucket's whole lifecycle config**.
   First read the current rules (`aws s3api get-bucket-lifecycle-configuration --bucket <b>`),
   add this rule to that list, then apply the merged list.
4. Standard-IA and Glacier IR bill a 30/90-day minimum and a per-GB retrieval fee; fine for
   logs that are read rarely after ingestion.

## Notes

- Watchtower reads **driver** logs only (`<log path>/*/driver/`), not executor logs or
  Spark event logs. Executor losses and memory kills still appear in the driver log4j.
- `silver.job_run_compute` is rebuilt each run with a lookback window. The hybrid
  views don't depend on it; they map clusters to runs from
  `system.lakeflow.job_task_run_timeline` directly.
- Schedule the pipeline as a triggered update (for example hourly) through a job
  with a pipeline task.
