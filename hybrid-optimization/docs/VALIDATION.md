# Validating findings

Findings are not presented until they are validated three ways. All results are appended to
`ops.validation_results` and shown on the dashboard's "Scope & validation" page.

## 1. Automated: ops layer vs raw sources (daily, `validate` task)

Re-computed straight from system tables and logs, independent of the ops views, per workspace:
total job DBUs, total contract cost, top 10 jobs by DBUs, runs, failed runs, task retries,
out-of-memory log lines, and average worker CPU of the most expensive job. A failure means
investigate before presenting, not stop.

## 2. Genie benchmarks (once per pilot, and after changing the Genie space)

1. Open the Genie space, Benchmarks tab.
2. Add every question in `src/genie/benchmarks.json` with its ground-truth SQL.
3. Run the benchmark; for each question record pass/fail in `notebooks/validate_findings.py`
   (kind = `genie_benchmark`).

## 3. Standard Databricks tools (for each workspace's top 3 actions)

| Finding | Open | Confirm |
| --- | --- | --- |
| OVERSIZED / IDLE | Compute > Metrics for a recent run | CPU and memory stay low across the run |
| MEMORY | Spark UI > Executors; Compute > Metrics | Failed tasks with OOM; memory near 100% |
| SPILL / HIGH_SHUFFLE / SKEW | Spark UI > Stages (sort by duration) | Spill (disk) on big stages; shuffle read size; max task far above median |
| LOST_EXECUTORS | Compute > Event log | Node lost / spot instance reclaimed events |
| FAILING / RETRIES | Job run page > failed task | Error and stack trace; repeated attempts |
| SLOW_QUERIES | Query history > Query profile | Scan, join or spill operator dominating time |
| DATA_LAYOUT | Catalog Explorer > table > Details / History | File count and size; last OPTIMIZE |
| MULTI_CLUSTER / SCHEDULE_PEAK | Job definition; Jobs list by schedule | Tasks with separate clusters; many jobs at the same hour |

Record each check in `notebooks/validate_findings.py` (kind = `manual`, check name includes the job id).
