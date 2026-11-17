# Hybrid Job Optimization (System Tables + Watchtower)

Find which prod Lakeflow jobs waste money or capacity (system tables), then see
**why** (Watchtower's parsed driver logs), in one per-job scorecard with a
prioritized recommendation, an append-only daily history, alerts and a Genie space.

**No-delete by design:** raw logs are kept, Watchtower's tables can't be reset,
the ops layer is views plus one `delta.appendOnly` table, and the SQL runner
refuses DROP / DELETE / TRUNCATE / OVERWRITE / VACUUM / RESTORE.

Complements `../workflow-observability` (multi-workspace, system tables only). This
project targets one workspace (prod, all jobs on job clusters) and adds log signals.

## How it fits together

```
prod job clusters --(cluster policy: log delivery)--> /Volumes/watchtower/default/cluster_logs
        |                                                     |
        |                                    Watchtower pipeline (hourly, retention OFF)
        |                                                     v
        |                                  watchtower.bronze.logs -> watchtower.silver.logs
        v                                                     |
system.compute.node_timeline  system.billing.usage/list_prices |   system.lakeflow.*
        \__________________________ watchtower.ops views _____/
                 v_job_cluster_map  (cluster_id -> job_id, job_run_id)
                 v_job_utilization_daily | v_job_cost_daily | v_job_log_signals_daily
                 v_job_run_health_daily  | v_job_scorecard_30d (top_recommendation)
                                   |
             hybrid-optimization-ops job (daily) -> ops.job_daily_history (append-only)
                                   |
                 AI/BI dashboard  |  Genie space  |  3 SQL alerts
```

## Layout

```
databricks.yml                   bundle, variables (catalog, workspace_id, warehouse_id...), dev/prod targets
resources/ops_job.yml            setup -> build_views -> daily_snapshot (serverless, daily 05:37 ET)
resources/alerts.yml             OOM jobs (24h), failed runs (yesterday), per-job cost spike
src/run_sql.py                   runs SQL files; ${catalog} ${workspace_id} ${snapshot_date_expr}; no-delete guard
src/sql/00_setup.sql             ops schema + append-only job_daily_history
src/sql/10_views.sql             six read-only views
src/sql/20_daily_snapshot.sql    idempotent daily INSERT (skips a day already written)
src/sql/90_verify_no_delete.sql  manual checks
src/dashboards/queries.sql       dashboard datasets + which visual to build
src/genie/genie_space.json       Genie tables, instructions, sample questions
src/genie/deploy_genie.py        create/update the Genie space (run as a human user)
policies/prod-job-cluster-policy.json   log delivery + init script (+ optional spot defaults)
watchtower/no-delete.patch       hardens Watchtower (retention OFF, no reset, prevent_destroy)
watchtower/prod-target.yml       Watchtower prod target with retention OFF
```

## Rollout

1. **Prereqs.** Account admin enables `system.billing`, `system.compute`, `system.lakeflow`.
   Catalog `watchtower` exists. The bucket behind its storage has no S3 Expiration rule.
2. **Log delivery.** Merge `policies/prod-job-cluster-policy.json` into the policy your prod
   jobs use (the two `aws_attributes` entries are optional defaults; drop them if you
   don't want spot changes). Jobs set `policy_id` and `apply_policy_default_values: true`.
   The jobs' run-as principal needs `READ VOLUME` + `WRITE VOLUME` on `cluster_logs`.
3. **Watchtower.** Follow `watchtower/README.md` (apply the patch, add the prod target, deploy).
   Check: `SELECT log_source, count(*) FROM watchtower.silver.logs GROUP BY 1`.
4. **This bundle.** Fill the `<...>` values in `databricks.yml`, then:

   ```bash
   cd hybrid-optimization
   databricks bundle validate -t prod
   databricks bundle deploy   -t prod
   databricks bundle run hybrid_ops -t prod
   # backfill one past day at a time (history is append-only, re-runs are no-ops):
   databricks bundle run hybrid_ops -t prod --params snapshot_date=2026-10-20
   ```

   The job's service principal needs SELECT on `system.billing`, `system.compute`,
   `system.lakeflow` and `watchtower.silver`, plus USE/CREATE on `watchtower.ops`.
5. **Genie.** `python src/genie/deploy_genie.py --catalog watchtower --warehouse-id <id>`
   (as a human user; `--dry-run` to preview).
6. **Dashboard.** Build from `src/dashboards/queries.sql` in the UI.
7. **Verify.** Run `src/sql/90_verify_no_delete.sql`.
8. **Grants.** Team gets SELECT on `watchtower.ops` and `watchtower.silver`; only the SP writes.

## Reading the scorecard

`top_recommendation` picks one action per job, in this order:

| Signal | Change | Success check |
| --- | --- | --- |
| `oom_events > 0` or `max_swap > 5` | r-family workers or larger driver; AQE skew handling; more shuffle partitions | 0 OOM for 7 days |
| `spill_events > 0` and `avg_mem > 70` | more shuffle partitions or more memory per core | spill drops, runtime falls |
| `lost_executor_events > 0` | `SPOT_WITH_FALLBACK`, `first_on_demand >= 1`, `zone_id: auto` | 0 lost executors |
| `failed_runs > 0` | fix the failure first (error lines in `silver.logs`) | 0 failed runs |
| `clusters_per_run > 1` | share one cluster across tasks (`job_cluster_key`) | 1 cluster per run |
| `p95_cpu < 40` and `avg_mem < 50` | cut `max_workers` 25-50% or one instance size down | cost down, runtime within ~10% |
| `avg_cpu > 85` | c-family nodes or more workers; test Photon on SQL/DataFrame jobs | runtime falls |

Change one thing per job, then compare 7 days before/after in `ops.job_daily_history`.

## Caveats

- Cost is **list price**; negotiated discounts make real spend lower.
- `node_timeline` covers classic compute only, and only worker nodes are measured
  (single-node jobs show blank CPU/memory).
- Watchtower reads driver logs only; the log patterns in `v_job_log_signals_daily` are
  regexes and may need tuning to your log formats.
- Not yet run against a live workspace. Static checks passed (YAML/JSON parse, Python
  compile, SQL parsed with sqlglot's databricks dialect, runner unit checks). Expect to
  fix minor column-name differences on first run.
