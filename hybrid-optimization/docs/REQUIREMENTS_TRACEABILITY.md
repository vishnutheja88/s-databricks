# Requirements traceability

Story: *As a Databricks platform or engineering team member, I want to review system tables, Spark logs, and
job logs and conduct a Genie-assisted optimization pilot on selected workspaces, so that I can identify cost
drivers, performance bottlenecks, and optimization opportunities and produce actionable recommendations.*

## Acceptance criteria

| # | Criterion | Delivered by | Evidence |
| --- | --- | --- | --- |
| 1 | Selected workspaces, clusters, jobs, review period documented | `workspaces`, `review_start`, `review_end` bundle variables; `ops.v_scope`; `ops.scope_history`; `docs/PILOT_SCOPE.md` | Dashboard "Scope & validation" page; jobs and clusters per workspace on "Workspaces" and "Cost & utilization" |
| 2 | Access to system tables, Spark logs, job logs confirmed | `access_check` task -> `ops.access_checks` (fails the run if a required source is unreadable) | Dashboard "Scope & validation" page |
| 3 | Compute usage, DBUs, duration, failures, retries, utilization analyzed | `v_job_daily_metrics`, `v_job_scorecard_30d`: dbus, cost, avg/max minutes, failed_runs, retries, CPU/memory/swap, idle | Dashboard pages "Cost & utilization", "Job performance", "Reliability" |
| 4 | High-cost or underutilized workloads identified | Scorecard ranking by cost; OVERSIZED / IDLE / COST_UP_UTIL_DOWN; `v_cluster_idle` | Executive summary, Cost & utilization |
| 5 | Slow jobs and likely Spark bottlenecks documented | Runtime and runtime trend; event-log shuffle, spill, skew; driver-log OOM, spill, lost executors; slow SQL | "Job performance" page; notebook section 4 |
| 6 | Genie queries tested on approved, governed data | Genie space over Unity Catalog `ops` views only, engineers-only grants; `src/genie/benchmarks.json` | `validation_results` kind `genie_benchmark` |
| 7 | Genie findings validated against source system tables and logs | `validate` task (independent raw-source queries) + manual profiling checks | `validation_results` kinds `auto` and `manual` |
| 8 | At least one opportunity per selected workspace | `workspace_rank`, `v_workspace_summary_latest.top_opportunity` | "Workspaces" page |
| 9 | Recommendations cover cost and performance | See table below | "Recommendations" page |
| 10 | Estimated benefits, effort, risks, owners documented | `est_monthly_savings_usd`, `est_annual_savings_usd`, `fix_effort`, `risk_level`, `risk`, `owner` | Executive summary action plan; notebook fix plan |
| 11 | Pilot results presented to stakeholders | Dashboard + `docs/PILOT_REPORT_OUTLINE.md` (deck built from it) | Stakeholder review |
| 12 | No production changes without review and approval | Everything is read-only/append-only; the notebook proposes changes, `fix_plan_history.approval_status`; deploying this bundle itself goes through change review | README rollout; CLAUDE.md hard rules |

## Recommendation areas

| Area | Signal(s) | Where |
| --- | --- | --- |
| Cluster sizing and autoscaling | OVERSIZED, CPU_BOUND, IDLE, COST_UP_UTIL_DOWN | category / finding_codes |
| Job or cluster scheduling | MULTI_CLUSTER, SCHEDULE_PEAK (`v_schedule_hotspots`) | category / finding_codes |
| Cluster policies | NO_POLICY; `policies/prod-job-cluster-policy.json` | finding_codes |
| Photon or runtime selection | PHOTON_CANDIDATE, RUNTIME_OUTDATED | finding_codes |
| Spark partitioning and file sizing | SPILL, DATA_LAYOUT (small files) | category / finding_codes |
| Query optimization | SLOW_QUERIES (`v_query_hotspots`) | finding_codes |
| Join and shuffle behavior | HIGH_SHUFFLE, SKEW (event logs) | finding_codes |
| Data layout and maintenance | DATA_LAYOUT (unmaintained tables: no clustering, no recent OPTIMIZE) | finding_codes |
| Idle-cluster reduction | IDLE, OVERSIZED, `v_cluster_idle` | finding_codes / Cost & utilization |

## Example Genie questions

All eight are the first sample questions in the Genie space and have ground-truth SQL in `src/genie/benchmarks.json`.

| Question | Answered from |
| --- | --- |
| Which jobs consumed the most DBUs during the review period? | `v_job_scorecard_latest.dbus` |
| Which clusters had the highest idle time? | `v_cluster_idle` |
| Which jobs had the longest runtime trend? | `runtime_change_pct`, `avg_minutes` |
| Which jobs experienced the most failures or retries? | `failed_runs`, `retries` |
| Which workloads have increasing cost but declining utilization? | `cost_change_pct`, `p95_cpu_change` |
| What jobs generated the largest shuffle volume? | `shuffle_read_gb`, `shuffle_write_gb` |
| Which clusters are consistently oversized? | `category = 'OVERSIZED'` |
| What optimization opportunities are suggested for the highest-cost jobs? | `top_recommendation`, `secondary_findings` |

## Deliverables

| # | Deliverable | Where |
| --- | --- | --- |
| 1 | Workspace and workload scope | `docs/PILOT_SCOPE.md` + Scope & validation page |
| 2 | Cost and utilization summary | Executive summary + Cost & utilization pages |
| 3 | Job performance analysis | Job performance + Reliability pages; notebook section 4 |
| 4 | Genie-assisted analysis results | Genie space; benchmark results in `validation_results` |
| 5 | Validated optimization recommendations | Scorecard + `validation_results` (auto, manual, genie_benchmark) |
| 6 | Prioritized action plan | `priority_rank` / `workspace_rank`; notebook fix plan; `fix_plan_history` |
| 7 | Pilot findings and stakeholder presentation | `docs/PILOT_REPORT_OUTLINE.md` + dashboard |
