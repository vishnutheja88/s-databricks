# Job Optimization: business guide

**Access:** engineers only. The dashboard, Genie space and log tables are shared with the
engineering group, because raw log lines can contain sensitive values. Engineers share
findings and savings with business owners.

**What it answers:** for the selected workspaces and review period, what jobs cost, where the cost and
performance problems are, which jobs to fix first, what to change, who owns it, and whether the fix worked.

**Where the value comes from:** every job gets one top fix type, secondary findings, an estimated
monthly saving, effort, risk, owner and a priority rank (overall and per workspace). Work goes
top-down by rank; every workspace gets at least one opportunity.

| Fix type | What it means | Estimated saving counted | Effort |
| --- | --- | --- | --- |
| OVERSIZED | Cluster mostly idle (p95 CPU < 40%, memory < 50%) | Job cost x idle share, max 50% | Low |
| LOST_EXECUTORS | Spot nodes reclaimed mid-run | Spend on failed runs | Low |
| MULTI_CLUSTER | Each task starts its own cluster | 10% of job cost | Medium |
| MEMORY | Out-of-memory errors or swapping | Spend on failed runs | Medium |
| FAILING | Runs fail for code or data reasons | Spend on failed runs | High |
| SPILL | Data spills to disk, runs slow | Runtime only, $0 counted | Medium |
| CPU_BOUND | Cluster maxed out, runs slow | Runtime only, $0 counted | Low |

All money is **contract cost**: usage x effective list price x our contract multiplier (1.093).
Savings are **estimates**.

Secondary findings cover the other areas: retries, data skew, large shuffle, slow SQL, small files and
table maintenance, outdated runtime, Photon candidates, missing cluster policy, peak-hour schedules,
idle node time, cost rising while utilization falls, and growing runtime.

## The 4-step loop

1. **See it:** open the **Job Optimization** dashboard. Executive summary (spend, estimated savings,
   failed-run spend, ranked action plan), Workspaces (top opportunity per workspace), Cost & utilization,
   Job performance, Reliability, Recommendations, Trends, Scope & validation.
2. **Ask it:** open the **Hybrid Job Optimization** Genie space and use the prompts below.
3. **Fix it:** an engineer runs `notebooks/optimization_insights.py`. It turns the top N jobs into a
   fix plan: current cluster config -> proposed change, plus the log lines behind each failure.
   It changes nothing itself; the engineer applies one change per job.
4. **Prove it:** confirm findings in Spark UI / compute metrics / query profile and record them in
   `notebooks/validate_findings.py`; after an approved change, compare 7 days before vs after in the
   fix-plan notebook. The Trends page shows the same over time.

## Genie questions

The pilot's eight questions (also benchmarked with ground-truth SQL in `src/genie/benchmarks.json`):

1. Which jobs consumed the most DBUs during the review period?
2. Which clusters had the highest idle time?
3. Which jobs had the longest runtime trend?
4. Which jobs experienced the most failures or retries?
5. Which workloads have increasing cost but declining utilization?
6. What jobs generated the largest shuffle volume?
7. Which clusters are consistently oversized?
8. What optimization opportunities are suggested for the highest-cost jobs?

More useful questions:

- What is the top optimization opportunity in each workspace, with owner and estimated saving?
- Which quick wins (low effort, low risk) save the most money?
- How much did we spend on failed runs, and which jobs caused it?
- Which jobs have data skew or spill, and what should we change?
- Which jobs run on an outdated runtime or without a cluster policy?
- Which tables written by our jobs have small-file or maintenance problems?
- Which hours have the most job starts, and which jobs could be moved?
- What are the slowest SQL queries in the review period?
- Give me a one-paragraph summary of the pilot findings for stakeholders.
