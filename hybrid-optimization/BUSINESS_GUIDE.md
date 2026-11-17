# Job Optimization: business guide

**Access:** engineers only. The dashboard, Genie space and log tables are shared with the
engineering group, because raw log lines can contain sensitive values. Engineers share
findings and savings with business owners.

**What it answers:** how much prod Databricks jobs cost, how much we can save, which
jobs to fix first, what to change, and whether the fix worked.

**Where the value comes from:** every prod job gets one fix type, an estimated monthly
saving, a fix effort and a priority rank. Work goes top-down by rank, cheapest wins first.

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

## The 4-step loop

1. **See it:** open the **Job Optimization** dashboard (Executive summary page). Spend, estimated
   savings per month and year, failed-run spend, and the ranked action list.
2. **Ask it:** open the **Hybrid Job Optimization** Genie space and use the prompts below.
3. **Fix it:** an engineer runs `notebooks/optimization_insights.py`. It turns the top N jobs into a
   fix plan: current cluster config -> proposed change, plus the log lines behind each failure.
   It changes nothing itself; the engineer applies one change per job.
4. **Prove it:** in the same notebook, set the job and change date to see 7 days before vs after
   (cost, runtime, failures, CPU, OOM). The Trends page shows the same over time.

## Genie prompts for business users

Copy and paste into the Genie space.

**Money and savings**
- How much did our prod jobs cost in the last 30 days, and how much can we save per month and per year?
- Show estimated savings by fix type.
- What are the top 10 fixes to do first, with estimated monthly savings and effort?
- Which quick wins (low effort) save the most money?
- How much money did we waste on failed runs last month, and which jobs caused it?

**Capacity and right-sizing**
- Which jobs are oversized, and how much would right-sizing save?
- Which jobs use less than 40% of their CPU but cost more than $500 a month?
- Which jobs start more than one cluster per run?

**Reliability**
- Which jobs are running out of memory, and what should we change?
- Which jobs failed most often in the last 30 days, and what did the failures cost?
- Which jobs lose executors, and is that costing us failed runs?

**Trends and accountability**
- Which jobs' cost grew the most in the last 30 days compared with the 30 days before?
- Show the daily cost trend for the most expensive job over the last 90 days.
- Did job "<name>" get cheaper or faster after <date>?
- Give me a one-paragraph summary of our job optimization opportunities for leadership.
