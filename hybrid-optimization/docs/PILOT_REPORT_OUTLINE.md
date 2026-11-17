# Pilot findings and stakeholder presentation (outline)

Build the deck from the dashboard once validation passes. One slide per item; every number comes
from the named source so it can be re-checked.

| Slide | Content | Source |
| --- | --- | --- |
| 1. Scope | Workspaces, review period, jobs and clusters analyzed, access confirmed | Scope & validation page; Workspaces page |
| 2. Cost and utilization | Spend (contract), top cost drivers, utilization, idle time | Executive summary; Cost & utilization |
| 3. Job performance | Slowest and slowing jobs, shuffle/spill/skew hot spots, failures and retries | Job performance; Reliability |
| 4. Genie-assisted analysis | The eight pilot questions with Genie answers; benchmark pass rate | Genie space; `validation_results` (genie_benchmark) |
| 5. Validation | Auto checks passed, manual tool confirmations | Scope & validation page |
| 6. Recommendations | Top action per workspace; cost and performance areas covered | Workspaces; Recommendations |
| 7. Prioritized action plan | Rank, job, owner, change, effort, risk, estimated monthly/annual saving | Executive summary table; notebook fix plan |
| 8. Approval and next steps | Changes requested for review, rollout order (one job at a time), how success is measured (before/after) | `fix_plan_history`; notebook section 5 |

Caveats to state: costs are contract cost at list x 1.093; savings are estimates; SQL query signals cover
SQL warehouse and serverless only; no production change was made during the pilot.
