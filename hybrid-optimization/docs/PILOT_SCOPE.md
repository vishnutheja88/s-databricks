# Pilot scope (fill in before the first run)

The values below must match `databricks.yml` (prod target). The daily job records them in
`ops.scope_history` and checks access in `ops.access_checks`.

| Item | Value |
| --- | --- |
| Workspaces (id : name) | `<id>:<name>`, `<id>:<name>` |
| Review period | `<YYYY-MM-DD>` to `<YYYY-MM-DD>` |
| Catalog for Watchtower + ops | `watchtower` |
| Jobs in scope | All jobs with usage in the selected workspaces during the review period (job count on the Workspaces page) |
| Clusters in scope | All job clusters those jobs launched (Cost & utilization page) |
| Cost basis | Contract cost = usage x effective list price x 1.093 |
| Pilot owner | `<name>` |
| Stakeholders | `<names / teams>` |
| Access: engineering group with SELECT on `watchtower.ops`, `watchtower.silver` | `<group name>` |
| Change approval process for recommended fixes | `<CAB / PR review>` |

## Access sign-off

- [ ] `system.billing`, `system.compute`, `system.lakeflow` enabled and readable by the job's service principal
- [ ] Optional: `system.access` (lineage) and `system.query` (query history) enabled
- [ ] Cluster log delivery to `/Volumes/watchtower/default/cluster_logs/cluster-logs` on in-scope job clusters
- [ ] Watchtower deployed with `watchtower/no-delete.patch`
- [ ] First run's "Access checks" table on the dashboard shows every required source OK
