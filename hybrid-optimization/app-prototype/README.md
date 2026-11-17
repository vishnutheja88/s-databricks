# Observability app prototype

`index.html` is a single-file UI prototype of the Lakehouse Job Observability console: filters on the
left, insights and reports in the middle, an optimization assistant on the right. It runs on demo data
shaped like `watchtower.ops.v_job_scorecard_latest` and scored with the same rules as the bundle.

- Open it locally: double-click `index.html` (fonts need internet; everything else is in the file).
- The assistant uses Claude when opened as a claude.ai artifact, and built-in answers anywhere else.
- To build the live Databricks App, give [HANDOFF_FOR_CLAUDE.md](HANDOFF_FOR_CLAUDE.md) to Claude Code in
  this repo: it covers architecture, endpoints, field mapping, permissions, rules and acceptance checks.
