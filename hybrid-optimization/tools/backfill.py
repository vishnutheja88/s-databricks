"""Backfill ops.job_daily_history one day at a time, oldest first.

    python3 tools/backfill.py --start 2026-09-01 --end 2026-11-14 --target prod

Runs `databricks bundle run hybrid_ops --params snapshot_date=<day>` per day
(run from the hybrid-optimization folder, authenticated to the workspace).
Days already written are skipped by the job itself, so re-running is safe.

Run it right after the first deploy: system tables keep node-level metrics
for a limited window, so older days lose their CPU/memory columns.
"""
import argparse
import datetime
import subprocess
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, help="first day, YYYY-MM-DD")
    p.add_argument("--end", required=True, help="last day, YYYY-MM-DD (at most 2 days ago)")
    p.add_argument("--target", default="prod")
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    start = datetime.date.fromisoformat(a.start)
    end = datetime.date.fromisoformat(a.end)
    latest = datetime.date.today() - datetime.timedelta(days=2)
    if end > latest:
        print(f"--end moved to {latest}: newer days are written by the daily schedule")
        end = latest
    if start > end:
        sys.exit("nothing to do: start is after end")

    day = start
    while day <= end:
        cmd = ["databricks", "bundle", "run", "hybrid_ops", "-t", a.target,
               "--params", f"snapshot_date={day.isoformat()}"]
        print(" ".join(cmd))
        if not a.dry_run:
            subprocess.run(cmd, check=True)
        day += datetime.timedelta(days=1)


if __name__ == "__main__":
    main()
