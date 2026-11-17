"""Shared parameters for every task in the hybrid-optimization job.

Scope = which workspaces and which review period the pilot analyzes:
  --workspaces    "1234567890123456:prod,2345678901234567:analytics"  (id:name, comma-separated)
  --review_start  YYYY-MM-DD  (empty = rolling: 30 days before review_end)
  --review_end    YYYY-MM-DD  (empty = today)

These become SQL placeholders (see substitutions()):
  ${catalog} ${workspace_ids} ${scope_values} ${review_start_expr} ${review_end_expr}
  ${review_label} ${price_multiplier} ${idle_cpu_pct} ${lts_code} ${snapshot_date_expr}
"""
import argparse
import datetime
import re


def add_common_args(p):
    p.add_argument("--catalog", required=True)
    p.add_argument("--workspaces", required=True, help="id:name,id:name")
    p.add_argument("--review_start", default="")
    p.add_argument("--review_end", default="")
    p.add_argument("--price_multiplier", default="1.093",
                   help="cost = usage_quantity x pricing.effective_list.default x this")
    p.add_argument("--idle_cpu_pct", default="10", help="worker node-minutes below this CPU percent count as idle")
    p.add_argument("--latest_lts", default="15.4", help="current LTS runtime, e.g. 15.4")
    p.add_argument("--snapshot_date", default="", help="YYYY-MM-DD for backfill; empty = 2 days ago")
    return p


def parse_workspaces(spec):
    out = []
    for item in [s.strip() for s in spec.split(",") if s.strip()]:
        wid, _, name = item.partition(":")
        wid, name = wid.strip(), (name.strip() or wid.strip())
        if not re.fullmatch(r"[0-9]+", wid):
            raise ValueError(f"workspace id must be numeric: {item}")
        if not re.fullmatch(r"[A-Za-z0-9 _.\-]+", name):
            raise ValueError(f"workspace name has unsupported characters: {name}")
        out.append((wid, name))
    if not out:
        raise ValueError("--workspaces is empty")
    if len({w for w, _ in out}) != len(out):
        raise ValueError("duplicate workspace id in --workspaces")
    return out


def _date(s):
    return datetime.date.fromisoformat(s.strip()) if s and s.strip() else None


def substitutions(a):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", a.catalog):
        raise ValueError(f"invalid catalog name: {a.catalog}")
    ws = parse_workspaces(a.workspaces)

    start, end = _date(a.review_start), _date(a.review_end)
    if start and end and start > end:
        raise ValueError("review_start is after review_end")
    end_expr = f"DATE'{end.isoformat()}'" if end else "current_date()"
    start_expr = f"DATE'{start.isoformat()}'" if start else f"date_sub({end_expr}, 30)"
    if start or end:
        label = f"{start.isoformat() if start else '30 days before end'} to {end.isoformat() if end else 'today'}"
    else:
        label = "rolling 30 days"

    mult = float(a.price_multiplier)
    if not 0 < mult < 5:
        raise ValueError(f"price_multiplier out of range: {a.price_multiplier}")
    idle = float(a.idle_cpu_pct)
    if not 0 < idle < 100:
        raise ValueError(f"idle_cpu_pct out of range: {a.idle_cpu_pct}")
    m = re.fullmatch(r"(\d+)\.(\d+)", a.latest_lts.strip())
    if not m:
        raise ValueError(f"latest_lts must look like 15.4, got: {a.latest_lts}")

    snap = _date(a.snapshot_date)
    return {
        "catalog": a.catalog,
        "workspace_ids": ", ".join(f"'{w}'" for w, _ in ws),
        "scope_values": ", ".join(f"('{w}', '{n}')" for w, n in ws),
        "review_start_expr": start_expr,
        "review_end_expr": end_expr,
        "review_label": label,
        "price_multiplier": repr(mult),
        "idle_cpu_pct": repr(idle),
        "lts_code": str(int(m.group(1)) * 100 + int(m.group(2))),
        "snapshot_date_expr": f"DATE'{snap.isoformat()}'" if snap else "date_sub(current_date(), 2)",
    }


def parser(description):
    return add_common_args(argparse.ArgumentParser(description=description))
