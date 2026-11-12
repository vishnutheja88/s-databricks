"""Create or update the Workflow Observability Genie space (idempotent).

Run as a HUMAN user (a service principal cannot own a Genie space):

    pip install databricks-sdk
    python src/genie/deploy_genie.py --profile DEFAULT \
        --catalog platform_obs_dev --warehouse-id <warehouse-id>

The space definition lives in src/genie/genie_space.json (tables,
instructions, sample questions). Edit that file and re-run to update the
space. The space is matched by title, so re-runs update instead of duplicating.
"""
import argparse
import hashlib
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _id(text: str) -> str:
    """Stable 32-hex id so re-runs don't reshuffle instructions/questions."""
    return hashlib.md5(text.encode()).hexdigest()


def build_serialized_space(spec: dict, catalog: str) -> str:
    tables = []
    for t in spec["tables"]:
        tbl = {
            "identifier": t["identifier"].replace("${catalog}", catalog),
            "description": [t["description"]],
        }
        cols = [{"column_name": c, "get_example_values": True} for c in t.get("example_value_columns", [])]
        if cols:
            tbl["column_configs"] = sorted(cols, key=lambda c: c["column_name"])
        tables.append(tbl)
    tables.sort(key=lambda t: t["identifier"])  # API expects tables sorted by identifier

    space = {
        "version": 1,
        "config": {
            "sample_questions": [{"id": _id(q), "question": [q]} for q in spec["sample_questions"]],
        },
        "data_sources": {"tables": tables},
        "instructions": {
            "text_instructions": [{"id": _id("instructions"), "content": [line + "\n" for line in spec["instructions"]]}],
        },
    }
    return json.dumps(space)


def find_space(w, title: str):
    token = None
    while True:
        query = {"page_size": 100}
        if token:
            query["page_token"] = token
        resp = w.api_client.do("GET", "/api/2.0/genie/spaces", query=query)
        for s in resp.get("spaces", []):
            if s.get("title") == title:
                return s
        token = resp.get("next_page_token")
        if not token:
            return None


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", required=True)
    p.add_argument("--warehouse-id", required=True)
    p.add_argument("--profile", default=None, help="Databricks CLI profile (default: env / DEFAULT)")
    p.add_argument("--title-suffix", default="", help="e.g. ' (dev)' to keep dev and prod spaces apart")
    p.add_argument("--parent-path", default=None, help="Workspace folder for the space (optional)")
    p.add_argument("--dry-run", action="store_true", help="print the payload, change nothing")
    a = p.parse_args()

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", a.catalog):
        sys.exit(f"invalid catalog name: {a.catalog}")

    with open(os.path.join(HERE, "genie_space.json")) as f:
        spec = json.load(f)
    title = spec["title"] + a.title_suffix
    serialized = build_serialized_space(spec, a.catalog)

    if a.dry_run:
        print(json.dumps(json.loads(serialized), indent=2))
        return

    from databricks.sdk import WorkspaceClient  # imported here so --dry-run works without the SDK

    w = WorkspaceClient(profile=a.profile) if a.profile else WorkspaceClient()
    existing = find_space(w, title)
    if existing:
        sid = existing["space_id"]
        w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{sid}", body={
            "serialized_space": serialized, "title": title,
            "description": spec["description"], "warehouse_id": a.warehouse_id,
        })
        print(f"Updated Genie space '{title}' ({sid})")
    else:
        body = {"serialized_space": serialized, "title": title,
                "description": spec["description"], "warehouse_id": a.warehouse_id}
        if a.parent_path:
            body["parent_path"] = a.parent_path
        sid = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
        print(f"Created Genie space '{title}' ({sid})")
    print(f"Open: {w.config.host.rstrip('/')}/genie/rooms/{sid}")
    print("Share it: open the space > Share > give CAN_RUN to your groups "
          "(they also need SELECT on the gold/silver/config tables).")


if __name__ == "__main__":
    main()
