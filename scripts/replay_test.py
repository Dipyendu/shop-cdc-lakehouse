"""Replay: drop the silver tables, rebuild them from bronze, and compare table hashes (all rows,
delete marks and _lsn included) before and after. The test also asserts that each table was
really recreated during the run (its version 0 is newer than the run start), so a run that
rebuilt nothing cannot pass. Replay merges into empty tables, so it does not exercise the
"apply only if newer" rule against stored rows; the disorder test does.

Usage: python scripts/replay_test.py [--bronze shop_bronze --silver shop_silver --prefix shop]
Exit code 0 only if every table is identical and was recreated.
"""
import argparse
import datetime as dt
import json
import sys

from shop.dbx import CATALOG, client, landing_root, pipeline_params, run_job, sql_conn
from shop.reconcile import COLUMNS, lake_rows, table_hash

p = argparse.ArgumentParser()
p.add_argument("--bronze", default="shop_bronze")
p.add_argument("--silver", default="shop_silver")
p.add_argument("--prefix", default="shop")
a = p.parse_args()
conn = sql_conn()


def hashes():
    out = {}
    for t in COLUMNS:
        rows = lake_rows(conn, f"{CATALOG}.{a.silver}.{t}", t, include_deleted=True)
        out[t] = {"rows": len(rows), "table_hash": table_hash(rows)}
    return out


def history(t):
    with conn.cursor() as cur:
        cur.execute(f"DESCRIBE HISTORY {CATALOG}.{a.silver}.{t}")
        rows = [r.asDict() for r in cur.fetchall()]
    first = min(rows, key=lambda r: r["version"])
    last = max(rows, key=lambda r: r["version"])
    return {"version0_operation": first["operation"], "version0_timestamp": first["timestamp"],
            "last_version": last["version"], "last_operation": last["operation"]}


before = hashes()
hist_before = {t: history(t) for t in COLUMNS}
started = dt.datetime.now(dt.UTC)
run = run_job(client(), f"replay: drop {a.silver}, rebuild from {a.bronze}", f"bronze {a.bronze}",
              pipeline_params(a.bronze, a.silver, f"{landing_root()}/{a.prefix}", None, "--rebuild-silver"))
after = hashes()
hist_after = {t: history(t) for t in COLUMNS}
recreated = {t: hist_after[t]["version0_timestamp"] >= started for t in COLUMNS}
identical = {t: before[t] == after[t] and before[t]["rows"] > 0 for t in COLUMNS}
ok = all(identical.values()) and all(recreated.values())
print(json.dumps({"before": before, "after": after, "run": run, "run_started_utc": started,
                  "history_before": hist_before, "history_after": hist_after,
                  "identical": identical, "recreated_during_run": recreated, "PASS": ok}, indent=2, default=str))
sys.exit(0 if ok else 1)
