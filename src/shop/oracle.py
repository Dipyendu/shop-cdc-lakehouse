"""Pure-Python model of the silver merge rule, used to check the Databricks result.

For each key, the event with the highest PostgreSQL log position (source.lsn) wins; a
delete keeps the key with all data columns NULL and _deleted = true. The order and
repetition of events do not matter. The result has the same rows as
reconcile.lake_rows(..., include_deleted=True): data columns, then _lsn, then _deleted.

Usage: python -m shop.oracle --landing landing --silver shop_silver
  compares the oracle's table hashes, built from the landing files, with the silver tables.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from decimal import Decimal
from pathlib import Path

from shop.reconcile import COLUMNS, table_hash

INT_COLS = {"customer_id", "product_id", "order_id", "order_item_id", "payment_id", "quantity"}
DEC_COLS = {"price", "unit_price", "amount"}
TS_COLS = {"created_at", "updated_at", "ordered_at"}


def typed(col: str, v):
    if v is None:
        return None
    if col in INT_COLS:
        return int(v)
    if col in DEC_COLS:
        return Decimal(v)
    if col in TS_COLS:
        return dt.datetime.fromisoformat(v)
    return v


def fold(lines, table: str, state: dict | None = None) -> dict:
    """Apply landing lines (JSON strings) for one table; returns {key: row tuple}."""
    cols = COLUMNS[table]
    pk = cols[0]
    state = {} if state is None else state
    for line in lines:
        rec = json.loads(line)
        ev = json.loads(rec["payload"])
        key = json.loads(rec["kafka_key"])[pk]
        lsn = int(ev["source"]["lsn"])
        cur = state.get(key)
        if cur is not None and cur[-2] >= lsn:
            continue  # not newer: duplicate or late event
        if ev["op"] == "d":
            state[key] = (key, *([None] * (len(cols) - 1)), lsn, True)
        else:
            after = ev["after"]
            state[key] = (*(typed(c, after[c]) for c in cols), lsn, False)
    return state


def fold_dir(landing: Path, table: str) -> dict:
    state: dict = {}
    for f in sorted((landing / table).glob("*.jsonl")):
        with open(f) as fh:
            fold(fh, table, state)
    return state


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--landing", default="landing")
    p.add_argument("--silver", required=True)
    a = p.parse_args(argv)
    from shop.dbx import CATALOG, sql_conn
    from shop.reconcile import lake_rows

    conn = sql_conn()
    out = {}
    for t in COLUMNS:
        model = list(fold_dir(Path(a.landing), t).values())
        lake = lake_rows(conn, f"{CATALOG}.{a.silver}.{t}", t, include_deleted=True)
        out[t] = {"oracle_rows": len(model), "silver_rows": len(lake),
                  "oracle_hash": table_hash(model), "silver_hash": table_hash(lake)}
        out[t]["identical"] = len(model) > 0 and out[t]["oracle_hash"] == out[t]["silver_hash"]
    out["ALL_IDENTICAL"] = all(v["identical"] for v in out.values() if isinstance(v, dict))
    print(json.dumps(out, indent=2))
    return 0 if out["ALL_IDENTICAL"] else 1


if __name__ == "__main__":
    sys.exit(main())
