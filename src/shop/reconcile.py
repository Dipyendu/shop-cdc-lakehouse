"""Compare every silver current-state table with its PostgreSQL table.

Both sides are read row by row and every column is turned into one canonical
text form (same function for both sides), then hashed per row:
  NULL -> null; integers -> digits; NUMERIC/DECIMAL -> exact string with its scale;
  timestamps -> UTC ISO-8601 with 6 fractional digits and 'Z'; booleans -> true/false.
A table passes when the row counts are equal and no row hash differs.

Usage:
  python -m shop.reconcile --catalog C [--silver shop_silver] [--override customers=C.S.T]
  python -m shop.reconcile --catalog C --table-hashes --silver S   (hashes only, lake side)
Needs DATABRICKS_HOST, DATABRICKS_TOKEN and SHOP_WAREHOUSE_HTTP_PATH.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import sys
from decimal import Decimal

COLUMNS = {
    "customers": ["customer_id", "name", "email", "city", "country", "created_at", "updated_at"],
    "products": ["product_id", "name", "category", "price", "updated_at"],
    "orders": ["order_id", "customer_id", "status", "ordered_at", "updated_at"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "updated_at"],
    "payments": ["payment_id", "order_id", "amount", "method", "status", "updated_at"],
}


def canon(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=dt.UTC)  # lake side reads in a UTC session
        return v.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if isinstance(v, str):
        return v
    raise TypeError(f"no canonical form for {type(v).__name__}")


def row_hash(row) -> str:
    text = json.dumps([canon(v) for v in row], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def table_hash(rows) -> str:
    h = hashlib.sha256()
    for rh in sorted(row_hash(r) for r in rows):
        h.update(rh.encode())
    return h.hexdigest()


def compare(source_rows, lake_rows, show: int = 20) -> dict:
    """Rows are tuples with the primary key first."""
    src = {r[0]: row_hash(r) for r in source_rows}
    lake = {r[0]: row_hash(r) for r in lake_rows}
    if len(src) != len(source_rows) or len(lake) != len(lake_rows):
        raise ValueError("duplicate primary key on one side")
    missing = sorted(set(src) - set(lake))
    extra = sorted(set(lake) - set(src))
    differing = sorted(k for k in set(src) & set(lake) if src[k] != lake[k])
    return {
        "source_rows": len(src), "lake_rows": len(lake),
        "missing_in_lake": len(missing), "extra_in_lake": len(extra), "differing_rows": len(differing),
        "examples": {"missing": missing[:show], "extra": extra[:show], "differing": differing[:show]},
        "pass": len(src) > 0 and not missing and not extra and not differing,  # an empty source proves nothing
    }


def pg_rows(dsn: str, table: str):
    import psycopg

    cols = COLUMNS[table]
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(cols)} FROM {table} ORDER BY 1")  # table and column names are fixed constants
        return [tuple(r) for r in cur.fetchall()]


def lake_rows(conn, full_name: str, table: str, include_deleted: bool = False):
    cols = list(COLUMNS[table])
    where = "" if include_deleted else "WHERE NOT _deleted"
    if include_deleted:
        cols += ["_lsn", "_deleted"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(cols)} FROM {full_name} {where} ORDER BY 1")
        return [tuple(r) for r in cur.fetchall()]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="silver vs PostgreSQL reconciliation")
    p.add_argument("--catalog", required=True)
    p.add_argument("--silver", default="shop_silver")
    p.add_argument("--dsn", default=os.environ.get("SHOP_PG_DSN", "postgresql://shop:shop@127.0.0.1:5433/shop"))
    p.add_argument("--override", action="append", default=[], help="table=catalog.schema.table")
    p.add_argument("--table-hashes", action="store_true", help="print lake table hashes (all rows, marks too)")
    a = p.parse_args(argv)
    overrides = dict(o.split("=", 1) for o in a.override)
    from shop.dbx import sql_conn

    conn = sql_conn()
    result = {}
    try:
        for t in COLUMNS:
            name = overrides.get(t, f"{a.catalog}.{a.silver}.{t}")
            if a.table_hashes:
                rows = lake_rows(conn, name, t, include_deleted=True)
                result[t] = {"table": name, "rows": len(rows), "table_hash": table_hash(rows)}
            else:
                result[t] = {"table": name, **compare(pg_rows(a.dsn, t), lake_rows(conn, name, t))}
    finally:
        conn.close()
    if not a.table_hashes:
        result["ALL_PASS"] = all(v["pass"] for v in result.values())
    print(json.dumps(result, indent=2, default=str))
    return 0 if a.table_hashes or result["ALL_PASS"] else 1


if __name__ == "__main__":
    sys.exit(main())
