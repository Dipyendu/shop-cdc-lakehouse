"""Reconciliation can-fail check: copy silver customers to a test schema, change one row,
and confirm that reconciliation reports exactly that row."""
import argparse
import json
import os
import sys

from shop.dbx import CATALOG, query, sql_conn
from shop.reconcile import compare, lake_rows, pg_rows

p = argparse.ArgumentParser()
p.add_argument("--silver", default="shop_silver")
a = p.parse_args()
DSN = os.environ.get("SHOP_PG_DSN", "postgresql://shop:shop@127.0.0.1:5433/shop")
S, T = f"{CATALOG}.{a.silver}.customers", f"{CATALOG}.shop_test.customers"
conn = sql_conn()
query(conn, f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.shop_test")
query(conn, f"CREATE OR REPLACE TABLE {T} AS SELECT * FROM {S}")
key = query(conn, f"SELECT min(customer_id) FROM {T} WHERE NOT _deleted")[0][0]
query(conn, f"UPDATE {T} SET city = concat(city, ' (changed)') WHERE customer_id = {key}")
r = compare(pg_rows(DSN, "customers"), lake_rows(conn, T, "customers"))
untouched = compare(pg_rows(DSN, "customers"), lake_rows(conn, S, "customers"))
out = {"changed_key": key, "change": f"city = city || ' (changed)' in {T}",
       "result_on_tampered_copy": r, "result_on_real_silver_table": untouched,
       "PASS": (not r["pass"]) and r["differing_rows"] == 1 and r["examples"]["differing"] == [key]
       and r["missing_in_lake"] == 0 and r["extra_in_lake"] == 0 and untouched["pass"]}
print(json.dumps(out, indent=2, default=str))
sys.exit(0 if out["PASS"] else 1)
