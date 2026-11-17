"""Upload the local landing files to LANDING_ROOT/<prefix>/ and run the pipeline job
(bronze + silver, then history) for one schema pair. Prints a JSON summary.

Usage: python scripts/run_pipeline.py --prefix shop --bronze shop_bronze --silver shop_silver --events N
"""
import argparse
import json
import time
from pathlib import Path

from shop.dbx import CATALOG, client, landing_root, pipeline_params, query, run_job, sql_conn
from shop.upload import upload

p = argparse.ArgumentParser()
p.add_argument("--prefix", required=True)
p.add_argument("--bronze", required=True)
p.add_argument("--silver", required=True)
p.add_argument("--events", required=True, help="label for the run ledger")
p.add_argument("--landing", default="landing")
p.add_argument("--extra", nargs="*", default=[], help="extra pipeline flags, e.g. --rebuild-silver")
a = p.parse_args()

w = client()
t0 = time.time()
stats = upload(Path(a.landing), landing_root(), w.files, prefix=a.prefix)
upload_s = round(time.time() - t0, 1)
params = pipeline_params(a.bronze, a.silver, f"{landing_root()}/{a.prefix}", None, *a.extra)
run = run_job(w, f"pipeline {a.bronze}/{a.silver} ({a.prefix}) {' '.join(a.extra)}".strip(), f"{a.events} events",
              params)
conn = sql_conn()
counts = {t: query(conn, f"SELECT count(*), count_if(_deleted) FROM {CATALOG}.{a.silver}.{t}")[0]
          for t in ("customers", "products", "orders", "order_items", "payments")}
print(json.dumps({"upload": stats, "upload_seconds": upload_s, "run": run,
                  "silver_rows_and_deleted_marks": counts}, indent=2, default=str))
