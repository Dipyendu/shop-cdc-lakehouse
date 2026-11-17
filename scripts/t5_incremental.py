"""Incremental pickup test: Auto Loader + Trigger.AvailableNow picks up only new files (real pipeline job).

Splits the landed customers file into 200-offset blocks (same naming as the lander),
uploads two, runs the job, uploads the third, runs again, and checks bronze by file.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

from shop.dbx import CATALOG, client, landing_root, pipeline_params, query, run_job, sql_conn, upload_files
from shop.lander import file_name

src = min(Path("landing/customers").glob("*.jsonl"))
out = Path("t5_files"); out.mkdir(exist_ok=True)
blocks = defaultdict(list)
for line in src.read_text().splitlines():
    blocks[json.loads(line)["kafka_offset"] // 200].append(line)
files = []
for b in sorted(blocks):
    offs = [json.loads(x)["kafka_offset"] for x in blocks[b]]
    f = out / file_name("shop.public.customers", 0, offs[0], offs[-1])
    f.write_text("\n".join(blocks[b]) + "\n")
    files.append((f, len(blocks[b])))
assert len(files) == 3, files
w = client()
tag = sys.argv[1] if len(sys.argv) > 1 else "t5"
root = f"{landing_root()}/{tag}"
params = pipeline_params(f"shop_{tag}_bronze", f"shop_{tag}_silver", root, "customers")
conn = sql_conn()
rep = {"tag": tag, "files": [(f.name, n) for f, n in files]}
upload_files(w, [f for f, _ in files[:2]], f"{root}/customers")
rep["run1"] = run_job(w, "incremental pickup test, run 1: customers, 2 files", f"{files[0][1] + files[1][1]} events", params)
q = (f"SELECT regexp_extract(file_name, '[^/]+$', 0) AS f, count(*) FROM {CATALOG}.shop_{tag}_bronze.customers_events "
     "GROUP BY 1 ORDER BY 1")
rep["bronze_after_run1"] = query(conn, q)
upload_files(w, [files[2][0]], f"{root}/customers")
rep["run2"] = run_job(w, "incremental pickup test, run 2: customers, +1 new file", f"{files[2][1]} events", params)
rep["bronze_after_run2"] = query(conn, q)
rep["ingested_at_per_file"] = query(conn, "SELECT regexp_extract(file_name, '[^/]+$', 0), min(ingested_at), "
                                    f"max(ingested_at) FROM {CATALOG}.shop_{tag}_bronze.customers_events GROUP BY 1 ORDER BY 1")
ok = (rep["bronze_after_run1"] == [(f.name, n) for f, n in files[:2]]
      and rep["bronze_after_run2"] == [(f.name, n) for f, n in files])
rep["result"] = "PASS" if ok else "FAIL"
print(json.dumps(rep, indent=2, default=str))
sys.exit(0 if ok else 1)
