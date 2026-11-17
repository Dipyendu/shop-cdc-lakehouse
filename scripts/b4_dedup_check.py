"""Dedup check: bronze stores an event delivered twice only once.
Uploads a copy of an already-loaded orders file under a new name, reruns the job,
and compares bronze and silver counts before and after."""
import json
import shutil
from pathlib import Path

from shop.dbx import CATALOG, client, landing_root, pipeline_params, query, run_job, sql_conn

w, conn = client(), sql_conn()
B, S = f"{CATALOG}.shop_dev_bronze", f"{CATALOG}.shop_dev_silver"
cols = query(conn, f"DESCRIBE TABLE {B}.orders_events")
sample = query(conn, f"SELECT kafka_topic, kafka_partition, kafka_offset, op, source_lsn, source_tx_id, source_ts, "
                     f"regexp_extract(file_name, '[^/]+$', 0), left(payload, 120) FROM {B}.orders_events "
                     f"ORDER BY kafka_offset LIMIT 3")
count_q = (f"SELECT (SELECT count(*) FROM {B}.orders_events) AS bronze_rows, (SELECT count(DISTINCT kafka_partition, "
           f"kafka_offset) FROM {B}.orders_events) AS distinct_events, (SELECT count(*) FROM {S}.orders) AS silver_rows")
before = query(conn, count_q)[0]
src = next(Path("landing/orders").glob("*.jsonl"))
dup = Path("disorder_stage/b4") / f"redelivered2__{src.name}"
dup.parent.mkdir(parents=True, exist_ok=True)
shutil.copyfile(src, dup)
n_dup = len(dup.read_text().splitlines())
with open(dup, "rb") as fh:
    w.files.upload(f"{landing_root()}/dev/orders/{dup.name}", fh, overwrite=False)
run = run_job(w, "dedup check: orders file redelivered under new name", f"{n_dup} events",
              pipeline_params("shop_dev_bronze", "shop_dev_silver", f"{landing_root()}/dev", "orders"))
after = query(conn, count_q)[0]
batch_log = query(conn, f"SELECT batch_id, events_in_batch, distinct_events, bronze_rows_added, files "
                         f"FROM {B}._batch_log WHERE table = 'orders' ORDER BY logged_at")
files = query(conn, f"SELECT regexp_extract(file_name, '[^/]+$', 0), count(*) FROM {B}.orders_events GROUP BY 1")
print(json.dumps({
    "bronze_columns": [c[:2] for c in cols], "bronze_sample_rows": sample,
    "redelivered_file": dup.name, "redelivered_events": n_dup, "run": run,
    "before_bronze_rows_distinct_events_silver_rows": before,
    "after_bronze_rows_distinct_events_silver_rows": after,
    "bronze_rows_by_file_after": files, "batch_log": batch_log,
    "PASS": before == after and before[0] == before[1],
}, indent=2, default=str))
