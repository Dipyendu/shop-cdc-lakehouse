"""End-to-end delay: one committed change in PostgreSQL until it is in the silver table.
Steps are timed separately: capture to Kafka, lander, upload, job run, silver check."""
import datetime as dt
import json
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, TopicPartition

from shop.dbx import CATALOG, client, landing_root, pipeline_params, query, run_job, sql_conn
from shop.lander import run as land
from shop.upload import upload

DSN, BOOT, TOPIC = "postgresql://shop:shop@127.0.0.1:5433/shop", "127.0.0.1:29092", "shop.public.customers"
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def end_offset():
    c = Consumer({"bootstrap.servers": BOOT, "group.id": "latency-probe"})
    try:
        return c.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=10)[1]
    finally:
        c.close()


before = end_offset()
with psycopg.connect(DSN) as conn, conn.cursor() as cur:
    cur.execute("SELECT min(customer_id) FROM customers")
    key = cur.fetchone()[0]
    email = f"latency.{int(time.time())}@example.com"
    cur.execute("UPDATE customers SET email = %s WHERE customer_id = %s", (email, key))
    conn.commit()
t = {"committed": time.time()}
while end_offset() <= before:
    if time.time() - t["committed"] > 120:
        raise SystemExit("change not captured within 120 s")
    time.sleep(0.2)
t["in_kafka"] = time.time()
lander = land(BOOT, Path("landing"), 1000, "shop-lander")
t["landed"] = time.time()
w = client()
up = upload(Path("landing"), landing_root(), w.files, prefix="shop")
t["uploaded"] = time.time()
run = run_job(w, "latency: one customer update through the full job", "1 change",
              pipeline_params("shop_bronze", "shop_silver", f"{landing_root()}/shop"))
t["job_done"] = time.time()
row = query(sql_conn(), f"SELECT email FROM {CATALOG}.shop_silver.customers WHERE customer_id = {key}")
t["checked"] = time.time()
names = list(t)
print(json.dumps({
    "changed": {"customer_id": key, "email": email}, "silver_email": row[0][0], "in_silver": row[0][0] == email,
    "committed_at_ist": dt.datetime.fromtimestamp(t["committed"], IST).strftime("%Y-%m-%d %H:%M:%S"),
    "step_seconds": {f"{names[i-1]} -> {names[i]}": round(t[names[i]] - t[names[i-1]], 2) for i in range(1, len(names))},
    "end_to_end_seconds_commit_to_silver": round(t["job_done"] - t["committed"], 1),
    "lander": lander, "upload": up, "job": run,
}, indent=2, default=str))
