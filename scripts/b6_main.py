"""Main run: generate N events into an empty database (connector already running),
wait until Debezium has published all of them, land them (zero lag), upload, run the
pipeline job, and reconcile. Every step is timed. Prints a JSON report.

Usage: python scripts/b6_main.py --events 100000 --seed 42 --block 1000
"""
import argparse
import datetime as dt
import json
import subprocess
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from shop.dbx import client, landing_root, pipeline_params, run_job
from shop.generator import counts, plan
from shop.lander import TOPIC_PREFIX
from shop.lander import run as land
from shop.upload import upload

p = argparse.ArgumentParser()
p.add_argument("--events", type=int, required=True)
p.add_argument("--seed", type=int, default=42)
p.add_argument("--block", type=int, default=1000)
p.add_argument("--bootstrap", default="127.0.0.1:29092")
p.add_argument("--bronze", default="shop_bronze")
p.add_argument("--silver", default="shop_silver")
p.add_argument("--prefix", default="shop")
p.add_argument("--report", default="docs/evidence/b6-generator-report.json")
p.add_argument("--resume-note", default=None)
p.add_argument("--resume-at-upload", action="store_true",
               help="data already generated and landed (first attempt hung in upload)")
a = p.parse_args()

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
rep, marks = {}, {}


def mark(name):
    marks[name] = time.time()
    print(f"step {name} at {dt.datetime.now(IST):%H:%M:%S}", file=sys.stderr, flush=True)
    rep.setdefault("clock_ist", {})[name] = dt.datetime.now(IST).strftime("%H:%M:%S")


def kafka_events_published() -> int:
    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": "b6-probe"})
    try:
        topics = [t for t in c.list_topics(timeout=10).topics if t.startswith(TOPIC_PREFIX)]
        total = 0
        for t in topics:
            lo, hi = c.get_watermark_offsets(TopicPartition(t, 0), timeout=10)
            total += hi - lo
        return total
    finally:
        c.close()


expected = counts(plan(a.seed, a.events))
n_deletes = sum(v["delete"] for v in expected.values())
rep["generator_counts"] = expected
rep["expected_kafka_records"] = a.events + n_deletes  # one tombstone after each delete

if a.resume_at_upload:
    rep["resumed"] = a.resume_note or ("The first attempt (started about 20:24 IST) generated, captured and landed all events, then hung "
                      "for about 33 minutes inside the upload on a request with no timeout. It was stopped, the upload got "
                      "timeouts and bounded retries, and this run resumed at the upload (repeat-safe).")
    ist = lambda ts: dt.datetime.fromtimestamp(ts, IST).strftime("%H:%M:%S")
    rep["first_attempt_file_times_ist"] = {
        "generator_report_written": ist(Path(a.report).stat().st_mtime),
        "newest_landing_file_written": ist(max(f.stat().st_mtime for f in Path("landing").rglob("*.jsonl"))),
    }
    rep["landing_events"] = sum(len(f.read_text().splitlines()) for f in Path("landing").rglob("*.jsonl"))
if not a.resume_at_upload:
    mark("generate_start")
    subprocess.run([sys.executable, "-m", "shop.generator", "--events", str(a.events), "--seed", str(a.seed),
                    "--report", a.report], check=True, stdout=subprocess.DEVNULL)
    mark("generate_end")
    while kafka_events_published() < rep["expected_kafka_records"]:
        if time.time() - marks["generate_end"] > 900:
            sys.exit("Debezium did not publish all events within 15 minutes")
        time.sleep(2)
    mark("kafka_caught_up")
    rep["kafka_records"] = kafka_events_published()
    rep["lander"] = land(a.bootstrap, Path("landing"), a.block, "shop-lander")
    mark("landed")
w = client()
mark("upload_start")
rep["upload"] = upload(Path("landing"), landing_root(), w.files, prefix=a.prefix)
mark("uploaded")
rep["job"] = run_job(w, f"main run: pipeline {a.bronze}/{a.silver} ({a.events} events)", f"{a.events} events",
                     pipeline_params(a.bronze, a.silver, f"{landing_root()}/{a.prefix}"))
mark("silver_done")
names = list(marks)
rep["step_seconds"] = {f"{names[i - 1]} -> {names[i]}": round(marks[names[i]] - marks[names[i - 1]], 1)
                       for i in range(1, len(names))}
if "generate_end" in marks:
    rep["end_to_end_seconds_last_db_change_to_silver"] = round(marks["silver_done"] - marks["generate_end"], 1)
print(json.dumps(rep, indent=2, default=str))
