"""Can-fail check for the batch guard. Three cases, each in its own landing folder and its own
pair of schemas: a landing file with one valid event and one event that has (1) no operation,
(2) no log position or (3) no key. Each run must fail the job, and nothing of that batch may be
stored in bronze or silver. The task error must contain the guard's own message, so a failure
for another reason does not pass. Exit code 0 when all three jobs failed as expected."""
import io
import json
import re
import sys

from shop.dbx import CATALOG, client, job_id, landing_root, pipeline_params, query, run_job, sql_conn

AFTER = {"customer_id": 1, "name": "A", "email": "a@x", "city": "Pune", "country": "IN",
         "created_at": "2026-11-17T00:00:00.000000Z", "updated_at": "2026-11-17T00:00:00.000000Z"}


def line(offset: int, key: str | None, payload: dict) -> str:
    return json.dumps({"kafka_topic": "shop.public.customers", "kafka_partition": 0, "kafka_offset": offset,
                       "kafka_key": key, "payload": json.dumps(payload)})


good = line(0, '{"customer_id":1}', {"op": "c", "source": {"lsn": 10, "ts_us": 1}, "after": AFTER})
after2 = {**AFTER, "customer_id": 2}
CASES = {
    "no_op": line(1, '{"customer_id":2}', {"source": {"lsn": 11, "ts_us": 2}, "after": after2}),
    "no_lsn": line(1, '{"customer_id":2}', {"op": "c", "source": {"ts_us": 2}, "after": after2}),
    "no_key": line(1, None, {"op": "c", "source": {"lsn": 11, "ts_us": 2}, "after": after2}),
}

w = client()
conn = sql_conn()
results = {}
for case, bad in CASES.items():
    root = f"{landing_root()}/guardtest_{case}"
    bronze, silver = f"shop_guard_{case}_bronze", f"shop_guard_{case}_silver"
    w.files.upload(f"{root}/customers/guard__p0__000000000000-000000000001.jsonl",
                   io.BytesIO((good + "\n" + bad + "\n").encode()), overwrite=True)
    try:
        run_job(w, f"guard check: one event with {case} (must fail)", "2 events",
                pipeline_params(bronze, silver, root, "customers"))
        failed, err = False, None
    except Exception as e:  # noqa: BLE001 - the SDK raises OperationFailed for a failed run
        failed, err = True, f"{type(e).__name__}: {str(e)[:300]}"
    stored = {t: query(conn, f"SELECT count(*) FROM {CATALOG}.{s}.customers{sfx}")[0][0]
              for t, s, sfx in (("bronze", bronze, "_events"), ("silver", silver, ""))}
    msg, run_id = None, None
    if failed:
        r = w.jobs.get_run(next(iter(w.jobs.list_runs(job_id=job_id(w), limit=1))).run_id)
        t = next(t for t in r.tasks if t.task_key == "bronze_silver")
        run_id = r.run_id
        m = re.search(r"customers batch \d+: \d+ event\(s\) without a valid op, source lsn or key",
                      w.jobs.get_run_output(t.run_id).error or "")
        msg = m.group(0) if m else None
    results[case] = {"run_id": run_id, "job_failed": failed, "job_error": err, "guard_message": msg,
                     "rows_stored": stored,
                     "PASS": failed and msg is not None and stored == {"bronze": 0, "silver": 0}}
    print(f"{case}: PASS={results[case]['PASS']}", file=sys.stderr, flush=True)
ok = all(r["PASS"] for r in results.values())
print(json.dumps({"cases": results, "PASS": ok}, indent=2))
sys.exit(0 if ok else 1)
