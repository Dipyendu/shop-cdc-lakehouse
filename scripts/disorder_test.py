"""Disorder test, in its own schemas and landing prefix.

Per table, the landing files (ordered by first offset) are split into thirds:
  run 1: the newest third; run 2: the oldest third;
  run 3: the middle third plus a copy of one earlier file under a new name
         (so the file loader does not skip it as already seen).
Late and duplicate events therefore reach the silver merge. The final silver
table hashes (all rows, delete marks and _lsn included) must equal those of the
in-order run in --reference.

With --no-require-newer the "apply only if newer" rule is switched off; the test
must then report a difference (can-fail check).

Usage: python scripts/disorder_test.py --tag dis --reference shop_silver [--no-require-newer]
Exit code 0 when the result is the expected one (identical; or, with --no-require-newer, different).
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

from shop.dbx import CATALOG, client, landing_root, pipeline_params, run_job, sql_conn
from shop.reconcile import COLUMNS, lake_rows, table_hash
from shop.upload import put_one

p = argparse.ArgumentParser()
p.add_argument("--tag", required=True, help="schemas shop_<tag>_bronze/_silver, landing prefix <tag>")
p.add_argument("--reference", default="shop_silver")
p.add_argument("--landing", default="landing")
p.add_argument("--no-require-newer", action="store_true")
p.add_argument("--events", default="?")
a = p.parse_args()

bronze, silver = f"shop_{a.tag}_bronze", f"shop_{a.tag}_silver"
root = f"{landing_root()}/{a.tag}"
w = client()
extra = ["--no-require-newer"] if a.no_require_newer else []
params = pipeline_params(bronze, silver, root, None, *extra)

thirds = {}
for t in COLUMNS:
    files = sorted(Path(a.landing, t).glob("*.jsonl"))
    if len(files) < 3:
        sys.exit(f"{t}: {len(files)} files; need at least 3 for thirds")
    k = len(files) // 3
    thirds[t] = {"oldest": files[:k], "middle": files[k:len(files) - k], "newest": files[len(files) - k:]}

stage_dir = Path("disorder_stage") / a.tag
report = {"tag": a.tag, "require_newer": not a.no_require_newer,
          "files_per_third": {t: {k: [f.name for f in v] for k, v in th.items()} for t, th in thirds.items()}}


def put(files, t):
    for f in files:
        put_one(w.files, f, f"{root}/{t}/{f.name}")
        print(f"uploaded {t}/{f.name}", file=sys.stderr, flush=True)


for i, part in enumerate(["newest", "oldest", "middle"], 1):
    for t, th in thirds.items():
        put(th[part], t)
        if part == "middle":
            # one earlier file again, under a new name (duplicates of already-merged events)
            src = th["oldest"][0]
            dup = stage_dir / t / f"dup__{src.name}"
            dup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dup)
            put([dup], t)
            report.setdefault("duplicate_copies", {})[t] = f"{dup.name} (copy of {src.name})"
    report[f"run{i}"] = run_job(w, f"disorder {a.tag} run {i} ({part} third{' + dup' if i == 3 else ''})"
                                   f"{' NO-REQUIRE-NEWER' if extra else ''}", f"{a.events} events total", params)

conn = sql_conn()
cmp = {}
for t in COLUMNS:
    got = lake_rows(conn, f"{CATALOG}.{silver}.{t}", t, include_deleted=True)
    ref = lake_rows(conn, f"{CATALOG}.{a.reference}.{t}", t, include_deleted=True)
    cmp[t] = {"rows": len(got), "reference_rows": len(ref), "hash": table_hash(got),
              "reference_hash": table_hash(ref)}
    cmp[t]["identical"] = cmp[t]["hash"] == cmp[t]["reference_hash"] and len(got) == len(ref) and len(got) > 0
    if not cmp[t]["identical"]:
        g = {r[0]: r for r in got}
        r_ = {r[0]: r for r in ref}
        cmp[t]["keys_differing"] = sum(1 for k in set(g) | set(r_) if g.get(k) != r_.get(k))
report["comparison"] = cmp
report["ALL_IDENTICAL"] = all(v["identical"] for v in cmp.values())
print(json.dumps(report, indent=2, default=str))
# Exit 0 when the result is the expected one: identical with the rule on, different with it off.
sys.exit(0 if report["ALL_IDENTICAL"] != a.no_require_newer else 1)
