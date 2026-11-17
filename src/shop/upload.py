"""Upload new landing files to LANDING_ROOT/<prefix>/<table>/ (default prefix shop; safe to repeat).

A remote file with the same name is skipped: the lander guarantees that a name
always stands for the same bytes. The first attempt never overwrites; a retry after a
failed attempt rewrites the same name with the same bytes. Each call has a timeout.

Usage: python -m shop.upload [--landing landing]
Needs DATABRICKS_HOST and DATABRICKS_TOKEN in the environment.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from shop.config import landing_root


def remote_names(files_api, remote_dir: str) -> set[str]:
    from databricks.sdk.errors import NotFound

    try:
        return {e.name for e in files_api.list_directory_contents(remote_dir) if not e.is_directory}
    except NotFound:
        files_api.create_directory(remote_dir)
        return set()


def put_one(files_api, f: Path, remote: str, attempts: int = 3) -> None:
    """Upload one file with a bounded number of attempts. The first attempt never
    overwrites; every retry overwrites, so a partial file left by a failed attempt is
    replaced by the full file (same name, same bytes)."""
    for i in range(1, attempts + 1):
        try:
            with open(f, "rb") as fh:
                files_api.upload(remote, fh, overwrite=i > 1)
            return
        except Exception as e:  # network error or timeout: retry, bounded
            print(f"attempt {i} failed for {f.name}: {type(e).__name__}: {str(e)[:150]}", file=sys.stderr, flush=True)
            if i == attempts:
                raise
            time.sleep(5 * i)


def upload(local: Path, root: str, files_api, prefix: str = "shop") -> dict:
    stats = {"uploaded": 0, "skipped_existing": 0}
    for table_dir in sorted(p for p in local.iterdir() if p.is_dir()):
        remote_dir = f"{root}/{prefix}/{table_dir.name}"
        have = remote_names(files_api, remote_dir)
        for f in sorted(table_dir.glob("*.jsonl")):
            if f.name in have:
                stats["skipped_existing"] += 1
                continue
            put_one(files_api, f, f"{remote_dir}/{f.name}")
            stats["uploaded"] += 1
            print(f"uploaded {table_dir.name}/{f.name}", file=sys.stderr, flush=True)
    return stats


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="landing files -> LANDING_ROOT")
    p.add_argument("--landing", default="landing")
    p.add_argument("--root", default=None, help="override LANDING_ROOT (tests only)")
    a = p.parse_args(argv)

    from shop.dbx import client

    print(json.dumps(upload(Path(a.landing), a.root or landing_root(), client().files)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
