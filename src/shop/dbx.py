"""Small helpers for driving Databricks from the laptop (job runs, SQL, files).

Every job run is logged to docs/run-ledger.md. If SHOP_RUN_STOP_LOG names a log file shared
with other projects on the workspace, no run starts while its last QUOTA-STOP has no later
QUOTA-RESUME.
"""

from __future__ import annotations

import datetime as dt
import os
import time
from pathlib import Path

from shop import config
from shop.config import landing_root

ROOT = Path(__file__).resolve().parents[2]
LEDGER = ROOT / "docs" / "run-ledger.md"
CATALOG = config.catalog()
CHECKPOINTS = config.checkpoints()
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def now_ist() -> str:
    return dt.datetime.now(IST).strftime("%Y-%m-%d %H:%M")


def quota_ok() -> bool:
    """False if SHOP_RUN_STOP_LOG is set and its last QUOTA-STOP has no later QUOTA-RESUME."""
    state = True
    log = Path(os.environ.get("SHOP_RUN_STOP_LOG") or "/nonexistent")
    if log.is_file():
        for line in log.read_text().splitlines():
            if line.startswith("QUOTA-STOP"):
                state = False
            elif line.startswith("QUOTA-RESUME"):
                state = True
    return state


def ledger(what: str, size: str, seconds: float, result: str, started: str) -> None:
    with open(LEDGER, "a") as f:
        f.write(f"| {started} | {what} | {size} | {seconds:.0f} s | {result} |\n")


def client():
    """Workspace client with request timeouts (no call may hang)."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.config import Config

    # The experimental Files client uploads through worker threads that did not honour these
    # timeouts (two uploads hung for 10 and 33 minutes); the plain Files API call does.
    return WorkspaceClient(config=Config(http_timeout_seconds=60, retry_timeout_seconds=180,
                                         files_ext_network_transfer_inactivity_timeout_seconds=60,
                                         disable_experimental_files_api_client=True))


def job_id(w, name: str = "shop_pipeline") -> int:
    ids = [j.job_id for j in w.jobs.list(name=f"[dev {w.current_user.me().user_name.split('@')[0]}] {name}")]
    if not ids:
        ids = [j.job_id for j in w.jobs.list() if j.settings.name.endswith(name)]
    if len(ids) != 1:
        raise RuntimeError(f"expected one job named *{name}, found {ids}")
    return ids[0]


def pipeline_params(bronze: str, silver: str, landing: str, tables: str | None = None, *extra: str) -> list[str]:
    p = [f"--catalog={CATALOG}", f"--bronze={bronze}", f"--silver={silver}", f"--landing={landing}",
         f"--checkpoints={CHECKPOINTS}/{bronze}"]
    if tables:
        p.append(f"--tables={tables}")
    return p + list(extra)


def run_job(w, what: str, size: str, python_params: list[str] | None = None, only: list[str] | None = None,
            name: str = "shop_pipeline") -> dict:
    """Run the job (optionally with overridden python params / only some tasks) and wait."""
    if not quota_ok():
        raise RuntimeError("run-stop log has a QUOTA-STOP without a later QUOTA-RESUME: no Databricks run")
    from databricks.sdk.service.jobs import RunResultState

    started, t0 = now_ist(), time.time()
    kw = {}
    if python_params is not None:
        kw["python_params"] = python_params
    if only:
        kw["only"] = only
    try:
        run = w.jobs.run_now_and_wait(job_id(w, name), timeout=dt.timedelta(minutes=40), **kw)
    except Exception as e:  # logged, then re-raised
        ledger(what, size, time.time() - t0, f"ERROR {type(e).__name__}: {str(e)[:120]}", started)
        raise
    secs = time.time() - t0
    state = run.state.result_state
    ok = state == RunResultState.SUCCESS
    ledger(what, size, secs, f"{state.value if state else run.state} run_id={run.run_id}", started)
    if not ok:
        raise RuntimeError(f"run {run.run_id} ended {state}: {run.state.state_message}")
    return {"run_id": run.run_id, "seconds": round(secs, 1), "started": started}


def sql_conn():
    from databricks import sql

    host = os.environ["DATABRICKS_HOST"].removeprefix("https://").rstrip("/")
    conn = sql.connect(server_hostname=host, http_path=os.environ["SHOP_WAREHOUSE_HTTP_PATH"],
                       access_token=os.environ["DATABRICKS_TOKEN"])
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return conn


def query(conn, q: str):
    with conn.cursor() as cur:
        cur.execute(q)
        return [tuple(r) for r in cur.fetchall()]


def upload_files(w, files: list[Path], remote_dir: str) -> int:
    n = 0
    for f in files:
        with open(f, "rb") as fh:
            w.files.upload(f"{remote_dir}/{f.name}", fh, overwrite=False)
        n += 1
    return n


__all__ = ["landing_root"]
