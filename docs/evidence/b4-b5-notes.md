# Bronze, silver and history notes (development run), 2026-11-16 20:23 IST

Catalog: `analytics`. The warehouse reports `current_catalog() = workspace`, but no catalog named `workspace` exists in this account (`SHOW CATALOGS`: analytics, samples, system). LANDING_ROOT = `/Volumes/analytics/shop_landing/landing` (conf/shop.toml).

Platform checks:
- Auth with a personal access token and `SELECT 1` on the SQL warehouse: passed at 20:08 and 20:10 IST.
- Volume landing: PASS 20:10 IST, in `analytics.shop_landing` (SDK Files API upload, list, read back; scripts/t3_volume.py).
- Job from the laptop: PASS: the bundle in databricks.yml deploys job `shop_pipeline` (serverless, environment version 4) and every run in docs/run-ledger.md was started from the laptop.
- Incremental file pickup: PASS, b4-t5-incremental.json (first attempt failed in the history task on an argument bug, ledger line 20:12; the pipeline task itself succeeded; re-run under tag t5b).

Bronze: one append-only event table per source table in `analytics.shop_dev_bronze` (dev run) with raw payload, op, source_lsn, source_tx_id, source_ts, kafka offset, file name. Duplicate delivery proof: b4-bronze-dedup.json (1,528 redelivered events reached the batch, 0 bronze rows added).

Silver and history on the development run (5,000 generator events + 4 demo events from the capture proof):
- b5-dev-run.json: job run and silver row counts (deleted keys kept as marked rows).
- b5-dev-reconcile.json: all five tables equal to PostgreSQL (counts equal, 0 differing row hashes).
- b5-dev-history-check.json: checks 1 to 3 pass for both tables; check 4 differs only for key 900001 in customers and products. Those are the demo rows from the capture proof (customer 900001: insert + email update + delete = 2 versions; product 900001: 1 version), which are not in the generator plan the check counts from. The 100,000-event run starts from an empty database with generator events only.
