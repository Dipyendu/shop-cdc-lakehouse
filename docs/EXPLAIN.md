# Design notes

This file explains each part of the pipeline: what it does, why it was built this way, what was considered and rejected, and what would change in production. The last section lists likely interview questions with short answers that point to files.

## Source database (`db/schema.sql`, `docker-compose.yml`)

PostgreSQL 17.12 runs with `wal_level=logical`, so the write-ahead log carries row-level changes that a reader can decode. Five tables (customers, products, orders, order_items, payments) each have a primary key and an `updated_at` column. The database sets `updated_at` itself: a default on insert and a trigger on update. The generator therefore cannot write a value the database would not have written. A publication, `shop_pub`, lists exactly these five tables, and the connector is told not to create its own (`publication.autocreate.mode=disabled`).

Rejected: letting Debezium create a publication for all tables. That depends on the connector user's privileges and silently includes any table added later.

Production: a managed PostgreSQL with a dedicated replication user, a monitored replication slot (an abandoned slot keeps WAL forever and fills the disk), and `REPLICA IDENTITY FULL` on tables whose deletes must carry the old row.

## Generator (`src/shop/generator.py`)

The generator has two parts. `plan(seed, n)` is a pure function that returns exactly `n` row-level changes; it touches no database and reads no clock, so the unit tests in `tests/test_generator.py` check determinism, exact counts, foreign-key consistency and two-decimal money values without Docker. `apply()` runs the plan, one statement per change, and fails if any statement does not touch exactly one row. One change is therefore one change event.

The plan models a shop: new orders with items and a payment, payment capture, shipping and delivery, cancellations with refunds, item removal, purging of cancelled orders (deletes of items, payments and the order), email and address changes, price changes, new customers and products, and deletes of customers without orders. At 100,000 events (Run A) the mix is 53.4 % inserts, 38.5 % updates and 8.1 % deletes (`docs/evidence/b1-generator.txt`).

The plan also gives an independent count of history versions (`expected_versions`), which the history check compares against.

## Capture (`connect/shop-postgres.json`)

Debezium Connect 3.7.1 reads the WAL through PostgreSQL's built-in `pgoutput` plugin and writes one Kafka topic per table (`shop.public.<table>`), one partition each. One partition keeps every key's events in log order, and the data volume does not need more.

Lossless types were chosen on purpose. `decimal.handling.mode=string` sends NUMERIC as an exact string with its scale (`"1234.50"`); the default sends bytes that need the schema to decode, and `double` loses precision. `timestamptz` arrives as an ISO-8601 UTC string with microseconds, which is PostgreSQL's own precision. Converters run with `schemas.enable=false` to keep files small; the column types live in the job code. Example events: `docs/evidence/b2-capture.txt`. `make connector` registers the connector with `PUT /connectors/shop-postgres/config`, which creates or updates it, so running it again is safe, and `curl -f` makes an HTTP error fail the step.

Under the default replica identity, a delete's `before` image carries the real primary key, and the other NOT NULL columns hold placeholders (`''`, `1970-01-01`). The pipeline reads the key from the Kafka message key and ignores the rest of `before`.

## Lander (`src/shop/lander.py`)

The broker is published only on the laptop's `127.0.0.1` (`docker-compose.yml`), so Databricks cannot read it; a small consumer on the laptop turns Kafka records into files. Files are cut at fixed offset blocks (`--block`, default 2,000; Run A used 1,000, `docs/evidence/run-a-landing-files.txt`, and Run B used 5,000, `docs/evidence/stretch-1m-local-state.txt`) and at the partition end seen when the run starts. The name holds the topic, partition and first and last offset. Each line keeps the raw Kafka value unchanged plus topic, partition, offset and key.

Offsets are committed only after the file is written to a temporary name, fsynced and renamed. If the process dies before the commit, the next run writes the same file again; an existing file with identical bytes is left alone, and different bytes stop the run. A re-run over the same offsets produced the same five files with the same SHA-256 hashes and no extra files (`docs/evidence/b3-lander-rerun.txt`). Tombstones (the null record Debezium sends after a delete) are skipped and counted. Two cases produce an overlapping file under a new name: the consumer group is reset to the start after new events joined a block whose earlier part was already written, or the process dies between writing a block and committing it and new events join that block before the next run. Bronze stores the repeated events once, because it keys on partition and offset, so results stay correct. `tests/test_lander_run.py` drives `run()` with a fake consumer and checks that every commit comes after the records below it are in files on disk, that a crash at a commit loses nothing, and that a run that cannot reach the end stops at its deadline.

Rejected: a Kafka Connect sink connector that writes files. It would remove the custom code, but the file names and the commit-after-fsync rule would then depend on the connector's own rotation settings, and testing the re-run property would be harder. It is a possible next step.

Production: the lander's job is done by a managed connector or by Spark reading Kafka directly when the lakehouse can reach the broker.

## Upload (`src/shop/upload.py`)

New files are copied to `LANDING_ROOT/shop/<table>/` through the Databricks Files API. A remote file with the same name is skipped, since a name always stands for the same bytes. `LANDING_ROOT` is set in one place, `conf/shop.toml`, and the Makefile passes it to the bundle. Every request has a timeout and each file gets at most three attempts; a retry overwrites, so a partial file left by a failed attempt is replaced (`tests/test_upload.py`). The SDK's experimental Files client is switched off in `src/shop/dbx.py`: its multi-threaded upload path did not apply the request timeout, and an upload on 17 Nov hung, even with timeouts set, until it was stopped. The timeouts themselves came from the first attempt at Run A, which hung for about 33 minutes on one upload request that had no timeout; the run was stopped and resumed at the upload step (`docs/evidence/b6-main-run.json`, field `resumed`).

Known limit: file names and the bronze key both come from Kafka offsets. If the topics were deleted and created again, offsets would restart at 0, the upload would skip new files whose names match old ones, and bronze would drop events whose partition and offset are already stored. No run here recreated a topic. A production version would key bronze on `(source_lsn, key, op)` and compare file sizes or checksums on upload.

## Bronze (`jobs/pipeline.py`)

Auto Loader reads each table's landing folder with `Trigger.AvailableNow`. Serverless jobs support only `AvailableNow` and the deprecated `Once` trigger ([Databricks serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)). Auto Loader keeps the list of files it has seen in its checkpoint, so each run reads only new files (`docs/evidence/b4-t5-incremental.json`).

Each micro-batch goes to `foreachBatch`. Bronze keeps one row per event with the raw payload, the operation, the source log position (`source.lsn`), the transaction id, the source timestamp, the Kafka offset and the file name. A MERGE inserts an event only if its `(kafka_partition, kafka_offset)` is not already stored. In the development run (5,000 generated events plus 4 from the capture test), a file delivered a second time under a new name had all 1,528 of its events reach the batch, and bronze added 0 rows (`docs/evidence/b4-bronze-dedup.json`, `_batch_log`). Before anything is stored, the batch fails if any event lacks a valid operation, log position or key (`check_required_fields`), because such an event could not be ordered.

## Silver current state (`jobs/pipeline.py`, `merge_silver`)

The whole batch, duplicates and late events included, goes to the silver merge. Within the batch, the newest event per key wins, ordered by `source.lsn`. The MERGE then updates a stored row only when the incoming `lsn` is greater than the stored `_lsn`. A delete becomes a row with a delete mark (`_deleted = true`) and its `lsn`, so a late, older insert or update for that key finds a newer stored row and is ignored.

Ordering by `lsn` per key is safe even with concurrent transactions. Two changes to the same row cannot both be uncommitted, because the second waits for the first's row lock, so the second change's WAL record is written after the first commits.

The result does not depend on the order or repetition of files: the disorder test shows this, and with the "only if newer" rule switched off the same test fails (`docs/evidence/b6-disorder.json`, `docs/evidence/b6-disorder-canfail.json`).

Rejected: a streaming MERGE straight from Kafka (the broker is published only on the laptop, and serverless jobs do not allow continuous triggers), and Delta Live Tables `APPLY CHANGES` (it would hide the ordering rule this project is meant to show, and Free Edition allows one active pipeline per type).

Production: the same merge, plus `OPTIMIZE` and clustering by key on large tables, and a retention rule for delete marks once no late event can arrive.

## History, slowly changing dimension type 2 (`jobs/history.py`)

`customers_history` and `products_history` have one row per version of the tracked columns (customers: name, email, city, country; products: name, category, price). A version starts at an insert or snapshot read, at an update that changes a tracked column, at an update that follows a delete, or at an update with no earlier event for the key; it ends at the next version or at the delete. The generator produces only the first two kinds. Each row carries `valid_from`, `valid_to`, the matching log positions and `is_current`. `valid_from` and `valid_to` are the source transaction commit times. The generator commits every 500 statements, so two versions written in one transaction share a commit time and the earlier one has a zero-length interval; use `valid_from_lsn` and `valid_to_lsn` for exact ordering and point-in-time joins. The tables are rebuilt in full from bronze on each run, ordered by `lsn`, so arrival order cannot affect them.

Rejected for now: an incremental SCD2 merge. A full rebuild is simpler to prove correct at this data size. Production with large history would switch to an incremental merge keyed on `(key, valid_from_lsn)`.

`src/shop/history_check.py` checks that intervals chain without overlap, that each live key has exactly one current row and each deleted key none, that the number of versions per key equals the count from the generator plan, and (since 17 Nov) that each current row equals the silver row on every tracked column. The first four pass on Run A (`docs/evidence/b6-history-check.json`); all five pass on Run B (`docs/evidence/b10/1m-history-check.json`).

## Proofs

- Reconciliation (`src/shop/reconcile.py`): reads every column on both sides, turns each value into one canonical text form (exact decimals, UTC timestamps with six fractional digits, `null` for NULL), hashes each row and compares by key. `docs/evidence/b6-reconcile.json`. The check can fail: one changed row in a copy is reported as exactly that row (`docs/evidence/b6-reconcile-canfail.json`).
- Replay: drop silver, rebuild from bronze, compare table hashes, and check in the Delta history that every table was recreated during the run (`scripts/replay_test.py`). It proves that bronze holds everything silver needs. The rebuild merges into empty tables, so it does not test the "apply only if newer" rule against stored rows; the disorder test does. `docs/evidence/b6-replay.json`, `docs/evidence/b10/1m-replay.json`.
- Merge oracle (`src/shop/oracle.py`): a pure-Python model of the merge rule, built from the landing files, gives the same table hashes as silver, delete marks and `_lsn` included (`docs/evidence/b10/1m-oracle.json`). `tests/test_oracle.py` checks the model on a worked example and against shuffled, repeated events.
- Disorder: three runs in separate schemas with the landing files in newest, oldest and middle thirds plus a re-delivered file (`scripts/disorder_test.py`).

Each proof script exits with a non-zero code when its check fails, and a comparison of empty tables does not pass.

## Marts (`dbt/`)

dbt builds three marts, each a full table rebuild (fine at this size): `daily_revenue`, `order_status_funnel`, and `revenue_by_customer_attributes`, which joins each order to the customer's history version that was valid when the order was inserted. The join uses log positions, not timestamps. Debezium's source timestamp is the commit time, and the generator commits in batches of 500 changes, so a customer inserted in the same transaction as an order would have a commit time later than the order's `ordered_at`. A singular test checks that every live order matches exactly one version. `docs/evidence/b7-dbt-build.txt`.

The funnel's `orders_reached_step` counts every order that ever reached a step, from the order events in bronze, so cancelled and purged orders count for every step they passed. The first version counted only current statuses and undercounted (9,115 placed instead of 11,061 on Run A); `dbt/tests/funnel_counts_every_order_ever_placed.sql` now fails on that version (`docs/evidence/b10/f1-funnel-test-red-on-old-model.txt`; old and new counts in `docs/evidence/b10/f1-funnel-old-vs-new.txt`).

Known limit: two staging models (`stg_orders` for the insert log position, `stg_order_steps` for the funnel) read raw JSON from bronze instead of silver. Carrying the insert log position and the status path in silver would restore the layering.

All orders were generated on one day, so `daily_revenue` has one row (Run A sample in `docs/evidence/b7-dbt-build.txt`). A demo with spread-out dates would need the generator to set `ordered_at` itself, which would stop the database from being the only source of time.

## Orchestration

A Databricks Asset Bundle (`databricks.yml`) defines the job `shop_pipeline` with two tasks, `bronze_silver` and then `history`. The job has no schedule: every run in `docs/run-ledger.md` was started by hand from the laptop. `make` targets run the laptop side. A production setup would add a schedule or a file-arrival trigger to the job, or call it from an orchestrator.

## Limits

The data is simulated. The lander runs on the laptop. Runs are incremental batches started by hand, not scheduled and not continuous streaming. The CI workflow runs on every push and pull request, but only lint, unit tests and `dbt parse`, without Databricks access; its first run on GitHub, on 17 Nov 2026, passed (see the repository's Actions tab).

## Interview questions

1. Why order by `source.lsn` and not by `updated_at` or Kafka offset? The log position is the database's own order of changes and is unique per change; `updated_at` can tie or go backwards with clock changes, and offsets only order one partition. See `jobs/pipeline.py`, `merge_silver`.
2. What happens if the same event arrives twice? A record delivered again with the same partition and offset is stored once in bronze (MERGE on partition and offset); a copy that Debezium sends again after a restart has a new offset, so bronze keeps both, and silver ignores the copy because its `lsn` is not newer. `docs/evidence/b4-bronze-dedup.json`.
3. What if an old event arrives after a newer one? The merge condition `s._lsn > t._lsn` skips it. `docs/evidence/b6-disorder.json` and the failing control `b6-disorder-canfail.json`.
4. How are deletes handled? A delete becomes a row with a delete mark that keeps its `lsn`, so a late insert cannot bring the key back. Reconciliation compares only rows without a delete mark with PostgreSQL.
5. How do you know the lakehouse equals the source? Row hashes of canonical text forms of every column, compared by key, with a planted difference to show the check can fail. `src/shop/reconcile.py`, `docs/evidence/b6-reconcile*.json`.
6. Why keep bronze if silver has the answer? Bronze is the replayable record: silver can be dropped and rebuilt from it with identical table hashes, and the history tables are rebuilt from bronze on every run (`jobs/history.py`). `docs/evidence/b6-replay.json`, `docs/evidence/b10/1m-replay.json`.
7. How does the lander avoid losing or duplicating data? It commits offsets only after an fsynced rename, and file names are fixed by offset blocks, so a retry over the same offsets gives the same name and bytes. If new events arrive between a crash and the retry, the retry can write an overlapping file under a new name; bronze then stores each event once. `src/shop/lander.py`, `docs/evidence/b3-lander-rerun.txt`.
8. Why not stream straight from Kafka into Databricks? The broker is published only on the laptop's `127.0.0.1`, so Databricks cannot read it, and serverless jobs allow only the `AvailableNow` trigger and the deprecated `Once` trigger.
9. How are money and time kept exact? Decimals travel as strings and land as `DECIMAL(10,2)`/`(12,2)`; timestamps travel as UTC ISO strings with microseconds. `connect/shop-postgres.json`, `docs/evidence/b2-capture.txt`.
10. How does the revenue-by-attributes mart get attributes "as they were"? It joins on the order's insert log position falling inside a history version, and a dbt test checks that each order matches exactly one version. `dbt/models/marts/revenue_by_customer_attributes.sql`.
11. What broke during the build? An upload request without a timeout hung for about 33 minutes (`docs/evidence/b6-main-run.json`); uploads now have timeouts and bounded retries. The warehouse reported a default catalog, `workspace`, that does not exist, so the project uses `analytics` (`docs/evidence/b4-b5-notes.md`).
12. What would you change for production? A managed connector in place of the lander, monitoring of the replication slot and of lag, incremental SCD2, table maintenance, and alerts on the reconciliation result run on a schedule.
