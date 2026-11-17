# Acceptance checks

The project counts as working when every check below passes. Each check names the evidence that shows it.

| # | Check | Evidence |
|---|---|---|
| 1 | `make ci` (lint, unit tests, dbt parse) exits 0. | [b10/1-make-ci.txt](evidence/b10/1-make-ci.txt) |
| 2 | An insert, an update and a delete in PostgreSQL each appear as a captured change event. | [b2-capture.txt](evidence/b2-capture.txt), [b10/2-capture.txt](evidence/b10/2-capture.txt) |
| 3 | Re-running the lander over the same offsets creates no new files and changes none. | [b3-lander-rerun.txt](evidence/b3-lander-rerun.txt), [b10/3-lander-rerun.json](evidence/b10/3-lander-rerun.json) |
| 4 | Reconciliation passes at 100,000 changes or more: equal row counts and zero differing row hashes for every table, and a planted difference is reported. | [b6-reconcile.json](evidence/b6-reconcile.json), [b10/1m-reconcile.json](evidence/b10/1m-reconcile.json), [b10/1m-reconcile-canfail.json](evidence/b10/1m-reconcile-canfail.json) |
| 5 | Replaying silver from bronze gives identical table hashes (Run A and Run B), and every table was recreated (checked on Run B, field `recreated_during_run`). | [b6-replay.json](evidence/b6-replay.json), [b10/1m-replay.json](evidence/b10/1m-replay.json) |
| 6 | The three-run disorder test gives identical table hashes, and with "apply only if newer" switched off it reports differences. | [b10/1m-disorder.json](evidence/b10/1m-disorder.json), [b10/1m-disorder-canfail.json](evidence/b10/1m-disorder-canfail.json) |
| 7 | The history checks pass: no overlapping intervals, one current row per live key, none per deleted key, version counts equal to the generator's, current rows equal to silver. | [b10/1m-history-check.json](evidence/b10/1m-history-check.json) |
| 8 | `dbt build` ends with zero failures. | [b10/8-dbt-build-100k.txt](evidence/b10/8-dbt-build-100k.txt), [b10/8-dbt-build-1m.txt](evidence/b10/8-dbt-build-1m.txt) |
| 9 | Every measured result in the README names its source, and `docs/EXPLAIN.md` covers every component. | [README.md](../README.md), [EXPLAIN.md](EXPLAIN.md) |
| 10 | The secret scan is clean, and the same scan finds a planted fake token. | [b0-secret-scan-proof.txt](evidence/b0-secret-scan-proof.txt), [b10/10-secret-scan.txt](evidence/b10/10-secret-scan.txt) |
| 11 | All work is committed and the working tree is clean. | `git status` |

The latest run of checks 1 to 3 and 8 to 10 is summarised in [b10-verification.txt](evidence/b10-verification.txt); checks 4 to 7 were last run on Run B (1,000,000 changes).
