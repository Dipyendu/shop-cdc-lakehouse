# Run ledger (Databricks runs)

| Start (IST) | What ran | Data size | Duration | Result |
|---|---|---|---|---|
| 2026-11-16 20:12 | incremental pickup test, run 1: customers, 2 files | 373 events | 154 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task history failed with message: Workload  |
| 2026-11-16 20:15 | incremental pickup test, run 1: customers, 2 files | 373 events | 54 s | SUCCESS run_id=892853211495937 |
| 2026-11-16 20:16 | incremental pickup test, run 2: customers, +1 new file | 97 events | 53 s | SUCCESS run_id=477265106593845 |
| 2026-11-16 20:18 | pipeline shop_dev_bronze/shop_dev_silver (dev) | 5004 events | 106 s | SUCCESS run_id=691941533767529 |
| 2026-11-16 20:21 | dedup check: orders file redelivered under new name | 1528 events | 44 s | SUCCESS run_id=422049046105040 |
| 2026-11-16 20:22 | dedup check: orders file redelivered under new name | 1528 events | 54 s | SUCCESS run_id=29845173677710 |
| 2026-11-16 21:01 | main run: pipeline shop_bronze/shop_silver (100000 events) | 100000 events | 204 s | SUCCESS run_id=241127120302279 |
| 2026-11-16 21:06 | replay: drop silver, rebuild from bronze | bronze of 100000 events | 65 s | SUCCESS run_id=950298620944821 |
| 2026-11-16 21:08 | disorder dis run 1 (newest third) | 100000 events total | 130 s | SUCCESS run_id=520700462225936 |
| 2026-11-16 21:10 | disorder dis run 2 (oldest third) | 100000 events total | 109 s | SUCCESS run_id=83066419027810 |
| 2026-11-16 21:12 | disorder dis run 3 (middle third + dup) | 100000 events total | 108 s | SUCCESS run_id=200013595066749 |
| 2026-11-16 21:15 | disorder dis2 run 1 (newest third) NO-REQUIRE-NEWER | 100000 events total | 130 s | SUCCESS run_id=909391368597180 |
| 2026-11-16 21:17 | disorder dis2 run 2 (oldest third) NO-REQUIRE-NEWER | 100000 events total | 111 s | SUCCESS run_id=74641496982888 |
| 2026-11-16 21:20 | disorder dis2 run 3 (middle third + dup) NO-REQUIRE-NEWER | 100000 events total | 109 s | SUCCESS run_id=294493915690707 |
| 2026-11-16 21:23 | latency: one customer update through the full job | 1 change | 63 s | SUCCESS run_id=186908203169593 |
| 2026-11-16 21:28 | replay: drop silver, rebuild from bronze | bronze of 100000 events | 54 s | SUCCESS run_id=702041785023073 |
| 2026-11-16 21:30 | disorder dis3 run 1 (newest third) | 100001 events total | 128 s | SUCCESS run_id=178316671694446 |
| 2026-11-16 21:33 | disorder dis3 run 2 (oldest third) | 100001 events total | 100 s | SUCCESS run_id=409849700675864 |
| 2026-11-16 21:35 | disorder dis3 run 3 (middle third + dup) | 100001 events total | 107 s | SUCCESS run_id=1015557537608124 |
| 2026-11-16 21:37 | disorder dis4 run 1 (newest third) NO-REQUIRE-NEWER | 100001 events total | 131 s | SUCCESS run_id=324849311760756 |
| 2026-11-16 21:40 | disorder dis4 run 2 (oldest third) NO-REQUIRE-NEWER | 100001 events total | 108 s | SUCCESS run_id=1054284753657141 |
| 2026-11-16 21:42 | disorder dis4 run 3 (middle third + dup) NO-REQUIRE-NEWER | 100001 events total | 106 s | SUCCESS run_id=702663745712997 |
| 2026-11-17 08:23 | main run: pipeline shop_1m_bronze/shop_1m_silver (1000000 events) | 1000000 events | 226 s | SUCCESS run_id=202790818208971 |
| 2026-11-17 08:28 | replay: drop shop_1m_silver, rebuild from shop_1m_bronze | bronze shop_1m_bronze | 56 s | SUCCESS run_id=661030861637968 |
| 2026-11-17 08:31 | disorder d1m2 run 1 (newest third) NO-REQUIRE-NEWER | 1000000 events total | 150 s | SUCCESS run_id=423130710216181 |
| 2026-11-17 08:35 | guard check: one event without an operation (must fail) | 2 events | 54 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 08:35 | disorder d1m2 run 2 (oldest third) NO-REQUIRE-NEWER | 1000000 events total | 119 s | SUCCESS run_id=179235353361802 |
| 2026-11-17 08:37 | guard check: one event without an operation (must fail) | 2 events | 52 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 08:38 | disorder d1m2 run 3 (middle third + dup) NO-REQUIRE-NEWER | 1000000 events total | 118 s | SUCCESS run_id=482990008289421 |
| 2026-11-17 08:41 | disorder d1m3 run 1 (newest third) | 1000000 events total | 142 s | SUCCESS run_id=919040517881400 |
| 2026-11-17 08:45 | disorder d1m3 run 2 (oldest third) | 1000000 events total | 108 s | SUCCESS run_id=1016638311053652 |
| 2026-11-17 08:48 | disorder d1m3 run 3 (middle third + dup) | 1000000 events total | 109 s | SUCCESS run_id=605288312864005 |
| 2026-11-17 11:10 | guard check: one event with no_op (must fail) | 2 events | 87 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 11:12 | guard check: one event with no_lsn (must fail) | 2 events | 55 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 11:13 | guard check: one event with no_key (must fail) | 2 events | 54 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 11:14 | guard check: one event with no_op (must fail) | 2 events | 27 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 11:15 | guard check: one event with no_lsn (must fail) | 2 events | 27 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 11:15 | guard check: one event with no_key (must fail) | 2 events | 28 s | ERROR OperationFailed: failed to reach TERMINATED or SKIPPED, got RunLifeCycleState.INTERNAL_ERROR: Task bronze_silver failed with message: Wor |
| 2026-11-17 11:21 | pipeline shop_bronze/shop_silver (shop) [catalog shop_demo: 5,000-event How-to-run check, docs/evidence/run-5000-how-to-run.txt] | 5000 events | 153 s | SUCCESS run_id=422409881070641 |
| 2026-11-17 11:25 | replay: drop shop_silver, rebuild from shop_bronze [catalog shop_demo: 5,000-event How-to-run check, docs/evidence/run-5000-how-to-run.txt] | bronze shop_bronze | 56 s | SUCCESS run_id=1065865241194261 |
| 2026-11-17 11:26 | disorder dis run 1 (newest third) [catalog shop_demo: 5,000-event How-to-run check, docs/evidence/run-5000-how-to-run.txt] | 5000 events total | 116 s | SUCCESS run_id=688473016411290 |
| 2026-11-17 11:29 | disorder dis run 2 (oldest third) [catalog shop_demo: 5,000-event How-to-run check, docs/evidence/run-5000-how-to-run.txt] | 5000 events total | 98 s | SUCCESS run_id=737247449138822 |
| 2026-11-17 11:31 | disorder dis run 3 (middle third + dup) [catalog shop_demo: 5,000-event How-to-run check, docs/evidence/run-5000-how-to-run.txt] | 5000 events total | 107 s | SUCCESS run_id=194022739580384 |
