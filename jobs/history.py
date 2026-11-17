"""History (SCD type 2) for customers and products, rebuilt from bronze.

One row per version of the tracked columns. A version starts at an insert, or
at an update that changes a tracked column; it ends at the next version or at
the delete. Order is the PostgreSQL log position (lsn), so the result does not
depend on the order in which files arrived. The table is rebuilt in full on
each run (small data; see docs/EXPLAIN.md).

Arguments: --catalog C --bronze S --silver S
"""

import argparse

from pyspark.sql import SparkSession

TRACKED = {
    "customers": ("customer_id", ["name", "email", "city", "country"]),
    "products": ("product_id", ["name", "category", "price"]),
}
TYPES = {"price": "DECIMAL(10,2)"}


def history_sql(catalog: str, bronze: str, table: str) -> str:
    pk, cols = TRACKED[table]
    after = ", ".join(
        f"CAST(get_json_object(payload, '$.after.{c}') AS {TYPES.get(c, 'STRING')}) AS {c}" for c in cols)
    tracked = ", ".join(f"coalesce(cast({c} AS STRING), '<null>')" for c in cols)
    return f"""
WITH ev AS (
  SELECT DISTINCT CAST(get_json_object(kafka_key, '$.{pk}') AS BIGINT) AS {pk},
         op, source_lsn, source_ts, {after}
  FROM {catalog}.{bronze}.{table}_events
),
marked AS (
  SELECT *, to_json(array({tracked})) AS _tracked,
         lag(to_json(array({tracked}))) OVER (PARTITION BY {pk} ORDER BY source_lsn) AS _prev_tracked,
         lag(op) OVER (PARTITION BY {pk} ORDER BY source_lsn) AS _prev_op
  FROM ev
),
boundaries AS (
  SELECT * FROM marked
  WHERE op IN ('c', 'r', 'd')
     OR (op = 'u' AND (_prev_op = 'd' OR _prev_tracked IS NULL OR _tracked <> _prev_tracked))
),
spans AS (
  SELECT *, lead(source_ts) OVER (PARTITION BY {pk} ORDER BY source_lsn) AS _next_ts,
            lead(source_lsn) OVER (PARTITION BY {pk} ORDER BY source_lsn) AS _next_lsn
  FROM boundaries
)
SELECT {pk}, {", ".join(cols)},
       source_ts AS valid_from, _next_ts AS valid_to,
       source_lsn AS valid_from_lsn, _next_lsn AS valid_to_lsn,
       _next_lsn IS NULL AS is_current
FROM spans WHERE op <> 'd'
"""


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver", required=True)
    a, _ = p.parse_known_args(argv)  # the job passes the pipeline parameters to every task
    spark = SparkSession.builder.getOrCreate()
    for table in TRACKED:
        if not spark.catalog.tableExists(f"{a.catalog}.{a.bronze}.{table}_events"):
            print(f"skip: no bronze table for {table}")
            continue
        target = f"{a.catalog}.{a.silver}.{table}_history"
        spark.sql(f"CREATE OR REPLACE TABLE {target} AS {history_sql(a.catalog, a.bronze, table)}")
        print(f"done: {target}")


if __name__ == "__main__":
    main()
