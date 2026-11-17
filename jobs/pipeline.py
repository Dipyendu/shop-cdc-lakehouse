"""Bronze + silver for the shop CDC pipeline (Databricks serverless, PySpark).

For each source table, one Auto Loader stream with Trigger.AvailableNow reads the
new landing files. Each micro-batch is:
  1. stored in bronze (append-only event table): an event already stored, by
     (kafka_partition, kafka_offset), is not stored again;
  2. passed whole - duplicates and late events included - to the silver merge.
The silver merge keeps one row per primary key. It orders events by the
PostgreSQL log position (source.lsn) and applies an event only if it is newer
than the stored row; a delete keeps the key as a marked row (_deleted = true),
so a late older event cannot bring it back.

Arguments:
  --catalog C --bronze S --silver S --landing PATH --checkpoints PATH
  [--tables a,b] [--rebuild-silver] [--no-require-newer]

--rebuild-silver drops the silver tables and rebuilds them from bronze (replay).
--no-require-newer switches off the "apply only if newer" rule (can-fail test).
"""

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# Columns of each source table, in order, with Spark types.
TABLES = {
    "customers": ("customer_id", [
        ("customer_id", "BIGINT"), ("name", "STRING"), ("email", "STRING"), ("city", "STRING"),
        ("country", "STRING"), ("created_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")]),
    "products": ("product_id", [
        ("product_id", "BIGINT"), ("name", "STRING"), ("category", "STRING"),
        ("price", "DECIMAL(10,2)"), ("updated_at", "TIMESTAMP")]),
    "orders": ("order_id", [
        ("order_id", "BIGINT"), ("customer_id", "BIGINT"), ("status", "STRING"),
        ("ordered_at", "TIMESTAMP"), ("updated_at", "TIMESTAMP")]),
    "order_items": ("order_item_id", [
        ("order_item_id", "BIGINT"), ("order_id", "BIGINT"), ("product_id", "BIGINT"),
        ("quantity", "INT"), ("unit_price", "DECIMAL(10,2)"), ("updated_at", "TIMESTAMP")]),
    "payments": ("payment_id", [
        ("payment_id", "BIGINT"), ("order_id", "BIGINT"), ("amount", "DECIMAL(12,2)"),
        ("method", "STRING"), ("status", "STRING"), ("updated_at", "TIMESTAMP")]),
}

LANDING_SCHEMA = "kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_key STRING, payload STRING"


def parse_events(raw: DataFrame) -> DataFrame:
    """Landing rows -> bronze columns (raw payload kept)."""
    p = F.col("payload")
    return raw.select(
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_key", "payload",
        F.get_json_object(p, "$.op").alias("op"),
        F.get_json_object(p, "$.source.lsn").cast("BIGINT").alias("source_lsn"),
        F.get_json_object(p, "$.source.txId").cast("BIGINT").alias("source_tx_id"),
        F.timestamp_micros(F.get_json_object(p, "$.source.ts_us").cast("BIGINT")).alias("source_ts"),
        F.col("_metadata.file_path").alias("file_name"),
        F.current_timestamp().alias("ingested_at"),
    )


def check_required_fields(events: DataFrame, table: str, batch_id: int) -> None:
    """Fail the batch if any event lacks an operation, a log position or a key.
    Such a row could not be ordered or merged; nothing of the batch is stored."""
    bad = events.where(F.col("op").isNull() | F.col("source_lsn").isNull() | F.col("kafka_key").isNull()
                       | ~F.col("op").isin("c", "u", "d", "r")).count()
    if bad:
        raise ValueError(f"{table} batch {batch_id}: {bad} event(s) without a valid op, source lsn or key")


def to_silver_rows(events: DataFrame, table: str) -> DataFrame:
    """Bronze events -> one candidate row per event, typed, with _lsn and _deleted."""
    pk, cols = TABLES[table]
    after_schema = ", ".join(f"{c} STRING" for c, _ in cols)
    key_schema = f"{pk} BIGINT"
    deleted = F.col("op") == "d"
    e = events.select(
        "op", "source_lsn", "source_ts",
        F.from_json("payload", f"after STRUCT<{after_schema}>").alias("v"),
        F.from_json("kafka_key", key_schema).alias("k"),
    )
    out = [F.col(f"k.{pk}").alias(pk)]
    for c, t in cols:
        if c == pk:
            continue
        out.append(F.when(deleted, F.lit(None)).otherwise(F.col(f"v.after.{c}")).cast(t).alias(c))
    return e.select(*out, F.col("source_lsn").alias("_lsn"), deleted.alias("_deleted"),
                    F.col("source_ts").alias("_source_ts"))


def latest_per_key(rows: DataFrame, pk: str) -> DataFrame:
    """Within one batch, keep the newest event per key (by log position)."""
    w = Window.partitionBy(pk).orderBy(F.col("_lsn").desc())
    return rows.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def ensure_tables(spark: SparkSession, catalog: str, bronze: str, silver: str, table: str) -> None:
    _, cols = TABLES[table]
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.{bronze}.{table}_events (
        kafka_topic STRING, kafka_partition INT, kafka_offset BIGINT, kafka_key STRING, payload STRING,
        op STRING, source_lsn BIGINT, source_tx_id BIGINT, source_ts TIMESTAMP,
        file_name STRING, ingested_at TIMESTAMP) USING DELTA""")
    col_ddl = ", ".join(f"{c} {t}" for c, t in cols)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog}.{silver}.{table} (
        {col_ddl}, _lsn BIGINT, _deleted BOOLEAN, _source_ts TIMESTAMP) USING DELTA""")


def merge_silver(spark: SparkSession, events: DataFrame, target: str, table: str, require_newer: bool) -> None:
    pk, cols = TABLES[table]
    src = latest_per_key(to_silver_rows(events, table), pk)
    view = f"_src_{table}"
    src.createOrReplaceTempView(view)
    names = [c for c, _ in cols] + ["_lsn", "_deleted", "_source_ts"]
    set_clause = ", ".join(f"t.{c} = s.{c}" for c in names)
    cond = "AND s._lsn > t._lsn" if require_newer else ""
    spark.sql(f"""MERGE INTO {target} t USING {view} s ON t.{pk} = s.{pk}
        WHEN MATCHED {cond} THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({", ".join(names)}) VALUES ({", ".join("s." + c for c in names)})""")


def store_bronze(spark: SparkSession, events: DataFrame, target: str, table: str) -> None:
    view = f"_bronze_{table}"
    events.dropDuplicates(["kafka_partition", "kafka_offset"]).createOrReplaceTempView(view)
    spark.sql(f"""MERGE INTO {target} t USING {view} s
        ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *""")


def run_table(spark, a, table: str) -> None:
    bronze_t = f"{a.catalog}.{a.bronze}.{table}_events"
    silver_t = f"{a.catalog}.{a.silver}.{table}"

    def batch(df: DataFrame, batch_id: int) -> None:
        spark_ = df.sparkSession
        events = parse_events(df)
        check_required_fields(events, table, batch_id)
        before = spark_.table(bronze_t).count()
        store_bronze(spark_, events, bronze_t, table)
        merge_silver(spark_, events, silver_t, table, a.require_newer)
        # Evidence: what reached this batch (and so the merge), and what bronze kept.
        stats = events.agg(F.count("*").alias("n"), F.countDistinct("kafka_partition", "kafka_offset").alias("d"),
                           F.collect_set(F.regexp_extract("file_name", "[^/]+$", 0)).alias("files")).first()
        spark_.createDataFrame(
            [(table, batch_id, stats["n"], stats["d"], spark_.table(bronze_t).count() - before,
              sorted(stats["files"]), a.require_newer)],
            "table STRING, batch_id BIGINT, events_in_batch BIGINT, distinct_events BIGINT, "
            "bronze_rows_added BIGINT, files ARRAY<STRING>, require_newer BOOLEAN",
        ).withColumn("logged_at", F.current_timestamp()).write.mode("append").saveAsTable(
            f"{a.catalog}.{a.bronze}._batch_log")

    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "json")
              .schema(LANDING_SCHEMA)
              .load(f"{a.landing}/{table}/")
              .select("*", "_metadata"))
    q = (stream.writeStream.foreachBatch(batch)
         .option("checkpointLocation", f"{a.checkpoints}/{table}")
         .trigger(availableNow=True).start())
    q.awaitTermination()


def rebuild_silver(spark, a, table: str) -> None:
    silver_t = f"{a.catalog}.{a.silver}.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {silver_t}")
    ensure_tables(spark, a.catalog, a.bronze, a.silver, table)
    events = spark.table(f"{a.catalog}.{a.bronze}.{table}_events")
    merge_silver(spark, events, silver_t, table, a.require_newer)


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--bronze", required=True)
    p.add_argument("--silver", required=True)
    p.add_argument("--landing", required=True)
    p.add_argument("--checkpoints", required=True)
    p.add_argument("--tables", default=",".join(TABLES))
    p.add_argument("--rebuild-silver", action="store_true")
    p.add_argument("--no-require-newer", dest="require_newer", action="store_false")
    a = p.parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    for s in (a.bronze, a.silver):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {a.catalog}.{s}")
    for table in a.tables.split(","):
        ensure_tables(spark, a.catalog, a.bronze, a.silver, table)
        if a.rebuild_silver:
            rebuild_silver(spark, a, table)
        else:
            run_table(spark, a, table)
        print(f"done: {table}")


if __name__ == "__main__":
    main()
