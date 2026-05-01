"""Silver layer: parse, type, validate, deduplicate, enrich, route bad rows to DLQ.

Design notes:
  * Permissive JSON parsing into a typed schema. Anything that fails parsing
    or required-field checks is routed to a Dead Letter Queue (DLQ) Delta
    table — never silently dropped.
  * Dedup uses event_id (canonical). Last-write-wins on ingest_ts so a re-run
    that re-emits the same event_id with newer data overwrites cleanly.
  * Catalog enrichment uses an explicit broadcast() hint. The catalog is
    small (~few MB at 5k products) so this is cheap and avoids shuffle.
"""
from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.logging_utils import get_logger, timed
from pipeline.schemas import SILVER_EVENT_SCHEMA, VALID_EVENT_TYPES
from pipeline.settings import Settings

log = get_logger("pipeline.silver")


def parse_bronze(bronze_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse the raw JSON column into typed columns.

    Returns (parsed, malformed). `malformed` keeps the original raw payload
    plus a reason so DLQ rows are debuggable.
    """
    schema = "struct<event_id:string,event_ts:string,event_type:string,user_id:string,session_id:string,product_id:string,quantity:string,price:string,currency:string,country:string,device:string,user_agent:string,referrer:string>"

    # Spark 4's permissive from_json returns a struct of all-null fields for
    # unparseable input rather than NULL itself, so we detect "couldn't parse
    # anything" by checking that every required field is null.
    parsed = bronze_df.withColumn("payload", F.from_json("raw", schema))

    flat = parsed.select(
        F.col("payload.event_id").alias("event_id"),
        F.col("payload.event_ts").alias("event_ts_str"),
        F.col("payload.event_type").alias("event_type"),
        F.col("payload.user_id").alias("user_id"),
        F.col("payload.session_id").alias("session_id"),
        F.col("payload.product_id").alias("product_id"),
        F.col("payload.quantity").cast("int").alias("quantity"),
        F.col("payload.price").cast("double").alias("price"),
        F.col("payload.currency").alias("currency"),
        F.col("payload.country").alias("country"),
        F.col("payload.device").alias("device"),
        F.col("payload").alias("_payload"),
        F.col("ingest_ts"),
        F.col("raw"),
    # try_to_timestamp returns NULL for unparseable strings instead of raising
    # under Spark 4 ANSI mode (e.g. injected "NOT-A-DATE" malformed events).
    ).withColumn("event_ts", F.try_to_timestamp("event_ts_str"))

    cant_parse = (
        F.col("_payload").isNull()
        | (
            F.col("event_id").isNull()
            & F.col("user_id").isNull()
            & F.col("event_type").isNull()
            & F.col("event_ts_str").isNull()
        )
    )
    required_missing = (
        F.col("event_id").isNull()
        | F.col("user_id").isNull()
        | F.col("event_type").isNull()
        | F.col("event_ts").isNull()
        | ~F.col("event_type").isin(*VALID_EVENT_TYPES)
    )

    dlq = flat.filter(required_missing).select(
        F.col("raw"),
        F.col("ingest_ts"),
        F.when(cant_parse, F.lit("json_parse_error"))
        .when(F.col("event_id").isNull(), F.lit("missing_event_id"))
        .when(F.col("user_id").isNull(), F.lit("missing_user_id"))
        .when(F.col("event_type").isNull(), F.lit("missing_event_type"))
        .when(F.col("event_ts").isNull(), F.lit("bad_event_ts"))
        .otherwise(F.lit("invalid_event_type"))
        .alias("dlq_reason"),
    )

    clean = flat.filter(~required_missing).drop("raw", "event_ts_str", "_payload")
    return clean, dlq


def deduplicate(df: DataFrame) -> DataFrame:
    """Dedup by event_id keeping the latest ingest_ts (last-write-wins)."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "ingest_ts")
    )


def enrich_with_catalog(events: DataFrame, catalog: DataFrame) -> DataFrame:
    """Broadcast-join the small product catalog onto events.

    We explicitly hint broadcast() rather than relying on autoBroadcastJoinThreshold
    so behavior is stable across cluster sizes / file stats.
    """
    cat = catalog.select(
        F.col("product_id"),
        F.col("product_name"),
        F.col("category"),
        F.col("brand"),
        F.col("list_price"),
    )
    return events.join(F.broadcast(cat), on="product_id", how="left")


def derive_partition_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("event_date", F.to_date("event_ts")).withColumn(
        "event_hour", F.hour("event_ts")
    )


def write_silver(df: DataFrame, silver_path: str, partitions: list[str]) -> int:
    """Idempotent write via MERGE on event_id."""
    spark = df.sparkSession
    Path(silver_path).mkdir(parents=True, exist_ok=True)
    df = df.select(*[c.name for c in SILVER_EVENT_SCHEMA.fields])
    n = df.count()

    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        (
            target.alias("t")
            .merge(df.alias("s"), "t.event_id = s.event_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write.mode("overwrite")
            .format("delta")
            .partitionBy(*partitions)
            .save(silver_path)
        )
    return n


def write_dlq(df: DataFrame, dlq_path: str) -> int:
    """DLQ is append-only Delta. Operators inspect it manually."""
    Path(dlq_path).mkdir(parents=True, exist_ok=True)
    n = df.count()
    if n == 0:
        return 0
    (df.write.mode("append").format("delta").save(dlq_path))
    return n


def run_silver(spark: SparkSession, settings: Settings) -> dict:
    bronze_path = settings.paths.bronze
    silver_path = settings.paths.silver
    dlq_path = settings.paths.dlq
    catalog_path = str(Path(settings.paths.bronze).parent / "catalog_products")

    with timed(log, "silver") as t:
        bronze = spark.read.format("delta").load(bronze_path)
        catalog = spark.read.format("delta").load(catalog_path)

        clean, dlq = parse_bronze(bronze)
        dedup = deduplicate(clean)
        enriched = enrich_with_catalog(dedup, catalog)
        partitioned = derive_partition_columns(enriched)
        n_silver = write_silver(partitioned, silver_path, settings.performance.partitions_silver)
        n_dlq = write_dlq(dlq, dlq_path)

    return {
        "rows_silver": n_silver,
        "rows_dlq": n_dlq,
        "elapsed_sec": t["elapsed_sec"],
        "path": silver_path,
    }
