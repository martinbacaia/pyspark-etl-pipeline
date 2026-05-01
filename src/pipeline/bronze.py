"""Bronze layer: idempotent ingestion of raw JSONL into Delta.

Bronze is intentionally dumb: read the bytes, slap on ingest metadata,
partition by event_date so downstream Silver can prune, write Delta.

Idempotency: we use Delta MERGE on event_id when an existing table is
present. First run does an INSERT-only fast path.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.logging_utils import get_logger, timed
from pipeline.settings import Settings

log = get_logger("pipeline.bronze")

_BRONZE_INPUT_SCHEMA = StructType([StructField("value", StringType(), True)])


def read_raw(spark: SparkSession, raw_path: str) -> DataFrame:
    """Read raw JSONL as text. We do NOT parse JSON here — that's Silver's job.

    Reading as text is the safest Bronze pattern: a corrupt JSON line cannot
    fail the ingest, and we keep an exact byte-for-byte copy of source data.
    """
    return spark.read.schema(_BRONZE_INPUT_SCHEMA).text(raw_path).withColumnRenamed("value", "raw")


def add_ingest_metadata(df: DataFrame, source_uri: str) -> DataFrame:
    """Tag each row with ingest_ts, source path, and a tentative event_date.

    `event_date` is best-effort extracted from a timestamp regex so we can
    partition cheaply. Rows where extraction fails go to a sentinel date
    so we still write them and Silver can route to DLQ.
    """
    ts_regex = r'"event_ts"\s*:\s*"(\d{4}-\d{2}-\d{2})'
    return (
        df.withColumn("ingest_ts", F.lit(datetime.now(timezone.utc)).cast("timestamp"))
        .withColumn("source", F.lit(source_uri))
        .withColumn(
            "event_date",
            F.coalesce(
                F.to_date(F.regexp_extract("raw", ts_regex, 1), "yyyy-MM-dd"),
                F.lit("1970-01-01").cast("date"),
            ),
        )
        # Cheap dedup key for Bronze — full event_id is parsed in Silver.
        .withColumn("ingest_hash", F.sha2(F.col("raw"), 256))
    )


def write_bronze(df: DataFrame, bronze_path: str) -> int:
    """Write to Delta partitioned by event_date. Idempotent via MERGE on ingest_hash."""
    spark = df.sparkSession
    n_in = df.count()
    Path(bronze_path).mkdir(parents=True, exist_ok=True)

    if DeltaTable.isDeltaTable(spark, bronze_path):
        log.info("bronze table exists → MERGE on ingest_hash (idempotent)")
        target = DeltaTable.forPath(spark, bronze_path)
        (
            target.alias("t")
            .merge(df.alias("s"), "t.ingest_hash = s.ingest_hash")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        log.info("bronze table fresh → INSERT (initial load)")
        (
            df.write.mode("overwrite")
            .format("delta")
            .partitionBy("event_date")
            .save(bronze_path)
        )
    return n_in


def run_bronze(spark: SparkSession, settings: Settings) -> dict:
    """Bronze pipeline entry point. Returns a stats dict."""
    raw = settings.paths.raw
    bronze = settings.paths.bronze

    with timed(log, "bronze") as t:
        df = read_raw(spark, raw)
        df = add_ingest_metadata(df, source_uri=raw)
        rows = write_bronze(df, bronze)

    return {"rows_ingested": rows, "elapsed_sec": t["elapsed_sec"], "path": bronze}
