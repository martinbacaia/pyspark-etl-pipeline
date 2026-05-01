"""Silver transformation tests using chispa for DataFrame equality."""
from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.silver import deduplicate, enrich_with_catalog, parse_bronze


def _bronze_row(spark, raw: str, ingest_ts: str = "2026-04-10 12:00:00"):
    return spark.createDataFrame(
        [(raw, datetime.fromisoformat(ingest_ts))],
        schema=StructType([
            StructField("raw", StringType(), True),
            StructField("ingest_ts", TimestampType(), True),
        ]),
    )


def test_parse_bronze_routes_invalid_json_to_dlq(spark):
    df = _bronze_row(spark, '{not valid json}')
    clean, dlq = parse_bronze(df)
    assert clean.count() == 0
    rows = dlq.collect()
    assert len(rows) == 1
    assert rows[0]["dlq_reason"] == "json_parse_error"


def test_parse_bronze_routes_missing_user_to_dlq(spark):
    rec = json.dumps({
        "event_id": "e1",
        "event_ts": "2026-04-10T12:00:00Z",
        "event_type": "view",
        "product_id": "PRD_000001",
    })
    df = _bronze_row(spark, rec)
    clean, dlq = parse_bronze(df)
    assert clean.count() == 0
    assert dlq.collect()[0]["dlq_reason"] == "missing_user_id"


def test_parse_bronze_keeps_valid_event(spark):
    rec = json.dumps({
        "event_id": "e1",
        "event_ts": "2026-04-10T12:00:00Z",
        "event_type": "view",
        "user_id": "U_1",
        "session_id": "S_1",
        "product_id": "PRD_1",
        "quantity": "1",
        "price": "9.99",
        "currency": "USD",
        "country": "US",
        "device": "mobile",
    })
    df = _bronze_row(spark, rec)
    clean, dlq = parse_bronze(df)
    assert clean.count() == 1
    assert dlq.count() == 0
    row = clean.collect()[0]
    assert row["event_id"] == "e1"
    assert row["price"] == 9.99
    assert row["quantity"] == 1


def test_deduplicate_keeps_latest(spark):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("payload", StringType(), True),
        StructField("ingest_ts", TimestampType(), True),
    ])
    older = datetime(2026, 4, 10, 10, 0, 0)
    newer = datetime(2026, 4, 10, 11, 0, 0)
    df = spark.createDataFrame(
        [("e1", "old", older), ("e1", "new", newer), ("e2", "x", older)],
        schema=schema,
    )
    out = deduplicate(df).orderBy("event_id").collect()
    assert len(out) == 2
    assert {r["event_id"]: r["payload"] for r in out} == {"e1": "new", "e2": "x"}


def test_enrich_with_catalog_left_join(spark):
    events = spark.createDataFrame(
        [("e1", "PRD_1"), ("e2", "PRD_MISSING")],
        ["event_id", "product_id"],
    )
    catalog = spark.createDataFrame(
        [("PRD_1", "Foo", "Books", "Acme", 9.99)],
        ["product_id", "product_name", "category", "brand", "list_price"],
    )
    out = enrich_with_catalog(events, catalog).orderBy("event_id").collect()
    assert out[0]["product_name"] == "Foo"
    assert out[1]["product_name"] is None  # left join preserves event with no match
