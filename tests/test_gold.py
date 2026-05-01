"""Gold aggregation tests."""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.gold import build_funnel, build_sessions, build_top_products

# Explicit schema: Spark 4 cannot infer types from rows where some columns are
# all-None (e.g. quantity/price for view events).
_SILVER_TEST_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("event_date", DateType(), True),
    StructField("event_hour", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("list_price", DoubleType(), True),
])


def _silver(spark, rows):
    return spark.createDataFrame(rows, schema=_SILVER_TEST_SCHEMA)


def test_build_sessions_aggregates_one_session(spark):
    rows = [
        ("e1", datetime(2026, 4, 10, 10, 0, 0), date(2026, 4, 10), 10, "view", "U1", "S1", "PRD_1", None, None, "USD", "US", "m", "X", "Books", "B", 10.0),
        ("e2", datetime(2026, 4, 10, 10, 5, 0), date(2026, 4, 10), 10, "add_to_cart", "U1", "S1", "PRD_1", 1, 10.0, "USD", "US", "m", "X", "Books", "B", 10.0),
        ("e3", datetime(2026, 4, 10, 10, 7, 0), date(2026, 4, 10), 10, "purchase", "U1", "S1", "PRD_1", 2, 10.0, "USD", "US", "m", "X", "Books", "B", 10.0),
    ]
    s = _silver(spark, rows)
    out = build_sessions(s, salt_buckets=4).collect()
    assert len(out) == 1
    r = out[0]
    assert r["session_id"] == "S1"
    assert r["event_count"] == 3
    assert r["converted"] is True
    assert r["revenue"] == 20.0  # price 10 * qty 2
    assert r["duration_sec"] == 7 * 60


def test_top_products_ranks_views(spark):
    rows = [
        ("e1", datetime(2026, 4, 10, 10, 0, 0), date(2026, 4, 10), 10, "view", "U1", "S1", "PRD_A", None, None, "USD", "US", "m", "A", "C", "B", 1.0),
        ("e2", datetime(2026, 4, 10, 10, 1, 0), date(2026, 4, 10), 10, "view", "U2", "S2", "PRD_A", None, None, "USD", "US", "m", "A", "C", "B", 1.0),
        ("e3", datetime(2026, 4, 10, 10, 2, 0), date(2026, 4, 10), 10, "view", "U3", "S3", "PRD_B", None, None, "USD", "US", "m", "B", "C", "B", 1.0),
    ]
    s = _silver(spark, rows)
    out = build_top_products(s).orderBy("rank_by_views").collect()
    assert out[0]["product_id"] == "PRD_A"
    assert out[0]["views"] == 2
    assert out[0]["rank_by_views"] == 1
    assert out[1]["product_id"] == "PRD_B"


def test_funnel_view_to_purchase(spark):
    rows = [
        # User 1 goes view -> cart -> purchase
        ("e1", datetime(2026, 4, 10, 10, 0, 0), date(2026, 4, 10), 10, "view", "U1", "S1", "PRD_A", None, None, "USD", "US", "m", "A", "C", "B", 1.0),
        ("e2", datetime(2026, 4, 10, 10, 1, 0), date(2026, 4, 10), 10, "add_to_cart", "U1", "S1", "PRD_A", 1, 1.0, "USD", "US", "m", "A", "C", "B", 1.0),
        ("e3", datetime(2026, 4, 10, 10, 2, 0), date(2026, 4, 10), 10, "purchase", "U1", "S1", "PRD_A", 1, 1.0, "USD", "US", "m", "A", "C", "B", 1.0),
        # User 2 only views
        ("e4", datetime(2026, 4, 10, 10, 3, 0), date(2026, 4, 10), 10, "view", "U2", "S2", "PRD_A", None, None, "USD", "US", "m", "A", "C", "B", 1.0),
    ]
    s = _silver(spark, rows)
    out = {row["step"]: row for row in build_funnel(s).collect()}
    assert out["view"]["users"] == 2
    assert out["add_to_cart"]["users"] == 1
    assert out["purchase"]["users"] == 1
    # 2 viewers → 1 cart = 50%
    assert out["view"]["conversion_to_next_pct"] == 50.0
