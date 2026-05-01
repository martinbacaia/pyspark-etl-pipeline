"""Gold aggregation tests."""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import functions as F

from pipeline.gold import build_funnel, build_sessions, build_top_products


def _silver(spark, rows):
    cols = [
        "event_id", "event_ts", "event_date", "event_hour", "event_type",
        "user_id", "session_id", "product_id", "quantity", "price",
        "currency", "country", "device", "product_name", "category",
        "brand", "list_price",
    ]
    return spark.createDataFrame(rows, cols)


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
