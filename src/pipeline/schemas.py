"""Authoritative schemas for each medallion layer."""
from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Raw event as it arrives in JSONL (everything is StringType to be permissive;
# we coerce in Silver). The reader is permissive: malformed rows -> _corrupt_record.
RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),       # ISO-8601 string
    StructField("event_type", StringType(), True),     # view|add_to_cart|purchase|...
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("price", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

# Silver: typed, validated, deduplicated.
SILVER_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("event_date", DateType(), False),
    StructField("event_hour", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
    StructField("device", StringType(), True),
    # Enrichment from product catalog
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("list_price", DoubleType(), True),
])

# Product catalog (small dim, broadcasted).
PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("list_price", DoubleType(), True),
    StructField("active", BooleanType(), True),
])

# Gold marts.
GOLD_SESSION_SCHEMA = StructType([
    StructField("session_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_date", DateType(), False),
    StructField("session_start", TimestampType(), False),
    StructField("session_end", TimestampType(), False),
    StructField("duration_sec", LongType(), True),
    StructField("event_count", LongType(), True),
    StructField("distinct_products", LongType(), True),
    StructField("event_types", ArrayType(StringType()), True),
    StructField("revenue", DoubleType(), True),
    StructField("converted", BooleanType(), True),
])

GOLD_TOP_PRODUCT_SCHEMA = StructType([
    StructField("event_date", DateType(), False),
    StructField("event_hour", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("views", LongType(), True),
    StructField("add_to_cart", LongType(), True),
    StructField("purchases", LongType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("rank_by_views", IntegerType(), True),
])

GOLD_FUNNEL_SCHEMA = StructType([
    StructField("event_date", DateType(), False),
    StructField("country", StringType(), True),
    StructField("step", StringType(), False),
    StructField("users", LongType(), True),
    StructField("conversion_to_next_pct", DoubleType(), True),
])

VALID_EVENT_TYPES = ("view", "add_to_cart", "remove_from_cart", "purchase", "search")
FUNNEL_ORDER = ("view", "add_to_cart", "purchase")
