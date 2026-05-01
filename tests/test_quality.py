from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.quality import (
    DataQualityError,
    assert_schema_subset,
    check_event_type_domain,
    check_null_pct,
    run_silver_checks,
)
from pipeline.settings import QualityCfg


def test_assert_schema_subset_pass(spark):
    df = spark.createDataFrame([("e", 1)], ["event_id", "n"])
    r = assert_schema_subset(df, {"event_id": "string"})
    assert r.passed


def test_assert_schema_subset_fail_missing(spark):
    df = spark.createDataFrame([("e",)], ["event_id"])
    r = assert_schema_subset(df, {"event_id": "string", "missing": "string"})
    assert not r.passed


def test_null_pct_violation(spark):
    df = spark.createDataFrame([(None,), ("x",)], ["c"])
    [r] = check_null_pct(df, {"c": 0.0})
    assert not r.passed


def test_event_type_domain_detects_invalid(spark):
    df = spark.createDataFrame([("view",), ("bogus",)], ["event_type"])
    r = check_event_type_domain(df)
    assert not r.passed


def test_run_silver_checks_raises_on_failure(spark):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_ts", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
    ])
    df = spark.createDataFrame(
        [("e1", datetime(2026, 4, 10, 10, 0, 0), "view", None)],
        schema=schema,
    )
    cfg = QualityCfg(
        silver_max_null_pct={"user_id": 0.0},
        silver_min_distinct_event_types=1,
    )
    with pytest.raises(DataQualityError):
        run_silver_checks(df, cfg, fail_fast=True)
