"""End-to-end test on a 10k-row synthetic dataset.

Exercises the full Bronze → Silver → Gold flow against Delta-on-disk.
Should run in well under 2 minutes on a laptop.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from pipeline.bronze import run_bronze
from pipeline.generator import build_product_catalog, generate_events, write_products, write_raw
from pipeline.gold import run_gold
from pipeline.quality import run_silver_checks
from pipeline.settings import (
    GeneratorCfg,
    Paths,
    PerformanceCfg,
    QualityCfg,
    Settings,
    SparkCfg,
)
from pipeline.silver import run_silver


@pytest.fixture
def settings(tmp_paths) -> Settings:
    return Settings(
        app_name="e2e-test",
        env="test",
        paths=Paths(**{k: v for k, v in tmp_paths.items() if k != "catalog"}),
        generator=GeneratorCfg(
            num_events=10_000,
            num_users=500,
            num_products=100,
            num_days=2,
            start_date=date(2026, 4, 1),
            seed=11,
            malformed_ratio=0.005,
            duplicate_ratio=0.02,
        ),
        spark=SparkCfg(master="local[2]", shuffle_partitions=4),
        quality=QualityCfg(
            silver_max_null_pct={
                "event_id": 0.0,
                "user_id": 0.0,
                "event_type": 0.0,
                "event_ts": 0.0,
            },
            silver_min_distinct_event_types=3,
            gold_min_sessions=1,
        ),
        performance=PerformanceCfg(salt_buckets=4),
    )


def test_full_pipeline_10k(spark, settings, tmp_paths):
    # Generate
    events = generate_events(spark, settings, num_events=10_000)
    write_raw(events, settings.paths.raw)
    catalog = build_product_catalog(spark, settings)
    write_products(catalog, tmp_paths["catalog"])

    # Patch catalog path so silver finds it (it derives catalog from bronze parent)
    # Settings places catalog at {bronze_parent}/catalog_products in production,
    # so we mirror that:
    catalog_dir = Path(settings.paths.bronze).parent / "catalog_products"
    if catalog_dir != Path(tmp_paths["catalog"]):
        Path(catalog_dir).parent.mkdir(parents=True, exist_ok=True)
        write_products(catalog, str(catalog_dir))

    # Bronze
    b = run_bronze(spark, settings)
    assert b["rows_ingested"] >= 10_000  # duplicates may inflate

    # Silver
    s = run_silver(spark, settings)
    assert s["rows_silver"] > 0

    silver_df = spark.read.format("delta").load(settings.paths.silver)
    # No duplicates after dedup
    assert silver_df.count() == silver_df.select("event_id").distinct().count()

    # Quality must pass
    run_silver_checks(silver_df, settings.quality, fail_fast=True)

    # Gold
    g = run_gold(spark, settings)
    assert g["rows_sessions"] > 0
    assert g["rows_top_products"] > 0
    # Funnel may be 0 rows for tiny country buckets but on 10k it shouldn't be
    assert g["rows_funnel"] >= 1

    # DLQ should have caught some malformed rows (we injected 0.5%)
    dlq_path = settings.paths.dlq
    if Path(dlq_path).exists():
        dlq_df = spark.read.format("delta").load(dlq_path)
        assert dlq_df.count() >= 1
