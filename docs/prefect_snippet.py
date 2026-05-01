"""Prefect integration sketch (illustrative)."""
from __future__ import annotations

from prefect import flow, task

from pipeline.bronze import run_bronze
from pipeline.gold import optimize_zorder, run_gold
from pipeline.quality import run_gold_checks, run_silver_checks
from pipeline.settings import load_settings
from pipeline.silver import run_silver
from pipeline.spark import build_spark


@task(retries=2, retry_delay_seconds=120)
def t_bronze(spark, settings):
    return run_bronze(spark, settings)


@task(retries=2)
def t_silver(spark, settings):
    out = run_silver(spark, settings)
    df = spark.read.format("delta").load(settings.paths.silver)
    run_silver_checks(df, settings.quality)
    return out


@task(retries=1)
def t_gold(spark, settings):
    out = run_gold(spark, settings)
    optimize_zorder(spark, settings)
    df = spark.read.format("delta").load(f"{settings.paths.gold}/fact_sessions")
    run_gold_checks(df, settings.quality)
    return out


@flow(name="ecommerce-events-etl")
def pipeline(config_path: str | None = None):
    settings = load_settings(config_path)
    spark = build_spark(settings, "prefect")
    try:
        b = t_bronze(spark, settings)
        s = t_silver(spark, settings, wait_for=[b])
        g = t_gold(spark, settings, wait_for=[s])
    finally:
        spark.stop()
    return {"bronze": b, "silver": s, "gold": g}


if __name__ == "__main__":
    pipeline()
