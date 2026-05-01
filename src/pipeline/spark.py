"""SparkSession factory with Delta Lake configured."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

from pipeline.settings import Settings


def build_spark(settings: Settings, app_suffix: str = "") -> SparkSession:
    """Build a SparkSession wired for Delta Lake + AQE + sane defaults.

    Delta JARs are pulled at runtime via spark.jars.packages so the project
    works without a pre-baked image.
    """
    cfg = settings.spark
    name = settings.app_name + (f"-{app_suffix}" if app_suffix else "")

    builder = (
        SparkSession.builder.appName(name)
        .master(cfg.master)
        .config("spark.sql.shufflePartitions", str(cfg.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(cfg.adaptive_enabled).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", str(cfg.arrow_enabled).lower())
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.executor.memory", cfg.executor_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.autoBroadcastJoinThreshold",
            str(settings.performance.broadcast_threshold_mb * 1024 * 1024),
        )
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.2.0")
        # Quieter logs in local runs
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


@contextmanager
def spark_session(settings: Settings, app_suffix: str = "") -> Iterator[SparkSession]:
    spark = build_spark(settings, app_suffix)
    try:
        yield spark
    finally:
        spark.stop()
