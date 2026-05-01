"""Pytest fixtures: a session-scoped local SparkSession with Delta enabled."""
from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Lightweight local Spark with Delta. One per test session.

    We use 2 shuffle partitions to keep tests fast; production uses 200.
    """
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-etl-pipeline-tests")
        .config("spark.sql.shufflePartitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.2.0")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def tmp_paths(tmp_path: Path) -> dict[str, str]:
    base = tmp_path
    paths = {
        "raw": str(base / "raw"),
        "bronze": str(base / "bronze"),
        "silver": str(base / "silver"),
        "gold": str(base / "gold"),
        "dlq": str(base / "dlq"),
        "checkpoints": str(base / "_chk"),
        "catalog": str(base / "catalog_products"),
    }
    yield paths
    # tmp_path is auto-cleaned by pytest
