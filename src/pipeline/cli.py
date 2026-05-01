"""End-to-end pipeline orchestrator."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from time import perf_counter

import click

from pipeline.bronze import run_bronze
from pipeline.gold import optimize_zorder, run_gold
from pipeline.logging_utils import get_logger
from pipeline.quality import run_gold_checks, run_silver_checks
from pipeline.settings import load_settings
from pipeline.silver import run_silver
from pipeline.spark import spark_session

log = get_logger("pipeline.cli")


@click.group()
def main() -> None:
    """ETL pipeline CLI."""


@main.command("run")
@click.option("--config", "config_path", default=None)
@click.option("--skip-bronze", is_flag=True, help="Skip Bronze (data already ingested)")
@click.option("--skip-silver", is_flag=True)
@click.option("--skip-gold", is_flag=True)
@click.option("--no-zorder", is_flag=True, help="Skip Z-ORDER optimization step")
@click.option("--no-quality", is_flag=True, help="Skip data quality assertions")
def run(
    config_path: str | None,
    skip_bronze: bool,
    skip_silver: bool,
    skip_gold: bool,
    no_zorder: bool,
    no_quality: bool,
) -> None:
    settings = load_settings(config_path)
    Path("benchmarks/results").mkdir(parents=True, exist_ok=True)
    stats: dict = {}
    t0 = perf_counter()

    with spark_session(settings, "pipeline") as spark:
        if not skip_bronze:
            stats["bronze"] = run_bronze(spark, settings)
        if not skip_silver:
            stats["silver"] = run_silver(spark, settings)
            if not no_quality:
                silver_df = spark.read.format("delta").load(settings.paths.silver)
                run_silver_checks(silver_df, settings.quality)
        if not skip_gold:
            stats["gold"] = run_gold(spark, settings)
            if not no_zorder:
                optimize_zorder(spark, settings)
            if not no_quality:
                sessions_df = spark.read.format("delta").load(
                    f"{settings.paths.gold}/fact_sessions"
                )
                run_gold_checks(sessions_df, settings.quality)

    stats["total_elapsed_sec"] = perf_counter() - t0
    log.info("PIPELINE OK in %.2fs — %s", stats["total_elapsed_sec"], json.dumps(stats, default=str))
    print(json.dumps(stats, indent=2, default=str))


@main.command("clean")
@click.option("--config", "config_path", default=None)
@click.confirmation_option(prompt="Delete all data dirs?")
def clean(config_path: str | None) -> None:
    """Wipe data dirs (bronze/silver/gold/dlq/checkpoints). Raw is preserved."""
    import shutil
    settings = load_settings(config_path)
    for p in [
        settings.paths.bronze,
        settings.paths.silver,
        settings.paths.gold,
        settings.paths.dlq,
        settings.paths.checkpoints,
    ]:
        if Path(p).exists():
            shutil.rmtree(p)
            log.info("removed %s", p)


if __name__ == "__main__":
    sys.exit(main())
