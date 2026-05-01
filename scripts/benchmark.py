"""Run the pipeline at multiple scales and persist results.

Usage:
    python scripts/benchmark.py --sizes 100000,1000000,10000000

Each run:
  1. wipes data dirs
  2. generates synthetic events at the target size
  3. runs Bronze -> Silver -> Gold (with quality + Z-ORDER)
  4. records elapsed seconds per stage and total

Output: benchmarks/results/benchmark-<host>-<timestamp>.json
"""
from __future__ import annotations

import json
import platform
import shutil
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import click

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from pipeline.bronze import run_bronze  # noqa: E402
from pipeline.generator import (  # noqa: E402
    build_product_catalog,
    generate_events,
    write_products,
    write_raw,
)
from pipeline.gold import optimize_zorder, run_gold  # noqa: E402
from pipeline.logging_utils import get_logger, timed  # noqa: E402
from pipeline.quality import run_gold_checks, run_silver_checks  # noqa: E402
from pipeline.settings import load_settings  # noqa: E402
from pipeline.silver import run_silver  # noqa: E402
from pipeline.spark import spark_session  # noqa: E402

log = get_logger("benchmark")


def _wipe(settings) -> None:
    for p in (
        settings.paths.raw,
        settings.paths.bronze,
        settings.paths.silver,
        settings.paths.gold,
        settings.paths.dlq,
        settings.paths.checkpoints,
        str(Path(settings.paths.bronze).parent / "catalog_products"),
    ):
        if Path(p).exists():
            shutil.rmtree(p, ignore_errors=True)


def _machine_info() -> dict:
    import os
    return {
        "host": socket.gethostname(),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def _run_one(size: int, config_path: str | None) -> dict:
    settings = load_settings(config_path)
    settings.generator.num_events = size

    _wipe(settings)

    out: dict = {"size": size}
    with spark_session(settings, f"bench-{size}") as spark:
        with timed(log, f"generate {size:,}") as g:
            events = generate_events(spark, settings, num_events=size)
            write_raw(events, settings.paths.raw)
            catalog = build_product_catalog(spark, settings)
            write_products(catalog, str(Path(settings.paths.bronze).parent / "catalog_products"))
        out["generate_sec"] = g["elapsed_sec"]

        out["bronze"] = run_bronze(spark, settings)
        out["silver"] = run_silver(spark, settings)
        silver_df = spark.read.format("delta").load(settings.paths.silver)
        run_silver_checks(silver_df, settings.quality)

        out["gold"] = run_gold(spark, settings)
        with timed(log, "zorder") as z:
            optimize_zorder(spark, settings)
        out["zorder_sec"] = z["elapsed_sec"]

        sessions_df = spark.read.format("delta").load(f"{settings.paths.gold}/fact_sessions")
        run_gold_checks(sessions_df, settings.quality)

        out["totals"] = {
            "ingest_to_gold_sec": (
                out["bronze"]["elapsed_sec"]
                + out["silver"]["elapsed_sec"]
                + out["gold"]["elapsed_sec"]
            ),
            "rows_silver": out["silver"]["rows_silver"],
        }
    return out


@click.command()
@click.option("--sizes", default="100000,1000000", help="Comma-separated event counts")
@click.option("--config", "config_path", default=None)
@click.option("--out", "out_path", default=None, help="Override output JSON path")
def main(sizes: str, config_path: str | None, out_path: str | None) -> None:
    size_list = [int(s.strip().replace("_", "")) for s in sizes.split(",") if s.strip()]
    info = _machine_info()
    results: list[dict] = []
    for s in size_list:
        log.info("=== benchmark size=%s ===", f"{s:,}")
        results.append(_run_one(s, config_path))

    payload = {"machine": info, "runs": results}
    out = Path(out_path) if out_path else (
        REPO / "benchmarks" / "results"
        / f"benchmark-{info['host']}-{int(time.time())}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str))
    log.info("results -> %s", out)
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
