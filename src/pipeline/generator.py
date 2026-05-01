"""Synthetic e-commerce event generator.

Designed to produce realistic skew and noise:
  * a small fraction of "power users" emit a heavy share of events (Zipf-like)
  * a configurable fraction of events are malformed (drop a key, garble JSON)
  * a configurable fraction are duplicated (same event_id) to exercise dedup
  * weighted event-type mix that is loosely funnel-shaped (more views than buys)

The generator runs *distributed* via Spark — we generate IDs/parameters with
mapPartitions so it scales linearly to 100M rows on a single laptop. The
product catalog is small and written in driver-mode.
"""
from __future__ import annotations

import json
import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import click
from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from pipeline.logging_utils import get_logger, timed
from pipeline.schemas import PRODUCT_SCHEMA
from pipeline.settings import Settings, load_settings
from pipeline.spark import spark_session

log = get_logger("pipeline.generator")

CATEGORIES = [
    "Electronics", "Books", "Apparel", "Home", "Beauty",
    "Sports", "Toys", "Grocery", "Automotive", "Garden",
]
DEVICES = ["mobile", "desktop", "tablet"]
COUNTRIES = ["US", "AR", "BR", "MX", "ES", "DE", "FR", "GB", "IN", "JP"]
CURRENCIES = ["USD", "EUR", "BRL", "ARS", "GBP", "JPY"]
EVENT_WEIGHTS = {
    "view": 0.70,
    "search": 0.12,
    "add_to_cart": 0.10,
    "remove_from_cart": 0.03,
    "purchase": 0.05,
}


@dataclass
class GenStats:
    events_written: int
    products_written: int
    elapsed_sec: float
    output_path: str


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def build_product_catalog(spark: SparkSession, settings: Settings) -> DataFrame:
    """Generate a small product dim with deterministic IDs (PRD_000001 …).

    Driver-side (catalog is tiny, ≤ a few MB). Returned DF is suitable for
    broadcast joins.
    """
    fake = Faker()
    Faker.seed(settings.generator.seed)
    random.seed(settings.generator.seed)

    n = settings.generator.num_products
    rows = []
    for i in range(n):
        cat = random.choice(CATEGORIES)
        rows.append((
            f"PRD_{i:06d}",
            fake.catch_phrase()[:80],
            cat,
            fake.company()[:40],
            round(random.uniform(2.0, 1500.0), 2),
            random.random() > 0.02,  # 2% inactive
        ))
    return spark.createDataFrame(rows, schema=PRODUCT_SCHEMA)


# ---------------------------------------------------------------------------
# Event generator (distributed)
# ---------------------------------------------------------------------------

def _row_dict(seed: int, opts: dict) -> dict | tuple[dict, str, bool]:
    """Generate one synthetic event.

    Returns either:
      * a plain dict with a single ``_raw`` key — sentinel for fully-corrupt
        JSON garbage that the partition encoder should pass through verbatim, OR
      * ``(record, event_id, duplicate_flag)`` for the normal path.

    Splitting this out of the partition-iterator keeps the hot loop simple
    and makes it cheap to unit-test (no Spark needed).
    """
    rng = random.Random(seed)

    n_users = opts["num_users"]
    n_products = opts["num_products"]
    power_user_ratio = opts["power_user_ratio"]
    start_ts: float = opts["start_ts_epoch"]
    span_sec: int = opts["span_sec"]
    malformed_ratio = opts["malformed_ratio"]
    duplicate_ratio = opts["duplicate_ratio"]

    # ~30% of events come from the top `power_user_ratio` users (Zipf-ish skew).
    if rng.random() < 0.30:
        # power user
        user_idx = rng.randrange(0, max(1, int(n_users * power_user_ratio)))
    else:
        user_idx = rng.randrange(0, n_users)

    user_id = f"U_{user_idx:08d}"
    product_id = f"PRD_{rng.randrange(0, n_products):06d}"

    # Pick event type weighted
    r = rng.random()
    cum = 0.0
    event_type = "view"
    for et, w in EVENT_WEIGHTS.items():
        cum += w
        if r <= cum:
            event_type = et
            break

    # Sessions: deterministic from user + day, with some intra-day variation
    ts_offset = rng.randrange(0, span_sec)
    event_ts = datetime.utcfromtimestamp(start_ts + ts_offset)
    session_bucket = rng.randrange(0, 6)  # up to ~6 sessions per user/day
    session_id = f"S_{user_id}_{event_ts.date().isoformat()}_{session_bucket}"

    quantity = rng.randint(1, 4) if event_type in ("add_to_cart", "purchase") else None
    price = round(rng.uniform(2.0, 1500.0), 2) if event_type in ("add_to_cart", "purchase") else None

    event_id = str(uuid.UUID(int=rng.getrandbits(128)))

    record = {
        "event_id": event_id,
        "event_ts": event_ts.isoformat(timespec="seconds") + "Z",
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "product_id": product_id,
        "quantity": quantity,
        "price": price,
        "currency": rng.choice(CURRENCIES),
        "country": rng.choice(COUNTRIES),
        "device": rng.choice(DEVICES),
        "user_agent": f"Mozilla/5.0 ({rng.choice(['X11', 'Win64', 'Macintosh'])})",
        "referrer": rng.choice(["google", "direct", "facebook", "newsletter", None]),
    }

    # Inject malformed records: drop required fields or corrupt JSON
    if rng.random() < malformed_ratio:
        kind = rng.choice(["missing_user", "bad_ts", "garbage"])
        if kind == "missing_user":
            record.pop("user_id", None)
        elif kind == "bad_ts":
            record["event_ts"] = "NOT-A-DATE"
        else:
            return {"_raw": '{"event_id":"' + event_id + '", broken-json}'}

    return record, event_id, (rng.random() < duplicate_ratio)


def _partition_to_jsonl(idx: int, opts: dict, count: int) -> list[str]:
    out: list[str] = []
    base_seed = opts["seed"] * 1_000_003 + idx
    for i in range(count):
        result = _row_dict(base_seed + i, opts)
        if isinstance(result, dict):  # already-malformed garbage
            out.append(result.get("_raw", "{}"))
            continue
        rec, _, dup = result
        line = json.dumps(rec)
        out.append(line)
        if dup:
            out.append(line)  # duplicate event_id
    return out


def generate_events(
    spark: SparkSession, settings: Settings, num_events: int | None = None
) -> DataFrame:
    """Generate `num_events` synthetic events as a Spark DataFrame of raw JSON strings.

    We parallelize per-partition: each partition generates its slice of rows
    in pure Python (fast — no JVM round-trip per row). Returned DF has a
    single column `value: string`.
    """
    cfg = settings.generator
    n = num_events or cfg.num_events

    start = datetime.combine(cfg.start_date, datetime.min.time())
    span_sec = cfg.num_days * 24 * 3600

    opts = {
        "num_users": cfg.num_users,
        "num_products": cfg.num_products,
        "power_user_ratio": cfg.power_user_ratio,
        "start_ts_epoch": start.timestamp(),
        "span_sec": span_sec,
        "malformed_ratio": cfg.malformed_ratio,
        "duplicate_ratio": cfg.duplicate_ratio,
        "seed": cfg.seed,
    }

    # ~250k rows per partition is a sweet spot for local generation
    target_rows_per_part = 250_000
    n_parts = max(spark.sparkContext.defaultParallelism, math.ceil(n / target_rows_per_part))
    base = n // n_parts
    rem = n % n_parts
    counts = [base + (1 if i < rem else 0) for i in range(n_parts)]
    seeds = list(range(n_parts))

    rdd = (
        spark.sparkContext.parallelize(list(zip(seeds, counts, strict=True)), numSlices=n_parts)
        .flatMap(lambda pair: _partition_to_jsonl(pair[0], opts, pair[1]))
    )

    return spark.createDataFrame(
        rdd.map(lambda s: (s,)),
        schema=StructType([StructField("value", StringType(), True)]),
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def write_raw(df: DataFrame, raw_path: str) -> int:
    """Write JSONL to data/raw partitioned by ingest date.

    We write as text (one JSON object per line) because Bronze must ingest
    raw bytes, not pre-typed Spark output. Returns row count.
    """
    Path(raw_path).mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").text(raw_path)
    return df.count()


def write_products(df: DataFrame, path: str) -> int:
    df.coalesce(1).write.mode("overwrite").format("delta").save(path)
    return df.count()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--config", "config_path", default=None, help="YAML config path")
@click.option("--num-events", type=int, default=None, help="Override num events")
@click.option("--out", "out_path", default=None, help="Override raw output path")
def cli(config_path: str | None, num_events: int | None, out_path: str | None) -> None:
    """Generate synthetic events into data/raw and the product catalog."""
    settings = load_settings(config_path)
    raw = out_path or settings.paths.raw
    catalog = str(Path(settings.paths.bronze).parent / "catalog_products")

    with spark_session(settings, "generator") as spark, timed(log, "generate_events") as t:
        events = generate_events(spark, settings, num_events=num_events)
        n = write_raw(events, raw)
        log.info("wrote %s events → %s", f"{n:,}", raw)
        catalog_df = build_product_catalog(spark, settings)
        m = write_products(catalog_df, catalog)
        log.info("wrote %s products → %s", f"{m:,}", catalog)
    log.info("done in %.2fs", t["elapsed_sec"])


if __name__ == "__main__":
    cli()
