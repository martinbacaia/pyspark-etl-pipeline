"""Gold layer: business marts.

Three marts:
  1. fact_sessions       — one row per (user, session) with derived metrics.
  2. fact_top_products   — hourly top products with view/cart/purchase counts.
  3. fact_funnel         — daily conversion funnel (view → cart → purchase) by country.

Skew handling: power users dominate event counts. For the session aggregate we
use *salting* on user_id so a single hot user's events spread across N reducers,
then we do a second pass to combine. AQE skew-join also kicks in on top.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.logging_utils import get_logger, timed
from pipeline.schemas import (
    FUNNEL_ORDER,
    GOLD_FUNNEL_SCHEMA,
    GOLD_SESSION_SCHEMA,
    GOLD_TOP_PRODUCT_SCHEMA,
)
from pipeline.settings import Settings

log = get_logger("pipeline.gold")


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def build_sessions(silver: DataFrame, salt_buckets: int) -> DataFrame:
    """Aggregate events into session-level facts.

    Salting: we add a uniformly random salt 0..N-1 to the (session_id, salt)
    key and aggregate twice. This keeps a single hot session from landing on
    one reducer.
    """
    salted = silver.withColumn(
        "_salt", (F.rand(seed=7) * salt_buckets).cast("int")
    )

    pre = (
        salted.groupBy("session_id", "_salt", "user_id", "event_date")
        .agg(
            F.min("event_ts").alias("session_start_part"),
            F.max("event_ts").alias("session_end_part"),
            F.count(F.lit(1)).alias("event_count_part"),
            F.collect_set("event_type").alias("event_types_part"),
            F.countDistinct("product_id").alias("distinct_products_part"),
            F.sum(
                F.when(
                    F.col("event_type") == F.lit("purchase"),
                    F.coalesce(F.col("price"), F.col("list_price")) * F.coalesce(F.col("quantity"), F.lit(1)),
                ).otherwise(F.lit(0.0))
            ).alias("revenue_part"),
        )
    )

    final = (
        pre.groupBy("session_id", "user_id", "event_date")
        .agg(
            F.min("session_start_part").alias("session_start"),
            F.max("session_end_part").alias("session_end"),
            F.sum("event_count_part").alias("event_count"),
            F.array_distinct(F.flatten(F.collect_list("event_types_part"))).alias("event_types"),
            F.sum("distinct_products_part").alias("distinct_products"),
            F.sum("revenue_part").alias("revenue"),
        )
        .withColumn(
            "duration_sec",
            (F.col("session_end").cast("long") - F.col("session_start").cast("long")),
        )
        .withColumn("converted", F.array_contains("event_types", "purchase"))
        .select(*[c.name for c in GOLD_SESSION_SCHEMA.fields])
    )
    return final


# ---------------------------------------------------------------------------
# Top products by hour
# ---------------------------------------------------------------------------

def build_top_products(silver: DataFrame) -> DataFrame:
    """Hourly aggregate of product engagement, ranked by views."""
    base = (
        silver.groupBy("event_date", "event_hour", "product_id", "product_name", "category")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("views"),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.sum(
                F.when(
                    F.col("event_type") == "purchase",
                    F.coalesce(F.col("price"), F.col("list_price")) * F.coalesce(F.col("quantity"), F.lit(1)),
                ).otherwise(F.lit(0.0))
            ).alias("revenue"),
        )
    )
    w = Window.partitionBy("event_date", "event_hour").orderBy(F.col("views").desc())
    return (
        base.withColumn("rank_by_views", F.row_number().over(w).cast("int"))
        .select(*[c.name for c in GOLD_TOP_PRODUCT_SCHEMA.fields])
    )


# ---------------------------------------------------------------------------
# Funnel
# ---------------------------------------------------------------------------

def build_funnel(silver: DataFrame) -> DataFrame:
    """Daily conversion funnel by country.

    A user counts at step S if any of their events that day is of type S.
    Funnel order is fixed by FUNNEL_ORDER.
    """
    flagged = silver.filter(F.col("event_type").isin(*FUNNEL_ORDER)).groupBy(
        "event_date", "country", "user_id"
    ).agg(F.collect_set("event_type").alias("types"))

    rows = []
    for step in FUNNEL_ORDER:
        rows.append(
            flagged.filter(F.array_contains("types", step))
            .groupBy("event_date", "country")
            .agg(F.countDistinct("user_id").alias("users"))
            .withColumn("step", F.lit(step))
        )
    union = rows[0]
    for r in rows[1:]:
        union = union.unionByName(r)

    step_idx = F.create_map(*[i for s, idx in zip(FUNNEL_ORDER, range(len(FUNNEL_ORDER))) for i in (F.lit(s), F.lit(idx))])
    w = Window.partitionBy("event_date", "country").orderBy(step_idx[F.col("step")])
    return (
        union.withColumn("next_users", F.lead("users").over(w))
        .withColumn(
            "conversion_to_next_pct",
            F.when(
                F.col("users") > 0,
                F.round(F.col("next_users") / F.col("users") * 100.0, 2),
            ).otherwise(F.lit(None).cast("double")),
        )
        .drop("next_users")
        .select(*[c.name for c in GOLD_FUNNEL_SCHEMA.fields])
    )


# ---------------------------------------------------------------------------
# Persistence + entry point
# ---------------------------------------------------------------------------

def _write(df: DataFrame, path: str, partitions: list[str]) -> int:
    Path(path).mkdir(parents=True, exist_ok=True)
    n = df.count()
    writer = df.write.mode("overwrite").format("delta")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.save(path)
    return n


def run_gold(spark: SparkSession, settings: Settings) -> dict:
    silver_path = settings.paths.silver
    gold_path = settings.paths.gold

    with timed(log, "gold") as t:
        silver = spark.read.format("delta").load(silver_path)
        # cache: read once, scan three times
        silver.cache()
        silver.count()

        sessions = build_sessions(silver, settings.performance.salt_buckets)
        top_products = build_top_products(silver)
        funnel = build_funnel(silver)

        n_sessions = _write(sessions, f"{gold_path}/fact_sessions", ["event_date"])
        n_top = _write(top_products, f"{gold_path}/fact_top_products", ["event_date"])
        n_funnel = _write(funnel, f"{gold_path}/fact_funnel", ["event_date"])
        silver.unpersist()

    return {
        "rows_sessions": n_sessions,
        "rows_top_products": n_top,
        "rows_funnel": n_funnel,
        "elapsed_sec": t["elapsed_sec"],
        "path": gold_path,
    }


def optimize_zorder(spark: SparkSession, settings: Settings) -> None:
    """OPTIMIZE … ZORDER BY on Silver to accelerate point queries.

    Z-order co-locates rows with similar (user_id, product_id) values into
    the same files → far better data skipping when querying by those keys.
    Run periodically after big writes.
    """
    cols = ", ".join(settings.performance.zorder_columns)
    log.info("OPTIMIZE silver ZORDER BY (%s)", cols)
    spark.sql(f"OPTIMIZE delta.`{settings.paths.silver}` ZORDER BY ({cols})")
