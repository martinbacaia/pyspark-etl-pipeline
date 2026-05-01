"""Performance helpers: salting for skew, partition recommendations.

These are utilities used by Gold and explained in the README. They are
kept in their own module so the optimization rationale is one place.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def salt_column(df: DataFrame, key: str, buckets: int, salt_col: str = "_salt") -> DataFrame:
    """Add a uniformly random salt 0..buckets-1.

    Use when grouping by a skewed key like user_id (power users dominate):
    a single hot key would otherwise collapse to one reducer. With a salt:
        df.groupBy(key, salt).agg(...)   # spreads across `buckets` reducers
    Then re-aggregate without the salt.
    """
    return df.withColumn(salt_col, (F.rand(seed=13) * buckets).cast("int"))


def recommend_shuffle_partitions(estimated_input_bytes: int, target_partition_mb: int = 128) -> int:
    """Spark guideline: ~128MB per shuffle partition.

    Helps justify the choice in the README rather than leaving the default 200.
    """
    target = target_partition_mb * 1024 * 1024
    if estimated_input_bytes <= 0:
        return 200
    return max(8, min(4000, int(round(estimated_input_bytes / target))))
