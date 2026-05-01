"""Data quality checks. Fail loud, never silently."""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.logging_utils import get_logger
from pipeline.schemas import VALID_EVENT_TYPES
from pipeline.settings import QualityCfg

log = get_logger("pipeline.quality")


class DataQualityError(Exception):
    """Raised when a hard data-quality threshold is violated."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def assert_schema_subset(df: DataFrame, required_columns: dict[str, str]) -> CheckResult:
    """Required columns must exist with the expected dtype string."""
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    missing = []
    wrong = []
    for col, dtype in required_columns.items():
        if col not in actual:
            missing.append(col)
        elif actual[col] != dtype:
            wrong.append((col, actual[col], dtype))
    if missing or wrong:
        detail = f"missing={missing} wrong_dtype={wrong}"
        return CheckResult("schema_subset", False, detail)
    return CheckResult("schema_subset", True, "ok")


def check_null_pct(df: DataFrame, max_null_pct: dict[str, float]) -> list[CheckResult]:
    """For each column, % of nulls must be <= threshold."""
    if not max_null_pct:
        return []
    total = df.count()
    if total == 0:
        return [CheckResult("null_pct", False, "empty dataset")]

    aggs = []
    for col in max_null_pct:
        aggs.append(F.sum(F.col(col).isNull().cast("long")).alias(col))
    row = df.agg(*aggs).first()
    counts = row.asDict() if row is not None else {}

    results = []
    for col, thr in max_null_pct.items():
        actual = counts.get(col, 0) / total
        ok = actual <= thr
        results.append(CheckResult(
            f"null_pct[{col}]",
            ok,
            f"actual={actual:.4f} threshold={thr}",
        ))
    return results


def check_event_type_cardinality(df: DataFrame, min_distinct: int) -> CheckResult:
    distinct = df.select("event_type").distinct().count()
    return CheckResult(
        "event_type_cardinality",
        distinct >= min_distinct,
        f"distinct={distinct} min={min_distinct}",
    )


def check_event_type_domain(df: DataFrame) -> CheckResult:
    """Every event_type must belong to the closed set."""
    bad = df.filter(~F.col("event_type").isin(*VALID_EVENT_TYPES)).limit(1).count()
    return CheckResult(
        "event_type_domain",
        bad == 0,
        f"unexpected_values={bad}",
    )


def check_price_range(df: DataFrame) -> CheckResult:
    """Price must be non-negative when present. Skipped if the column is absent."""
    if "price" not in df.columns:
        return CheckResult("price_range", True, "skipped (column absent)")
    bad = df.filter((F.col("price").isNotNull()) & (F.col("price") < 0)).limit(1).count()
    return CheckResult("price_range", bad == 0, f"negative_prices={bad}")


def run_silver_checks(silver_df: DataFrame, cfg: QualityCfg, fail_fast: bool = True) -> list[CheckResult]:
    required = {
        "event_id": "string",
        "event_ts": "timestamp",
        "event_type": "string",
        "user_id": "string",
    }
    results: list[CheckResult] = [assert_schema_subset(silver_df, required)]
    results.extend(check_null_pct(silver_df, cfg.silver_max_null_pct))
    results.append(check_event_type_cardinality(silver_df, cfg.silver_min_distinct_event_types))
    results.append(check_event_type_domain(silver_df))
    results.append(check_price_range(silver_df))
    _report(results, fail_fast)
    return results


def run_gold_checks(sessions_df: DataFrame, cfg: QualityCfg, fail_fast: bool = True) -> list[CheckResult]:
    n = sessions_df.count()
    results = [
        CheckResult("gold_min_sessions", n >= cfg.gold_min_sessions, f"sessions={n} min={cfg.gold_min_sessions}"),
        CheckResult(
            "no_negative_durations",
            sessions_df.filter(F.col("duration_sec") < 0).limit(1).count() == 0,
            "duration_sec >= 0",
        ),
    ]
    _report(results, fail_fast)
    return results


def _report(results: list[CheckResult], fail_fast: bool) -> None:
    failed = [r for r in results if not r.passed]
    for r in results:
        log.info("DQ %-30s %s — %s", r.name, "PASS" if r.passed else "FAIL", r.detail)
    if failed and fail_fast:
        names = ", ".join(r.name for r in failed)
        raise DataQualityError(f"data quality failures: {names}")
