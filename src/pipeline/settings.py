"""Pipeline settings. YAML-first with env override (PIPELINE__SECTION__KEY)."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "conf" / "config.yaml"


class Paths(BaseModel):
    raw: str
    bronze: str
    silver: str
    gold: str
    dlq: str
    checkpoints: str

    def absolute(self, base: Path) -> "Paths":
        def abspath(p: str) -> str:
            pp = Path(p)
            return str(pp if pp.is_absolute() else (base / pp).resolve())
        return Paths(
            raw=abspath(self.raw),
            bronze=abspath(self.bronze),
            silver=abspath(self.silver),
            gold=abspath(self.gold),
            dlq=abspath(self.dlq),
            checkpoints=abspath(self.checkpoints),
        )


class GeneratorCfg(BaseModel):
    num_events: int = 1_000_000
    num_users: int = 50_000
    num_products: int = 5_000
    num_days: int = 7
    start_date: date = date(2026, 4, 1)
    power_user_ratio: float = 0.005
    malformed_ratio: float = 0.002
    duplicate_ratio: float = 0.01
    seed: int = 42


class SparkCfg(BaseModel):
    master: str = "local[*]"
    shuffle_partitions: int = 200
    driver_memory: str = "4g"
    executor_memory: str = "4g"
    arrow_enabled: bool = True
    adaptive_enabled: bool = True


class QualityCfg(BaseModel):
    silver_max_null_pct: dict[str, float] = Field(default_factory=dict)
    silver_min_distinct_event_types: int = 4
    gold_min_sessions: int = 1


class PerformanceCfg(BaseModel):
    broadcast_threshold_mb: int = 32
    salt_buckets: int = 16
    zorder_columns: list[str] = Field(default_factory=lambda: ["user_id", "product_id"])
    partitions_silver: list[str] = Field(default_factory=lambda: ["event_date"])
    partitions_gold: list[str] = Field(default_factory=lambda: ["event_date"])


class Settings(BaseModel):
    app_name: str = "pyspark-etl-pipeline"
    env: str = "local"
    paths: Paths
    generator: GeneratorCfg = GeneratorCfg()
    spark: SparkCfg = SparkCfg()
    quality: QualityCfg = QualityCfg()
    performance: PerformanceCfg = PerformanceCfg()


def _deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _env_overrides(prefix: str = "PIPELINE__") -> dict[str, Any]:
    """PIPELINE__SPARK__SHUFFLE_PARTITIONS=400 -> {'spark': {'shuffle_partitions': 400}}."""
    out: dict[str, Any] = {}
    for k, v in os.environ.items():
        if not k.startswith(prefix):
            continue
        path = [p.lower() for p in k[len(prefix):].split("__")]
        cur = out
        for p in path[:-1]:
            cur = cur.setdefault(p, {})
        cur[path[-1]] = _coerce(v)
    return out


def _coerce(s: str) -> Any:
    if s.lower() in {"true", "false"}:
        return s.lower() == "true"
    try:
        if "." in s:
            return float(s)
        return int(s.replace("_", ""))
    except ValueError:
        return s


def load_settings(config_path: str | Path | None = None) -> Settings:
    cfg_path = Path(config_path) if config_path else DEFAULT_CONFIG
    with open(cfg_path) as f:
        raw = yaml.safe_load(f) or {}
    raw = _deep_merge(raw, _env_overrides())
    s = Settings(**raw)
    s.paths = s.paths.absolute(REPO_ROOT)
    return s
