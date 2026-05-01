from __future__ import annotations

from pathlib import Path

import yaml

from pipeline.settings import load_settings


def test_load_default(tmp_path: Path) -> None:
    cfg = {
        "app_name": "x",
        "paths": {
            "raw": "data/raw", "bronze": "data/b", "silver": "data/s",
            "gold": "data/g", "dlq": "data/d", "checkpoints": "data/c",
        },
    }
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(cfg))
    s = load_settings(p)
    assert s.app_name == "x"
    assert Path(s.paths.bronze).is_absolute()


def test_env_override(tmp_path: Path, monkeypatch) -> None:
    cfg = {
        "app_name": "x",
        "paths": {
            "raw": "data/raw", "bronze": "data/b", "silver": "data/s",
            "gold": "data/g", "dlq": "data/d", "checkpoints": "data/c",
        },
    }
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(cfg))
    monkeypatch.setenv("PIPELINE__SPARK__SHUFFLE_PARTITIONS", "999")
    s = load_settings(p)
    assert s.spark.shuffle_partitions == 999
