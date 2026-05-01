"""Generate synthetic raw events into data/raw."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline.generator import cli

if __name__ == "__main__":
    cli()
