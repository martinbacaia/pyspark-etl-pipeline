"""Convenience entry point: `python scripts/run_pipeline.py [--config ...]`."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline.cli import main

if __name__ == "__main__":
    sys.exit(main(["run", *sys.argv[1:]], standalone_mode=False))
