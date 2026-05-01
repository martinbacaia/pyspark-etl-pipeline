"""Structured logging for the pipeline."""
from __future__ import annotations

import logging
import sys
from contextlib import contextmanager
from time import perf_counter
from typing import Iterator

_FMT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(_FMT))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


@contextmanager
def timed(logger: logging.Logger, label: str) -> Iterator[dict]:
    """Context manager that logs elapsed time and exposes it via a dict for callers."""
    info: dict = {"label": label}
    t0 = perf_counter()
    logger.info("→ %s …", label)
    try:
        yield info
    finally:
        info["elapsed_sec"] = perf_counter() - t0
        logger.info("✓ %s in %.2fs", label, info["elapsed_sec"])
