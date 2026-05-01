"""Generator unit tests (no Spark required for the row-level helper)."""
from __future__ import annotations

from pipeline.generator import EVENT_WEIGHTS, _row_dict


def test_row_dict_deterministic_with_same_seed() -> None:
    a = _row_dict(123, _opts())
    b = _row_dict(123, _opts())
    assert a == b


def test_row_dict_event_type_in_known_set() -> None:
    rec, _, _ = _row_dict(7, _opts(malformed_ratio=0.0))
    assert rec["event_type"] in EVENT_WEIGHTS


def test_row_dict_can_emit_malformed() -> None:
    """If we crank malformed_ratio to 1.0, every row should be malformed."""
    seen_malformed = False
    for s in range(50):
        result = _row_dict(s, _opts(malformed_ratio=1.0))
        if isinstance(result, dict) and "_raw" in result:
            seen_malformed = True
            break
        rec, _, _ = result
        if "user_id" not in rec or rec.get("event_ts") == "NOT-A-DATE":
            seen_malformed = True
            break
    assert seen_malformed


def _opts(**overrides):
    base = {
        "num_users": 1000,
        "num_products": 200,
        "power_user_ratio": 0.01,
        "start_ts_epoch": 1_700_000_000,
        "span_sec": 86_400 * 3,
        "malformed_ratio": 0.0,
        "duplicate_ratio": 0.0,
        "seed": 1,
    }
    base.update(overrides)
    return base
