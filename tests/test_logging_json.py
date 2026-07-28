"""Tests for JSON log output, specifically that ``extra`` survives.

Regression guard: the formatter used to build a fixed 4-key payload and drop
every caller-supplied field, so all the structured logging in this package was
decorative. The resolver's cadence and per-show tick numbers are only useful if
they actually reach the log.
"""

from __future__ import annotations

import json
import logging

from setlist_stash.logging_setup import JsonFormatter


def _fmt(**extra: object) -> dict[str, object]:
    record = logging.LogRecord(
        "setlist_stash.resolve", logging.INFO, __file__, 1, "tick", None, None
    )
    for k, v in extra.items():
        setattr(record, k, v)
    out: dict[str, object] = json.loads(JsonFormatter().format(record))
    return out


def test_base_shape_unchanged() -> None:
    out = _fmt()
    assert out["level"] == "INFO"
    assert out["logger"] == "setlist_stash.resolve"
    assert out["msg"] == "tick"
    assert "ts" in out


def test_extra_fields_are_emitted() -> None:
    out = _fmt(show_date="2026-07-27", stable_polls=12, stable_quiet_window_seconds=1800)
    assert out["show_date"] == "2026-07-27"
    assert out["stable_polls"] == 12
    assert out["stable_quiet_window_seconds"] == 1800


def test_unserializable_extra_is_stringified_not_raised() -> None:
    out = _fmt(err=object())
    assert isinstance(out["err"], str)


def test_extra_cannot_clobber_the_base_keys() -> None:
    """A stray ``extra`` key must not be able to forge level/logger/ts.

    ``msg`` is not tested here because the stdlib already refuses it: passing
    ``extra={"msg": ...}`` to a logger call raises KeyError before the record
    is ever built.
    """
    out = _fmt(level="SPOOFED", logger="SPOOFED", ts="SPOOFED")
    assert out["level"] == "INFO"
    assert out["logger"] == "setlist_stash.resolve"
    assert out["ts"] != "SPOOFED"
