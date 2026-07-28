"""Structured logging setup.

JSON when ``LOG_FORMAT=json`` (the default in production), otherwise plain
text for local dev. No third-party deps; the standard logging module is
enough.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

# Attributes every LogRecord carries. Anything on a record that is NOT in this
# set came from a caller's ``extra={...}``, which is exactly what we want to
# emit. Built from a throwaway record rather than hardcoded so it can't drift
# out of date with the stdlib.
_STANDARD_RECORD_ATTRS = frozenset(
    vars(
        logging.LogRecord("", logging.INFO, "", 0, "", None, None)
    )
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    """Minimal JSON log formatter — no PII, no secrets, just shape.

    Caller-supplied ``extra={...}`` fields ARE emitted. They used to be dropped
    silently, which made every structured call site in this package a no-op:
    ``show_date``, ``track_count``, ``stable_polls``, resolver tick counts and
    error context were all being written and thrown away, so production logs
    said "live partial scoring" with none of the numbers that make it useful.

    Only JSON-serializable values pass through; anything else is stringified
    rather than raising, because a formatter that can throw takes the log line
    (and sometimes the caller) with it. Nothing here should ever carry a secret
    — keep ``extra`` to identifiers and counts.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key in _STANDARD_RECORD_ATTRS or key in payload or key.startswith("_"):
                continue
            try:
                json.dumps(value)
            except (TypeError, ValueError):
                value = str(value)
            payload[key] = value
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(log_format: str = "json", level: int = logging.INFO) -> None:
    """Wire root logging once at process start."""
    handler = logging.StreamHandler(sys.stdout)
    if log_format.lower() == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s :: %(message)s")
        )

    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
