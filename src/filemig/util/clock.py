"""Time helpers. All persisted timestamps are ISO-8601 UTC strings."""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def iso_to_epoch(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()
