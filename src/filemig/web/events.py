"""In-process publish/subscribe event bus for live status.

The store publishes lightweight event dicts here as state changes happen; the
SSE endpoint subscribes and streams them to the browser. This is intentionally
in-memory only — the durable audit trail lives in the ``events`` table. A new
subscriber simply starts receiving events from the moment it connects.
"""

from __future__ import annotations

import asyncio
from typing import Any


class EventBus:
    def __init__(self, max_queue: int = 1000) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._max_queue = max_queue

    def publish(self, event: dict[str, Any]) -> None:
        """Fan out to all subscribers. Never blocks; drops on a full queue."""
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow consumer; drop this event for them rather than stall the
                # producer. The dashboard reconciles via polling fallback.
                pass

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self._max_queue)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(q)


# Process-wide singleton (single-user local app).
bus = EventBus()
