"""Server-Sent Events stream for live status.

Subscribes to the in-process event bus and streams events to the browser as
``data: {json}\\n\\n`` frames. A periodic comment keeps the connection alive
through proxies. The dashboard's polling fallback covers any dropped events.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import Request
from fastapi.responses import StreamingResponse

from filemig.web.events import bus

KEEPALIVE_SECONDS = 15.0


async def _event_generator(request: Request) -> AsyncIterator[str]:
    queue = bus.subscribe()
    try:
        # Prime the stream so the client's onopen fires immediately.
        yield "event: ready\ndata: {}\n\n"
        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"
                continue
            yield f"data: {json.dumps(event)}\n\n"
    finally:
        bus.unsubscribe(queue)


def event_stream(request: Request) -> StreamingResponse:
    return StreamingResponse(
        _event_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
