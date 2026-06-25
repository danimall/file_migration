"""Shared async HTTP plumbing: retry, backoff, and Retry-After handling.

Both Dropbox and Google return 429 (and Google 403 rateLimitExceeded) under
load. The rules differ slightly:

* Dropbox 429 *always* carries a ``Retry-After`` header (seconds) — we sleep
  exactly that long; retries that ignore it count against the limit.
* Google asks for *truncated exponential backoff with jitter* on 429/5xx and
  403 rateLimitExceeded/userRateLimitExceeded.

:func:`request_with_retries` implements both: honor Retry-After when present,
otherwise back off exponentially with jitter. The caller classifies which
status codes are retryable.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable

import httpx

from filemig.domain.errors import RetryableError

# Status codes we retry by default.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

_MAX_BACKOFF = 64.0  # cap a single sleep at 64s


def parse_retry_after(response: httpx.Response) -> float | None:
    """Return Retry-After in seconds, if present and numeric."""
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        # HTTP-date form is uncommon for these APIs; ignore and fall back.
        return None


def backoff_seconds(attempt: int, base: float = 1.0) -> float:
    """Truncated exponential backoff with full jitter."""
    ceiling = min(_MAX_BACKOFF, base * (2**attempt))
    return random.uniform(0.0, ceiling)


def is_google_rate_limit(response: httpx.Response) -> bool:
    """Detect Google's 403 rate-limit reasons (vs a fatal 403)."""
    if response.status_code != 403:
        return False
    try:
        reasons = {
            e.get("reason")
            for e in response.json().get("error", {}).get("errors", [])
        }
    except Exception:
        return False
    return bool(reasons & {"rateLimitExceeded", "userRateLimitExceeded"})


async def request_with_retries(
    send: Callable[[], Awaitable[httpx.Response]],
    *,
    max_attempts: int = 6,
    retryable_status: set[int] | None = None,
    treat_google_403_ratelimit: bool = False,
) -> httpx.Response:
    """Call ``send`` until it returns a non-retryable response or attempts run out.

    ``send`` builds and issues a fresh request each call (so it can be retried
    safely). Returns the final response; the caller inspects its status.
    """
    codes = retryable_status if retryable_status is not None else RETRYABLE_STATUS
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            response = await send()
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError,
                httpx.PoolTimeout, httpx.ConnectTimeout, httpx.ReadTimeout) as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                raise RetryableError(f"network error after retries: {exc}") from exc
            await asyncio.sleep(backoff_seconds(attempt))
            continue

        retryable = response.status_code in codes or (
            treat_google_403_ratelimit and is_google_rate_limit(response)
        )
        if not retryable:
            return response
        if attempt == max_attempts - 1:
            return response  # let the caller surface the final failing response

        retry_after = parse_retry_after(response)
        delay = retry_after if retry_after is not None else backoff_seconds(attempt)
        await asyncio.sleep(delay)

    if last_exc is not None:
        raise RetryableError(f"request failed after retries: {last_exc}")
    raise RetryableError("request failed after retries")
