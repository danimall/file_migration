"""Error taxonomy shared across clients and the pipeline.

The worker distinguishes three broad outcomes:

* :class:`RetryableError` — transient; re-queue with backoff (counts an attempt).
* :class:`FatalError` — permanent for this file; escalate to ``dead`` (never
  auto-deleted, surfaced for human attention).
* :class:`VerificationMismatch` — a safety-critical hash/size mismatch; always
  fatal for the file. This is the failure that must *never* lead to a source
  deletion.
"""

from __future__ import annotations


class FilemigError(Exception):
    """Base class for all application errors."""


class RetryableError(FilemigError):
    """Transient failure; the operation may succeed on retry."""

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class FatalError(FilemigError):
    """Permanent failure for this file; do not retry."""


class VerificationMismatch(FatalError):
    """A hash or size mismatch was detected. SAFETY-CRITICAL: the file is not
    identical at the destination, so it must never become delete-eligible."""

    def __init__(self, leg: str, expected: str, actual: str) -> None:
        self.leg = leg
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"verification mismatch in {leg}: expected {expected!r}, got {actual!r}"
        )


class QuotaExceeded(RetryableError):
    """The daily upload quota window is exhausted; back off until it rolls."""


class UploadSessionExpired(RetryableError):
    """A Drive resumable upload session URI returned 404; mint a new session
    and restart this file's upload from the staged bytes (no re-download)."""


class AuthError(FilemigError):
    """OAuth token missing, invalid, or refresh failed."""
