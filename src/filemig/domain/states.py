"""File and batch state machines.

The ``files`` table is the core work table; every row walks the file state
machine below. ``db.store`` is the single writer and validates every transition
against :data:`FILE_TRANSITIONS` before persisting it. This keeps the
safety-critical invariant — *a file is only ever delete-eligible once both
verification legs have passed* — enforced in one place.
"""

from __future__ import annotations

from enum import StrEnum


class FileState(StrEnum):
    DISCOVERED = "discovered"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    DOWNLOADED = "downloaded"
    UPLOADING = "uploading"
    UPLOADED = "uploaded"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    DELETING = "deleting"
    DONE = "done"
    FAILED = "failed"  # retryable; re-queued with backoff while attempts remain
    DEAD = "dead"  # exhausted / fatal / hash mismatch — NEVER auto-deleted
    SKIPPED = "skipped"  # deliberately excluded by the user; not an error


# Transient states are interrupted work that a crash can leave behind. On
# startup, recovery resets any row in one of these states (or whose lease has
# expired) back to its ``prev_durable_state`` and re-queues it. Durable states
# are safe resting points.
TRANSIENT_STATES: frozenset[FileState] = frozenset(
    {
        FileState.DOWNLOADING,
        FileState.UPLOADING,
        FileState.VERIFYING,
        FileState.DELETING,
    }
)

DURABLE_STATES: frozenset[FileState] = frozenset(
    {
        FileState.DISCOVERED,
        FileState.QUEUED,
        FileState.DOWNLOADED,
        FileState.UPLOADED,
        FileState.VERIFIED,
        FileState.DONE,
        FileState.FAILED,
        FileState.DEAD,
        FileState.SKIPPED,
    }
)

# In-progress states are mid-pipeline (interrupted work). On startup, recovery
# rewinds any row in one of these — or with an expired lease — back to
# ``queued`` so a worker reclaims it. Progress *fields* (staged path, local
# hashes, Drive session URI + offset) are preserved on the row, and a
# resume-aware worker skips phases that are already complete. This includes the
# durable ``downloaded``/``uploaded`` states, which the claim query would
# otherwise never pick up.
IN_PROGRESS_STATES: frozenset[FileState] = frozenset(
    {
        FileState.DOWNLOADING,
        FileState.DOWNLOADED,
        FileState.UPLOADING,
        FileState.UPLOADED,
        FileState.VERIFYING,
        # A crash mid-deletion leaves a leaseless DELETING row; recovery rewinds
        # it via recovery_target, which maps it back to VERIFIED (intact on
        # Drive, source delete simply retried) rather than re-running the copy.
        FileState.DELETING,
    }
)

# Allowed forward (and recovery/error) transitions for a file.
FILE_TRANSITIONS: dict[FileState, frozenset[FileState]] = {
    FileState.DISCOVERED: frozenset({FileState.QUEUED, FileState.DEAD}),
    FileState.QUEUED: frozenset(
        {FileState.DOWNLOADING, FileState.UPLOADED, FileState.VERIFIED}
    ),  # UPLOADED/VERIFIED allow idempotency short-circuit when already on Drive
    FileState.DOWNLOADING: frozenset(
        # UPLOADED is reachable here only via the idempotency short-circuit: a
        # re-claimed file that a prior (crashed) run already uploaded+hashed.
        {FileState.DOWNLOADED, FileState.UPLOADED, FileState.QUEUED,
         FileState.FAILED, FileState.DEAD}
    ),
    FileState.DOWNLOADED: frozenset(
        {FileState.UPLOADING, FileState.QUEUED, FileState.FAILED, FileState.DEAD}
    ),
    FileState.UPLOADING: frozenset(
        {FileState.UPLOADED, FileState.DOWNLOADED, FileState.FAILED, FileState.DEAD}
    ),
    FileState.UPLOADED: frozenset(
        {FileState.VERIFYING, FileState.FAILED, FileState.DEAD}
    ),
    FileState.VERIFYING: frozenset(
        {FileState.VERIFIED, FileState.UPLOADED, FileState.FAILED, FileState.DEAD}
    ),
    FileState.VERIFIED: frozenset({FileState.DELETING}),
    FileState.DELETING: frozenset(
        {FileState.DONE, FileState.VERIFIED, FileState.FAILED, FileState.DEAD}
    ),
    FileState.DONE: frozenset(),
    # A failed file is retryable: it gets re-queued, or escalated to dead.
    FileState.FAILED: frozenset({FileState.QUEUED, FileState.DEAD}),
    FileState.DEAD: frozenset({FileState.QUEUED}),  # manual revival only
}


class BatchState(StrEnum):
    PROPOSED = "proposed"
    COPY_APPROVED = "copy_approved"
    RUNNING = "running"
    AWAITING_DELETE_APPROVAL = "awaiting_delete_approval"
    DELETING = "deleting"
    DONE = "done"
    CANCELLED = "cancelled"


# Copy/verify progress within a running batch is tracked via the batch's
# counters (copied_count / verified_count), not as separate stored states. A
# batch advances from ``running`` to ``awaiting_delete_approval`` once every
# file has settled (verified or dead).
BATCH_TRANSITIONS: dict[BatchState, frozenset[BatchState]] = {
    BatchState.PROPOSED: frozenset({BatchState.COPY_APPROVED, BatchState.CANCELLED}),
    BatchState.COPY_APPROVED: frozenset({BatchState.RUNNING}),
    BatchState.RUNNING: frozenset({BatchState.AWAITING_DELETE_APPROVAL}),
    # A "finished" batch can be put back to RUNNING to retry stragglers/failed
    # files (the Resume action) — it then re-settles to awaiting-delete-approval.
    BatchState.AWAITING_DELETE_APPROVAL: frozenset(
        {BatchState.DELETING, BatchState.RUNNING}),
    BatchState.DELETING: frozenset({BatchState.DONE, BatchState.AWAITING_DELETE_APPROVAL}),
    BatchState.DONE: frozenset(),
    BatchState.CANCELLED: frozenset(),
}


class IllegalTransition(Exception):
    """Raised when a requested state transition is not permitted."""

    def __init__(self, kind: str, frm: str, to: str) -> None:
        self.kind = kind
        self.frm = frm
        self.to = to
        super().__init__(f"illegal {kind} transition: {frm} -> {to}")


def assert_file_transition(frm: FileState, to: FileState) -> None:
    if to not in FILE_TRANSITIONS.get(frm, frozenset()):
        raise IllegalTransition("file", frm, to)


def assert_batch_transition(frm: BatchState, to: BatchState) -> None:
    if to not in BATCH_TRANSITIONS.get(frm, frozenset()):
        raise IllegalTransition("batch", frm, to)


def recovery_target(prev_durable: str | None) -> FileState:
    """Where a crashed transient row should be reset to.

    We rewind to ``QUEUED`` so the worker re-derives progress from durable
    facts (Drive resumable session URI + confirmed offset persisted on the
    row), rather than trusting an interrupted in-memory state.
    """
    if prev_durable and prev_durable in {s.value for s in DURABLE_STATES}:
        # Verified/done files never need rewinding.
        if prev_durable in {FileState.VERIFIED.value, FileState.DONE.value}:
            return FileState(prev_durable)
    return FileState.QUEUED
