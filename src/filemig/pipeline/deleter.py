"""Manual-approval source deletion (soft delete to Dropbox trash).

Deletion NEVER happens automatically. The user approves a batch's deletion in
the UI; only then does this run. Safety rails:

* Only files in state ``verified`` (both legs passed) are eligible — enforced by
  re-reading state here, not trusting the caller.
* Each delete passes ``parent_rev`` = the rev we verified, so Dropbox refuses to
  delete a file that changed since discovery (the change is reported, not
  force-deleted).
* delete_batch is asynchronous: we poll delete_batch/check until it settles and
  inspect EVERY per-entry result, marking each file done or recording its error.
* Deletes are serialized (one batch in flight) to avoid
  too_many_write_operations on the namespace.

This is a *soft* delete: files go to Dropbox trash (recoverable ~30 days), not
permanent deletion.
"""

from __future__ import annotations

import asyncio

from filemig.clients.dropbox_client import DropboxClient
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.errors import RetryableError
from filemig.domain.states import BatchState, FileState
from filemig.util.clock import utcnow_iso

# Chunk size for delete_batch. No documented hard cap, but we keep batches
# modest to bound poll payloads and namespace write-lock contention.
DELETE_CHUNK = 100
POLL_INTERVAL = 1.5
POLL_MAX_ATTEMPTS = 200


async def delete_verified_batch(
    client: DropboxClient, store: Store, batch: models.Batch
) -> dict[str, int]:
    """Soft-delete the verified files of an approved batch. Returns a summary."""
    files = await store.files_in_batch(batch.id)
    eligible = [f for f in files if f.state == FileState.VERIFIED]
    summary = {"deleted": 0, "failed": 0, "skipped": len(files) - len(eligible)}

    for start in range(0, len(eligible), DELETE_CHUNK):
        chunk = eligible[start : start + DELETE_CHUNK]
        # Move each to `deleting` (records intent + audit) before the API call.
        for f in chunk:
            await store.transition_file(f.id, FileState.DELETING)
        await _delete_chunk(client, store, chunk, summary)

    return summary


async def _delete_chunk(
    client: DropboxClient, store: Store, chunk: list[models.FileRecord],
    summary: dict[str, int],
) -> None:
    entries = [{"path": f.dbx_id, "parent_rev": f.dbx_rev} for f in chunk]
    by_path = {f.dbx_id: f for f in chunk}

    launch = await client.delete_batch(entries)
    if isinstance(launch, str):
        result = await _poll(client, launch)
    else:
        result = launch  # completed synchronously

    entries_result = result.get("entries", [])
    if not entries_result:
        # Couldn't read per-entry results: be conservative, do not mark done.
        # The files remain verified (safely on Drive); only the source deletion
        # is unconfirmed.
        for f in chunk:
            await store.revert_delete_to_verified(
                f.id, "delete result unreadable; source deletion unconfirmed")
        summary["failed"] += len(chunk)
        return

    # delete_batch preserves entry order; map results back positionally.
    for f, entry in zip(chunk, entries_result):
        if entry.get(".tag") == "success":
            await store.transition_file(f.id, FileState.DONE)
            summary["deleted"] += 1
        else:
            # parent_rev mismatch / not_found / other — never force; report it.
            # The file stays verified (intact on Drive); the source survives.
            reason = entry.get(".tag", "unknown")
            await store.revert_delete_to_verified(
                f.id, f"source delete failed: {reason}")
            summary["failed"] += 1


async def _poll(client: DropboxClient, async_job_id: str) -> dict:
    for _ in range(POLL_MAX_ATTEMPTS):
        status = await client.delete_batch_check(async_job_id)
        tag = status.get(".tag")
        if tag == "complete":
            return status
        if tag == "failed":
            raise RetryableError(f"delete_batch job failed: {status}")
        await asyncio.sleep(POLL_INTERVAL)
    raise RetryableError("delete_batch poll timed out")


async def run_batch_deletion(
    client: DropboxClient, store: Store, batch_id: int
) -> dict[str, int]:
    """Top-level entry: validate the batch is approved, then delete."""
    batch = await store.get_batch(batch_id)
    if batch is None:
        raise KeyError(f"batch {batch_id} not found")
    if batch.state != BatchState.DELETING:
        raise ValueError(
            f"batch {batch_id} is {batch.state}, not approved for deletion")
    summary = await delete_verified_batch(client, store, batch)
    await store.refresh_batch_counters(batch_id)
    await store.transition_batch(batch_id, BatchState.DONE,
                                 fields={"completed_at": utcnow_iso()})
    return summary
