"""Quota-aware, folder-by-folder batch scheduler.

Google enforces a hard 750 GiB/day per-user upload cap. We track our own
rolling-24h ledger (bytes counted at each Leg-2 success) and never propose a
batch that would exceed the *effective* cap (750 GiB minus a safety margin).

Batches are proposed **folder by folder**: files are grouped in tree order so a
whole folder migrates together (easy to spot-check in Drive). A batch fills
until it reaches the per-batch byte target, the file-count limit, or the
remaining daily quota — whichever comes first.

The scheduler only *proposes*; copying never starts until the user approves a
batch. A separate in-flight gate (:func:`upload_allowed`) re-checks the quota
before each file's upload so a long-running batch still respects the cap.
"""

from __future__ import annotations

from filemig.config import Settings
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.states import FileState


async def quota_status(store: Store, settings: Settings, run_id: int) -> models.QuotaStatus:
    used, reset = await store.quota_used_24h(run_id)
    return models.QuotaStatus(
        effective_cap_bytes=settings.effective_daily_cap_bytes,
        used_24h_bytes=used,
        window_reset_epoch=reset,
    )


async def upload_allowed(
    store: Store, settings: Settings, run_id: int, file_size: int
) -> bool:
    """In-flight gate: may we start uploading a file of this size now?

    We allow a single file to cross the cap only if nothing has been uploaded
    in the current window (mirrors Drive letting the one in-flight file that
    breaks the limit finish). Otherwise the file must fit in the remaining
    quota.
    """
    status = await quota_status(store, settings, run_id)
    if file_size <= status.remaining_bytes:
        return True
    return status.used_24h_bytes == 0  # nothing used yet: allow the first file


async def propose_batch(
    store: Store, settings: Settings, run: models.Run
) -> models.Batch | None:
    """Build the next proposed batch from queued/discovered files.

    Returns the created batch, or None if there is no quota headroom or no work.
    """
    status = await quota_status(store, settings, run.id)
    remaining = status.remaining_bytes
    if remaining <= 0:
        return None

    # Pull unassigned files in folder-by-folder (tree) order.
    candidates = await _unassigned_files(store, run.id)
    if not candidates:
        return None

    target = min(remaining, settings.batch_target_bytes)
    picked: list[int] = []
    total = 0
    for f in candidates:
        if len(picked) >= settings.batch_file_limit:
            break
        if not picked:
            # First file: allow it even if it alone exceeds target, as long as
            # it fits the remaining daily quota (or is a lone oversized file
            # under the full cap that the in-flight gate will permit).
            if f.size > remaining and f.size >= settings.effective_daily_cap_bytes:
                # A single file larger than an entire day's effective cap can
                # never be scheduled in one window. Dead-letter it so it is
                # surfaced for human attention (reconcile lists DEAD files)
                # instead of silently stranding it forever in `discovered`.
                await store.mark_dead(
                    f.id,
                    f"file size {f.size} exceeds effective daily cap "
                    f"{settings.effective_daily_cap_bytes}; cannot migrate in one window")
                continue
            picked.append(f.id)
            total += f.size
            continue
        if total + f.size > target:
            break
        picked.append(f.id)
        total += f.size

    if not picked:
        return None
    return await store.create_batch(run.id, picked, total)


async def plan_all_batches_from_folders(
    store: Store, settings: Settings, run: models.Run, path_lowers: list[str]
) -> dict[str, int]:
    """Plan ALL batches needed to migrate the entire selection.

    Chunks every eligible file under the selected folders into as many proposed
    batches as needed, bounded by the per-batch file limit and byte target. The
    daily upload quota is NOT applied here — these are just plans; the worker's
    in-flight quota gate throttles actual uploads at runtime (pausing near the
    750 GiB/day cap and resuming once the window rolls). Returns a summary.
    """
    file_ids = await store.queue_subtree_files(run.id, path_lowers)
    return await _chunk_into_batches(store, settings, run, file_ids)


async def plan_all_remaining(
    store: Store, settings: Settings, run: models.Run
) -> dict[str, int]:
    """Plan batches for EVERY unbatched, still-``discovered`` file in the run,
    regardless of folder. Catches orphaned files (e.g. those at the account
    root, or never assigned to a batch by an earlier action)."""
    file_ids = await store.all_unbatched_discovered(run.id)
    return await _chunk_into_batches(store, settings, run, file_ids)


async def _chunk_into_batches(
    store: Store, settings: Settings, run: models.Run, file_ids: list[int]
) -> dict[str, int]:
    """Chunk a list of file ids into proposed batches, bounded by the per-batch
    file limit and byte target. Oversized files (> a day's cap) are dead-let."""
    summary = {"selected_files": len(file_ids), "batches": 0,
               "queued_files": 0, "queued_bytes": 0, "skipped_oversized": 0}
    if not file_ids:
        return summary

    file_limit = settings.batch_file_limit
    byte_target = settings.batch_target_bytes
    cap = settings.effective_daily_cap_bytes

    chunk: list[int] = []
    chunk_bytes = 0
    for fid in file_ids:
        f = await store.get_file(fid)
        if f is None:
            continue
        if f.size >= cap:
            await store.mark_dead(
                f.id, f"file size {f.size} exceeds effective daily cap; "
                "cannot migrate in one window")
            summary["skipped_oversized"] += 1
            continue
        if chunk and (len(chunk) >= file_limit
                      or chunk_bytes + f.size > byte_target):
            await store.create_batch(run.id, chunk, chunk_bytes)
            summary["batches"] += 1
            summary["queued_files"] += len(chunk)
            summary["queued_bytes"] += chunk_bytes
            chunk, chunk_bytes = [], 0
        chunk.append(f.id)
        chunk_bytes += f.size

    if chunk:
        await store.create_batch(run.id, chunk, chunk_bytes)
        summary["batches"] += 1
        summary["queued_files"] += len(chunk)
        summary["queued_bytes"] += chunk_bytes
    return summary


async def propose_batch_from_folders(
    store: Store, settings: Settings, run: models.Run, path_lowers: list[str]
) -> tuple[models.Batch | None, dict[str, int]]:
    """Build a proposed batch from the files under the selected folders.

    Honors the rolling daily quota and the per-batch byte/file caps, so a huge
    selection is split: this proposes the FIRST batch from the selection and
    reports how much is left over (the user can propose again to continue).

    Returns (batch_or_None, info) where info has selected/queued/remaining
    counts so the UI can explain what happened.
    """
    status = await quota_status(store, settings, run.id)
    remaining_quota = status.remaining_bytes
    file_ids = await store.queue_subtree_files(run.id, path_lowers)
    info = {"selected_files": len(file_ids), "queued_files": 0,
            "remaining_files": 0, "queued_bytes": 0, "remaining_bytes": 0,
            "quota_full": int(remaining_quota <= 0), "stop_reason": None}
    if not file_ids or remaining_quota <= 0:
        info["remaining_files"] = len(file_ids)
        return None, info

    # Pull sizes in selection order (already folder-grouped by the query). A
    # batch is bounded by THREE caps, whichever is hit first: the per-batch file
    # limit, the per-batch byte target, and the remaining daily quota. We record
    # which one stopped us so the UI can explain the split to the user.
    target = min(remaining_quota, settings.batch_target_bytes)
    picked: list[int] = []
    total = 0
    total_bytes_all = 0  # bytes across the whole selection (for leftover calc)
    stop_reason = None
    for fid in file_ids:
        f = await store.get_file(fid)
        if f is None:
            continue
        total_bytes_all += f.size
        if stop_reason is not None:
            continue  # keep summing leftover bytes, but stop picking
        if len(picked) >= settings.batch_file_limit:
            stop_reason = "file_limit"
            continue
        if picked and total + f.size > target:
            stop_reason = "quota" if target == remaining_quota else "byte_target"
            continue
        if not picked and f.size > remaining_quota and \
                f.size >= settings.effective_daily_cap_bytes:
            await store.mark_dead(
                f.id, f"file size {f.size} exceeds effective daily cap; "
                "cannot migrate in one window")
            continue
        picked.append(f.id)
        total += f.size

    if not picked:
        info["remaining_files"] = len(file_ids)
        return None, info
    batch = await store.create_batch(run.id, picked, total)
    info["queued_files"] = len(picked)
    info["queued_bytes"] = total
    info["remaining_files"] = len(file_ids) - len(picked)
    info["remaining_bytes"] = total_bytes_all - total
    info["stop_reason"] = stop_reason
    return batch, info


async def _unassigned_files(store: Store, run_id: int) -> list[models.FileRecord]:
    """Files not yet in a batch, ordered folder-by-folder then by name.

    ``files_in_state`` already orders by id; we re-sort by (parent, path) to get
    folder locality. Discovered and failed-but-retryable files both qualify.
    """
    discovered = await store.files_in_state(run_id, FileState.DISCOVERED, limit=100000)
    files = [f for f in discovered if f.batch_id is None]
    files.sort(key=lambda f: (f.parent_path_lower, f.src_path_lower))
    return files
