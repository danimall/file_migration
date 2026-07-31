"""Tests for the store: transitions, leasing, recovery, batches, quota."""

from __future__ import annotations

import pytest

from filemig.db.store import Store, utcnow_iso_in
from filemig.domain.states import BatchState, FileState, IllegalTransition
from tests.conftest import make_discovered_file


async def _seed_file(store: Store, run_id: int, **kw) -> int:
    f = make_discovered_file(**kw)
    await store.upsert_discovered_file(run_id, f, "sha" * 16)
    rows = await store.files_in_state(run_id, FileState.DISCOVERED)
    return rows[-1].id


async def test_create_run_and_get_active(store: Store):
    run = await store.create_run("uuid-1", "Migration Root")
    active = await store.get_active_run()
    assert active is not None
    assert active.id == run.id
    assert active.run_uuid == "uuid-1"


async def test_upsert_file_idempotent_same_rev_preserves_state(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    await store.transition_file(fid, FileState.QUEUED)
    # Re-discover with the same rev: state must be preserved.
    await store.upsert_discovered_file(run.id, make_discovered_file(), "sha" * 16)
    f = await store.get_file(fid)
    assert f.state == FileState.QUEUED


async def test_upsert_file_changed_rev_resets_to_discovered(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    await store.transition_file(fid, FileState.QUEUED)
    await store.transition_file(fid, FileState.DOWNLOADING)
    await store.transition_file(fid, FileState.DOWNLOADED)
    # Re-discover with a new rev: must reset to discovered for re-migration.
    await store.upsert_discovered_file(
        run.id, make_discovered_file(rev="rev2"), "sha" * 16)
    f = await store.get_file(fid)
    assert f.state == FileState.DISCOVERED
    assert f.dbx_rev == "rev2"


async def test_illegal_transition_raises(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    await store.transition_file(fid, FileState.QUEUED)
    await store.transition_file(fid, FileState.DOWNLOADING)
    await store.transition_file(fid, FileState.DOWNLOADED)
    await store.transition_file(fid, FileState.UPLOADING)
    await store.transition_file(fid, FileState.UPLOADED)
    # Cannot jump uploaded -> deleting.
    with pytest.raises(IllegalTransition):
        await store.transition_file(fid, FileState.DELETING)


async def test_claim_next_file_requires_running_batch(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    batch = await store.create_batch(run.id, [fid], total_bytes=1234)
    # Files are still discovered and the batch is proposed -> nothing claimable.
    assert await store.claim_next_file(run.id, "worker-1") is None
    # Approve + run the batch and queue its files.
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    claimed = await store.claim_next_file(run.id, "worker-1")
    assert claimed is not None
    assert claimed.state == FileState.DOWNLOADING
    assert claimed.leased_by == "worker-1"
    # A second claim finds nothing (only file is leased).
    assert await store.claim_next_file(run.id, "worker-2") is None


async def test_lease_guard_blocks_stale_worker(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    batch = await store.create_batch(run.id, [fid], 1)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    claimed = await store.claim_next_file(run.id, "worker-1")
    # A different worker must not be able to transition it.
    with pytest.raises(PermissionError):
        await store.transition_file(claimed.id, FileState.DOWNLOADED,
                                    expect_worker="worker-2")


async def test_recovery_resets_transient_rows(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    batch = await store.create_batch(run.id, [fid], 1)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    await store.claim_next_file(run.id, "worker-1")  # now downloading
    reset = await store.recover_interrupted(run.id)
    assert reset == 1
    f = await store.get_file(fid)
    assert f.state == FileState.QUEUED
    assert f.leased_by is None


async def test_recovery_leaves_verified_untouched(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    for s in [FileState.QUEUED, FileState.DOWNLOADING, FileState.DOWNLOADED,
              FileState.UPLOADING, FileState.UPLOADED, FileState.VERIFYING,
              FileState.VERIFIED]:
        await store.transition_file(fid, s)
    reset = await store.recover_interrupted(run.id)
    assert reset == 0
    assert (await store.get_file(fid)).state == FileState.VERIFIED


async def test_record_failure_retries_then_dies(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    await store.transition_file(fid, FileState.QUEUED)
    await store.transition_file(fid, FileState.DOWNLOADING)
    f = await store.record_failure(fid, "boom", max_attempts=2)
    assert f.state == FileState.QUEUED and f.attempts == 1
    await store.transition_file(fid, FileState.DOWNLOADING)
    f = await store.record_failure(fid, "boom again", max_attempts=2)
    assert f.state == FileState.DEAD and f.attempts == 2


async def test_batch_advances_to_awaiting_delete_when_all_verified(store: Store):
    run = await store.create_run("u", "root")
    f1 = await _seed_file(store, run.id, dbx_id="id:1", path="/a.txt")
    f2 = await _seed_file(store, run.id, dbx_id="id:2", path="/b.txt")
    batch = await store.create_batch(run.id, [f1, f2], 2)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    for fid in (f1, f2):
        for s in [FileState.DOWNLOADING, FileState.DOWNLOADED, FileState.UPLOADING,
                  FileState.UPLOADED, FileState.VERIFYING, FileState.VERIFIED]:
            await store.transition_file(fid, s)
    b = await store.refresh_batch_counters(batch.id)
    assert b.verified_count == 2
    assert b.state == BatchState.AWAITING_DELETE_APPROVAL


async def test_quota_ledger_accumulates(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    await store.record_upload(run.id, fid, 100)
    await store.record_upload(run.id, fid, 250)
    used, reset = await store.quota_used_24h(run.id)
    assert used == 350
    assert reset is not None


async def test_resume_batch_retries_dead_and_stragglers(store: Store):
    run = await store.create_run("u", "root")
    f1 = await _seed_file(store, run.id, dbx_id="id:1", path="/a.txt")
    f2 = await _seed_file(store, run.id, dbx_id="id:2", path="/b.txt")
    f3 = await _seed_file(store, run.id, dbx_id="id:3", path="/c.txt")
    batch = await store.create_batch(run.id, [f1, f2, f3], 3)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    # f1 verifies; f2 and f3 die -> all settled -> batch awaits delete approval.
    for s in [FileState.DOWNLOADING, FileState.DOWNLOADED, FileState.UPLOADING,
              FileState.UPLOADED, FileState.VERIFYING, FileState.VERIFIED]:
        await store.transition_file(f1, s)
    await store.mark_dead(f2, "boom")
    await store.mark_dead(f3, "boom")
    b = await store.refresh_batch_counters(batch.id)
    assert b.state == BatchState.AWAITING_DELETE_APPROVAL

    # Resume: the 2 dead files should be re-queued; f1 (verified) untouched.
    summary = await store.resume_batch(batch.id)
    assert summary["requeued"] == 2
    assert (await store.get_file(f1)).state == FileState.VERIFIED
    assert (await store.get_file(f2)).state == FileState.QUEUED
    assert (await store.get_file(f2)).last_error is None  # cleared
    assert (await store.get_file(f3)).state == FileState.QUEUED
    assert (await store.get_batch(batch.id)).state == BatchState.RUNNING


async def test_skip_files_matching_excludes_pending_only(store: Store):
    run = await store.create_run("u", "root")
    venv1 = await _seed_file(store, run.id, dbx_id="id:1", path="/python/venv/a.py")
    venv2 = await _seed_file(store, run.id, dbx_id="id:2", path="/python/venv/b.pyc")
    real = await _seed_file(store, run.id, dbx_id="id:3", path="/pictures/x.jpg")
    batch = await store.create_batch(run.id, [venv1, venv2, real], 3)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    # Put one venv file mid-flight (downloading) — it must NOT be skipped.
    await store.claim_next_file(run.id, "w", order="fifo")  # claims venv1
    summary = await store.skip_files_matching(run.id, ["%/venv/%"])
    assert summary["skipped"] == 1  # only venv2 (queued); venv1 is in-flight
    assert (await store.get_file(venv2)).state == FileState.SKIPPED
    assert (await store.get_file(venv1)).state == FileState.DOWNLOADING  # untouched
    assert (await store.get_file(real)).state == FileState.QUEUED  # not matched


async def test_skipped_files_let_batch_settle(store: Store):
    run = await store.create_run("u", "root")
    a = await _seed_file(store, run.id, dbx_id="id:a", path="/venv/a.py")
    b = await _seed_file(store, run.id, dbx_id="id:b", path="/keep/b.jpg")
    batch = await store.create_batch(run.id, [a, b], 2)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    await store.skip_files_matching(run.id, ["%/venv/%"])
    # Verify the kept file; batch should then settle (skipped + verified == all).
    for s in [FileState.DOWNLOADING, FileState.DOWNLOADED, FileState.UPLOADING,
              FileState.UPLOADED, FileState.VERIFYING, FileState.VERIFIED]:
        await store.transition_file(b, s)
    bat = await store.refresh_batch_counters(batch.id)
    assert bat.state == BatchState.AWAITING_DELETE_APPROVAL


async def test_claim_order_largest_first(store: Store):
    run = await store.create_run("u", "root")
    small = await _seed_file(store, run.id, dbx_id="id:s", path="/s", size=10)
    big = await _seed_file(store, run.id, dbx_id="id:b", path="/b", size=10_000)
    mid = await _seed_file(store, run.id, dbx_id="id:m", path="/m", size=500)
    batch = await store.create_batch(run.id, [small, big, mid], 10510)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    # Largest-first: big, then mid, then small.
    c1 = await store.claim_next_file(run.id, "w", order="largest")
    c2 = await store.claim_next_file(run.id, "w", order="largest")
    c3 = await store.claim_next_file(run.id, "w", order="largest")
    assert [c1.id, c2.id, c3.id] == [big, mid, small]


async def test_resume_batch_noop_when_all_verified(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed_file(store, run.id)
    batch = await store.create_batch(run.id, [fid], 1)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    for s in [FileState.DOWNLOADING, FileState.DOWNLOADED, FileState.UPLOADING,
              FileState.UPLOADED, FileState.VERIFYING, FileState.VERIFIED]:
        await store.transition_file(fid, s)
    await store.refresh_batch_counters(batch.id)
    summary = await store.resume_batch(batch.id)
    assert summary["requeued"] == 0
    # Batch state unchanged (nothing to retry).
    assert (await store.get_batch(batch.id)).state == BatchState.AWAITING_DELETE_APPROVAL
