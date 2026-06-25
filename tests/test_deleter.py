"""Tests for manual-approval source deletion (soft delete + parent_rev guard)."""

from __future__ import annotations

import pytest

from filemig.db.store import Store, utcnow_iso_in
from filemig.domain import models
from filemig.domain.states import BatchState, FileState
from filemig.pipeline import deleter
from filemig.util.identity import src_path_sha256


class FakeDropboxDelete:
    """Models delete_batch: async job, then per-entry results. A configured set
    of paths fail with a parent_rev conflict."""

    def __init__(self, fail_paths: set[str] | None = None):
        self.fail_paths = fail_paths or set()
        self.launched: list[list[dict]] = []
        self._jobs: dict[str, list[dict]] = {}
        self._n = 0

    async def delete_batch(self, entries):
        self.launched.append(entries)
        results = []
        for e in entries:
            if e["path"] in self.fail_paths:
                results.append({".tag": "path_lookup_error"})
            else:
                results.append({".tag": "success",
                                "metadata": {".tag": "file", "id": e["path"]}})
        self._n += 1
        job = f"job-{self._n}"
        self._jobs[job] = results
        return job  # always async

    async def delete_batch_check(self, async_job_id):
        return {".tag": "complete", "entries": self._jobs[async_job_id]}


async def _verified_batch(store: Store, sizes: dict[str, int]):
    run = await store.create_run("u", "root")
    ids = []
    for path, size in sizes.items():
        f = models.DiscoveredFile(
            dbx_id=path, path_lower=path.lower(), path_display=path,
            parent_path_lower="/x", rev="rev-" + path, size=size,
            content_hash="c" * 64, is_downloadable=True)
        await store.upsert_discovered_file(run.id, f, src_path_sha256(run.run_uuid, path))
        ids.append((await store.files_in_state(run.id, FileState.DISCOVERED))[-1].id)
    batch = await store.create_batch(run.id, ids, sum(sizes.values()))
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    # Drive everything to verified.
    for fid in ids:
        for s in [FileState.DOWNLOADING, FileState.DOWNLOADED, FileState.UPLOADING,
                  FileState.UPLOADED, FileState.VERIFYING, FileState.VERIFIED]:
            await store.transition_file(fid, s)
    await store.refresh_batch_counters(batch.id)
    return run, batch, ids


async def test_deletion_requires_approved_state(store: Store):
    run, batch, ids = await _verified_batch(store, {"/x/a": 1})
    # Batch is awaiting_delete_approval, not yet DELETING -> refuse.
    with pytest.raises(ValueError):
        await deleter.run_batch_deletion(FakeDropboxDelete(), store, batch.id)


async def test_happy_path_soft_deletes_and_finishes(store: Store):
    run, batch, ids = await _verified_batch(store, {"/x/a": 1, "/x/b": 2})
    await store.transition_batch(batch.id, BatchState.DELETING,
                                 fields={"delete_approved_at": utcnow_iso_in(0)})
    client = FakeDropboxDelete()
    summary = await deleter.run_batch_deletion(client, store, batch.id)
    assert summary == {"deleted": 2, "failed": 0, "skipped": 0}
    for fid in ids:
        assert (await store.get_file(fid)).state == FileState.DONE
    assert (await store.get_batch(batch.id)).state == BatchState.DONE
    # parent_rev was passed for the guard.
    assert all("parent_rev" in e for e in client.launched[0])


async def test_parent_rev_conflict_keeps_file_verified(store: Store):
    run, batch, ids = await _verified_batch(store, {"/x/a": 1, "/x/b": 2})
    await store.transition_batch(batch.id, BatchState.DELETING,
                                 fields={"delete_approved_at": utcnow_iso_in(0)})
    # File /x/b fails (changed since discovery) -> must NOT be deleted.
    client = FakeDropboxDelete(fail_paths={"/x/b"})
    summary = await deleter.run_batch_deletion(client, store, batch.id)
    assert summary["deleted"] == 1 and summary["failed"] == 1
    states = {(await store.get_file(fid)).state for fid in ids}
    assert FileState.DONE in states
    # The failed file is back to verified (intact on Drive, source preserved).
    failed = [await store.get_file(fid) for fid in ids]
    survivor = [f for f in failed if f.dbx_id == "/x/b"][0]
    assert survivor.state == FileState.VERIFIED
    assert "source delete failed" in (survivor.last_error or "")
