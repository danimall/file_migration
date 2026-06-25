"""Tests for the quota-aware, folder-by-folder batch scheduler."""

from __future__ import annotations

from filemig.config import Settings
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.states import FileState
from filemig.pipeline import scheduler
from filemig.util.identity import src_path_sha256

_GIB = 1024**3


async def _seed(store: Store, run: models.Run, path: str, size: int) -> int:
    f = models.DiscoveredFile(
        dbx_id=f"id:{path}", path_lower=path.lower(), path_display=path,
        parent_path_lower=path.rsplit("/", 1)[0].lower() or "", rev="r",
        size=size, content_hash="c" * 64, is_downloadable=True)
    await store.upsert_discovered_file(run.id, f, src_path_sha256(run.run_uuid, f.path_lower))
    rows = await store.files_in_state(run.id, FileState.DISCOVERED)
    return rows[-1].id


async def test_propose_groups_folder_by_folder(store: Store):
    run = await store.create_run("u", "root")
    # Interleave folders on insert; scheduler should regroup by folder.
    await _seed(store, run, "/b/2.txt", 10)
    await _seed(store, run, "/a/1.txt", 10)
    await _seed(store, run, "/b/1.txt", 10)
    await _seed(store, run, "/a/2.txt", 10)
    settings = Settings(batch_target_bytes=10**12, batch_file_limit=100)
    batch = await scheduler.propose_batch(store, settings, run)
    files = await store.files_in_batch(batch.id)
    paths = [f.src_path_lower for f in files]
    # Folder /a files come before folder /b files (folder-by-folder order).
    assert paths == ["/a/1.txt", "/a/2.txt", "/b/1.txt", "/b/2.txt"]


async def test_propose_respects_byte_target(store: Store):
    run = await store.create_run("u", "root")
    for i in range(10):
        await _seed(store, run, f"/a/{i:02d}.bin", 100)
    settings = Settings(batch_target_bytes=350, batch_file_limit=100)
    batch = await scheduler.propose_batch(store, settings, run)
    # 350-byte target -> at most 3 files of 100 bytes (4th would exceed).
    assert batch.file_count == 3
    assert batch.total_bytes == 300


async def test_propose_respects_file_limit(store: Store):
    run = await store.create_run("u", "root")
    for i in range(10):
        await _seed(store, run, f"/a/{i:02d}.bin", 1)
    settings = Settings(batch_target_bytes=10**9, batch_file_limit=4)
    batch = await scheduler.propose_batch(store, settings, run)
    assert batch.file_count == 4


async def test_propose_none_when_quota_full(store: Store):
    run = await store.create_run("u", "root")
    fid = await _seed(store, run, "/a/1.bin", 100)
    settings = Settings(daily_cap_bytes=1000, daily_cap_margin=0.0)
    # Fill the rolling window to the cap.
    await store.record_upload(run.id, fid, 1000)
    assert await scheduler.propose_batch(store, settings, run) is None


async def test_upload_allowed_gate(store: Store):
    run = await store.create_run("u", "root")
    settings = Settings(daily_cap_bytes=1000, daily_cap_margin=0.0)
    # Fresh window: a large file is allowed (mirrors Drive letting the first
    # file that breaks the cap finish).
    assert await scheduler.upload_allowed(store, settings, run.id, 5000)
    fid = await _seed(store, run, "/a/1.bin", 1)
    await store.record_upload(run.id, fid, 900)
    # 100 bytes remain: a 50-byte file fits, a 500-byte file does not.
    assert await scheduler.upload_allowed(store, settings, run.id, 50)
    assert not await scheduler.upload_allowed(store, settings, run.id, 500)


async def test_effective_cap_applies_margin():
    settings = Settings(daily_cap_bytes=1000 * _GIB, daily_cap_margin=0.05)
    assert settings.effective_daily_cap_bytes == int(1000 * _GIB * 0.95)
