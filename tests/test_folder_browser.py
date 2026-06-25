"""Tests for the hybrid folder browser: subtree stats, child listing, and
folder-scoped batch proposal."""

from __future__ import annotations

from filemig.config import Settings
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.states import BatchState, FileState
from filemig.pipeline import scheduler
from filemig.util.identity import src_path_sha256


async def _seed(store: Store, run: models.Run, path: str, size: int):
    parent = path.rsplit("/", 1)[0]
    f = models.DiscoveredFile(
        dbx_id=f"id:{path}", path_lower=path.lower(), path_display=path,
        parent_path_lower=parent.lower(), rev="r", size=size,
        content_hash="c" * 64, is_downloadable=True)
    await store.upsert_discovered_file(run.id, f, src_path_sha256(run.run_uuid, f.path_lower))


async def _folders(store: Store, run: models.Run, paths: list[str]):
    for p in paths:
        parent = p.rsplit("/", 1)[0] if "/" in p.strip("/") else ""
        depth = p.strip("/").count("/") + 1
        await store.upsert_folder(run.id, models.DiscoveredFolder(
            p.lower(), p, parent.lower(), depth))


async def test_list_child_folders_excludes_root_and_has_stats(store: Store):
    run = await store.create_run("u", "root")
    await store.upsert_folder(run.id, models.DiscoveredFolder("", "/", None, 0))
    await _folders(store, run, ["/Photos", "/Photos/2019", "/Docs"])
    await _seed(store, run, "/Photos/a.jpg", 100)
    await _seed(store, run, "/Photos/2019/b.jpg", 200)
    await _seed(store, run, "/Docs/c.pdf", 50)

    top = await store.list_child_folders(run.id, None)
    names = [n.name for n in top]
    assert names == ["Docs", "Photos"]  # alpha by display; no "/" root row
    photos = next(n for n in top if n.name == "Photos")
    # Subtree stats include the nested 2019 folder's file.
    assert photos.file_count == 2
    assert photos.total_bytes == 300
    assert photos.has_child_folders is True
    docs = next(n for n in top if n.name == "Docs")
    assert docs.file_count == 1
    assert docs.has_child_folders is False


async def test_verified_files_count_as_migrated_in_copy_only_mode(store: Store):
    """A folder whose files are all 'verified' (the terminal state in copy-only
    mode — they never reach 'done') must report as fully migrated, NOT 'queued'."""
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/Done"])
    await _seed(store, run, "/Done/a", 10)
    await _seed(store, run, "/Done/b", 20)
    # Walk both files to 'verified' (copy complete, not deleted).
    for f in await store.files_in_state(run.id, FileState.DISCOVERED):
        for s in [FileState.QUEUED, FileState.DOWNLOADING, FileState.DOWNLOADED,
                  FileState.UPLOADING, FileState.UPLOADED, FileState.VERIFYING,
                  FileState.VERIFIED]:
            await store.transition_file(f.id, s)
    node = next(n for n in await store.list_child_folders(run.id, None)
                if n.name == "Done")
    assert node.migrated_count == 2
    assert node.fully_migrated is True
    assert node.in_progress_count == 0


async def test_partial_and_dead_counts(store: Store):
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/Mix"])
    await _seed(store, run, "/Mix/ok", 10)
    await _seed(store, run, "/Mix/bad", 10)
    await _seed(store, run, "/Mix/pending", 10)
    files = {f.src_path_lower: f for f in
             await store.files_in_state(run.id, FileState.DISCOVERED)}
    # Assign all three to a batch (so they count as in-flight work), then make
    # one verified, one dead, one still queued.
    await store.create_batch(run.id, [f.id for f in files.values()], 30)
    ok = files["/mix/ok"]
    for s in [FileState.QUEUED, FileState.DOWNLOADING, FileState.DOWNLOADED,
              FileState.UPLOADING, FileState.UPLOADED, FileState.VERIFYING,
              FileState.VERIFIED]:
        await store.transition_file(ok.id, s)
    await store.mark_dead(files["/mix/bad"].id, "boom")
    await store.transition_file(files["/mix/pending"].id, FileState.QUEUED)
    node = next(n for n in await store.list_child_folders(run.id, None)
                if n.name == "Mix")
    assert node.migrated_count == 1
    assert node.dead_count == 1
    assert node.in_progress_count == 1
    assert node.fully_migrated is False
    assert node.partially_migrated is True


async def test_subtree_prefix_does_not_leak_sibling(store: Store):
    """A folder named '/Photo' must not pull in files under '/Photos'."""
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/Photo", "/Photos"])
    await _seed(store, run, "/Photos/x.jpg", 999)
    await _seed(store, run, "/Photo/y.jpg", 1)
    top = await store.list_child_folders(run.id, None)
    photo = next(n for n in top if n.name == "Photo")
    assert photo.file_count == 1 and photo.total_bytes == 1


async def test_propose_batch_from_folders_scopes_to_selection(store: Store):
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/Keep", "/Skip"])
    await _seed(store, run, "/Keep/a", 10)
    await _seed(store, run, "/Keep/b", 20)
    await _seed(store, run, "/Skip/c", 30)
    settings = Settings(batch_target_bytes=10**12, batch_file_limit=1000)
    batch, info = await scheduler.propose_batch_from_folders(
        store, settings, run, ["/keep"])
    assert batch is not None
    assert info["queued_files"] == 2
    files = await store.files_in_batch(batch.id)
    assert {f.src_path_lower for f in files} == {"/keep/a", "/keep/b"}
    # The /Skip file was untouched.
    skip_files = await store.files_in_state(run.id, FileState.DISCOVERED)
    assert any(f.src_path_lower == "/skip/c" for f in skip_files)


async def test_plan_all_batches_chunks_entire_selection(store: Store):
    """Selecting a big folder plans ALL batches needed (not just the first),
    bounded by the per-batch file limit — independent of the daily quota."""
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/Pics"])
    for i in range(10):
        await _seed(store, run, f"/Pics/{i:03d}", 100)
    settings = Settings(batch_file_limit=3, batch_target_bytes=10**12,
                        daily_cap_bytes=10**12, daily_cap_margin=0.0)
    info = await scheduler.plan_all_batches_from_folders(
        store, settings, run, ["/pics"])
    # 10 files / 3-per-batch -> 4 batches (3+3+3+1), all files queued.
    assert info["batches"] == 4
    assert info["queued_files"] == 10
    assert info["queued_bytes"] == 1000
    batches = await store.list_batches(run.id)
    assert len(batches) == 4
    assert all(b.state == BatchState.PROPOSED for b in batches)


async def test_start_all_runs_every_proposed_batch(store: Store):
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/X"])
    for i in range(6):
        await _seed(store, run, f"/X/{i}", 10)
    settings = Settings(batch_file_limit=2, batch_target_bytes=10**12,
                        daily_cap_bytes=10**12, daily_cap_margin=0.0)
    await scheduler.plan_all_batches_from_folders(store, settings, run, ["/x"])
    summary = await store.start_all_proposed(run.id)
    assert summary["batches"] == 3
    assert summary["files"] == 6
    batches = await store.list_batches(run.id)
    assert all(b.state == BatchState.RUNNING for b in batches)
    # Files are now queued and claimable.
    queued = await store.files_in_state(run.id, FileState.QUEUED)
    assert len(queued) == 6


async def test_propose_from_folders_splits_on_quota(store: Store):
    run = await store.create_run("u", "root")
    await _folders(store, run, ["/Big"])
    for i in range(5):
        await _seed(store, run, f"/Big/{i}", 100)
    # Target only fits 2 files per batch.
    settings = Settings(batch_target_bytes=250, batch_file_limit=1000,
                        daily_cap_bytes=10**9, daily_cap_margin=0.0)
    batch, info = await scheduler.propose_batch_from_folders(
        store, settings, run, ["/big"])
    assert info["queued_files"] == 2
    assert info["remaining_files"] == 3  # left for a follow-up batch
