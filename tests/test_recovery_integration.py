"""Crash-recovery integration: a file interrupted mid-upload resumes without
re-downloading, and recovery preserves its staged file + upload offset."""

from __future__ import annotations

from pathlib import Path

from filemig.config import Settings
from filemig.db.connection import Database
from filemig.db.store import Store, utcnow_iso_in
from filemig.domain import models
from filemig.domain.states import BatchState, FileState
from filemig.pipeline.recovery import recover
from filemig.util.identity import src_path_sha256


async def _store(tmp_path):
    settings = Settings(data_dir=tmp_path)
    settings.ensure_dirs()
    db = Database(settings.db_path)
    await db.connect()
    return settings, Store(db)


async def test_recovery_rewinds_uploading_and_keeps_staging(tmp_path):
    settings, store = await _store(tmp_path)
    run = await store.create_run("r", "root")
    f = models.DiscoveredFile("id:1", "/a/x.bin", "/a/x.bin", "/a", "rev1", 1000,
                              "c" * 64, True)
    await store.upsert_discovered_file(run.id, f, src_path_sha256(run.run_uuid, "/a/x.bin"))
    fid = (await store.files_in_state(run.id, FileState.DISCOVERED))[0].id
    batch = await store.create_batch(run.id, [fid], 1000)
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)

    # Simulate progress up to mid-upload, with a staged file on disk.
    staged = settings.resolved_staging_dir / f"file-{fid}.part"
    staged.write_bytes(b"x" * 1000)
    await store.claim_next_file(run.id, "w0")  # -> downloading
    await store.transition_file(fid, FileState.DOWNLOADED, fields={
        "staged_path": str(staged), "local_dbx_hash": "c" * 64,
        "local_md5": "m", "local_sha256": "s", "local_size": 1000, "leg1_ok": 1})
    await store.transition_file(fid, FileState.UPLOADING, fields={
        "upload_session_uri": "https://sess/1", "upload_offset": 512})

    # Crash + recover.
    reset = await recover(store, settings, run)
    assert reset == 1
    rec = await store.get_file(fid)
    assert rec.state == FileState.QUEUED          # rewound for re-claim
    assert rec.leg1_ok                            # download work preserved
    assert rec.local_md5 == "m"
    assert rec.upload_session_uri == "https://sess/1"  # resume the SAME session
    assert rec.upload_offset == 512               # resume offset preserved
    assert Path(rec.staged_path).exists()         # staged bytes kept


async def test_recovery_of_crashed_deletion_returns_to_verified(tmp_path):
    """A crash mid-deletion must leave the file recoverable as VERIFIED (it is
    intact on Drive; only the source delete is retried) — never rewound to
    QUEUED to re-run the whole copy."""
    settings, store = await _store(tmp_path)
    run = await store.create_run("r", "root")
    f = models.DiscoveredFile("id:1", "/a/x.bin", "/a/x.bin", "/a", "rev1", 10,
                              "c" * 64, True)
    await store.upsert_discovered_file(run.id, f, src_path_sha256(run.run_uuid, "/a/x.bin"))
    fid = (await store.files_in_state(run.id, FileState.DISCOVERED))[0].id
    # Walk to verified, then into deleting (lease already cleared at verified).
    for s in [FileState.QUEUED, FileState.DOWNLOADING, FileState.DOWNLOADED,
              FileState.UPLOADING, FileState.UPLOADED, FileState.VERIFYING,
              FileState.VERIFIED, FileState.DELETING]:
        await store.transition_file(fid, s)
    reset = await store.recover_interrupted(run.id)
    assert reset == 1
    assert (await store.get_file(fid)).state == FileState.VERIFIED


async def test_recovery_sweeps_orphan_staging(tmp_path):
    settings, store = await _store(tmp_path)
    run = await store.create_run("r", "root")
    # An orphan .part with no owning row should be swept.
    orphan = settings.resolved_staging_dir / "file-999.part"
    orphan.write_bytes(b"junk")
    await recover(store, settings, run)
    assert not orphan.exists()
