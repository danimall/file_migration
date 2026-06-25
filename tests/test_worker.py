"""End-to-end worker pipeline tests with in-memory fake clients.

The fakes implement just the methods the worker calls, modelling Dropbox
download (with a real content_hash over the bytes) and Drive resumable upload +
checksums. This exercises the full state machine, both verification legs, the
mismatch->dead safety path, and upload resume — without real network.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from filemig.config import Settings
from filemig.db.connection import Database
from filemig.db.store import Store, utcnow_iso_in
from filemig.domain import models
from filemig.domain.states import BatchState, FileState
from filemig.hashing.dropbox_content_hash import compute_dropbox_content_hash
from filemig.pipeline.folders import FolderManager
from filemig.pipeline.worker import FileWorker


class FakeDropbox:
    def __init__(self, content: bytes, content_hash: str | None = None):
        self.content = content
        self.content_hash = content_hash or compute_dropbox_content_hash(content)

    async def download_stream(self, path, rev=None):
        meta = {"content_hash": self.content_hash, "rev": rev, "size": len(self.content)}

        async def gen():
            # Yield in two chunks to exercise streaming.
            mid = len(self.content) // 2
            yield self.content[:mid]
            yield self.content[mid:]

        return meta, gen()


class FakeDrive:
    """Models resumable upload accumulation + checksum readback."""

    def __init__(self, *, corrupt: bool = False):
        self.sessions: dict[str, bytearray] = {}
        self.files: dict[str, bytes] = {}
        self.props: dict[str, dict] = {}
        self._seq = 0
        self.corrupt = corrupt
        self.root_created = False

    async def create_folder(self, name, parent_id):
        self._seq += 1
        return f"folder-{self._seq}"

    async def find_by_app_property(self, key, value):
        out = []
        for fid, props in self.props.items():
            if props.get(key) == value:
                data = self.files[fid]
                out.append({"id": fid, "md5Checksum": hashlib.md5(data).hexdigest(),
                            "sha256Checksum": hashlib.sha256(data).hexdigest(),
                            "size": str(len(data)), "appProperties": props})
        return out

    async def initiate_resumable(self, name, parent_id, total_size, app_properties,
                                 mime_type="application/octet-stream"):
        self._seq += 1
        uri = f"https://upload/session/{self._seq}"
        self.sessions[uri] = bytearray()
        self._pending_props = app_properties
        return uri

    async def put_chunk(self, session_uri, chunk, offset, total_size):
        buf = self.sessions[session_uri]
        buf.extend(chunk)
        if len(buf) >= total_size:
            self._seq += 1
            fid = f"file-{self._seq}"
            data = bytes(buf)
            if self.corrupt:
                data = data + b"x"  # tamper so Leg 2 must fail
            self.files[fid] = data
            self.props[fid] = self._pending_props
            return "done", {"id": fid,
                            "md5Checksum": hashlib.md5(data).hexdigest(),
                            "sha256Checksum": hashlib.sha256(data).hexdigest(),
                            "size": str(len(data))}
        return "incomplete", None

    async def probe_offset(self, session_uri, total_size):
        return len(self.sessions.get(session_uri, b""))

    async def get_checksums(self, file_id):
        data = self.files[file_id]
        return {"md5Checksum": hashlib.md5(data).hexdigest(),
                "sha256Checksum": hashlib.sha256(data).hexdigest(),
                "size": str(len(data))}


async def _setup(tmp_path, content: bytes, drive, content_hash=None):
    settings = Settings(data_dir=tmp_path, worker_concurrency=1,
                        upload_chunk_bytes=262144, staging_min_free_bytes=0)
    settings.ensure_dirs()
    db = Database(settings.db_path)
    await db.connect()
    store = Store(db)
    run = await store.create_run("run-uuid-xyz", "Migration Root")
    f = models.DiscoveredFile(
        dbx_id="id:1", path_lower="/photos/a.bin", path_display="/Photos/a.bin",
        parent_path_lower="/photos", rev="rev1", size=len(content),
        content_hash=content_hash or compute_dropbox_content_hash(content),
        is_downloadable=True)
    from filemig.util.identity import src_path_sha256
    await store.upsert_folder(run.id, models.DiscoveredFolder("", "/", None, 0))
    await store.upsert_folder(run.id, models.DiscoveredFolder("/photos", "/Photos", "", 1))
    await store.upsert_discovered_file(run.id, f, src_path_sha256(run.run_uuid, f.path_lower))
    file = (await store.files_in_state(run.id, FileState.DISCOVERED))[0]
    batch = await store.create_batch(run.id, [file.id], len(content))
    await store.transition_batch(batch.id, BatchState.COPY_APPROVED,
                                 fields={"copy_approved_at": utcnow_iso_in(0)})
    await store.transition_batch(batch.id, BatchState.RUNNING, requeue_files=True)
    folders = FolderManager(drive, store, run)
    await folders.ensure_root()
    fake_dbx = FakeDropbox(content, content_hash)
    worker = FileWorker(worker_id="w0", settings=settings, store=store,
                        dropbox=fake_dbx, drive=drive, folders=folders, run=run)
    return settings, store, run, worker, batch


async def test_happy_path_verifies_and_records_quota(tmp_path):
    content = b"hello world " * 100000  # ~1.2 MB, multi-chunk
    drive = FakeDrive()
    settings, store, run, worker, batch = await _setup(tmp_path, content, drive)
    claimed = await store.claim_next_file(run.id, "w0")
    await worker.process(claimed)
    file = await store.get_file(claimed.id)
    assert file.state == FileState.VERIFIED
    assert file.leg1_ok and file.leg2_ok
    assert file.dst_file_id is not None
    # Quota ledger recorded the upload.
    used, _ = await store.quota_used_24h(run.id)
    assert used == len(content)
    # Staged temp file cleaned up.
    assert not Path(settings.resolved_staging_dir, f"file-{file.id}.part").exists()


async def test_leg1_mismatch_marks_dead_and_never_uploads(tmp_path):
    content = b"abc" * 1000
    drive = FakeDrive()
    # Lie about the content hash so Leg 1 fails.
    settings, store, run, worker, batch = await _setup(
        tmp_path, content, drive, content_hash="deadbeef" * 8)
    claimed = await store.claim_next_file(run.id, "w0")
    from filemig.domain.errors import VerificationMismatch
    with pytest.raises(VerificationMismatch):
        await worker.process(claimed)
    file = await store.get_file(claimed.id)
    assert file.state == FileState.DEAD
    assert not file.leg2_ok
    assert drive.files == {}  # nothing uploaded


async def test_leg2_mismatch_marks_dead(tmp_path):
    content = b"xyz" * 5000
    drive = FakeDrive(corrupt=True)  # Drive returns a checksum for tampered bytes
    settings, store, run, worker, batch = await _setup(tmp_path, content, drive)
    claimed = await store.claim_next_file(run.id, "w0")
    from filemig.domain.errors import VerificationMismatch
    with pytest.raises(VerificationMismatch):
        await worker.process(claimed)
    file = await store.get_file(claimed.id)
    assert file.state == FileState.DEAD


class NoChecksumDrive(FakeDrive):
    """Drive that never returns md5/sha256 (only size) — must NOT verify."""

    async def put_chunk(self, session_uri, chunk, offset, total_size):
        buf = self.sessions[session_uri]
        buf.extend(chunk)
        if len(buf) >= total_size:
            self._seq += 1
            fid = f"file-{self._seq}"
            data = bytes(buf)
            self.files[fid] = data
            self.props[fid] = self._pending_props
            return "done", {"id": fid, "size": str(len(data))}  # no checksums
        return "incomplete", None

    async def get_checksums(self, file_id):
        return {"size": str(len(self.files[file_id]))}  # no checksums


async def test_no_drive_checksum_refuses_to_verify(tmp_path):
    """SAFETY: a file must never reach verified on size alone (no content hash
    compared on the destination leg)."""
    content = b"safety!" * 4000
    drive = NoChecksumDrive()
    settings, store, run, worker, batch = await _setup(tmp_path, content, drive)
    claimed = await store.claim_next_file(run.id, "w0")
    from filemig.domain.errors import VerificationMismatch
    with pytest.raises(VerificationMismatch):
        await worker.process(claimed)
    file = await store.get_file(claimed.id)
    assert file.state == FileState.DEAD
    assert not file.leg2_ok


async def test_idempotent_rerun_short_circuits(tmp_path):
    content = b"data!" * 20000
    drive = FakeDrive()
    settings, store, run, worker, batch = await _setup(tmp_path, content, drive)
    claimed = await store.claim_next_file(run.id, "w0")
    await worker.process(claimed)
    n_files_after_first = len(drive.files)
    # Reset the row to queued (simulate a re-run) but keep local hashes/dst id.
    await store.requeue_file(claimed.id)
    re = await store.claim_next_file(run.id, "w0")
    await worker.process(re)
    # No second upload happened — short-circuit verified via appProperties.
    assert len(drive.files) == n_files_after_first


async def test_zero_byte_file_migrates_and_verifies(tmp_path):
    """Empty files (e.g. .localized markers) must copy+verify, not die on an
    unfinalized resumable upload."""
    content = b""
    drive = FakeDrive()
    settings, store, run, worker, batch = await _setup(tmp_path, content, drive)
    claimed = await store.claim_next_file(run.id, "w0")
    await worker.process(claimed)
    file = await store.get_file(claimed.id)
    assert file.state == FileState.VERIFIED
    assert file.leg1_ok and file.leg2_ok
    assert file.dst_file_id is not None


async def test_resume_after_download_does_not_illegal_transition(tmp_path):
    """A file re-claimed after its download already completed must step through
    `downloaded` (not jump downloading->uploading) so it resumes cleanly."""
    content = b"resume me " * 50000
    drive = FakeDrive()
    settings, store, run, worker, batch = await _setup(tmp_path, content, drive)
    # Simulate a crash right after download+Leg1: row is downloaded with hashes
    # and a staged file, then recovery rewinds it to queued.
    claimed = await store.claim_next_file(run.id, "w0")  # downloading
    await worker._download_and_verify_leg1(claimed)       # -> downloaded + staged
    await store.recover_interrupted(run.id)               # rewinds to queued
    # Re-claim and process: must not raise an illegal-transition error.
    re = await store.claim_next_file(run.id, "w0")        # downloading again
    assert re is not None
    await worker.process(re)
    file = await store.get_file(re.id)
    assert file.state == FileState.VERIFIED
    assert file.last_error is None or "illegal" not in (file.last_error or "")
    assert (await store.get_file(claimed.id)).state == FileState.VERIFIED
