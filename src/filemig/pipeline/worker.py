"""Per-file migration pipeline.

A worker claims a queued file and walks it through the state machine:

    queued -> downloading -> downloaded -> uploading -> uploaded
           -> verifying -> verified

Safety is enforced by two independent verification legs; a file becomes
``verified`` (and thus delete-eligible) only if BOTH pass:

* **Leg 1** — recompute Dropbox's ``content_hash`` over the downloaded bytes and
  compare to the metadata Dropbox returned. Proves the download is intact.
* **Leg 2** — compare Drive's ``md5Checksum`` (and ``sha256Checksum`` if present)
  plus size to what we computed locally. Proves the upload is intact.

We stage each file to a temp file (one streaming pass computes all hashes),
verify Leg 1 against the staged bytes, then resumable-upload from the staged
file (so a chunk retry has re-readable bytes and mid-file resume works), and
finally verify Leg 2 before deleting the temp file.

Any mismatch escalates the file to ``dead`` — it is never deleted from the
source.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from filemig.clients.drive_client import DriveClient, next_chunk_size
from filemig.clients.dropbox_client import DropboxClient
from filemig.config import Settings
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.errors import (
    FatalError,
    RetryableError,
    UploadSessionExpired,
    VerificationMismatch,
)
from filemig.domain.states import FileState
from filemig.hashing.multihash import MultiHasher
from filemig.pipeline import scheduler
from filemig.pipeline.folders import FolderManager
from filemig.util.identity import app_properties


class FileWorker:
    def __init__(
        self, *, worker_id: str, settings: Settings, store: Store,
        dropbox: DropboxClient, drive: DriveClient, folders: FolderManager,
        run: models.Run,
    ) -> None:
        self.worker_id = worker_id
        self._settings = settings
        self._store = store
        self._dropbox = dropbox
        self._drive = drive
        self._folders = folders
        self._run = run

    async def process(self, file: models.FileRecord) -> None:
        """Run a claimed file (in state ``downloading``) to completion.

        Exceptions are caught and recorded as failures/deaths by the pool; this
        method may raise RetryableError/FatalError/VerificationMismatch.
        """
        # 0. Idempotency: a prior (crashed) attempt may already have a verified
        #    copy on Drive. We can only short-circuit SAFELY when this row still
        #    carries the local md5 it computed at download time (it is persisted
        #    at the `downloaded` state, so it survives a crash mid-upload).
        if await self._try_short_circuit(file):
            return

        # 1. Download + stage + Leg 1 — unless a prior run already completed it
        #    (leg1_ok + a staged file still on disk). This is the resume path:
        #    recovery rewinds in-progress rows to `queued` but preserves their
        #    progress fields, so we can skip a re-download and reuse the staged
        #    bytes (Leg 2's md5 check will still catch any staging corruption).
        if self._download_already_complete(file):
            # The row was just claimed into `downloading`; step it back to the
            # durable `downloaded` resting point so the upload phase starts from
            # a legal state (downloading -> uploading is not a valid transition).
            if file.state == FileState.DOWNLOADING:
                file = await self._store.transition_file(
                    file.id, FileState.DOWNLOADED, expect_worker=self.worker_id)
        else:
            await self._download_and_verify_leg1(file)
            file = await self._store.get_file(file.id)  # refresh persisted hashes

        # 2. Upload (resumable) + Leg 2. probe_offset resumes mid-file.
        await self._upload_and_verify_leg2(file)

    @staticmethod
    def _download_already_complete(file: models.FileRecord) -> bool:
        return bool(
            file.leg1_ok and file.local_md5 and file.staged_path
            and Path(file.staged_path).exists()
        )

    # -- step 0: idempotency ------------------------------------------------

    async def _try_short_circuit(self, file: models.FileRecord) -> bool:
        """Jump straight to verification iff a Drive file tagged with this
        source's identity exists and its md5 matches the md5 we already computed
        locally. Without a local md5 we cannot prove the remote copy is intact,
        so we fall through to the normal (re-stage + verify) path."""
        from filemig.util.identity import KEY_SRC_PATH

        if not file.local_md5:
            return False
        try:
            matches = await self._drive.find_by_app_property(
                KEY_SRC_PATH, file.src_path_sha256)
        except RetryableError:
            return False  # probe failed; fall through to normal path
        for m in matches:
            md5 = m.get("md5Checksum")
            if md5 and md5 == file.local_md5:
                await self._store.transition_file(
                    file.id, FileState.UPLOADED,
                    fields={"dst_file_id": m["id"], "dst_md5": md5, "leg1_ok": 1,
                            "upload_offset": file.local_size or file.size},
                    expect_worker=self.worker_id)
                await self._verify_leg2_from_metadata(
                    file.id, m["id"], md5, m.get("sha256Checksum"), m.get("size"))
                return True
        return False

    # -- step 1: download + Leg 1 ------------------------------------------

    async def _download_and_verify_leg1(self, file: models.FileRecord) -> None:
        staged = self._staged_path(file)
        staged.parent.mkdir(parents=True, exist_ok=True)
        hasher = MultiHasher()

        if file.is_downloadable:
            meta, stream = await self._dropbox.download_stream(
                file.src_path_display, rev=file.dbx_rev)
            expected = meta.get("content_hash")
            leg1_kind = "content_hash"
        else:
            meta, stream = await self._dropbox.export_stream(
                file.src_path_display, file.export_format)
            expected = meta.get("export_metadata", {}).get("export_hash") or \
                meta.get("export_hash")
            leg1_kind = "export_hash"

        try:
            with open(staged, "wb") as fh:
                async for chunk in stream:
                    fh.write(chunk)
                    hasher.update(chunk)
        except Exception:
            staged.unlink(missing_ok=True)
            raise

        result = hasher.result()
        actual = result.dropbox_content_hash  # comparable to content_hash/export_hash

        # Leg 1: download integrity. For downloadable files Dropbox always
        # returns content_hash; its absence is itself a red flag, so we refuse
        # to proceed rather than skip the check. (Exported cloud docs may
        # legitimately lack an export_hash — Dropbox does not always provide one
        # — so Leg 1 is best-effort there and the mandatory Leg 2 checksum below
        # still proves the destination matches the bytes we exported.)
        if file.is_downloadable and not expected:
            staged.unlink(missing_ok=True)
            await self._store.mark_dead(
                file.id, "Leg1: Dropbox returned no content_hash; cannot verify download")
            raise VerificationMismatch("leg1", "content_hash", "absent")
        if expected and actual != expected:
            staged.unlink(missing_ok=True)
            await self._store.transition_file(
                file.id, FileState.DEAD,
                fields={"last_error": f"Leg1 {leg1_kind} mismatch"},
                detail={"leg": "leg1", "expected": expected, "actual": actual})
            raise VerificationMismatch("leg1", expected, actual)

        await self._store.transition_file(
            file.id, FileState.DOWNLOADED,
            fields={
                "staged_path": str(staged),
                "local_dbx_hash": result.dropbox_content_hash,
                "local_md5": result.md5,
                "local_sha256": result.sha256,
                "local_size": result.size,
                "export_hash": expected if not file.is_downloadable else None,
                "leg1_ok": 1,
            },
            expect_worker=self.worker_id,
        )

    # -- step 2: upload + Leg 2 --------------------------------------------

    async def _upload_and_verify_leg2(self, file: models.FileRecord) -> None:
        # Quota gate: respect the rolling 750 GiB/day cap.
        if not await scheduler.upload_allowed(
            self._store, self._settings, self._run.id, file.local_size or file.size):
            raise RetryableError("daily upload quota window full; will resume later")

        staged = Path(file.staged_path) if file.staged_path else self._staged_path(file)
        if not staged.exists():
            # Staged file vanished (e.g. cleaned after a crash); rewind to redo.
            await self._store.transition_file(
                file.id, FileState.QUEUED,
                fields={"staged_path": None, "upload_session_uri": None,
                        "upload_offset": 0},
                expect_worker=self.worker_id)
            raise RetryableError("staged file missing; re-queued for re-download")

        await self._store.transition_file(
            file.id, FileState.UPLOADING, expect_worker=self.worker_id)

        parent_id = await self._folders.resolve(file.parent_path_lower)
        total = file.local_size or file.size
        name = file.src_path_display.rsplit("/", 1)[-1]

        session_uri = file.upload_session_uri
        offset = file.upload_offset
        completed: dict | None = None

        if session_uri:
            # Resume an existing session: ask the server for the true offset.
            try:
                probed = await self._drive.probe_offset(session_uri, total)
            except UploadSessionExpired:
                session_uri = None
            else:
                if probed is None:
                    completed = await self._drive.get_checksums(file.dst_file_id) \
                        if file.dst_file_id else None
                    offset = total
                else:
                    offset = probed

        if session_uri is None:
            props = app_properties(
                self._run.run_uuid, file.src_path_sha256, file.src_content_hash)
            session_uri = await self._drive.initiate_resumable(
                name, parent_id, total, props)
            offset = 0
            await self._store.update_file_fields(
                file.id, upload_session_uri=session_uri, upload_offset=0)

        # Stream the staged file from ``offset`` in chunks. For a zero-byte file
        # there is nothing to stream, but the resumable session must still be
        # finalized by sending a single final empty chunk (Content-Range
        # "bytes */0" via a 0-length PUT), which returns the created file.
        if completed is None:
            if total == 0:
                status, body = await self._drive.put_chunk(session_uri, b"", 0, 0)
                if status == "done":
                    completed = body
            elif offset < total:
                completed = await self._upload_chunks(
                    file, staged, session_uri, offset, total)

        # Determine the Drive file id + checksums for Leg 2.
        if completed and "id" in completed:
            dst_file_id = completed["id"]
            md5 = completed.get("md5Checksum")
            sha256 = completed.get("sha256Checksum")
            dst_size = completed.get("size")
        else:
            dst_file_id = file.dst_file_id
            if not dst_file_id:
                raise RetryableError("upload finished without a file id; will retry")
            meta = await self._drive.get_checksums(dst_file_id)
            md5, sha256, dst_size = (meta.get("md5Checksum"),
                                     meta.get("sha256Checksum"), meta.get("size"))

        await self._store.transition_file(
            file.id, FileState.UPLOADED,
            fields={"dst_file_id": dst_file_id, "upload_offset": total},
            expect_worker=self.worker_id)
        await self._verify_leg2_from_metadata(file.id, dst_file_id, md5, sha256, dst_size)

        # Record the upload against the rolling quota and clean up staging.
        await self._store.record_upload(self._run.id, file.id, total)
        staged.unlink(missing_ok=True)

    async def _upload_chunks(
        self, file: models.FileRecord, staged: Path, session_uri: str,
        offset: int, total: int,
    ) -> dict | None:
        chunk_cfg = self._settings.upload_chunk_bytes
        completed: dict | None = None
        with open(staged, "rb") as fh:
            fh.seek(offset)
            while offset < total:
                size = next_chunk_size(total - offset, chunk_cfg)
                data = fh.read(size)
                if not data:
                    break
                status, body = await self._drive.put_chunk(
                    session_uri, data, offset, total)
                offset += len(data)
                # Persist the confirmed offset at each chunk boundary so a crash
                # resumes mid-file (not at every 256 KiB — once per chunk).
                await self._store.update_file_fields(file.id, upload_offset=offset)
                await self._store.renew_lease(file.id, self.worker_id)
                if status == "done":
                    completed = body
                    break
        return completed

    # -- Leg 2 helper -------------------------------------------------------

    async def _verify_leg2_from_metadata(
        self, file_id: int, dst_file_id: str, md5: str | None,
        sha256: str | None, dst_size: str | int | None,
    ) -> None:
        file = await self._store.get_file(file_id)
        if file is None:
            raise FatalError(f"file {file_id} vanished during verification")
        await self._store.transition_file(
            file_id, FileState.VERIFYING,
            fields={"dst_md5": md5, "dst_sha256": sha256,
                    "dst_size": int(dst_size) if dst_size is not None else None},
            expect_worker=self.worker_id)

        # Size check (cheap first gate).
        if dst_size is not None and file.local_size is not None and \
                int(dst_size) != file.local_size:
            await self._fail_leg2(file_id, "size", str(file.local_size), str(dst_size))

        # Leg 2 is the destination-integrity proof and MUST compare a real
        # content hash — size alone is never sufficient to call a file verified
        # (a length-preserving corruption would slip through). We upload all
        # content as binary (application/octet-stream), so Drive computes and
        # returns md5Checksum for every file we create; a missing md5/sha256 is
        # therefore anomalous and we refuse to verify rather than fall back to
        # size-only.
        compared = False
        if md5:
            if not file.local_md5 or md5 != file.local_md5:
                await self._fail_leg2(file_id, "md5", file.local_md5 or "n/a", md5)
            compared = True
        if sha256:
            if not file.local_sha256 or sha256 != file.local_sha256:
                await self._fail_leg2(file_id, "sha256", file.local_sha256 or "n/a", sha256)
            compared = True
        if not compared:
            # No Drive checksum to compare against — cannot prove byte identity.
            await self._fail_leg2(file_id, "no-checksum", "md5/sha256 expected", "absent")

        await self._store.transition_file(
            file_id, FileState.VERIFIED, fields={"leg2_ok": 1},
            expect_worker=self.worker_id)

    async def _fail_leg2(self, file_id: int, leg: str, expected: str, actual: str) -> None:
        await self._store.transition_file(
            file_id, FileState.DEAD,
            fields={"last_error": f"Leg2 {leg} mismatch"},
            detail={"leg": "leg2", "field": leg, "expected": expected, "actual": actual})
        raise VerificationMismatch(f"leg2/{leg}", expected, actual)

    # -- staging path -------------------------------------------------------

    def _staged_path(self, file: models.FileRecord) -> Path:
        return self._settings.resolved_staging_dir / f"file-{file.id}.part"

    async def has_disk_headroom(self, file: models.FileRecord) -> bool:
        """True if staging this file keeps free space above the configured floor."""
        try:
            usage = shutil.disk_usage(self._settings.resolved_staging_dir)
        except FileNotFoundError:
            os.makedirs(self._settings.resolved_staging_dir, exist_ok=True)
            usage = shutil.disk_usage(self._settings.resolved_staging_dir)
        needed = (file.local_size or file.size)
        return (usage.free - needed) >= self._settings.staging_min_free_bytes
