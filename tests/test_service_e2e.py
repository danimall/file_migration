"""Full-flow integration test through the MigrationService + HTTP routes.

Uses fake Dropbox/Drive clients injected into the service, then drives the
public actions the dashboard buttons trigger: scan -> propose -> approve copy ->
(workers run) -> verification gate -> approve delete -> reconcile.
"""

from __future__ import annotations

import hashlib

import pytest

from filemig.config import Settings
from filemig.domain.states import BatchState, FileState
from filemig.hashing.dropbox_content_hash import compute_dropbox_content_hash
from filemig.service import MigrationService


class FakeDropbox:
    def __init__(self, files: dict[str, bytes]):
        # path_display -> bytes
        self._files = files
        self._deleted: set[str] = set()

    async def list_folder(self, path="", recursive=True):
        for disp, data in self._files.items():
            low = disp.lower()
            parent = low.rsplit("/", 1)[0] if "/" in low else ""
            yield {
                ".tag": "file", "id": f"id:{disp}", "path_lower": low,
                "path_display": disp, "rev": "rev-" + disp, "size": len(data),
                "content_hash": compute_dropbox_content_hash(data),
                "is_downloadable": True,
            }

    async def download_stream(self, path, rev=None):
        data = self._files[path]
        meta = {"content_hash": compute_dropbox_content_hash(data), "rev": rev,
                "size": len(data)}

        async def gen():
            yield data
        return meta, gen()

    async def delete_batch(self, entries):
        results = []
        for e in entries:
            self._deleted.add(e["path"])
            results.append({".tag": "success", "metadata": {".tag": "file"}})
        return {".tag": "complete", "entries": results}

    async def delete_batch_check(self, job):  # pragma: no cover - sync path used
        return {".tag": "complete", "entries": []}


class FakeDrive:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.props: dict[str, dict] = {}
        self._seq = 0

    async def storage_quota(self):
        return {"usage": "0", "limit": str(5 * 1024**4)}

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

    async def initiate_resumable(self, name, parent_id, total, props, mime_type="x"):
        self._seq += 1
        self._cur = (f"sess-{self._seq}", bytearray(), props)
        return self._cur[0]

    async def put_chunk(self, uri, chunk, offset, total):
        _, buf, props = self._cur
        buf.extend(chunk)
        if len(buf) >= total:
            self._seq += 1
            fid = f"file-{self._seq}"
            data = bytes(buf)
            self.files[fid] = data
            self.props[fid] = props
            return "done", {"id": fid, "md5Checksum": hashlib.md5(data).hexdigest(),
                            "sha256Checksum": hashlib.sha256(data).hexdigest(),
                            "size": str(len(data))}
        return "incomplete", None

    async def probe_offset(self, uri, total):
        return len(self._cur[1])

    async def get_checksums(self, fid):
        data = self.files[fid]
        return {"md5Checksum": hashlib.md5(data).hexdigest(),
                "sha256Checksum": hashlib.sha256(data).hexdigest(),
                "size": str(len(data))}


@pytest.fixture
async def service(tmp_path):
    settings = Settings(data_dir=tmp_path, worker_concurrency=1,
                        upload_chunk_bytes=262144, staging_min_free_bytes=0)
    svc = MigrationService(settings)
    # Inject fakes + pretend both providers are connected.
    files = {f"/docs/{i}.bin": (b"payload-%d-" % i) * 5000 for i in range(3)}
    svc.dropbox = FakeDropbox(files)
    svc.drive = FakeDrive()
    svc.tokens.save_refresh_token("dropbox", "x")
    svc.tokens.save_refresh_token("google", "y")
    await svc.startup()
    yield svc
    await svc.shutdown()


async def _drain(svc, timeout=5.0):
    """Wait until no files remain queued/in-progress."""
    import asyncio
    from filemig.domain.states import IN_PROGRESS_STATES
    active = {s.value for s in IN_PROGRESS_STATES} | {FileState.QUEUED.value}
    for _ in range(int(timeout / 0.05)):
        counts = await svc.state_counts()
        if not any(counts.get(s, 0) for s in active):
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"workers did not drain: {await svc.state_counts()}")


async def test_full_migration_flow(service):
    svc = service
    # 1. Scan.
    await svc.start_scan()
    await svc._scan_task
    assert svc.scan_summary.total_files == 3

    # 2. Propose a batch (folder-by-folder, all in /docs).
    batch = await svc.propose_batch()
    assert batch is not None and batch.file_count == 3

    # 3. Approve copy -> workers run.
    await svc.approve_copy(batch.id)
    await _drain(svc)

    refreshed = await svc.store.refresh_batch_counters(batch.id)
    assert refreshed.verified_count == 3
    assert refreshed.state == BatchState.AWAITING_DELETE_APPROVAL

    # 4. Reconcile: everything confirmed on Drive.
    report = await svc.reconcile()
    assert report.confirmed == 3 and report.ok

    # 5. SAFETY: deletion is disabled — approve_delete refuses and NOTHING is
    # removed from Dropbox. This is a copy-and-verify-only build.
    with pytest.raises(ValueError, match="disabled"):
        await svc.approve_delete(batch.id)
    assert len(svc.dropbox._deleted) == 0  # sources untouched


async def test_approve_delete_is_disabled(service):
    svc = service
    await svc.start_scan()
    await svc._scan_task
    batch = await svc.propose_batch()
    with pytest.raises(ValueError, match="disabled"):
        await svc.approve_delete(batch.id)
