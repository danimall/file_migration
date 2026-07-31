"""Asyncio worker pool that drives files through the migration pipeline.

Each worker loops: claim the next queued file (from a running, approved batch),
process it, and record the outcome. Concurrency is bounded by
``settings.worker_concurrency``. The pool can be paused (workers idle without
dropping in-flight files) and stopped (graceful drain).

Crash recovery runs once at startup via :func:`recover` before workers begin,
resetting interrupted rows to a safe state.
"""

from __future__ import annotations

import asyncio
import contextlib

from filemig.clients.drive_client import DriveClient
from filemig.clients.dropbox_client import DropboxClient
from filemig.config import Settings
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.errors import FatalError, RetryableError, VerificationMismatch
from filemig.pipeline.folders import FolderManager
from filemig.pipeline.worker import FileWorker
from filemig.web.events import bus

IDLE_SLEEP = 2.0  # seconds to wait when there is no claimable work


class WorkerPool:
    def __init__(
        self, *, settings: Settings, store: Store, dropbox: DropboxClient,
        drive: DriveClient, run: models.Run,
    ) -> None:
        self._settings = settings
        self._store = store
        self._dropbox = dropbox
        self._drive = drive
        self._run = run
        self._folders = FolderManager(drive, store, run)
        self._paused = asyncio.Event()
        self._paused.set()  # set = running; cleared = paused
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    @property
    def is_paused(self) -> bool:
        return not self._paused.is_set()

    def pause(self) -> None:
        self._paused.clear()
        bus.publish({"kind": "control", "detail": {"paused": True}})

    def resume(self) -> None:
        self._paused.set()
        bus.publish({"kind": "control", "detail": {"paused": False}})

    async def start(self) -> None:
        await self._folders.ensure_root()
        for i in range(self._settings.worker_concurrency):
            self._tasks.append(asyncio.create_task(self._run_worker(f"worker-{i}")))

    async def stop(self) -> None:
        self._stop.set()
        self._paused.set()  # unblock any paused workers so they can exit
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._tasks.clear()

    async def _run_worker(self, worker_id: str) -> None:
        worker = FileWorker(
            worker_id=worker_id, settings=self._settings, store=self._store,
            dropbox=self._dropbox, drive=self._drive, folders=self._folders,
            run=self._run)
        while not self._stop.is_set():
            await self._paused.wait()
            if self._stop.is_set():
                break
            file = await self._store.claim_next_file(
                self._run.id, worker_id, order=self._settings.claim_order)
            if file is None:
                await asyncio.sleep(IDLE_SLEEP)
                continue
            await self._process_one(worker, file)
            if file.batch_id is not None:
                await self._store.refresh_batch_counters(file.batch_id)

    async def _process_one(
        self, worker: FileWorker, file: models.FileRecord
    ) -> None:
        # Disk-headroom gate: defer large files when staging space is tight.
        if not await worker.has_disk_headroom(file):
            await self._store.requeue_file(file.id)
            await asyncio.sleep(IDLE_SLEEP)
            return
        try:
            await worker.process(file)
        except VerificationMismatch:
            # The file was already escalated to `dead` by the worker; nothing to
            # retry — it must never be deleted from the source.
            pass
        except FatalError as exc:
            await self._store.mark_dead(file.id, str(exc))
        except RetryableError as exc:
            await self._store.record_failure(
                file.id, str(exc), self._settings.max_attempts)
        except Exception as exc:  # noqa: BLE001 — defensive catch-all
            await self._store.record_failure(
                file.id, f"unexpected: {exc}", self._settings.max_attempts)
