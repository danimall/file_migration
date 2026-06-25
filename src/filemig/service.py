"""Application service: owns shared singletons and orchestrates the pipeline.

One instance lives for the process lifetime (created in the FastAPI lifespan).
It holds the database, token store, auth manager, HTTP client, API clients, the
active run, and the worker pool, and exposes the high-level actions the web
routes call (connect, scan, propose/approve batches, pause/resume, approve
deletion, reconcile).
"""

from __future__ import annotations

import asyncio
import secrets

import httpx

from filemig.clients.auth import AuthManager
from filemig.clients.drive_client import DriveClient
from filemig.clients.dropbox_client import DropboxClient
from filemig.clients.tokens import TokenStore
from filemig.config import Settings
from filemig.db.connection import Database
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.states import BatchState, FileState
# NOTE: pipeline.deleter is intentionally NOT imported — source deletion is
# disabled in this copy-and-verify-only build (see DROPBOX_DELETION_ENABLED).
from filemig.pipeline import discovery, reconcile, scheduler
from filemig.pipeline.pool import WorkerPool
from filemig.pipeline.recovery import recover
from filemig.util.clock import utcnow_iso

DEFAULT_RUN_NAME = "Dropbox Migration"


class MigrationService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.db = Database(settings.db_path)
        self.tokens = TokenStore(settings.secrets_dir)
        self.auth = AuthManager(settings, self.tokens)
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, read=300.0))
        self.dropbox = DropboxClient(self.auth, self._http)
        self.drive = DriveClient(self.auth, self._http)
        self.run: models.Run | None = None
        self.pool: WorkerPool | None = None
        self._scan_task: asyncio.Task | None = None
        self.scan_summary: models.ScanSummary | None = None
        # Transient OAuth flow state (single-user, in-memory).
        self._oauth_state: dict[str, str] = {}

    # -- lifecycle ----------------------------------------------------------

    async def startup(self) -> None:
        self.settings.ensure_dirs()
        await self.db.connect()
        store = self.store
        self.run = await store.get_active_run()
        if self.run is None:
            self.run = await store.create_run(secrets.token_hex(8), DEFAULT_RUN_NAME)
        # Crash recovery before any worker starts.
        await recover(store, self.settings, self.run)
        # Launch the pool only if both providers are connected.
        if self.both_connected():
            await self._ensure_pool()

    async def shutdown(self) -> None:
        if self.pool is not None:
            await self.pool.stop()
        if self._scan_task is not None:
            self._scan_task.cancel()
        await self._http.aclose()
        await self.db.close()

    @property
    def store(self) -> Store:
        return Store(self.db)

    # -- auth ---------------------------------------------------------------

    def both_connected(self) -> bool:
        return self.auth.is_connected("dropbox") and self.auth.is_connected("google")

    def new_oauth_state(self, provider: str) -> str:
        state = secrets.token_urlsafe(24)
        self._oauth_state[provider] = state
        return state

    def check_oauth_state(self, provider: str, state: str) -> bool:
        return secrets.compare_digest(self._oauth_state.get(provider, ""), state)

    async def maybe_start_pool(self) -> None:
        if self.both_connected() and self.pool is None:
            await self._ensure_pool()

    async def _ensure_pool(self) -> None:
        assert self.run is not None
        self.pool = WorkerPool(
            settings=self.settings, store=self.store, dropbox=self.dropbox,
            drive=self.drive, run=self.run)
        await self.pool.start()

    # -- scanning -----------------------------------------------------------

    @property
    def is_scanning(self) -> bool:
        return self._scan_task is not None and not self._scan_task.done()

    async def start_scan(self) -> None:
        if self.is_scanning:
            return
        self._scan_task = asyncio.create_task(self._run_scan())

    async def _run_scan(self) -> None:
        assert self.run is not None
        self.scan_summary = await discovery.discover(
            self.dropbox, self.store, self.run)

    # -- folder browsing (hybrid: cached scan + lazy live fetch) ------------

    async def browse_folder(
        self, path_lower: str | None, *, refresh: bool = False
    ) -> list[models.FolderNode]:
        """List child folders of ``path_lower`` (None = top level).

        Serves cached scan data instantly. If a level has no cached children
        (or ``refresh`` is set) and both providers are connected, it fetches
        that one level live from Dropbox first, then returns the cached view.
        """
        assert self.run is not None
        store = self.store
        children = await store.list_child_folders(self.run.id, path_lower)
        should_fetch = refresh or (not children and self.auth.is_connected("dropbox"))
        if should_fetch and self.auth.is_connected("dropbox"):
            try:
                await discovery.discover_level(
                    self.dropbox, store, self.run, path_lower or "")
                children = await store.list_child_folders(self.run.id, path_lower)
            except Exception:  # noqa: BLE001 — browsing must not 500
                pass
        return children

    # -- batches ------------------------------------------------------------

    async def propose_batch(self) -> models.Batch | None:
        assert self.run is not None
        return await scheduler.propose_batch(self.store, self.settings, self.run)

    async def propose_batch_from_folders(
        self, path_lowers: list[str]
    ) -> tuple[models.Batch | None, dict[str, int]]:
        assert self.run is not None
        return await scheduler.propose_batch_from_folders(
            self.store, self.settings, self.run, path_lowers)

    async def plan_all_batches(self, path_lowers: list[str]) -> dict[str, int]:
        """Plan every batch needed to migrate the whole selection (no copying
        starts — the batches are proposed; use start_all to run them)."""
        assert self.run is not None
        return await scheduler.plan_all_batches_from_folders(
            self.store, self.settings, self.run, path_lowers)

    async def start_all(self) -> dict[str, int]:
        """Run every proposed batch (the 'Start all' action)."""
        assert self.run is not None
        summary = await self.store.start_all_proposed(self.run.id)
        if summary["batches"]:
            await self.maybe_start_pool()
            if self.pool:
                self.pool.resume()
        return summary

    async def approve_copy(self, batch_id: int) -> None:
        store = self.store
        await store.transition_batch(
            batch_id, BatchState.COPY_APPROVED,
            fields={"copy_approved_at": utcnow_iso()})
        await store.transition_batch(batch_id, BatchState.RUNNING, requeue_files=True)
        await self.maybe_start_pool()

    async def cancel_batch(self, batch_id: int) -> None:
        await self.store.transition_batch(batch_id, BatchState.CANCELLED)

    async def resume_batch(self, batch_id: int) -> dict[str, int]:
        """Retry a batch's incomplete files (failed/dead/queued stragglers)."""
        summary = await self.store.resume_batch(batch_id)
        if summary["requeued"]:
            await self.maybe_start_pool()
            if self.pool:  # ensure workers aren't paused
                self.pool.resume()
        return summary

    async def approve_delete(self, batch_id: int) -> dict[str, int]:
        """DISABLED in this copy-and-verify-only build.

        Source deletion is turned off (see DROPBOX_DELETION_ENABLED). This build
        never removes anything from Dropbox; the user deletes sources manually.
        """
        raise ValueError(
            "Source deletion is disabled in this build — it is copy-and-verify "
            "only. Files are verified on Drive; delete them from Dropbox yourself.")

    # -- controls -----------------------------------------------------------

    def pause(self) -> None:
        if self.pool:
            self.pool.pause()

    def resume(self) -> None:
        if self.pool:
            self.pool.resume()

    @property
    def is_paused(self) -> bool:
        return self.pool.is_paused if self.pool else False

    # -- reporting ----------------------------------------------------------

    async def quota_status(self) -> models.QuotaStatus:
        assert self.run is not None
        return await scheduler.quota_status(self.store, self.settings, self.run.id)

    async def state_counts(self) -> dict[str, int]:
        assert self.run is not None
        return await self.store.count_files_by_state(self.run.id)

    async def activity(self) -> dict:
        """A compact live-progress snapshot for the global status bar, scoped to
        the work the user has actually queued (files assigned to a batch).

        Includes a recent-throughput estimate and an ETA so the UI can show a
        progress bar and a "time remaining". ``active`` is True whenever there is
        in-flight or queued work.
        """
        assert self.run is not None
        c = await self.store.count_files_by_state(self.run.id)
        in_flight = (c.get("downloading", 0) + c.get("uploading", 0)
                     + c.get("verifying", 0) + c.get("downloaded", 0)
                     + c.get("uploaded", 0))
        queued = c.get("queued", 0)

        # Progress is measured over batched files (the work in motion), by bytes
        # so big files weigh correctly.
        prog = await self.store.batched_progress(self.run.id)
        rate = await self.store.recent_upload_rate(self.run.id, window_seconds=60)
        remaining_bytes = max(0, prog["total_bytes"] - prog["done_bytes"])
        eta_seconds = int(remaining_bytes / rate) if rate > 0 and remaining_bytes else None

        return {
            "scanning": self.is_scanning,
            "paused": self.is_paused,
            "queued": queued,
            "in_flight": in_flight,
            "downloading": c.get("downloading", 0),
            "uploading": c.get("uploading", 0),
            "verifying": c.get("verifying", 0),
            "verified": c.get("verified", 0),
            "done": c.get("done", 0),
            "failed": c.get("failed", 0),
            "dead": c.get("dead", 0),
            "discovered": c.get("discovered", 0),
            "active": self.is_scanning or in_flight > 0 or queued > 0,
            # batched-work progress (bytes + files)
            "total_files": prog["total_files"],
            "done_files": prog["done_files"],
            "total_bytes": prog["total_bytes"],
            "done_bytes": prog["done_bytes"],
            "pct": (round(100 * prog["done_bytes"] / prog["total_bytes"], 1)
                    if prog["total_bytes"] else 0.0),
            "rate_bps": rate,
            "eta_seconds": eta_seconds,
        }

    async def total_discovered_bytes(self) -> int:
        assert self.run is not None
        return await self.store.total_bytes(self.run.id)

    async def list_batches(self) -> list[models.Batch]:
        assert self.run is not None
        return await self.store.list_batches(self.run.id)

    async def reconcile(self) -> reconcile.ReconcileReport:
        assert self.run is not None
        return await reconcile.reconcile(self.drive, self.store, self.run)

    async def drive_quota(self) -> dict:
        return await self.drive.storage_quota()
