"""Create the Google Drive folder tree mirroring the Dropbox hierarchy.

Folders are created depth-first so a child is only created after its parent has
a Drive id. The mapping ``src_path_lower -> dst_folder_id`` is cached in the
``folders`` table, so this is idempotent and resumable: a re-run only creates
folders still missing a ``dst_folder_id``.

The run's root folder (an app-created folder named after the migration) is the
parent of every top-level Dropbox folder, which keeps everything under
``drive.file`` scope (the app owns the whole subtree it creates).
"""

from __future__ import annotations

import asyncio

from filemig.clients.drive_client import DriveClient
from filemig.db.store import Store
from filemig.domain import models


class FolderManager:
    """Resolves and caches destination folder ids, creating them on demand."""

    def __init__(self, drive: DriveClient, store: Store, run: models.Run) -> None:
        self._drive = drive
        self._store = store
        self._run = run
        self._cache: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def ensure_root(self) -> str:
        """Create the app-owned root folder for this run if needed."""
        if self._run.dst_root_id:
            self._cache[""] = self._run.dst_root_id
            return self._run.dst_root_id
        async with self._lock:
            if self._run.dst_root_id:
                return self._run.dst_root_id
            folder = await self._store.get_folder(self._run.id, "")
            if folder and folder.dst_folder_id:
                self._run.dst_root_id = folder.dst_folder_id
                self._cache[""] = folder.dst_folder_id
                return folder.dst_folder_id
            root_id = await self._drive.create_folder(self._run.dst_root_name, None)
            await self._store.set_run_root(self._run.id, root_id)
            await self._store.set_folder_dst(self._run.id, "", root_id)
            self._run.dst_root_id = root_id
            self._cache[""] = root_id
            return root_id

    async def resolve(self, src_path_lower: str) -> str:
        """Return the Drive folder id for a source path, creating ancestors."""
        if src_path_lower in self._cache:
            return self._cache[src_path_lower]
        async with self._lock:
            return await self._resolve_locked(src_path_lower)

    async def _resolve_locked(self, src_path_lower: str) -> str:
        if src_path_lower in self._cache:
            return self._cache[src_path_lower]
        if src_path_lower == "":
            return await self.ensure_root()

        folder = await self._store.get_folder(self._run.id, src_path_lower)
        if folder and folder.dst_folder_id:
            self._cache[src_path_lower] = folder.dst_folder_id
            return folder.dst_folder_id

        parent_path = (src_path_lower.rsplit("/", 1)[0]
                       if "/" in src_path_lower else "")
        parent_id = await self._resolve_locked(parent_path)
        name = src_path_lower.rsplit("/", 1)[-1]
        # Use the display name from the folder row when available.
        if folder is not None:
            name = folder.src_path_display.rsplit("/", 1)[-1]
        dst_id = await self._drive.create_folder(name, parent_id)
        await self._store.set_folder_dst(self._run.id, src_path_lower, dst_id)
        self._cache[src_path_lower] = dst_id
        return dst_id
