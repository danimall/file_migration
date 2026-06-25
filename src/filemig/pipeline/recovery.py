"""Startup crash recovery.

Run once during application startup, BEFORE the worker pool launches. It resets
any file left in a transient state (downloading/uploading/verifying/deleting) or
holding an expired lease back to a safe, durable state so a worker can pick it
up cleanly. Mid-file resumable uploads survive because the Drive session URI and
confirmed offset are persisted on the row; recovery rewinds the *state*, not the
upload progress.
"""

from __future__ import annotations

import os
from pathlib import Path

from filemig.config import Settings
from filemig.db.store import Store
from filemig.domain import models
from filemig.web.events import bus


async def recover(store: Store, settings: Settings, run: models.Run) -> int:
    """Reset interrupted rows and sweep orphaned staging files. Returns count.

    Rows that already reached ``downloaded`` (Leg 1 passed, ``local_dbx_hash``
    set) keep their staged file so the worker can resume the *upload* rather
    than re-download. Only staging files with no such owning row are swept.
    """
    count = await store.recover_interrupted(run.id)
    await _sweep_orphan_staging(store, settings, run)
    if count:
        bus.publish({"kind": "recovery", "detail": {"reset": count}})
    return count


async def _sweep_orphan_staging(store: Store, settings: Settings, run: models.Run) -> None:
    staging = Path(settings.resolved_staging_dir)
    if not staging.exists():
        return
    keep = await store.staged_paths_to_keep(run.id)
    for entry in staging.glob("*.part"):
        if str(entry) in keep:
            continue
        try:
            os.remove(entry)
        except OSError:
            pass
