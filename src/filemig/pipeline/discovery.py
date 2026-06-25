"""Dropbox discovery: walk the account and upsert files + the folder tree.

We enumerate the whole account recursively, derive the folder tree from the
file/folder entries, and upsert everything into the store. Discovery is
idempotent: re-running picks up new/changed files (changed ``rev`` resets a
file for re-migration) without disturbing in-flight progress.

We also surface a :class:`ScanSummary` so the dashboard can warn about cloud
docs (which take the export path), folders deeper than Drive's 100-level limit,
and path-case collisions (Dropbox ``path_lower`` is case-folded; two files
differing only by case would collide on the destination).
"""

from __future__ import annotations

from filemig.clients.dropbox_client import DropboxClient, to_discovered
from filemig.db.store import Store
from filemig.domain import models
from filemig.util.identity import src_path_sha256

# Drive rejects My Drive hierarchies deeper than 100 levels.
DRIVE_MAX_DEPTH = 100


def _depth(path_lower: str) -> int:
    p = path_lower.strip("/")
    return 0 if not p else p.count("/") + 1


def _parent(path_lower: str) -> str | None:
    if not path_lower or path_lower == "":
        return None
    return path_lower.rsplit("/", 1)[0] if "/" in path_lower else ""


async def discover_level(
    client: DropboxClient, store: Store, run: models.Run, path_lower: str
) -> int:
    """Lazily enumerate ONE folder level (non-recursive) and upsert it.

    Used by the hybrid folder browser: expanding a folder fetches just its
    direct children from Dropbox so newly-created folders appear without a full
    recursive re-scan. Returns the number of files discovered at this level.

    ``path_lower`` is the Dropbox path of the folder to list ("" for the root).
    We pass the path through verbatim; Dropbox's list_folder uses "" for root.
    """
    new_files = 0
    async for entry in client.list_folder(path=path_lower, recursive=False):
        tag = entry.get(".tag")
        if tag == "folder":
            fp = entry["path_lower"]
            await store.upsert_folder(run.id, models.DiscoveredFolder(
                fp, entry["path_display"], _parent(fp), _depth(fp)))
        elif tag == "file":
            df = to_discovered(entry)
            if df is None:
                continue
            await store.upsert_discovered_file(
                run.id, df, src_path_sha256(run.run_uuid, df.path_lower))
            new_files += 1
    return new_files


async def discover(
    client: DropboxClient, store: Store, run: models.Run
) -> models.ScanSummary:
    summary = models.ScanSummary()
    seen_display: dict[str, str] = {}  # path_lower -> first display seen (collision check)
    folders_emitted: set[str] = {""}
    # Ensure the root folder exists in the map.
    await store.upsert_folder(run.id, models.DiscoveredFolder("", "/", None, 0))

    async for entry in client.list_folder(path="", recursive=True):
        tag = entry.get(".tag")
        if tag == "folder":
            path_lower = entry["path_lower"]
            await _emit_folder(store, run, path_lower, entry["path_display"],
                               folders_emitted, summary)
            continue
        if tag != "file":
            continue

        df = to_discovered(entry)
        if df is None:
            continue

        # Collision check: same case-folded path, different display name.
        prev = seen_display.get(df.path_lower)
        if prev is not None and prev != df.path_display:
            summary.path_collisions.append(df.path_display)
        seen_display[df.path_lower] = df.path_display

        # Make sure the file's parent folder chain exists in the map.
        await _ensure_parents(store, run, df.parent_path_lower, folders_emitted, summary)

        path_sha = src_path_sha256(run.run_uuid, df.path_lower)
        await store.upsert_discovered_file(run.id, df, path_sha)

        summary.total_files += 1
        summary.total_bytes += df.size
        if not df.is_downloadable:
            summary.cloud_doc_count += 1

    return summary


async def _ensure_parents(
    store: Store, run: models.Run, path_lower: str,
    emitted: set[str], summary: models.ScanSummary,
) -> None:
    """Walk up from a file's parent, emitting any missing folder rows."""
    chain: list[str] = []
    cur: str | None = path_lower
    while cur is not None and cur not in emitted:
        chain.append(cur)
        cur = _parent(cur)
    for p in reversed(chain):
        await _emit_folder(store, run, p, p or "/", emitted, summary)


async def _emit_folder(
    store: Store, run: models.Run, path_lower: str, path_display: str,
    emitted: set[str], summary: models.ScanSummary,
) -> None:
    if path_lower in emitted:
        return
    depth = _depth(path_lower)
    if depth > DRIVE_MAX_DEPTH:
        summary.deep_folder_count += 1
    await store.upsert_folder(
        run.id,
        models.DiscoveredFolder(path_lower, path_display, _parent(path_lower), depth),
    )
    emitted.add(path_lower)
