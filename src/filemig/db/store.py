"""The store: the single, validated writer for all migration state.

Every state transition flows through here so the safety invariant — a file is
delete-eligible only after both verification legs pass — is enforced in one
place. Each transition also writes an append-only ``events`` row (durable
audit) and publishes a lightweight event on the in-process bus (for SSE).
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from filemig.db.connection import Database
from filemig.domain import models
from filemig.domain.states import (
    BatchState,
    FileState,
    assert_batch_transition,
    assert_file_transition,
    recovery_target,
)
from filemig.util.clock import iso_to_epoch, utcnow_iso
from filemig.web.events import bus

_FILE_COLUMNS = [
    "id", "run_id", "batch_id", "dbx_id", "src_path_lower", "src_path_display",
    "parent_path_lower", "dbx_rev", "size", "is_downloadable", "export_format",
    "src_content_hash", "src_path_sha256", "is_shared", "local_dbx_hash",
    "local_md5", "local_sha256", "local_size", "export_hash", "dst_file_id",
    "dst_md5", "dst_sha256", "dst_size", "upload_session_uri", "upload_offset",
    "leg1_ok", "leg2_ok", "state", "prev_durable_state", "attempts",
    "last_error", "staged_path", "leased_by", "leased_until", "discovered_at",
    "updated_at", "verified_at", "deleted_at",
]


def _row_to_file(row: sqlite3.Row) -> models.FileRecord:
    d = dict(row)
    return models.FileRecord(
        id=d["id"], run_id=d["run_id"], batch_id=d["batch_id"], dbx_id=d["dbx_id"],
        src_path_lower=d["src_path_lower"], src_path_display=d["src_path_display"],
        parent_path_lower=d["parent_path_lower"], dbx_rev=d["dbx_rev"], size=d["size"],
        is_downloadable=bool(d["is_downloadable"]), export_format=d["export_format"],
        src_content_hash=d["src_content_hash"], src_path_sha256=d["src_path_sha256"],
        local_dbx_hash=d["local_dbx_hash"], local_md5=d["local_md5"],
        local_sha256=d["local_sha256"], local_size=d["local_size"],
        export_hash=d["export_hash"], dst_file_id=d["dst_file_id"], dst_md5=d["dst_md5"],
        dst_sha256=d["dst_sha256"], dst_size=d["dst_size"],
        upload_session_uri=d["upload_session_uri"], upload_offset=d["upload_offset"],
        leg1_ok=bool(d["leg1_ok"]), leg2_ok=bool(d["leg2_ok"]),
        state=FileState(d["state"]), prev_durable_state=d["prev_durable_state"],
        attempts=d["attempts"], last_error=d["last_error"], staged_path=d["staged_path"],
        leased_by=d["leased_by"], leased_until=d["leased_until"],
        discovered_at=d["discovered_at"], updated_at=d["updated_at"],
        verified_at=d["verified_at"], deleted_at=d["deleted_at"],
    )


def _row_to_batch(row: sqlite3.Row) -> models.Batch:
    d = dict(row)
    return models.Batch(
        id=d["id"], run_id=d["run_id"], seq=d["seq"], state=BatchState(d["state"]),
        file_count=d["file_count"], total_bytes=d["total_bytes"],
        copied_count=d["copied_count"], verified_count=d["verified_count"],
        failed_count=d["failed_count"], deleted_count=d["deleted_count"],
        proposed_at=d["proposed_at"], copy_approved_at=d["copy_approved_at"],
        delete_approved_at=d["delete_approved_at"], completed_at=d["completed_at"],
    )


class Store:
    def __init__(self, db: Database) -> None:
        self.db = db

    # -- runs ---------------------------------------------------------------

    async def create_run(self, run_uuid: str, dst_root_name: str) -> models.Run:
        ts = utcnow_iso()
        rid = await self.db.execute(
            "INSERT INTO runs(run_uuid, dst_root_name, status, created_at) "
            "VALUES(?,?,?,?)",
            (run_uuid, dst_root_name, "active", ts),
        )
        return models.Run(rid, run_uuid, None, dst_root_name, "active", ts)

    async def get_active_run(self) -> models.Run | None:
        row = await self.db.fetchone(
            "SELECT * FROM runs WHERE status != 'done' ORDER BY id DESC LIMIT 1"
        )
        if row is None:
            return None
        d = dict(row)
        return models.Run(d["id"], d["run_uuid"], d["dst_root_id"],
                          d["dst_root_name"], d["status"], d["created_at"])

    async def set_run_root(self, run_id: int, dst_root_id: str) -> None:
        await self.db.execute(
            "UPDATE runs SET dst_root_id=? WHERE id=?", (dst_root_id, run_id)
        )

    # -- folders ------------------------------------------------------------

    async def upsert_folder(self, run_id: int, f: models.DiscoveredFolder) -> None:
        await self.db.execute(
            "INSERT INTO folders(run_id, src_path_lower, src_path_display, "
            "parent_path_lower, depth, created_at) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(run_id, src_path_lower) DO UPDATE SET "
            "src_path_display=excluded.src_path_display, "
            "parent_path_lower=excluded.parent_path_lower, depth=excluded.depth",
            (run_id, f.path_lower, f.path_display, f.parent_path_lower, f.depth,
             utcnow_iso()),
        )

    async def set_folder_dst(self, run_id: int, src_path_lower: str, dst_id: str) -> None:
        await self.db.execute(
            "UPDATE folders SET dst_folder_id=? WHERE run_id=? AND src_path_lower=?",
            (dst_id, run_id, src_path_lower),
        )

    async def get_folder(self, run_id: int, src_path_lower: str) -> models.Folder | None:
        row = await self.db.fetchone(
            "SELECT * FROM folders WHERE run_id=? AND src_path_lower=?",
            (run_id, src_path_lower),
        )
        if row is None:
            return None
        d = dict(row)
        return models.Folder(d["id"], d["run_id"], d["src_path_lower"],
                             d["src_path_display"], d["parent_path_lower"],
                             d["dst_folder_id"], d["depth"], d["created_at"])

    async def list_child_folders(
        self, run_id: int, parent_path_lower: str | None
    ) -> list[models.FolderNode]:
        """Direct child folders of ``parent_path_lower`` (None = top level),
        each annotated with subtree file count + bytes and migration progress.

        Uses a path-prefix range scan over the indexed ``src_path_lower`` so a
        folder's whole-subtree totals are one query, not a recursive walk.
        """
        # Top level (parent_path_lower is None) means the children of the root,
        # whose stored parent_path_lower is the empty string. We never list the
        # root folder ("" / "/") itself as a selectable node. For a non-root
        # parent we match its exact path.
        effective_parent = "" if parent_path_lower is None else parent_path_lower
        rows = await self.db.fetchall(
            "SELECT src_path_lower, src_path_display, depth, dst_folder_id "
            "FROM folders WHERE run_id=? AND parent_path_lower = ? "
            "  AND src_path_lower != '' "
            "ORDER BY src_path_display",
            (run_id, effective_parent),
        )
        nodes: list[models.FolderNode] = []
        for r in rows:
            stats = await self._subtree_stats(run_id, r["src_path_lower"])
            has_children = await self._has_child_folders(run_id, r["src_path_lower"])
            nodes.append(models.FolderNode(
                src_path_lower=r["src_path_lower"],
                src_path_display=r["src_path_display"],
                depth=r["depth"],
                has_child_folders=has_children,
                dst_folder_id=r["dst_folder_id"],
                **stats,
            ))
        return nodes

    async def _has_child_folders(self, run_id: int, path_lower: str) -> bool:
        row = await self.db.fetchone(
            "SELECT 1 FROM folders WHERE run_id=? AND parent_path_lower=? LIMIT 1",
            (run_id, path_lower))
        return row is not None

    async def _subtree_stats(self, run_id: int, path_lower: str) -> dict[str, int]:
        """Whole-subtree file count, bytes, and migrated count for a folder.

        Matches files whose path is the folder itself or under it via a prefix
        range (``path/`` <= p < ``path0``), which the src_path_lower index serves
        efficiently.
        """
        prefix = path_lower + "/"
        hi = path_lower + "0"  # '0' is the byte right after '/'
        row = await self.db.fetchone(
            "SELECT COUNT(*) n, COALESCE(SUM(size),0) b, "
            # 'migrated' = safely on Drive. In copy-only mode files rest at
            # 'verified'; with deletion enabled they progress to 'done'. Both
            # mean the copy is complete and verified, so count them together.
            "  COALESCE(SUM(CASE WHEN state IN ('verified','done') THEN 1 ELSE 0 END),0) migrated, "
            "  COALESCE(SUM(CASE WHEN state='dead' THEN 1 ELSE 0 END),0) dead, "
            # 'in progress' = assigned to a batch but not yet verified/dead.
            "  COALESCE(SUM(CASE WHEN batch_id IS NOT NULL "
            "      AND state NOT IN ('verified','done','dead') THEN 1 ELSE 0 END),0) in_progress "
            "FROM files WHERE run_id=? AND src_path_lower >= ? AND src_path_lower < ?",
            (run_id, prefix, hi))
        return {"file_count": row["n"], "total_bytes": row["b"],
                "migrated_count": row["migrated"], "dead_count": row["dead"],
                "in_progress_count": row["in_progress"]}

    async def all_unbatched_discovered(self, run_id: int, limit: int = 1000000) -> list[int]:
        """Every still-``discovered`` file not yet assigned to a batch — i.e. all
        eligible work regardless of folder (catches root-level/orphaned files)."""
        rows = await self.db.fetchall(
            "SELECT id FROM files WHERE run_id=? AND batch_id IS NULL AND state=? "
            "ORDER BY src_path_lower LIMIT ?",
            (run_id, FileState.DISCOVERED.value, limit))
        return [r["id"] for r in rows]

    async def queue_subtree_files(
        self, run_id: int, path_lowers: list[str], limit: int = 100000
    ) -> list[int]:
        """File ids under any of the given folders that are unassigned and
        still ``discovered`` — i.e. eligible to be added to a new batch."""
        if not path_lowers:
            return []
        clauses = []
        params: list[object] = [run_id]
        for p in path_lowers:
            clauses.append("(src_path_lower >= ? AND src_path_lower < ?)")
            params.extend([p + "/", p + "0"])
        rows = await self.db.fetchall(
            f"SELECT id FROM files WHERE run_id=? AND batch_id IS NULL "
            f"AND state=? AND ({' OR '.join(clauses)}) ORDER BY src_path_lower LIMIT ?",
            (run_id, FileState.DISCOVERED.value, *params[1:], limit),
        )
        return [r["id"] for r in rows]

    # -- files: discovery ---------------------------------------------------

    async def upsert_discovered_file(
        self, run_id: int, f: models.DiscoveredFile, src_path_sha256: str
    ) -> None:
        """Insert a newly-discovered file. If it already exists with the same
        rev, leave its progress untouched (idempotent re-scan). If the rev
        changed, reset it to ``discovered`` so it is re-migrated."""
        ts = utcnow_iso()
        await self.db.execute(
            "INSERT INTO files(run_id, dbx_id, src_path_lower, src_path_display, "
            "parent_path_lower, dbx_rev, size, is_downloadable, export_format, "
            "src_content_hash, src_path_sha256, is_shared, state, discovered_at, "
            "updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(run_id, dbx_id) DO UPDATE SET "
            "  src_path_lower=excluded.src_path_lower, "
            "  src_path_display=excluded.src_path_display, "
            "  parent_path_lower=excluded.parent_path_lower, "
            "  size=excluded.size, is_downloadable=excluded.is_downloadable, "
            "  export_format=excluded.export_format, "
            "  src_content_hash=excluded.src_content_hash, "
            "  is_shared=excluded.is_shared, "
            "  state=CASE WHEN files.dbx_rev != excluded.dbx_rev "
            "             THEN 'discovered' ELSE files.state END, "
            "  dbx_rev=excluded.dbx_rev, updated_at=excluded.updated_at",
            (run_id, f.dbx_id, f.path_lower, f.path_display, f.parent_path_lower,
             f.rev, f.size, int(f.is_downloadable), f.export_format, f.content_hash,
             src_path_sha256, int(f.is_shared), FileState.DISCOVERED.value, ts, ts),
        )

    # -- files: reads -------------------------------------------------------

    async def get_file(self, file_id: int) -> models.FileRecord | None:
        row = await self.db.fetchone("SELECT * FROM files WHERE id=?", (file_id,))
        return _row_to_file(row) if row else None

    async def files_in_state(
        self, run_id: int, state: FileState, limit: int = 1000
    ) -> list[models.FileRecord]:
        rows = await self.db.fetchall(
            "SELECT * FROM files WHERE run_id=? AND state=? ORDER BY id LIMIT ?",
            (run_id, state.value, limit),
        )
        return [_row_to_file(r) for r in rows]

    async def files_in_batch(self, batch_id: int) -> list[models.FileRecord]:
        rows = await self.db.fetchall(
            "SELECT * FROM files WHERE batch_id=? ORDER BY src_path_lower", (batch_id,)
        )
        return [_row_to_file(r) for r in rows]

    async def count_files_by_state(self, run_id: int) -> dict[str, int]:
        rows = await self.db.fetchall(
            "SELECT state, COUNT(*) c FROM files WHERE run_id=? GROUP BY state",
            (run_id,),
        )
        return {r["state"]: r["c"] for r in rows}

    async def total_bytes(self, run_id: int) -> int:
        row = await self.db.fetchone(
            "SELECT COALESCE(SUM(size),0) b FROM files WHERE run_id=?", (run_id,))
        return row["b"] if row else 0

    async def batched_progress(self, run_id: int) -> dict[str, int]:
        """Progress across files assigned to a batch (the queued work), counting
        a file 'done' once it is verified or fully done. Bytes-weighted."""
        done_states = (FileState.VERIFIED.value, FileState.DONE.value)
        row = await self.db.fetchone(
            "SELECT COUNT(*) tf, COALESCE(SUM(size),0) tb, "
            "  COALESCE(SUM(CASE WHEN state IN (?,?) THEN 1 ELSE 0 END),0) df, "
            "  COALESCE(SUM(CASE WHEN state IN (?,?) THEN size ELSE 0 END),0) db "
            "FROM files WHERE run_id=? AND batch_id IS NOT NULL",
            (*done_states, *done_states, run_id),
        )
        return {"total_files": row["tf"], "total_bytes": row["tb"],
                "done_files": row["df"], "done_bytes": row["db"]}

    async def recent_upload_rate(self, run_id: int, window_seconds: int = 60) -> float:
        """Bytes/sec uploaded over the last ``window_seconds`` (0 if idle)."""
        cutoff = utcnow_iso_in(-window_seconds)
        rows = await self.db.fetchall(
            "SELECT bytes_uploaded, uploaded_at FROM upload_ledger "
            "WHERE run_id=? AND uploaded_at >= ? ORDER BY uploaded_at",
            (run_id, cutoff))
        if len(rows) < 1:
            return 0.0
        total = sum(r["bytes_uploaded"] for r in rows)
        first = iso_to_epoch(rows[0]["uploaded_at"])
        last = iso_to_epoch(rows[-1]["uploaded_at"])
        span = max(last - first, 1.0)  # avoid div-by-zero for a single sample
        return total / span

    # -- files: leasing -----------------------------------------------------

    async def claim_next_file(
        self, run_id: int, worker_id: str, lease_seconds: int = 1800,
        order: str = "fifo",
    ) -> models.FileRecord | None:
        """Atomically claim the next queued file belonging to a running batch.

        ``order`` selects which queued file to take next:
          * ``"fifo"``        — insertion order (default).
          * ``"largest"``     — biggest file first (prioritize big data; tiny
                                latency-bound files mop up afterward).
          * ``"smallest"``    — smallest first (quick early wins).

        Returns the claimed file moved into ``downloading`` (or ``None``). The
        claim, state transition, and event all happen in one transaction.
        """
        lease_until = utcnow_iso_in(lease_seconds)
        order_by = {
            "fifo": "f.id",
            "largest": "f.size DESC, f.id",
            "smallest": "f.size ASC, f.id",
        }.get(order, "f.id")

        def txn(conn: sqlite3.Connection) -> models.FileRecord | None:
            row = conn.execute(
                "SELECT f.* FROM files f JOIN batches b ON f.batch_id=b.id "
                "WHERE f.run_id=? AND f.state=? AND b.state='running' "
                "  AND (f.leased_until IS NULL OR f.leased_until < ?) "
                f"ORDER BY {order_by} LIMIT 1",
                (run_id, FileState.QUEUED.value, utcnow_iso()),
            ).fetchone()
            if row is None:
                return None
            fid = row["id"]
            conn.execute(
                "UPDATE files SET state=?, prev_durable_state=?, leased_by=?, "
                "leased_until=?, updated_at=? WHERE id=?",
                (FileState.DOWNLOADING.value, FileState.QUEUED.value, worker_id,
                 lease_until, utcnow_iso(), fid),
            )
            _insert_event(conn, run_id=run_id, file_id=fid, batch_id=row["batch_id"],
                          kind="state_change", frm=FileState.QUEUED.value,
                          to=FileState.DOWNLOADING.value, detail=None)
            return _row_to_file(conn.execute(
                "SELECT * FROM files WHERE id=?", (fid,)).fetchone())

        result = await self.db.transaction(txn)
        if result is not None:
            _publish(run_id, result.id, result.batch_id, "state_change",
                     FileState.QUEUED.value, FileState.DOWNLOADING.value)
        return result

    async def renew_lease(self, file_id: int, worker_id: str,
                          lease_seconds: int = 1800) -> None:
        await self.db.execute(
            "UPDATE files SET leased_until=? WHERE id=? AND leased_by=?",
            (utcnow_iso_in(lease_seconds), file_id, worker_id),
        )

    # -- files: transitions -------------------------------------------------

    async def transition_file(
        self, file_id: int, to: FileState, *, fields: dict[str, Any] | None = None,
        detail: dict[str, Any] | None = None, expect_worker: str | None = None,
    ) -> models.FileRecord:
        """Validate and apply a file state transition atomically with an event.

        ``fields`` updates extra columns in the same write. ``expect_worker``
        guards against a stale worker writing after its lease was reclaimed.
        """
        def txn(conn: sqlite3.Connection) -> models.FileRecord:
            row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
            if row is None:
                raise KeyError(f"file {file_id} not found")
            frm = FileState(row["state"])
            if expect_worker is not None and row["leased_by"] != expect_worker:
                raise PermissionError(
                    f"lease lost for file {file_id}: held by {row['leased_by']!r}"
                )
            assert_file_transition(frm, to)
            sets = dict(fields or {})
            sets["state"] = to.value
            sets["updated_at"] = utcnow_iso()
            # Track the last durable state for crash recovery.
            if to.value in {s.value for s in _DURABLE}:
                sets["prev_durable_state"] = to.value
            if to == FileState.VERIFIED:
                sets["verified_at"] = utcnow_iso()
            if to == FileState.DONE:
                sets["deleted_at"] = utcnow_iso()
            if to in (FileState.DONE, FileState.VERIFIED, FileState.DEAD):
                sets["leased_by"] = None
                sets["leased_until"] = None
            assignments = ", ".join(f"{k}=?" for k in sets)
            conn.execute(
                f"UPDATE files SET {assignments} WHERE id=?",
                (*sets.values(), file_id),
            )
            _insert_event(conn, run_id=row["run_id"], file_id=file_id,
                          batch_id=row["batch_id"], kind="state_change",
                          frm=frm.value, to=to.value,
                          detail=json.dumps(detail) if detail else None)
            return _row_to_file(conn.execute(
                "SELECT * FROM files WHERE id=?", (file_id,)).fetchone())

        result = await self.db.transaction(txn)
        _publish(result.run_id, result.id, result.batch_id, "state_change",
                 None, to.value, detail)
        return result

    async def update_file_fields(self, file_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utcnow_iso()
        assignments = ", ".join(f"{k}=?" for k in fields)
        await self.db.execute(
            f"UPDATE files SET {assignments} WHERE id=?",
            (*fields.values(), file_id),
        )

    async def mark_dead(self, file_id: int, error: str) -> models.FileRecord:
        """Escalate a file to ``dead`` (non-retryable). Never auto-deleted.

        This is an error path, so it bypasses transition validation — a fatal
        error can occur in any work state and must always land in ``dead``.
        """
        def txn(conn: sqlite3.Connection) -> models.FileRecord:
            row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
            if row is None:
                raise KeyError(f"file {file_id} not found")
            conn.execute(
                "UPDATE files SET state=?, last_error=?, leased_by=NULL, "
                "leased_until=NULL, updated_at=? WHERE id=?",
                (FileState.DEAD.value, error[:2000], utcnow_iso(), file_id))
            _insert_event(conn, run_id=row["run_id"], file_id=file_id,
                          batch_id=row["batch_id"], kind="error",
                          frm=row["state"], to=FileState.DEAD.value,
                          detail=json.dumps({"error": error[:500]}))
            return _row_to_file(conn.execute(
                "SELECT * FROM files WHERE id=?", (file_id,)).fetchone())

        result = await self.db.transaction(txn)
        _publish(result.run_id, file_id, result.batch_id, "error", None,
                 FileState.DEAD.value, {"error": error[:500]})
        return result

    async def revert_delete_to_verified(
        self, file_id: int, error: str
    ) -> models.FileRecord:
        """A source deletion did not succeed; the file is still verified on the
        destination, so return it to ``verified`` and record why the source was
        not removed. The file remains delete-eligible for a later retry."""
        return await self.transition_file(
            file_id, FileState.VERIFIED, fields={"last_error": error[:2000]},
            detail={"delete_error": error[:500]})

    async def requeue_file(self, file_id: int) -> models.FileRecord:
        """Release the lease and return a file to the work queue (error path,
        bypasses validation)."""
        def txn(conn: sqlite3.Connection) -> models.FileRecord:
            row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
            if row is None:
                raise KeyError(f"file {file_id} not found")
            conn.execute(
                "UPDATE files SET state=?, leased_by=NULL, leased_until=NULL, "
                "updated_at=? WHERE id=?",
                (FileState.QUEUED.value, utcnow_iso(), file_id))
            return _row_to_file(conn.execute(
                "SELECT * FROM files WHERE id=?", (file_id,)).fetchone())

        return await self.db.transaction(txn)

    async def record_failure(
        self, file_id: int, error: str, max_attempts: int
    ) -> models.FileRecord:
        """Increment attempts; re-queue if budget remains, else escalate to dead."""
        def txn(conn: sqlite3.Connection) -> models.FileRecord:
            row = conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()
            if row is None:
                raise KeyError(f"file {file_id} not found")
            attempts = row["attempts"] + 1
            frm = FileState(row["state"])
            # Retry while budget remains, else escalate to dead (never deleted).
            to = FileState.QUEUED if attempts < max_attempts else FileState.DEAD
            conn.execute(
                "UPDATE files SET state=?, attempts=?, last_error=?, leased_by=NULL, "
                "leased_until=NULL, updated_at=? WHERE id=?",
                (to.value, attempts, error[:2000], utcnow_iso(), file_id),
            )
            _insert_event(conn, run_id=row["run_id"], file_id=file_id,
                          batch_id=row["batch_id"], kind="error", frm=frm.value,
                          to=to.value, detail=json.dumps({"error": error[:500],
                                                          "attempts": attempts}))
            return _row_to_file(conn.execute(
                "SELECT * FROM files WHERE id=?", (file_id,)).fetchone())

        result = await self.db.transaction(txn)
        _publish(result.run_id, result.id, result.batch_id, "error", None,
                 result.state.value, {"error": error[:500]})
        return result

    # -- recovery -----------------------------------------------------------

    async def recover_interrupted(self, run_id: int) -> int:
        """Reset transient/expired-lease rows back to their recovery target.

        Run once on startup before workers launch. Returns the count reset.
        """
        def txn(conn: sqlite3.Connection) -> int:
            # Reclaim any mid-pipeline row (transient OR the durable
            # downloaded/uploaded/verifying states the claim query won't pick
            # up) and any row whose lease expired. Rewind to queued; the
            # resume-aware worker skips already-complete phases using the
            # progress fields preserved on the row.
            in_progress = tuple(s.value for s in _IN_PROGRESS)
            placeholders = ",".join("?" for _ in in_progress)
            rows = conn.execute(
                f"SELECT * FROM files WHERE run_id=? AND ("
                f"  state IN ({placeholders}) OR "
                f"  (leased_until IS NOT NULL AND leased_until < ?))",
                (run_id, *in_progress, utcnow_iso()),
            ).fetchall()
            count = 0
            for row in rows:
                # recovery_target keeps a verified/done resting point (so a
                # crashed DELETING row returns to VERIFIED, not back through the
                # whole copy pipeline) and rewinds everything else to queued.
                target = recovery_target(row["prev_durable_state"])
                conn.execute(
                    "UPDATE files SET state=?, leased_by=NULL, leased_until=NULL, "
                    "updated_at=? WHERE id=?",
                    (target.value, utcnow_iso(), row["id"]),
                )
                _insert_event(conn, run_id=run_id, file_id=row["id"],
                              batch_id=row["batch_id"], kind="recovery",
                              frm=row["state"], to=target.value, detail=None)
                count += 1
            return count

        return await self.db.transaction(txn)

    async def staged_paths_to_keep(self, run_id: int) -> set[str]:
        """Staged files worth keeping across a restart: rows that completed
        download (Leg 1 passed) but have not yet been verified."""
        rows = await self.db.fetchall(
            "SELECT staged_path FROM files WHERE run_id=? AND staged_path IS NOT NULL "
            "AND leg1_ok=1 AND state NOT IN (?,?,?)",
            (run_id, FileState.VERIFIED.value, FileState.DONE.value,
             FileState.DEAD.value),
        )
        return {r["staged_path"] for r in rows if r["staged_path"]}

    # -- batches ------------------------------------------------------------

    async def create_batch(
        self, run_id: int, file_ids: list[int], total_bytes: int
    ) -> models.Batch:
        def txn(conn: sqlite3.Connection) -> models.Batch:
            seq_row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) + 1 AS s FROM batches WHERE run_id=?",
                (run_id,),
            ).fetchone()
            seq = seq_row["s"]
            ts = utcnow_iso()
            cur = conn.execute(
                "INSERT INTO batches(run_id, seq, state, file_count, total_bytes, "
                "proposed_at) VALUES(?,?,?,?,?,?)",
                (run_id, seq, BatchState.PROPOSED.value, len(file_ids), total_bytes, ts),
            )
            bid = int(cur.lastrowid)
            if file_ids:
                conn.executemany(
                    "UPDATE files SET batch_id=?, updated_at=? WHERE id=? AND batch_id IS NULL",
                    [(bid, ts, fid) for fid in file_ids],
                )
            return _row_to_batch(conn.execute(
                "SELECT * FROM batches WHERE id=?", (bid,)).fetchone())

        result = await self.db.transaction(txn)
        _publish(run_id, None, result.id, "batch_proposed", None,
                 BatchState.PROPOSED.value)
        return result

    async def get_batch(self, batch_id: int) -> models.Batch | None:
        row = await self.db.fetchone("SELECT * FROM batches WHERE id=?", (batch_id,))
        return _row_to_batch(row) if row else None

    async def list_batches(self, run_id: int) -> list[models.Batch]:
        rows = await self.db.fetchall(
            "SELECT * FROM batches WHERE run_id=? ORDER BY seq", (run_id,)
        )
        return [_row_to_batch(r) for r in rows]

    async def transition_batch(
        self, batch_id: int, to: BatchState, *, fields: dict[str, Any] | None = None,
        requeue_files: bool = False,
    ) -> models.Batch:
        def txn(conn: sqlite3.Connection) -> models.Batch:
            row = conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise KeyError(f"batch {batch_id} not found")
            frm = BatchState(row["state"])
            if frm != to:
                assert_batch_transition(frm, to)
            sets = dict(fields or {})
            sets["state"] = to.value
            assignments = ", ".join(f"{k}=?" for k in sets)
            conn.execute(f"UPDATE batches SET {assignments} WHERE id=?",
                         (*sets.values(), batch_id))
            if requeue_files:
                # Move this batch's discovered files into the work queue.
                conn.execute(
                    "UPDATE files SET state=?, prev_durable_state=?, updated_at=? "
                    "WHERE batch_id=? AND state IN (?,?)",
                    (FileState.QUEUED.value, FileState.QUEUED.value, utcnow_iso(),
                     batch_id, FileState.DISCOVERED.value, FileState.FAILED.value),
                )
            _insert_event(conn, run_id=row["run_id"], file_id=None,
                          batch_id=batch_id, kind="batch_state_change",
                          frm=frm.value, to=to.value, detail=None)
            return _row_to_batch(conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone())

        result = await self.db.transaction(txn)
        _publish(result.run_id, None, batch_id, "batch_state_change", None, to.value)
        return result

    async def start_all_proposed(self, run_id: int) -> dict[str, int]:
        """Approve + run every PROPOSED batch (the 'Start all' action).

        Moves each proposed batch to running and re-queues its discovered files
        so workers pick them up. Returns how many batches/files were started.
        """
        def txn(conn: sqlite3.Connection) -> dict[str, int]:
            rows = conn.execute(
                "SELECT id FROM batches WHERE run_id=? AND state=? ORDER BY seq",
                (run_id, BatchState.PROPOSED.value)).fetchall()
            ts = utcnow_iso()
            files = 0
            for r in rows:
                bid = r["id"]
                conn.execute(
                    "UPDATE batches SET state=?, copy_approved_at=? WHERE id=?",
                    (BatchState.RUNNING.value, ts, bid))
                cur = conn.execute(
                    "UPDATE files SET state=?, prev_durable_state=?, updated_at=? "
                    "WHERE batch_id=? AND state IN (?,?)",
                    (FileState.QUEUED.value, FileState.QUEUED.value, ts, bid,
                     FileState.DISCOVERED.value, FileState.FAILED.value))
                files += cur.rowcount
                _insert_event(conn, run_id=run_id, file_id=None, batch_id=bid,
                              kind="batch_state_change",
                              frm=BatchState.PROPOSED.value,
                              to=BatchState.RUNNING.value, detail=None)
            return {"batches": len(rows), "files": files}

        result = await self.db.transaction(txn)
        _publish(run_id, None, None, "batches_started", None, "running")
        return result

    async def skip_files_matching(
        self, run_id: int, patterns: list[str]
    ) -> dict[str, int]:
        """Mark not-yet-started files whose path matches any LIKE pattern as
        ``skipped`` (a deliberate, terminal exclusion — not a failure).

        Only touches files that haven't begun transferring (queued/discovered).
        In-flight and already-verified files are left alone, so this is safe to
        run while the transfer is live. Returns counts + bytes skipped, and
        advances any batch that is now fully settled.
        """
        if not patterns:
            return {"skipped": 0, "skipped_bytes": 0}
        like = " OR ".join("src_path_lower LIKE ?" for _ in patterns)

        def txn(conn: sqlite3.Connection) -> dict[str, int]:
            sel = conn.execute(
                f"SELECT COUNT(*) c, COALESCE(SUM(size),0) b FROM files "
                f"WHERE run_id=? AND state IN (?,?) AND ({like})",
                (run_id, FileState.QUEUED.value, FileState.DISCOVERED.value,
                 *patterns)).fetchone()
            n, nbytes = sel["c"], sel["b"]
            if n:
                conn.execute(
                    f"UPDATE files SET state=?, last_error='skipped by user', "
                    f"leased_by=NULL, leased_until=NULL, updated_at=? "
                    f"WHERE run_id=? AND state IN (?,?) AND ({like})",
                    (FileState.SKIPPED.value, utcnow_iso(), run_id,
                     FileState.QUEUED.value, FileState.DISCOVERED.value, *patterns))
                # Advance any batch that is now fully settled.
                for br in conn.execute(
                    "SELECT id, state, run_id FROM batches WHERE run_id=? AND state=?",
                    (run_id, BatchState.RUNNING.value)).fetchall():
                    _maybe_settle_batch(conn, br["id"], br["run_id"])
            return {"skipped": n, "skipped_bytes": nbytes}

        result = await self.db.transaction(txn)
        _publish(run_id, None, None, "files_skipped", None, None,
                 {"skipped": result["skipped"]})
        return result

    async def resume_batch(self, batch_id: int) -> dict[str, int]:
        """Re-activate a batch to retry its incomplete files.

        Re-queues every file in the batch that is not verified/done — including
        ``dead`` (failed verification/fatal), ``failed`` (retryable), and any
        stragglers still ``discovered`` — clearing stale leases, error text, and
        any stale resumable-upload session so they restart cleanly. Sets the
        batch back to ``running`` so workers pick them up. Returns a summary.
        """
        def txn(conn: sqlite3.Connection) -> dict[str, int]:
            row = conn.execute("SELECT * FROM batches WHERE id=?",
                               (batch_id,)).fetchone()
            if row is None:
                raise KeyError(f"batch {batch_id} not found")
            retryable = (FileState.DEAD.value, FileState.FAILED.value,
                         FileState.DISCOVERED.value)
            placeholders = ",".join("?" for _ in retryable)
            cur = conn.execute(
                f"UPDATE files SET state=?, prev_durable_state=?, attempts=0, "
                f"last_error=NULL, leased_by=NULL, leased_until=NULL, "
                f"upload_session_uri=NULL, upload_offset=0, updated_at=? "
                f"WHERE batch_id=? AND state IN ({placeholders})",
                (FileState.QUEUED.value, FileState.QUEUED.value, utcnow_iso(),
                 batch_id, *retryable),
            )
            requeued = cur.rowcount
            frm = BatchState(row["state"])
            # Only re-run if there is actually something to do.
            if requeued > 0 and frm != BatchState.RUNNING:
                assert_batch_transition(frm, BatchState.RUNNING)
                conn.execute("UPDATE batches SET state=? WHERE id=?",
                             (BatchState.RUNNING.value, batch_id))
                _insert_event(conn, run_id=row["run_id"], file_id=None,
                              batch_id=batch_id, kind="batch_state_change",
                              frm=frm.value, to=BatchState.RUNNING.value,
                              detail=None)
            return {"requeued": requeued, "run_id": row["run_id"]}

        result = await self.db.transaction(txn)
        _publish(result["run_id"], None, batch_id, "batch_resumed", None, "running")
        return {"requeued": result["requeued"]}

    async def refresh_batch_counters(self, batch_id: int) -> models.Batch:
        """Recompute a batch's counters from its files and advance its state
        when copy/verify milestones are reached."""
        def txn(conn: sqlite3.Connection) -> models.Batch:
            row = conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
            if row is None:
                raise KeyError(f"batch {batch_id} not found")
            _recompute_batch(conn, batch_id, row["run_id"])
            return _row_to_batch(conn.execute(
                "SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone())

        return await self.db.transaction(txn)

    # -- quota / ledger -----------------------------------------------------

    async def record_upload(self, run_id: int, file_id: int, n_bytes: int) -> None:
        await self.db.execute(
            "INSERT INTO upload_ledger(run_id, file_id, bytes_uploaded, uploaded_at) "
            "VALUES(?,?,?,?)",
            (run_id, file_id, n_bytes, utcnow_iso()),
        )

    async def quota_used_24h(self, run_id: int) -> tuple[int, float | None]:
        """Return (bytes uploaded in the last 24h, epoch when the window frees)."""
        cutoff = utcnow_iso_in(-86400)
        rows = await self.db.fetchall(
            "SELECT bytes_uploaded, uploaded_at FROM upload_ledger "
            "WHERE run_id=? AND uploaded_at >= ? ORDER BY uploaded_at",
            (run_id, cutoff),
        )
        used = sum(r["bytes_uploaded"] for r in rows)
        reset = iso_to_epoch(rows[0]["uploaded_at"]) + 86400 if rows else None
        return used, reset

    # -- folders pending ----------------------------------------------------

    async def folders_pending_creation(self, run_id: int) -> list[models.Folder]:
        rows = await self.db.fetchall(
            "SELECT * FROM folders WHERE run_id=? AND dst_folder_id IS NULL "
            "ORDER BY depth, src_path_lower",
            (run_id,),
        )
        return [models.Folder(r["id"], r["run_id"], r["src_path_lower"],
                              r["src_path_display"], r["parent_path_lower"],
                              r["dst_folder_id"], r["depth"], r["created_at"])
                for r in rows]


# ---- module-level helpers (run inside the txn thread) --------------------

from datetime import timedelta  # noqa: E402

from filemig.domain.states import DURABLE_STATES as _DURABLE  # noqa: E402
from filemig.domain.states import IN_PROGRESS_STATES as _IN_PROGRESS  # noqa: E402


def utcnow_iso_in(seconds: float) -> str:
    from datetime import datetime, timezone
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _recompute_batch(conn: sqlite3.Connection, batch_id: int, run_id: int) -> None:
    """Recompute a batch's counters and advance running -> awaiting-delete once
    every file has settled (verified, dead, or skipped). Runs inside a txn."""
    counts = {r["state"]: r["c"] for r in conn.execute(
        "SELECT state, COUNT(*) c FROM files WHERE batch_id=? GROUP BY state",
        (batch_id,)).fetchall()}
    total = sum(counts.values())
    done = counts.get(FileState.DONE.value, 0)
    verified = counts.get(FileState.VERIFIED.value, 0) + done
    dead = counts.get(FileState.DEAD.value, 0)
    skipped = counts.get(FileState.SKIPPED.value, 0)
    uploaded = (counts.get(FileState.UPLOADED.value, 0)
                + counts.get(FileState.VERIFYING.value, 0) + verified)
    settled = verified + dead + skipped
    conn.execute(
        "UPDATE batches SET copied_count=?, verified_count=?, failed_count=?, "
        "deleted_count=? WHERE id=?",
        (uploaded, verified, dead, done, batch_id))
    state_row = conn.execute("SELECT state FROM batches WHERE id=?",
                             (batch_id,)).fetchone()
    if state_row and state_row["state"] == BatchState.RUNNING.value \
            and total > 0 and settled >= total:
        conn.execute("UPDATE batches SET state=? WHERE id=?",
                     (BatchState.AWAITING_DELETE_APPROVAL.value, batch_id))
        _insert_event(conn, run_id=run_id, file_id=None, batch_id=batch_id,
                      kind="batch_state_change", frm=BatchState.RUNNING.value,
                      to=BatchState.AWAITING_DELETE_APPROVAL.value, detail=None)


def _maybe_settle_batch(conn: sqlite3.Connection, batch_id: int, run_id: int) -> None:
    """Alias used by bulk operations (skip) to advance a batch if now settled."""
    _recompute_batch(conn, batch_id, run_id)


def _insert_event(conn: sqlite3.Connection, *, run_id: int | None,
                  file_id: int | None, batch_id: int | None, kind: str,
                  frm: str | None, to: str | None, detail: str | None) -> None:
    conn.execute(
        "INSERT INTO events(run_id, file_id, batch_id, kind, from_state, to_state, "
        "detail, created_at) VALUES(?,?,?,?,?,?,?,?)",
        (run_id, file_id, batch_id, kind, frm, to, detail, utcnow_iso()),
    )


def _publish(run_id: int | None, file_id: int | None, batch_id: int | None,
             kind: str, frm: str | None, to: str | None,
             detail: dict[str, Any] | None = None) -> None:
    bus.publish({
        "run_id": run_id, "file_id": file_id, "batch_id": batch_id,
        "kind": kind, "from": frm, "to": to, "detail": detail,
        "ts": utcnow_iso(),
    })
