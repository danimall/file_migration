"""Typed domain models.

These mirror the SQLite rows (``db/schema.sql``) but are plain dataclasses so
the pipeline and web layers never touch raw tuples. The store converts rows
<-> models in one place.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from filemig.domain.states import BatchState, FileState


@dataclass(slots=True)
class Run:
    id: int
    run_uuid: str
    dst_root_id: str | None
    dst_root_name: str
    status: str
    created_at: str


@dataclass(slots=True)
class Folder:
    id: int
    run_id: int
    src_path_lower: str
    src_path_display: str
    parent_path_lower: str | None
    dst_folder_id: str | None
    depth: int
    created_at: str


@dataclass(slots=True)
class FolderNode:
    """A folder in the browse tree, annotated with whole-subtree stats."""

    src_path_lower: str
    src_path_display: str
    depth: int
    has_child_folders: bool
    dst_folder_id: str | None
    file_count: int
    total_bytes: int
    migrated_count: int  # verified or done — safely on Drive
    dead_count: int      # failed verification / fatal — never copied
    in_progress_count: int  # batched but not yet verified

    @property
    def name(self) -> str:
        return self.src_path_display.rsplit("/", 1)[-1] or self.src_path_display

    @property
    def fully_migrated(self) -> bool:
        """Every file in the subtree is safely on Drive."""
        return self.file_count > 0 and self.migrated_count == self.file_count

    @property
    def partially_migrated(self) -> bool:
        return 0 < self.migrated_count < self.file_count

    @property
    def remaining_count(self) -> int:
        """Files not yet on Drive (neither migrated nor dead)."""
        return self.file_count - self.migrated_count - self.dead_count


@dataclass(slots=True)
class FileRecord:
    id: int
    run_id: int
    batch_id: int | None
    # Dropbox identity / metadata
    dbx_id: str
    src_path_lower: str
    src_path_display: str
    parent_path_lower: str
    dbx_rev: str
    size: int
    is_downloadable: bool
    export_format: str | None
    src_content_hash: str | None
    src_path_sha256: str
    # Locally computed (single streaming pass)
    local_dbx_hash: str | None
    local_md5: str | None
    local_sha256: str | None
    local_size: int | None
    export_hash: str | None
    # Drive side
    dst_file_id: str | None
    dst_md5: str | None
    dst_sha256: str | None
    dst_size: int | None
    upload_session_uri: str | None
    upload_offset: int
    # Verification
    leg1_ok: bool
    leg2_ok: bool
    # State machine
    state: FileState
    prev_durable_state: str | None
    attempts: int
    last_error: str | None
    staged_path: str | None
    leased_by: str | None
    leased_until: str | None
    # Timestamps
    discovered_at: str
    updated_at: str
    verified_at: str | None
    deleted_at: str | None

    @property
    def is_shared(self) -> bool:
        # Heuristic placeholder; populated by discovery via sharing_info.
        return False


@dataclass(slots=True)
class Batch:
    id: int
    run_id: int
    seq: int
    state: BatchState
    file_count: int
    total_bytes: int
    copied_count: int
    verified_count: int
    failed_count: int
    deleted_count: int
    proposed_at: str
    copy_approved_at: str | None
    delete_approved_at: str | None
    completed_at: str | None


@dataclass(slots=True)
class Event:
    id: int
    run_id: int | None
    file_id: int | None
    batch_id: int | None
    kind: str
    from_state: str | None
    to_state: str | None
    detail: str | None
    created_at: str


@dataclass(slots=True)
class DiscoveredFile:
    """A file entry produced by Dropbox discovery, before it becomes a row."""

    dbx_id: str
    path_lower: str
    path_display: str
    parent_path_lower: str
    rev: str
    size: int
    content_hash: str | None
    is_downloadable: bool
    export_format: str | None = None
    is_shared: bool = False


@dataclass(slots=True)
class DiscoveredFolder:
    path_lower: str
    path_display: str
    parent_path_lower: str | None
    depth: int


@dataclass(slots=True)
class QuotaStatus:
    effective_cap_bytes: int
    used_24h_bytes: int
    window_reset_epoch: float | None = None  # when the oldest in-window byte ages out

    @property
    def remaining_bytes(self) -> int:
        return max(0, self.effective_cap_bytes - self.used_24h_bytes)


@dataclass(slots=True)
class ScanSummary:
    total_files: int = 0
    total_bytes: int = 0
    cloud_doc_count: int = 0
    deep_folder_count: int = 0  # folders beyond Drive's 100-level limit
    path_collisions: list[str] = field(default_factory=list)
