"""Final reconciliation pass.

Independently verifies the destination against the source by re-probing Drive
for each expected file (via its appProperties identity tag) and comparing
checksums to what we recorded locally. Produces a discrepancy report so the user
can trust the migration before (and after) approving any deletions.

This does not mutate migration state; it only reports.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from filemig.clients.drive_client import DriveClient
from filemig.db.store import Store
from filemig.domain import models
from filemig.domain.states import FileState
from filemig.util.identity import KEY_SRC_PATH


@dataclass(slots=True)
class ReconcileReport:
    checked: int = 0
    confirmed: int = 0
    missing_on_drive: list[str] = field(default_factory=list)
    checksum_mismatch: list[str] = field(default_factory=list)
    dead: list[str] = field(default_factory=list)
    not_yet_verified: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not (self.missing_on_drive or self.checksum_mismatch)


async def reconcile(
    drive: DriveClient, store: Store, run: models.Run
) -> ReconcileReport:
    report = ReconcileReport()
    counts = await store.count_files_by_state(run.id)
    report.dead = []  # populated below by scanning dead rows

    # Verify each file that should be on Drive (verified or done).
    for state in (FileState.VERIFIED, FileState.DONE):
        files = await store.files_in_state(run.id, state, limit=10**9)
        for f in files:
            report.checked += 1
            try:
                matches = await drive.find_by_app_property(
                    KEY_SRC_PATH, f.src_path_sha256)
            except Exception:
                report.missing_on_drive.append(f.src_path_display)
                continue
            match = _pick_match(matches, f)
            if match is None:
                report.missing_on_drive.append(f.src_path_display)
                continue
            if not _checksums_agree(match, f):
                report.checksum_mismatch.append(f.src_path_display)
                continue
            report.confirmed += 1

    # Surface files that are not safely on Drive yet.
    for f in await store.files_in_state(run.id, FileState.DEAD, limit=10**9):
        report.dead.append(f.src_path_display)
    pending = sum(counts.get(s.value, 0) for s in (
        FileState.DISCOVERED, FileState.QUEUED, FileState.DOWNLOADED,
        FileState.UPLOADED))
    if pending:
        report.not_yet_verified = [f"{pending} file(s) not yet verified"]

    return report


def _pick_match(matches: list[dict], f: models.FileRecord) -> dict | None:
    if not matches:
        return None
    if f.dst_file_id:
        for m in matches:
            if m.get("id") == f.dst_file_id:
                return m
    return matches[0]


def _checksums_agree(match: dict, f: models.FileRecord) -> bool:
    md5 = match.get("md5Checksum")
    if md5 and f.local_md5:
        return md5 == f.local_md5
    size = match.get("size")
    if size is not None and f.local_size is not None:
        return int(size) == f.local_size
    # Nothing comparable: treat as unconfirmed.
    return False
