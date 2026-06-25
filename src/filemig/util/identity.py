"""Stable identity hashing for Drive idempotency tagging.

Google Drive allows duplicate file names in a folder (identity is the file ID),
so re-run safety relies on tagging each created file with private
``appProperties``. Drive limits each property to **124 bytes for key+value
combined**, so we must HASH the source path rather than store it raw.

We stamp three properties on every uploaded file:
* ``mrid``  — the migration run uuid (short)
* ``sp``    — sha256(run_uuid + "\\0" + path_lower), hex (the dedupe key)
* ``ch``    — the Dropbox content_hash (so a probe can confirm identity)

Each key is 2-3 chars and each value (64 hex chars) keeps key+value < 124 bytes.
"""

from __future__ import annotations

import hashlib

KEY_RUN = "mrid"
KEY_SRC_PATH = "sp"
KEY_CONTENT_HASH = "ch"


def src_path_sha256(run_uuid: str, path_lower: str) -> str:
    h = hashlib.sha256()
    h.update(run_uuid.encode("utf-8"))
    h.update(b"\x00")
    h.update(path_lower.encode("utf-8"))
    return h.hexdigest()


def app_properties(run_uuid: str, path_sha: str, content_hash: str | None) -> dict[str, str]:
    props = {KEY_RUN: run_uuid[:32], KEY_SRC_PATH: path_sha}
    if content_hash:
        props[KEY_CONTENT_HASH] = content_hash
    # Sanity: enforce the 124-byte key+value budget Drive imposes.
    for k, v in props.items():
        if len(k.encode("utf-8")) + len(v.encode("utf-8")) > 124:
            raise ValueError(f"appProperty {k!r} exceeds 124-byte limit")
    return props
