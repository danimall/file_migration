"""Dropbox ``content_hash`` algorithm.

Reference: https://www.dropbox.com/developers/reference/content-hash

Algorithm (EXACT — getting any detail wrong yields false verification
failures):

1. Split the file into blocks of **4 MiB = 4 * 1024 * 1024 = 4,194,304 bytes**.
2. Compute the SHA-256 of each block.
3. Concatenate the **raw 32-byte binary digests** of the blocks in order.
   (The classic bug is concatenating the *hex strings* — do NOT do that.)
4. Compute the SHA-256 of the concatenated digests.
5. The ``content_hash`` is that final digest, lowercase hex (64 chars).

An empty file has no blocks, so the concatenation is empty and the result is
the SHA-256 of the empty string.
"""

from __future__ import annotations

import hashlib

DROPBOX_BLOCK_SIZE = 4 * 1024 * 1024  # 4 MiB = 4,194,304 bytes


class DropboxContentHasher:
    """Incremental hasher mirroring Dropbox's reference implementation.

    Feed bytes via :meth:`update`; the instance buffers across calls so callers
    can stream arbitrary chunk sizes. Call :meth:`hexdigest` when done.
    """

    def __init__(self) -> None:
        self._overall = hashlib.sha256()
        self._block = hashlib.sha256()
        self._block_pos = 0  # bytes accumulated in the current 4 MiB block
        self._finalized = False

    def update(self, data: bytes) -> None:
        if self._finalized:
            raise RuntimeError("DropboxContentHasher already finalized")
        offset = 0
        n = len(data)
        while offset < n:
            if self._block_pos == DROPBOX_BLOCK_SIZE:
                # Current block is full: fold its raw digest into the overall
                # hash and start a fresh block.
                self._overall.update(self._block.digest())
                self._block = hashlib.sha256()
                self._block_pos = 0

            space = DROPBOX_BLOCK_SIZE - self._block_pos
            take = min(space, n - offset)
            self._block.update(data[offset : offset + take])
            self._block_pos += take
            offset += take

    def _finalize(self) -> None:
        if self._finalized:
            return
        # Fold the final (possibly partial, possibly empty-for-nonempty-input)
        # block. We only fold a trailing block if it actually received bytes;
        # for a completely empty input there are zero blocks.
        if self._block_pos > 0:
            self._overall.update(self._block.digest())
        self._finalized = True

    def digest(self) -> bytes:
        self._finalize()
        return self._overall.digest()

    def hexdigest(self) -> str:
        self._finalize()
        return self._overall.hexdigest()


def compute_dropbox_content_hash(data: bytes) -> str:
    """Convenience: compute the content_hash of an in-memory bytes object."""
    hasher = DropboxContentHasher()
    hasher.update(data)
    return hasher.hexdigest()
