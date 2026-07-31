"""Single-pass multi-hashing.

While a file is streamed from Dropbox to local staging, we compute every hash
we will ever need in one pass over the bytes:

* the Dropbox ``content_hash`` — to verify the *download* against Dropbox's own
  metadata (verification Leg 1);
* ``md5`` and ``sha256`` — to verify the *upload* against Google Drive's
  ``md5Checksum`` / ``sha256Checksum`` after it lands (verification Leg 2);
* the byte ``size`` — a cheap sanity check on both legs.

The two legs use *different* algorithms on purpose: Dropbox's content_hash and
Drive's md5 are not comparable to each other. Each leg checks the bytes we held
locally against one service, which transitively chains Dropbox -> local -> Drive.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from filemig.hashing.dropbox_content_hash import DropboxContentHasher


@dataclass(frozen=True, slots=True)
class MultiHashResult:
    dropbox_content_hash: str
    md5: str
    sha256: str
    size: int


class MultiHasher:
    """Feed bytes via :meth:`update`; read results via :meth:`result`."""

    def __init__(self) -> None:
        self._dbx = DropboxContentHasher()
        # Google Drive exposes MD5 as a content checksum. It is never used for
        # authentication or another security decision; SHA-256 is computed too.
        self._md5 = hashlib.md5(usedforsecurity=False)
        self._sha256 = hashlib.sha256()
        self._size = 0

    def update(self, data: bytes) -> None:
        if not data:
            return
        self._dbx.update(data)
        self._md5.update(data)
        self._sha256.update(data)
        self._size += len(data)

    def result(self) -> MultiHashResult:
        return MultiHashResult(
            dropbox_content_hash=self._dbx.hexdigest(),
            md5=self._md5.hexdigest(),
            sha256=self._sha256.hexdigest(),
            size=self._size,
        )
