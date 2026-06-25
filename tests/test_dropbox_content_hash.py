"""Tests for the safety-critical Dropbox content_hash implementation.

We anchor correctness three ways:

1. The exact, publicly-known empty-file vector.
2. An *independently written* naive reference implementation, cross-checked
   against the streaming hasher across block boundaries and arbitrary update()
   chunkings. This catches the two classic bugs (concatenating hex strings
   instead of raw digests; wrong block size) because the naive impl is written
   separately and would not share an incidental bug.
3. The published worked example's exact 3-block split for a 9,711,423-byte
   input, plus an opt-in check against the real published constant when the
   reference fixture (Dropbox's milky-way-nasa.jpg) is available.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from filemig.hashing.dropbox_content_hash import (
    DROPBOX_BLOCK_SIZE,
    DropboxContentHasher,
    compute_dropbox_content_hash,
)

# SHA-256 of the empty string — the content_hash of any empty file.
EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

# Dropbox's published worked example.
PUBLISHED_SIZE = 9_711_423
PUBLISHED_HASH = "485291fa0ee50c016982abbfa943957bcd231aae0492ccbaa22c58e3997b35e0"
PUBLISHED_BLOCKS = [4_194_304, 4_194_304, 1_322_815]


def naive_content_hash(data: bytes) -> str:
    """A deliberately separate, whole-buffer reference implementation."""
    block_digests = []
    for start in range(0, len(data), DROPBOX_BLOCK_SIZE):
        block = data[start : start + DROPBOX_BLOCK_SIZE]
        block_digests.append(hashlib.sha256(block).digest())  # RAW 32-byte digest
    concatenated = b"".join(block_digests)
    return hashlib.sha256(concatenated).hexdigest()


def deterministic_bytes(n: int) -> bytes:
    """Reproducible pseudo-random bytes of length n (seeded, no randomness)."""
    out = bytearray()
    counter = 0
    while len(out) < n:
        out.extend(hashlib.sha256(counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(out[:n])


def test_block_size_is_4_mib():
    assert DROPBOX_BLOCK_SIZE == 4 * 1024 * 1024 == 4_194_304


def test_empty_file_is_sha256_of_empty_string():
    assert compute_dropbox_content_hash(b"") == EMPTY_SHA256
    assert compute_dropbox_content_hash(b"") == hashlib.sha256(b"").hexdigest()


@pytest.mark.parametrize(
    "size",
    [
        0,
        1,
        1024,
        DROPBOX_BLOCK_SIZE - 1,
        DROPBOX_BLOCK_SIZE,
        DROPBOX_BLOCK_SIZE + 1,
        2 * DROPBOX_BLOCK_SIZE,
        2 * DROPBOX_BLOCK_SIZE + 7,
        PUBLISHED_SIZE,
    ],
)
def test_matches_naive_reference(size):
    data = deterministic_bytes(size)
    assert compute_dropbox_content_hash(data) == naive_content_hash(data)


def test_published_example_block_split():
    """The 9,711,423-byte size must split into exactly [4194304, 4194304, 1322815]."""
    blocks = []
    remaining = PUBLISHED_SIZE
    while remaining > 0:
        take = min(DROPBOX_BLOCK_SIZE, remaining)
        blocks.append(take)
        remaining -= take
    assert blocks == PUBLISHED_BLOCKS


def test_single_byte_is_not_hex_concat_bug():
    """A one-block file's hash is sha256(raw_sha256(block)), NOT
    sha256(hex_sha256(block)). This pins the raw-vs-hex distinction."""
    data = b"hello dropbox"
    block_digest_raw = hashlib.sha256(data).digest()
    expected = hashlib.sha256(block_digest_raw).hexdigest()
    wrong_hex_concat = hashlib.sha256(
        hashlib.sha256(data).hexdigest().encode()
    ).hexdigest()
    assert compute_dropbox_content_hash(data) == expected
    assert compute_dropbox_content_hash(data) != wrong_hex_concat


@pytest.mark.parametrize("chunk", [1, 7, 4096, DROPBOX_BLOCK_SIZE, DROPBOX_BLOCK_SIZE + 13])
def test_streaming_chunking_is_invariant(chunk):
    """Result must be independent of how bytes are fed via update()."""
    data = deterministic_bytes(2 * DROPBOX_BLOCK_SIZE + 123)
    hasher = DropboxContentHasher()
    for i in range(0, len(data), chunk):
        hasher.update(data[i : i + chunk])
    assert hasher.hexdigest() == naive_content_hash(data)


def test_finalize_is_idempotent():
    hasher = DropboxContentHasher()
    hasher.update(b"abc")
    first = hasher.hexdigest()
    assert hasher.hexdigest() == first  # repeated reads are stable
    assert hasher.digest().hex() == first


def test_update_after_finalize_raises():
    hasher = DropboxContentHasher()
    hasher.update(b"abc")
    hasher.hexdigest()
    with pytest.raises(RuntimeError):
        hasher.update(b"more")


@pytest.mark.skipif(
    not os.environ.get("FILEMIG_MILKY_WAY_FIXTURE"),
    reason="set FILEMIG_MILKY_WAY_FIXTURE to the path of Dropbox's milky-way-nasa.jpg",
)
def test_published_constant_with_real_fixture():
    """Opt-in: verify against Dropbox's exact published constant using the real
    reference file (dropbox/dropbox-api-content-hasher milky-way-nasa.jpg)."""
    path = Path(os.environ["FILEMIG_MILKY_WAY_FIXTURE"])
    data = path.read_bytes()
    assert len(data) == PUBLISHED_SIZE
    assert compute_dropbox_content_hash(data) == PUBLISHED_HASH
