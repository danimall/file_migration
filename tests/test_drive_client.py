"""Tests for the Drive client: chunk sizing + resume-probe semantics."""

from __future__ import annotations

import httpx
import pytest
import respx

from filemig.clients.auth import AuthManager
from filemig.clients.drive_client import DriveClient, next_chunk_size
from filemig.clients.tokens import TokenStore
from filemig.config import UPLOAD_CHUNK_GRANULARITY, Settings
from filemig.domain.errors import UploadSessionExpired


@pytest.fixture
def drive(tmp_path):
    settings = Settings(data_dir=tmp_path)
    store = TokenStore(settings.secrets_dir)
    store.save_refresh_token("google", "refresh")
    auth = AuthManager(settings, store)
    # Pre-seed a live access token so no refresh network call is made.
    from filemig.clients.tokens import TokenSet
    from filemig.util.clock import now_epoch
    auth._cache["google"] = TokenSet("refresh", "live-access", now_epoch() + 3600)
    client = httpx.AsyncClient()
    return DriveClient(auth, client)


def test_next_chunk_size_final_chunk_any_size():
    # remaining smaller than configured -> exact remainder (final chunk).
    assert next_chunk_size(123, 32 * 1024 * 1024) == 123


def test_next_chunk_size_rounds_to_256kib_multiple():
    configured = 32 * 1024 * 1024 + 7  # not a clean multiple
    size = next_chunk_size(10**9, configured)
    assert size % UPLOAD_CHUNK_GRANULARITY == 0
    assert size <= configured


@respx.mock
async def test_probe_offset_parses_range(drive):
    uri = "https://upload.example/session/abc"
    respx.put(uri).mock(
        return_value=httpx.Response(308, headers={"Range": "bytes=0-262143"})
    )
    # 262144 bytes received -> resume at offset 262144.
    assert await drive.probe_offset(uri, total_size=10_000_000) == 262144


@respx.mock
async def test_probe_offset_no_range_means_zero(drive):
    uri = "https://upload.example/session/abc"
    respx.put(uri).mock(return_value=httpx.Response(308))
    assert await drive.probe_offset(uri, total_size=10_000_000) == 0


@respx.mock
async def test_probe_offset_complete(drive):
    uri = "https://upload.example/session/abc"
    respx.put(uri).mock(return_value=httpx.Response(200, json={"id": "x"}))
    assert await drive.probe_offset(uri, total_size=10_000_000) is None


@respx.mock
async def test_probe_offset_session_expired(drive):
    uri = "https://upload.example/session/abc"
    respx.put(uri).mock(return_value=httpx.Response(404))
    with pytest.raises(UploadSessionExpired):
        await drive.probe_offset(uri, total_size=10_000_000)


@respx.mock
async def test_put_chunk_done_and_incomplete(drive):
    uri = "https://upload.example/session/abc"
    # First call: 308 incomplete. Second: 200 done.
    route = respx.put(uri)
    route.side_effect = [
        httpx.Response(308, headers={"Range": "bytes=0-262143"}),
        httpx.Response(200, json={"id": "file-1", "md5Checksum": "abc"}),
    ]
    status, body = await drive.put_chunk(uri, b"x" * 262144, 0, 524288)
    assert status == "incomplete" and body is None
    status, body = await drive.put_chunk(uri, b"y" * 262144, 262144, 524288)
    assert status == "done" and body["id"] == "file-1"


@respx.mock
async def test_put_chunk_404_raises_session_expired(drive):
    uri = "https://upload.example/session/abc"
    respx.put(uri).mock(return_value=httpx.Response(404))
    with pytest.raises(UploadSessionExpired):
        await drive.put_chunk(uri, b"x" * 262144, 0, 262144)


@respx.mock
async def test_initiate_resumable_returns_session_uri(drive):
    respx.post("https://www.googleapis.com/upload/drive/v3/files").mock(
        return_value=httpx.Response(
            200, headers={"Location": "https://upload.example/session/xyz"})
    )
    uri = await drive.initiate_resumable(
        "a.txt", "parent-id", 1000, {"sp": "deadbeef"})
    assert uri == "https://upload.example/session/xyz"
