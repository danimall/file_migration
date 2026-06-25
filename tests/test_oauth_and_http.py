"""Tests for PKCE generation, token storage perms, and HTTP retry logic."""

from __future__ import annotations

import base64
import hashlib
import stat

import httpx
import pytest

from filemig.clients import http as httputil
from filemig.clients.oauth_google import generate_pkce
from filemig.clients.tokens import TokenStore


def test_pkce_challenge_is_s256_of_verifier():
    verifier, challenge = generate_pkce()
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()
    ).rstrip(b"=").decode()
    assert challenge == expected
    # RFC 7636: verifier length 43..128 chars.
    assert 43 <= len(verifier) <= 128


def test_pkce_is_random_each_call():
    assert generate_pkce()[0] != generate_pkce()[0]


def test_token_store_file_is_0600(tmp_path):
    store = TokenStore(tmp_path)
    store.save_refresh_token("dropbox", "secret-refresh")
    path = tmp_path / "dropbox_token.json"
    assert store.load("dropbox").refresh_token == "secret-refresh"
    mode = stat.S_IMODE(path.stat().st_mode)
    assert mode == 0o600
    assert store.is_connected("dropbox")
    assert not store.is_connected("google")


def test_parse_retry_after():
    resp = httpx.Response(429, headers={"Retry-After": "7"})
    assert httputil.parse_retry_after(resp) == 7.0
    assert httputil.parse_retry_after(httpx.Response(429)) is None


def test_is_google_rate_limit():
    rl = httpx.Response(403, json={"error": {"errors": [{"reason": "rateLimitExceeded"}]}})
    fatal = httpx.Response(403, json={"error": {"errors": [{"reason": "insufficientPermissions"}]}})
    assert httputil.is_google_rate_limit(rl)
    assert not httputil.is_google_rate_limit(fatal)


async def test_request_with_retries_honors_retry_after(monkeypatch):
    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(httputil.asyncio, "sleep", fake_sleep)

    calls = {"n": 0}

    async def send():
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200, text="ok")

    resp = await httputil.request_with_retries(send, max_attempts=5)
    assert resp.status_code == 200
    assert calls["n"] == 3
    assert sleeps == [5.0, 5.0]  # honored the header exactly, twice


async def test_request_with_retries_gives_up_and_returns_last(monkeypatch):
    async def fake_sleep(s):
        return None

    monkeypatch.setattr(httputil.asyncio, "sleep", fake_sleep)

    async def send():
        return httpx.Response(503)

    resp = await httputil.request_with_retries(send, max_attempts=3)
    assert resp.status_code == 503
