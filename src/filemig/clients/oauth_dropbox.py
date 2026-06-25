"""Dropbox OAuth2 (authorization-code, offline access).

Flow:
1. Redirect the user to the authorize URL with ``token_access_type=offline``
   (REQUIRED to receive a refresh_token) and the three scopes we need.
2. Dropbox redirects back with ``?code=...``; exchange it at
   ``https://api.dropboxapi.com/oauth2/token`` (note: api.dropboxapi.com, not
   api.dropbox.com) for an access token + refresh token.
3. Persist only the refresh token; mint access tokens on demand.

Scopes: files.metadata.read, files.content.read, files.content.write
(content.write is required to delete).
"""

from __future__ import annotations

import time
from urllib.parse import urlencode

import httpx

from filemig.clients.tokens import TokenSet, TokenStore
from filemig.config import Settings
from filemig.domain.errors import AuthError

AUTHORIZE_URL = "https://www.dropbox.com/oauth2/authorize"
TOKEN_URL = "https://api.dropboxapi.com/oauth2/token"  # api.dropboxapi.com!
SCOPES = ["files.metadata.read", "files.content.read", "files.content.write"]
PROVIDER = "dropbox"


def build_authorize_url(settings: Settings, state: str) -> str:
    params = {
        "client_id": settings.dropbox_app_key,
        "response_type": "code",
        "redirect_uri": settings.dropbox_redirect_uri,
        "token_access_type": "offline",  # REQUIRED for a refresh token
        "scope": " ".join(SCOPES),
        "state": state,
    }
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


async def exchange_code(
    settings: Settings, store: TokenStore, code: str
) -> TokenSet:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            TOKEN_URL,
            data={
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": settings.dropbox_redirect_uri,
                "client_id": settings.dropbox_app_key,
                "client_secret": settings.dropbox_app_secret,
            },
        )
    if resp.status_code != 200:
        raise AuthError(f"dropbox token exchange failed: {resp.status_code} {resp.text}")
    payload = resp.json()
    refresh = payload.get("refresh_token")
    if not refresh:
        raise AuthError(
            "dropbox did not return a refresh_token — was token_access_type=offline set?"
        )
    store.save_refresh_token(PROVIDER, refresh)
    return TokenSet(
        refresh_token=refresh,
        access_token=payload["access_token"],
        expires_at=time.time() + payload.get("expires_in", 14400),
    )


async def refresh_access_token(
    settings: Settings, refresh_token: str
) -> tuple[str, float]:
    """Return (access_token, expires_at_epoch)."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": settings.dropbox_app_key,
                "client_secret": settings.dropbox_app_secret,
            },
        )
    if resp.status_code != 200:
        raise AuthError(f"dropbox token refresh failed: {resp.status_code} {resp.text}")
    payload = resp.json()
    return payload["access_token"], time.time() + payload.get("expires_in", 14400)
