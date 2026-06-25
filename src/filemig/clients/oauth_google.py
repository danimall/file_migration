"""Google OAuth2 for a Desktop-app client (loopback + PKCE).

Flow:
1. Generate a PKCE verifier/challenge (S256) and redirect the user to Google's
   authorization endpoint with ``prompt=consent`` (to force a fresh refresh
   token). Do NOT send ``access_type=offline`` — it is not a native-app
   parameter; installed apps always receive a refresh token.
2. Google redirects to our loopback ``redirect_uri`` with ``?code=...``;
   exchange it (with the PKCE ``code_verifier``) at
   ``https://oauth2.googleapis.com/token``.
3. Persist only the refresh token.

Scope: https://www.googleapis.com/auth/drive.file (non-sensitive). Sufficient
because the app owns every folder/file it creates. Publish the consent screen
to Production so refresh tokens do not expire after 7 days.
"""

from __future__ import annotations

import base64
import hashlib
import secrets
import time
from urllib.parse import urlencode

import httpx

from filemig.clients.tokens import TokenSet, TokenStore
from filemig.config import Settings
from filemig.domain.errors import AuthError

AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URL = "https://oauth2.googleapis.com/token"
SCOPE = "https://www.googleapis.com/auth/drive.file"
PROVIDER = "google"


def generate_pkce() -> tuple[str, str]:
    """Return (code_verifier, code_challenge) using S256."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode()
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def build_authorize_url(settings: Settings, state: str, code_challenge: str) -> str:
    params = {
        "client_id": settings.google_client_id,
        "response_type": "code",
        "redirect_uri": settings.loopback_redirect_uri,
        "scope": SCOPE,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "state": state,
        "prompt": "consent",  # force a fresh refresh token
    }
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


async def exchange_code(
    settings: Settings, store: TokenStore, code: str, code_verifier: str
) -> TokenSet:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            TOKEN_URL,
            data={
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": settings.loopback_redirect_uri,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "code_verifier": code_verifier,
            },
        )
    if resp.status_code != 200:
        raise AuthError(f"google token exchange failed: {resp.status_code} {resp.text}")
    payload = resp.json()
    refresh = payload.get("refresh_token")
    if not refresh:
        raise AuthError(
            "google did not return a refresh_token — ensure prompt=consent and that "
            "the app has not already been granted (revoke and retry)."
        )
    store.save_refresh_token(PROVIDER, refresh)
    return TokenSet(
        refresh_token=refresh,
        access_token=payload["access_token"],
        expires_at=time.time() + payload.get("expires_in", 3600),
    )


async def refresh_access_token(
    settings: Settings, refresh_token: str
) -> tuple[str, float]:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
            },
        )
    if resp.status_code != 200:
        raise AuthError(f"google token refresh failed: {resp.status_code} {resp.text}")
    payload = resp.json()
    return payload["access_token"], time.time() + payload.get("expires_in", 3600)
