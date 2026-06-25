"""Auth manager: provides fresh access tokens for the API clients.

Holds the in-memory access token per provider and refreshes it from the stored
refresh token when missing or near expiry. Refreshes are serialized per
provider so concurrent workers don't stampede the token endpoint.
"""

from __future__ import annotations

import asyncio

from filemig.clients import oauth_dropbox, oauth_google
from filemig.clients.tokens import TokenSet, TokenStore
from filemig.config import Settings
from filemig.domain.errors import AuthError


class AuthManager:
    def __init__(self, settings: Settings, store: TokenStore) -> None:
        self._settings = settings
        self._store = store
        self._cache: dict[str, TokenSet] = {}
        self._locks: dict[str, asyncio.Lock] = {
            "dropbox": asyncio.Lock(),
            "google": asyncio.Lock(),
        }

    def is_connected(self, provider: str) -> bool:
        return self._store.is_connected(provider)

    async def access_token(self, provider: str) -> str:
        async with self._locks[provider]:
            token = self._cache.get(provider)
            if token is None:
                stored = self._store.load(provider)
                if stored is None:
                    raise AuthError(f"{provider} is not connected")
                token = stored
                self._cache[provider] = token
            if token.is_expired():
                await self._refresh(provider, token)
            assert token.access_token is not None
            return token.access_token

    async def _refresh(self, provider: str, token: TokenSet) -> None:
        if provider == "dropbox":
            access, expires = await oauth_dropbox.refresh_access_token(
                self._settings, token.refresh_token)
        elif provider == "google":
            access, expires = await oauth_google.refresh_access_token(
                self._settings, token.refresh_token)
        else:
            raise AuthError(f"unknown provider {provider!r}")
        token.access_token = access
        token.expires_at = expires

    def invalidate(self, provider: str) -> None:
        """Force a refresh on next use (e.g. after a 401)."""
        token = self._cache.get(provider)
        if token is not None:
            token.access_token = None
