"""Persistent OAuth token storage.

Refresh tokens are long-lived secrets. We store them in a JSON file under the
secrets directory with ``0600`` permissions (owner read/write only). Access
tokens are cached in memory only — they are short-lived and cheap to refresh.

Never log token values.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(slots=True)
class TokenSet:
    refresh_token: str
    # In-memory only fields (not persisted): the live access token + expiry.
    access_token: str | None = None
    expires_at: float = 0.0  # epoch seconds

    def is_expired(self, skew: float = 120.0) -> bool:
        from filemig.util.clock import now_epoch
        return self.access_token is None or now_epoch() >= (self.expires_at - skew)


class TokenStore:
    """Stores one TokenSet per provider ("dropbox" / "google")."""

    def __init__(self, secrets_dir: Path) -> None:
        self._dir = secrets_dir

    def _path(self, provider: str) -> Path:
        return self._dir / f"{provider}_token.json"

    def load(self, provider: str) -> TokenSet | None:
        path = self._path(provider)
        if not path.exists():
            return None
        data = json.loads(path.read_text("utf-8"))
        return TokenSet(refresh_token=data["refresh_token"])

    def save_refresh_token(self, provider: str, refresh_token: str) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        path = self._path(provider)
        # Write with restrictive perms from creation, then persist atomically.
        tmp = path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump({"refresh_token": refresh_token}, fh)
        finally:
            pass
        os.replace(tmp, path)
        try:
            path.chmod(0o600)
        except OSError:
            pass

    def is_connected(self, provider: str) -> bool:
        return self._path(provider).exists()

    def delete(self, provider: str) -> None:
        self._path(provider).unlink(missing_ok=True)
