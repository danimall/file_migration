"""Application configuration.

Settings are loaded from environment variables (prefix ``FILEMIG_``) and an
optional ``.env`` file in the working directory. Paths default to a per-user
data directory so the tool works out of the box on a single Mac.
"""

from __future__ import annotations

import functools
from pathlib import Path

from pydantic import Field, computed_field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Google's hard per-user upload cap: 750 GiB/day across My Drive + all shared
# drives. Unraisable. A 1 TB migration must therefore span multiple days.
_GIB = 1024**3
DAILY_CAP_BYTES_DEFAULT = 750 * _GIB
LOOPBACK_HOST = "127.0.0.1"

# Drive resumable uploads require every non-final chunk to be an exact multiple
# of 256 KiB. We default to 32 MiB (a multiple of 256 KiB) for efficiency.
UPLOAD_CHUNK_GRANULARITY = 256 * 1024


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FILEMIG_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---- OAuth credentials ----
    dropbox_app_key: str = ""
    dropbox_app_secret: str = ""
    google_client_id: str = ""
    google_client_secret: str = ""

    # ---- Server ----
    host: str = LOOPBACK_HOST
    port: int = 8765

    @field_validator("host")
    @classmethod
    def require_loopback_host(cls, value: str) -> str:
        """Keep the unauthenticated single-user dashboard local to this Mac."""
        if value != LOOPBACK_HOST:
            raise ValueError(
                "FILEMIG_HOST must be 127.0.0.1; remote binding is disabled"
            )
        return value

    # ---- Storage ----
    # Base data dir; if unset, defaults to ~/.filemig (see computed properties).
    data_dir: Path | None = None
    staging_dir: Path | None = None

    # ---- Worker tuning ----
    worker_concurrency: int = 2
    upload_chunk_bytes: int = 32 * 1024 * 1024  # 32 MiB (256 KiB multiple)
    max_attempts: int = 5
    # Order in which workers claim queued files: "fifo" | "largest" | "smallest".
    # "largest" prioritizes big data first; the many tiny, latency-bound files
    # then mop up afterward.
    claim_order: str = "fifo"

    # ---- Quota / batching ----
    daily_cap_bytes: int = DAILY_CAP_BYTES_DEFAULT
    daily_cap_margin: float = 0.05  # ~5% safety margin -> ~712 GiB effective
    batch_target_bytes: int = 100 * _GIB
    batch_file_limit: int = 2000

    # ---- Staging disk safety ----
    staging_min_free_bytes: int = 100 * _GIB

    # ---- Behaviour ----
    dry_run: bool = False

    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_data_dir(self) -> Path:
        base = self.data_dir or (Path.home() / ".filemig")
        return base.expanduser()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def resolved_staging_dir(self) -> Path:
        base = self.staging_dir or (self.resolved_data_dir / "staging")
        return base.expanduser()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def secrets_dir(self) -> Path:
        return self.resolved_data_dir / ".secrets"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def db_path(self) -> Path:
        return self.resolved_data_dir / "filemig.sqlite"

    @property
    def effective_daily_cap_bytes(self) -> int:
        """Daily upload cap after applying the safety margin."""
        return int(self.daily_cap_bytes * (1.0 - self.daily_cap_margin))

    @property
    def loopback_redirect_uri(self) -> str:
        """Google Desktop-app loopback redirect for the OAuth callback."""
        return f"http://{self.host}:{self.port}/auth/google/callback"

    @property
    def dropbox_redirect_uri(self) -> str:
        return f"http://{self.host}:{self.port}/auth/dropbox/callback"

    def ensure_dirs(self) -> None:
        """Create data/staging/secrets directories with safe permissions."""
        self.resolved_data_dir.mkdir(parents=True, exist_ok=True)
        self.resolved_staging_dir.mkdir(parents=True, exist_ok=True)
        self.secrets_dir.mkdir(parents=True, exist_ok=True)
        # Restrict the secrets dir to the owner.
        try:
            self.secrets_dir.chmod(0o700)
        except OSError:
            pass


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
