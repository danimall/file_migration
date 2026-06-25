"""Dropbox HTTP API v2 client (the migration SOURCE).

Endpoints used:
* POST /2/files/list_folder (+ /continue)   — recursive discovery
* POST content:/2/files/download            — stream file bytes
* POST /2/files/export                       — export non-downloadable cloud docs
* POST /2/files/delete_batch (+ /check)      — soft delete (to trash) after verify
* POST /2/users/get_current_account          — path-root namespace

All requests honor 429 Retry-After via request_with_retries. Access tokens are
fetched from the AuthManager and refreshed automatically; on a 401 we
invalidate and retry once.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx

from filemig.clients.auth import AuthManager
from filemig.clients.http import request_with_retries
from filemig.domain import models
from filemig.domain.errors import FatalError, RetryableError

API = "https://api.dropboxapi.com"
CONTENT = "https://content.dropboxapi.com"

# SAFETY KILL SWITCH. While False, this client is physically incapable of
# deleting anything from Dropbox: the delete methods raise before any API call
# is made. The migration runs copy-and-verify only; the user deletes sources
# themselves. Flip to True (and re-enable the deletion flow in the service/UI)
# only once deletion is explicitly trusted.
DROPBOX_DELETION_ENABLED = False


class DropboxClient:
    def __init__(self, auth: AuthManager, client: httpx.AsyncClient) -> None:
        self._auth = auth
        self._http = client

    async def _headers(self) -> dict[str, str]:
        token = await self._auth.access_token("dropbox")
        return {"Authorization": f"Bearer {token}"}

    async def _rpc(self, path: str, body: dict[str, Any] | None) -> dict[str, Any]:
        """Call an RPC-style endpoint (JSON in, JSON out)."""
        async def send() -> httpx.Response:
            headers = await self._headers()
            headers["Content-Type"] = "application/json"
            # Dropbox expects the literal null body to be omitted, not "null".
            content = json.dumps(body) if body is not None else None
            return await self._http.post(f"{API}{path}", headers=headers, content=content)

        resp = await request_with_retries(send)
        if resp.status_code == 401:
            self._auth.invalidate("dropbox")
            resp = await request_with_retries(send)
        if resp.status_code != 200:
            _raise_for_dropbox(path, resp)
        return resp.json()

    # -- account ------------------------------------------------------------

    async def get_current_account(self) -> dict[str, Any]:
        return await self._rpc("/2/users/get_current_account", None)

    # -- discovery ----------------------------------------------------------

    async def list_folder(
        self, path: str = "", recursive: bool = True
    ) -> AsyncIterator[dict[str, Any]]:
        """Yield every entry under ``path`` (root = "" NOT "/")."""
        result = await self._rpc(
            "/2/files/list_folder",
            {
                "path": path,
                "recursive": recursive,
                "include_deleted": False,
                "include_mounted_folders": True,
                "include_non_downloadable_files": True,
                "limit": 2000,
            },
        )
        for entry in result["entries"]:
            yield entry
        while result.get("has_more"):
            result = await self._rpc(
                "/2/files/list_folder/continue", {"cursor": result["cursor"]}
            )
            for entry in result["entries"]:
                yield entry

    # -- download -----------------------------------------------------------

    async def download_stream(
        self, path: str, rev: str | None = None
    ) -> tuple[dict[str, Any], AsyncIterator[bytes]]:
        """Open a streaming download. Returns (FileMetadata, byte-iterator).

        The metadata (incl. content_hash) is echoed in the Dropbox-API-Result
        header. The caller must fully consume or close the iterator.
        """
        arg: dict[str, Any] = {"path": rev_path(path, rev)}
        token = await self._auth.access_token("dropbox")
        req = self._http.build_request(
            "POST",
            f"{CONTENT}/2/files/download",
            headers={
                "Authorization": f"Bearer {token}",
                "Dropbox-API-Arg": _api_arg(arg),
            },
        )
        resp = await self._http.send(req, stream=True)
        if resp.status_code == 401:
            await resp.aclose()
            self._auth.invalidate("dropbox")
            token = await self._auth.access_token("dropbox")
            req = self._http.build_request(
                "POST", f"{CONTENT}/2/files/download",
                headers={"Authorization": f"Bearer {token}",
                         "Dropbox-API-Arg": _api_arg(arg)},
            )
            resp = await self._http.send(req, stream=True)
        if resp.status_code != 200:
            text = (await resp.aread()).decode("utf-8", "replace")
            await resp.aclose()
            _raise_status("/2/files/download", resp.status_code, text)
        meta = json.loads(resp.headers["Dropbox-API-Result"])
        return meta, _aiter_and_close(resp)

    async def export_stream(
        self, path: str, export_format: str | None = None
    ) -> tuple[dict[str, Any], AsyncIterator[bytes]]:
        """Export a non-downloadable cloud doc. Returns (ExportResult, bytes)."""
        arg: dict[str, Any] = {"path": path}
        if export_format:
            arg["export_format"] = export_format
        token = await self._auth.access_token("dropbox")
        req = self._http.build_request(
            "POST", f"{CONTENT}/2/files/export",
            headers={"Authorization": f"Bearer {token}", "Dropbox-API-Arg": _api_arg(arg)},
        )
        resp = await self._http.send(req, stream=True)
        if resp.status_code != 200:
            text = (await resp.aread()).decode("utf-8", "replace")
            await resp.aclose()
            _raise_status("/2/files/export", resp.status_code, text)
        meta = json.loads(resp.headers["Dropbox-API-Result"])
        return meta, _aiter_and_close(resp)

    # -- delete (soft, to trash) -------------------------------------------
    # DISABLED by the DROPBOX_DELETION_ENABLED kill switch. These methods raise
    # before issuing any API call, so the app cannot remove anything from
    # Dropbox. The migration is copy-and-verify only.

    async def delete_batch(self, entries: list[dict[str, str]]) -> str | dict:
        """Launch a batch soft-delete. DISABLED: raises unless deletion is
        explicitly enabled. ``entries`` is a list of {"path": ...,
        optionally "parent_rev": ...}."""
        if not DROPBOX_DELETION_ENABLED:
            raise FatalError(
                "Dropbox deletion is disabled (DROPBOX_DELETION_ENABLED=False). "
                "This build is copy-and-verify only; delete sources yourself.")
        result = await self._rpc("/2/files/delete_batch", {"entries": entries})
        tag = result.get(".tag")
        if tag == "async_job_id":
            return result["async_job_id"]
        return result  # complete

    async def delete_batch_check(self, async_job_id: str) -> dict[str, Any]:
        """Poll a delete_batch job. DISABLED while deletion is off."""
        if not DROPBOX_DELETION_ENABLED:
            raise FatalError("Dropbox deletion is disabled.")
        return await self._rpc(
            "/2/files/delete_batch/check", {"async_job_id": async_job_id}
        )


# ---- helpers --------------------------------------------------------------

def rev_path(path: str, rev: str | None) -> str:
    """Dropbox accepts ``rev:<rev>`` as a path to pin a specific revision."""
    return f"rev:{rev}" if rev else path


def _api_arg(arg: dict[str, Any]) -> str:
    """Serialize a Dropbox-API-Arg header (must be HTTP-header-safe ASCII)."""
    text = json.dumps(arg)
    # Escape non-ASCII per Dropbox guidance (\\uXXXX), so unicode paths are safe.
    return "".join(c if ord(c) < 0x80 else f"\\u{ord(c):04x}" for c in text)


async def _aiter_and_close(resp: httpx.Response) -> AsyncIterator[bytes]:
    try:
        async for chunk in resp.aiter_bytes():
            yield chunk
    finally:
        await resp.aclose()


def _raise_for_dropbox(path: str, resp: httpx.Response) -> None:
    _raise_status(path, resp.status_code, resp.text)


def _raise_status(path: str, status: int, text: str) -> None:
    msg = f"dropbox {path} -> {status}: {text[:300]}"
    if status == 429 or status >= 500:
        raise RetryableError(msg)
    raise FatalError(msg)


def to_discovered(entry: dict[str, Any]) -> models.DiscoveredFile | None:
    """Convert a list_folder 'file' entry into a DiscoveredFile (else None)."""
    if entry.get(".tag") != "file":
        return None
    path_lower = entry["path_lower"]
    parent = path_lower.rsplit("/", 1)[0] if "/" in path_lower else ""
    export_format = None
    if not entry.get("is_downloadable", True):
        export_format = entry.get("export_info", {}).get("export_as")
    return models.DiscoveredFile(
        dbx_id=entry["id"],
        path_lower=path_lower,
        path_display=entry["path_display"],
        parent_path_lower=parent,
        rev=entry["rev"],
        size=entry["size"],
        content_hash=entry.get("content_hash"),
        is_downloadable=entry.get("is_downloadable", True),
        export_format=export_format,
        is_shared=bool(entry.get("sharing_info")),
    )
