"""Google Drive API v3 client (the migration DESTINATION).

Endpoints / methods used:
* about.get?fields=storageQuota                 — preflight capacity
* files.create (folder)                          — build the folder tree
* files.list (appProperties probe)               — idempotency / dedupe
* files.get?fields=md5Checksum,sha256Checksum,size — verification (Leg 2)
* resumable upload (initiate + chunked PUT + resume probe)

Resumable upload rules (critical):
* Non-final chunks MUST be exact multiples of 256 KiB (262,144 bytes).
* Resume by an empty PUT with ``Content-Range: bytes */<total>``; a 308 returns
  a ``Range`` header (``bytes=0-N``) meaning resume at offset N+1; 200/201 means
  done; 404 means the session expired -> mint a new session and restart.
"""

from __future__ import annotations

from typing import Any

import httpx

from filemig.clients.auth import AuthManager
from filemig.clients.http import request_with_retries
from filemig.config import UPLOAD_CHUNK_GRANULARITY
from filemig.domain.errors import (
    FatalError,
    RetryableError,
    UploadSessionExpired,
)

API = "https://www.googleapis.com/drive/v3"
UPLOAD = "https://www.googleapis.com/upload/drive/v3"
FOLDER_MIME = "application/vnd.google-apps.folder"


class DriveClient:
    def __init__(self, auth: AuthManager, client: httpx.AsyncClient) -> None:
        self._auth = auth
        self._http = client

    async def _auth_header(self) -> dict[str, str]:
        token = await self._auth.access_token("google")
        return {"Authorization": f"Bearer {token}"}

    async def _request(
        self, method: str, url: str, *, retry_403_rl: bool = True, **kw: Any
    ) -> httpx.Response:
        async def send() -> httpx.Response:
            headers = {**(await self._auth_header()), **kw.pop("headers", {})}
            return await self._http.request(method, url, headers=headers, **kw)

        resp = await request_with_retries(send, treat_google_403_ratelimit=retry_403_rl)
        if resp.status_code == 401:
            self._auth.invalidate("google")
            resp = await request_with_retries(
                send, treat_google_403_ratelimit=retry_403_rl)
        return resp

    # -- preflight ----------------------------------------------------------

    async def storage_quota(self) -> dict[str, Any]:
        resp = await self._request("GET", f"{API}/about",
                                   params={"fields": "storageQuota,user"})
        if resp.status_code != 200:
            _raise("about.get", resp)
        return resp.json().get("storageQuota", {})

    # -- folders ------------------------------------------------------------

    async def create_folder(self, name: str, parent_id: str | None) -> str:
        body: dict[str, Any] = {"name": name, "mimeType": FOLDER_MIME}
        if parent_id:
            body["parents"] = [parent_id]
        resp = await self._request("POST", f"{API}/files",
                                   params={"fields": "id"}, json=body)
        if resp.status_code not in (200, 201):
            _raise("files.create(folder)", resp)
        return resp.json()["id"]

    # -- idempotency probe --------------------------------------------------

    async def find_by_app_property(
        self, key: str, value: str
    ) -> list[dict[str, Any]]:
        """Return files tagged with appProperties[key]==value (and not trashed).

        ``files.list`` costs 100 quota units — callers should batch probes per
        folder, not per file.
        """
        q = (f"appProperties has {{ key='{_escape(key)}' and "
             f"value='{_escape(value)}' }} and trashed=false")
        resp = await self._request(
            "GET", f"{API}/files",
            params={"q": q, "fields": "files(id,name,md5Checksum,sha256Checksum,size,appProperties)",
                    "spaces": "drive", "pageSize": 100},
        )
        if resp.status_code != 200:
            _raise("files.list", resp)
        return resp.json().get("files", [])

    # -- verification (Leg 2) ----------------------------------------------

    async def get_checksums(self, file_id: str) -> dict[str, Any]:
        resp = await self._request(
            "GET", f"{API}/files/{file_id}",
            params={"fields": "md5Checksum,sha256Checksum,size"},
        )
        if resp.status_code != 200:
            _raise("files.get", resp)
        return resp.json()

    # -- resumable upload ---------------------------------------------------

    async def initiate_resumable(
        self, name: str, parent_id: str, total_size: int,
        app_properties: dict[str, str], mime_type: str = "application/octet-stream",
    ) -> str:
        """Start a resumable session; return the session URI."""
        metadata: dict[str, Any] = {"name": name, "parents": [parent_id]}
        if app_properties:
            metadata["appProperties"] = app_properties
        headers = {
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": mime_type,
            "X-Upload-Content-Length": str(total_size),
        }
        resp = await self._request(
            "POST", f"{UPLOAD}/files",
            params={"uploadType": "resumable", "fields": "id,md5Checksum,sha256Checksum,size"},
            headers=headers, json=metadata,
        )
        if resp.status_code not in (200, 201):
            _raise("initiate resumable", resp)
        location = resp.headers.get("Location")
        if not location:
            raise FatalError("resumable initiate: no Location/session URI returned")
        return location

    async def put_chunk(
        self, session_uri: str, chunk: bytes, offset: int, total_size: int,
    ) -> tuple[str, dict[str, Any] | None]:
        """PUT one chunk. Returns (status, completed_file_json_or_None).

        status is one of: "incomplete" (308; keep going), "done" (200/201,
        file json returned). A 404 raises UploadSessionExpired; other non-2xx
        raise. Non-final chunks must be 256 KiB multiples (enforced by caller).
        """
        if total_size == 0:
            # Finalize a zero-byte file: empty body, Content-Range "bytes */0".
            content_range = "bytes */0"
        else:
            end = offset + len(chunk) - 1
            content_range = f"bytes {offset}-{end}/{total_size}"
        headers = {
            **(await self._auth_header()),
            "Content-Length": str(len(chunk)),
            "Content-Range": content_range,
        }
        # No automatic retry on the data PUT itself — on failure we re-probe the
        # offset and resume, which is safer than blindly resending.
        resp = await self._http.put(session_uri, headers=headers, content=chunk)
        if resp.status_code in (200, 201):
            return "done", resp.json()
        if resp.status_code == 308:
            return "incomplete", None
        if resp.status_code == 404:
            raise UploadSessionExpired("resumable session expired (404)")
        if resp.status_code == 401:
            self._auth.invalidate("google")
            raise RetryableError("401 during chunk upload; will re-auth and resume")
        if resp.status_code >= 500 or resp.status_code == 429:
            raise RetryableError(f"chunk upload {resp.status_code}; will resume")
        _raise("put_chunk", resp)

    async def probe_offset(self, session_uri: str, total_size: int) -> int | None:
        """Query how many bytes the server has. Returns the next byte offset to
        send, or None if the upload is already complete. Raises
        UploadSessionExpired on 404."""
        headers = {
            **(await self._auth_header()),
            "Content-Length": "0",
            "Content-Range": f"bytes */{total_size}",
        }
        resp = await self._http.put(session_uri, headers=headers)
        if resp.status_code in (200, 201):
            return None  # already complete
        if resp.status_code == 404:
            raise UploadSessionExpired("resumable session expired (404)")
        if resp.status_code == 308:
            rng = resp.headers.get("Range")
            if not rng:
                return 0  # no bytes received yet
            # Range: bytes=0-N  -> resume at N+1
            try:
                last = int(rng.split("-")[-1])
            except (ValueError, IndexError):
                return 0
            return last + 1
        raise RetryableError(f"probe_offset unexpected status {resp.status_code}")


# ---- helpers --------------------------------------------------------------

def next_chunk_size(remaining: int, configured: int) -> int:
    """Pick a chunk size: configured size, but the final chunk is the exact
    remainder. Non-final chunks are rounded down to a 256 KiB multiple."""
    if remaining <= configured:
        return remaining  # final chunk: any size allowed
    size = (configured // UPLOAD_CHUNK_GRANULARITY) * UPLOAD_CHUNK_GRANULARITY
    return max(UPLOAD_CHUNK_GRANULARITY, size)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _raise(op: str, resp: httpx.Response) -> None:
    msg = f"drive {op} -> {resp.status_code}: {resp.text[:300]}"
    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableError(msg)
    raise FatalError(msg)
