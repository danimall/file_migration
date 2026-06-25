"""FastAPI routes for the migration dashboard.

The dashboard is server-rendered (Jinja2) and updated live via SSE, with HTMX
fragments for interactive pieces. All mutating actions are POSTs; the
safety-critical one — ``approve-delete`` — is gated server-side so it can only
run when every non-dead file in the batch is verified.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from filemig.clients import oauth_dropbox, oauth_google
from filemig.web.sse import event_stream

router = APIRouter()
_templates = Jinja2Templates(
    directory=str(Path(__file__).resolve().parent.parent / "templates"))


def svc(request: Request):
    return request.app.state.service


def settings(request: Request):
    return request.app.state.service.settings


# -- dashboard --------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    service = svc(request)
    return _templates.TemplateResponse(request, "dashboard.html", {
        "dropbox_connected": service.auth.is_connected("dropbox"),
        "google_connected": service.auth.is_connected("google"),
        "scan_summary": service.scan_summary,
        "is_scanning": service.is_scanning,
        "is_paused": service.is_paused,
    })


# -- auth: dropbox ----------------------------------------------------------

@router.get("/auth/dropbox/connect")
async def dropbox_connect(request: Request):
    service = svc(request)
    state = service.new_oauth_state("dropbox")
    url = oauth_dropbox.build_authorize_url(service.settings, state)
    return RedirectResponse(url)


@router.get("/auth/dropbox/callback")
async def dropbox_callback(request: Request, code: str = "", state: str = ""):
    service = svc(request)
    if not service.check_oauth_state("dropbox", state):
        raise HTTPException(400, "invalid OAuth state")
    await oauth_dropbox.exchange_code(service.settings, service.tokens, code)
    await service.maybe_start_pool()
    return RedirectResponse("/", status_code=303)


# -- auth: google -----------------------------------------------------------

@router.get("/auth/google/connect")
async def google_connect(request: Request):
    service = svc(request)
    state = service.new_oauth_state("google")
    verifier, challenge = oauth_google.generate_pkce()
    service._oauth_state["google_verifier"] = verifier
    url = oauth_google.build_authorize_url(service.settings, state, challenge)
    return RedirectResponse(url)


@router.get("/auth/google/callback")
async def google_callback(request: Request, code: str = "", state: str = ""):
    service = svc(request)
    if not service.check_oauth_state("google", state):
        raise HTTPException(400, "invalid OAuth state")
    verifier = service._oauth_state.get("google_verifier", "")
    await oauth_google.exchange_code(service.settings, service.tokens, code, verifier)
    await service.maybe_start_pool()
    return RedirectResponse("/", status_code=303)


@router.get("/auth/status", response_class=HTMLResponse)
async def auth_status(request: Request):
    service = svc(request)
    quota = None
    if service.auth.is_connected("google"):
        try:
            quota = await service.drive_quota()
        except Exception as exc:  # noqa: BLE001
            quota = {"error": str(exc)}
    return _templates.TemplateResponse(request, "_connection.html", {
        "dropbox_connected": service.auth.is_connected("dropbox"),
        "google_connected": service.auth.is_connected("google"),
        "drive_quota": quota,
    })


# -- discovery --------------------------------------------------------------

@router.post("/scan", response_class=HTMLResponse)
async def scan(request: Request):
    service = svc(request)
    await service.start_scan()
    return await scan_status(request)


@router.get("/scan/status", response_class=HTMLResponse)
async def scan_status(request: Request):
    service = svc(request)
    counts = await service.state_counts() if service.run else {}
    total_files = sum(counts.values())
    total_bytes = await service.total_discovered_bytes() if service.run else 0
    return _templates.TemplateResponse(request, "_scan.html", {
        "is_scanning": service.is_scanning,
        "summary": service.scan_summary,
        "counts": counts,
        "total_files": total_files,
        "total_bytes": total_bytes,
    })


# -- live activity bar ------------------------------------------------------

@router.get("/activity", response_class=HTMLResponse)
async def activity(request: Request):
    service = svc(request)
    act = await service.activity() if service.run else {"active": False}
    return _templates.TemplateResponse(request, "_activity.html", {"a": act})


# -- folder browser ---------------------------------------------------------

@router.get("/browse", response_class=HTMLResponse)
async def browse(request: Request, path: str = "", refresh: bool = False):
    service = svc(request)
    if service.run is None:
        raise HTTPException(409, "no active run")
    parent = path or None
    children = await service.browse_folder(parent, refresh=refresh)
    return _templates.TemplateResponse(request, "_browse.html", {
        "children": children, "parent": parent,
    })


# -- batches ----------------------------------------------------------------

@router.post("/batches/propose-folders", response_class=HTMLResponse)
async def propose_folders(request: Request):
    """Plan ALL batches needed to migrate the whole selection (the batches are
    proposed — nothing copies until 'Start all'). Splits a large selection into
    as many batches as the per-batch caps require."""
    service = svc(request)
    paths = await _selected_folders(request)
    if not paths:
        return await _render_batches(request, notice={
            "kind": "warn", "text": "No folders selected — check one or more folders first."})
    info = await service.plan_all_batches(paths)
    gib = 1073741824
    if info["batches"] == 0:
        if info.get("skipped_oversized"):
            text = (f"Nothing queued — {info['skipped_oversized']} file(s) exceed the "
                    "daily cap and were flagged; see failed files.")
        else:
            text = "Those folders have no files left to migrate (already queued or done)."
        return await _render_batches(request, notice={"kind": "warn", "text": text})
    text = (f"✓ Planned {info['batches']} batch(es): {info['queued_files']:,} files "
            f"({info['queued_bytes'] / gib:.2f} GiB) ready. "
            f"Review below, then click “Start all” to copy & verify everything.")
    if info.get("skipped_oversized"):
        text += f" ({info['skipped_oversized']} oversized file(s) skipped.)"
    return await _render_batches(request, notice={"kind": "ok", "text": text})


@router.post("/batches/start-all", response_class=HTMLResponse)
async def start_all(request: Request):
    service = svc(request)
    summary = await service.start_all()
    notice = ({"kind": "ok",
               "text": f"▶ Started {summary['batches']} batch(es) — copying & "
                       f"verifying {summary['files']:,} files. Watch the progress bar above."}
              if summary["batches"] else
              {"kind": "warn", "text": "No proposed batches to start. Select folders and plan a batch first."})
    return await _render_batches(request, notice=notice)


async def _selected_folders(request: Request) -> list[str]:
    """Read the selected folder paths from a JSON body or form field."""
    ctype = request.headers.get("content-type", "")
    if "application/json" in ctype:
        try:
            body = await request.json()
            return [str(p) for p in (body.get("folders") or []) if p]
        except Exception:
            return []
    form = await request.form()
    return [v for v in form.getlist("folders") if v]


@router.post("/batches/propose", response_class=HTMLResponse)
async def propose(request: Request):
    service = svc(request)
    batch = await service.propose_batch()
    notice = ({"kind": "ok",
               "text": f"✓ Proposed batch #{batch.seq}: {batch.file_count:,} files "
                       f"({batch.total_bytes / 1073741824:.2f} GiB)."}
              if batch else
              {"kind": "warn", "text": "Nothing left to propose."})
    return await _render_batches(request, notice=notice)


async def _render_batches(request: Request, notice: dict | None = None) -> HTMLResponse:
    service = svc(request)
    quota = await service.quota_status() if service.run else None
    return _templates.TemplateResponse(request, "_batch_list.html", {
        "batches": await service.list_batches() if service.run else [],
        "quota": quota,
        "is_paused": service.is_paused,
        "notice": notice,
    })


@router.get("/batches", response_class=HTMLResponse)
async def batches(request: Request):
    return await _render_batches(request)


@router.get("/batches/{batch_id}", response_class=HTMLResponse)
async def batch_detail(request: Request, batch_id: int):
    service = svc(request)
    batch = await service.store.get_batch(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    files = await service.store.files_in_batch(batch_id)
    return _templates.TemplateResponse(request, "_batch_detail.html", {
        "batch": batch, "files": files,
    })


@router.post("/batches/{batch_id}/approve-copy", response_class=HTMLResponse)
async def approve_copy(request: Request, batch_id: int):
    try:
        await svc(request).approve_copy(batch_id)
    except KeyError:
        raise HTTPException(404, "batch not found")
    return await batches(request)


@router.post("/batches/{batch_id}/cancel", response_class=HTMLResponse)
async def cancel_batch(request: Request, batch_id: int):
    try:
        await svc(request).cancel_batch(batch_id)
    except KeyError:
        raise HTTPException(404, "batch not found")
    return await batches(request)


@router.post("/batches/{batch_id}/resume", response_class=HTMLResponse)
async def resume_batch(request: Request, batch_id: int):
    try:
        summary = await svc(request).resume_batch(batch_id)
    except KeyError:
        raise HTTPException(404, "batch not found")
    n = summary["requeued"]
    notice = ({"kind": "ok",
               "text": f"↻ Resumed batch #{batch_id}: retrying {n:,} incomplete file(s)."}
              if n else
              {"kind": "warn", "text": f"Batch #{batch_id} has nothing to retry — all files are verified."})
    return await _render_batches(request, notice=notice)


@router.get("/batches/{batch_id}/verification", response_class=HTMLResponse)
async def verification(request: Request, batch_id: int):
    service = svc(request)
    batch = await service.store.get_batch(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    files = await service.store.files_in_batch(batch_id)
    from filemig.domain.states import FileState
    non_dead = [f for f in files if f.state != FileState.DEAD]
    verified = [f for f in non_dead if f.state == FileState.VERIFIED]
    gate_open = len(verified) == len(non_dead) and len(non_dead) > 0
    return _templates.TemplateResponse(request, "_verification.html", {
        "batch": batch, "files": files, "gate_open": gate_open,
        "verified_count": len(verified), "non_dead_count": len(non_dead),
    })


@router.post("/batches/{batch_id}/approve-delete", response_class=HTMLResponse)
async def approve_delete(request: Request, batch_id: int, confirm: str = Form("")):
    # Copy-and-verify-only build: source deletion is disabled at every layer.
    # This endpoint refuses outright (403) so nothing can ever be deleted via it.
    raise HTTPException(
        403, "Source deletion is disabled — this build is copy-and-verify only. "
        "Delete files from Dropbox yourself once you're satisfied.")


# -- controls ---------------------------------------------------------------

@router.post("/control/pause", response_class=HTMLResponse)
async def pause(request: Request):
    svc(request).pause()
    return await batches(request)


@router.post("/control/resume", response_class=HTMLResponse)
async def resume(request: Request):
    svc(request).resume()
    return await batches(request)


# -- reconcile --------------------------------------------------------------

@router.post("/reconcile", response_class=HTMLResponse)
async def run_reconcile(request: Request):
    service = svc(request)
    report = await service.reconcile()
    return _templates.TemplateResponse(request, "_reconcile.html", {"report": report})


# -- live status ------------------------------------------------------------

@router.get("/events")
async def events(request: Request):
    return event_stream(request)


@router.get("/fragments/overview", response_class=HTMLResponse)
async def overview(request: Request):
    """Polling fallback: the batch list + quota gauge in one fragment."""
    return await batches(request)
