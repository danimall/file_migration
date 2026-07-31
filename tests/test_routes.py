"""HTTP route smoke + error-handling tests (no real OAuth needed).

These pin the behaviours verified during the live smoke test: the dashboard and
fragments render, OAuth connect redirects are well-formed, and batch actions on
a missing/invalid batch return clean 4xx codes rather than leaking a 500.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from filemig.app import CSRF_HEADER, create_app
from filemig.config import Settings


@pytest.fixture
def client(tmp_path):
    settings = Settings(data_dir=tmp_path,
                        dropbox_app_key="dbxkey", google_client_id="gid")
    app = create_app(settings)
    with TestClient(app, base_url="http://127.0.0.1:8765") as c:
        c.headers.update({CSRF_HEADER: app.state.csrf_token})
        yield c


def test_dashboard_renders(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "copy-and-verify only" in r.text
    assert r.headers["x-frame-options"] == "DENY"
    assert r.headers["content-security-policy"] == "frame-ancestors 'none'"


@pytest.mark.parametrize("path", ["/auth/status", "/batches", "/scan/status",
                                  "/fragments/overview", "/activity"])
def test_fragments_render(client, path):
    assert client.get(path).status_code == 200


def test_activity_reports_idle_when_no_work(client):
    r = client.get("/activity")
    assert r.status_code == 200
    assert "idle" in r.text.lower()


def test_dropbox_connect_redirect_is_offline(client):
    r = client.get("/auth/dropbox/connect", follow_redirects=False)
    assert r.status_code == 307
    loc = r.headers["location"]
    assert "dropbox.com/oauth2/authorize" in loc
    assert "token_access_type=offline" in loc  # required for a refresh token
    assert "files.metadata.read" in loc
    assert "files.content.read" in loc
    assert "files.content.write" not in loc  # copy-only build stays read-only


def test_google_connect_redirect_is_pkce_consent(client):
    r = client.get("/auth/google/connect", follow_redirects=False)
    assert r.status_code == 307
    loc = r.headers["location"]
    assert "accounts.google.com/o/oauth2/v2/auth" in loc
    assert "code_challenge_method=S256" in loc
    assert "prompt=consent" in loc
    assert "drive.file" in loc
    assert "access_type=offline" not in loc  # NOT a native-app param


def test_missing_batch_actions_return_404_not_500(client):
    assert client.get("/batches/999").status_code == 404
    assert client.get("/batches/999/verification").status_code == 404
    assert client.post("/batches/999/approve-copy").status_code == 404
    assert client.post("/batches/999/cancel").status_code == 404


def test_approve_delete_is_disabled(client):
    # Copy-and-verify-only build: the delete endpoint refuses outright (403),
    # regardless of confirmation or batch id.
    assert client.post("/batches/1/approve-delete",
                       data={"confirm": "yes"}).status_code == 403
    assert client.post("/batches/999/approve-delete").status_code == 403


def test_mutating_routes_require_csrf(client):
    assert client.post("/scan", headers={CSRF_HEADER: ""}).status_code == 403


def test_untrusted_host_is_rejected(client):
    assert client.get("/", headers={"host": "attacker.example"}).status_code == 400


def test_remote_binding_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="remote binding is disabled"):
        Settings(data_dir=tmp_path, host="0.0.0.0")
