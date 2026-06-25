"""HTTP route smoke + error-handling tests (no real OAuth needed).

These pin the behaviours verified during the live smoke test: the dashboard and
fragments render, OAuth connect redirects are well-formed, and batch actions on
a missing/invalid batch return clean 4xx codes rather than leaking a 500.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from filemig.app import create_app
from filemig.config import Settings


@pytest.fixture
def client(tmp_path):
    settings = Settings(data_dir=tmp_path,
                        dropbox_app_key="dbxkey", google_client_id="gid")
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def test_dashboard_renders(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "verify-before-delete" in r.text


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
    assert "files.content.write" in loc  # required to delete


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
