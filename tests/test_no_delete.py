"""Proves the copy-and-verify-only guarantee: the app cannot delete from Dropbox.

The kill switch lives at the lowest layer (the Dropbox client), so even a direct
call raises before any network request — defense in depth above the
service/route guards.
"""

from __future__ import annotations

import httpx
import pytest

from filemig.clients import dropbox_client, oauth_dropbox
from filemig.clients.auth import AuthManager
from filemig.clients.dropbox_client import DropboxClient
from filemig.clients.tokens import TokenStore
from filemig.config import Settings
from filemig.domain.errors import FatalError


def test_kill_switch_is_off_by_default():
    assert dropbox_client.DROPBOX_DELETION_ENABLED is False
    assert "files.content.write" not in oauth_dropbox.SCOPES


@pytest.fixture
def client(tmp_path):
    settings = Settings(data_dir=tmp_path)
    store = TokenStore(settings.secrets_dir)
    store.save_refresh_token("dropbox", "x")
    auth = AuthManager(settings, store)
    return DropboxClient(auth, httpx.AsyncClient())


async def test_delete_batch_raises_before_any_api_call(client):
    # No respx mock is registered, so if it tried to hit the network the test
    # would error differently; instead it must raise FatalError immediately.
    with pytest.raises(FatalError, match="disabled"):
        await client.delete_batch([{"path": "id:1", "parent_rev": "r"}])


async def test_delete_batch_check_raises(client):
    with pytest.raises(FatalError, match="disabled"):
        await client.delete_batch_check("job-1")
