"""Shared pytest fixtures."""

from __future__ import annotations

import pytest_asyncio

from filemig.db.connection import Database
from filemig.db.store import Store
from filemig.domain import models


@pytest_asyncio.fixture
async def db(tmp_path):
    database = Database(tmp_path / "test.sqlite")
    await database.connect()
    yield database
    await database.close()


@pytest_asyncio.fixture
async def store(db):
    return Store(db)


def make_discovered_file(
    dbx_id: str = "id:abc",
    path: str = "/Photos/a.jpg",
    parent: str = "/photos",
    rev: str = "rev1",
    size: int = 1234,
    content_hash: str = "h" * 64,
    is_downloadable: bool = True,
) -> models.DiscoveredFile:
    return models.DiscoveredFile(
        dbx_id=dbx_id,
        path_lower=path.lower(),
        path_display=path,
        parent_path_lower=parent.lower(),
        rev=rev,
        size=size,
        content_hash=content_hash,
        is_downloadable=is_downloadable,
    )
