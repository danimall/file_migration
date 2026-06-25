"""SQLite connection management.

A single SQLite file is the source of truth. SQLite permits only one writer at
a time, so we keep exactly one connection and serialize all access through an
``asyncio.Lock``. The blocking sqlite calls run in a worker thread so the event
loop is never stalled.

WAL mode + ``synchronous=NORMAL`` gives durable, crash-safe writes with good
throughput for our small, frequent state updates.
"""

from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Iterable, Sequence
from importlib import resources
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA foreign_keys=ON")


def _load_schema_sql() -> str:
    return resources.files("filemig.db").joinpath("schema.sql").read_text("utf-8")


class Database:
    """Owns the single writer connection and serializes all access."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._conn: sqlite3.Connection | None = None

    @property
    def path(self) -> Path:
        return self._path

    async def connect(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = await asyncio.to_thread(self._open)
        await self._migrate()

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._path,
            check_same_thread=False,  # guarded by our asyncio.Lock instead
            isolation_level=None,  # autocommit; we manage transactions explicitly
        )
        conn.row_factory = sqlite3.Row
        _apply_pragmas(conn)
        return conn

    async def close(self) -> None:
        if self._conn is not None:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    @property
    def _c(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database.connect() was not called")
        return self._conn

    # -- migrations ---------------------------------------------------------

    async def _migrate(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._migrate_sync)

    def _migrate_sync(self) -> None:
        version = self._c.execute("PRAGMA user_version").fetchone()[0]
        if version < SCHEMA_VERSION:
            self._c.executescript(_load_schema_sql())
            self._c.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    # -- queries ------------------------------------------------------------

    async def fetchone(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        async with self._lock:
            return await asyncio.to_thread(self._fetchone_sync, sql, params)

    def _fetchone_sync(self, sql: str, params: Sequence[Any]) -> sqlite3.Row | None:
        return self._c.execute(sql, params).fetchone()

    async def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        async with self._lock:
            return await asyncio.to_thread(self._fetchall_sync, sql, params)

    def _fetchall_sync(self, sql: str, params: Sequence[Any]) -> list[sqlite3.Row]:
        return self._c.execute(sql, params).fetchall()

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Run a single write statement; returns lastrowid."""
        async with self._lock:
            return await asyncio.to_thread(self._execute_sync, sql, params)

    def _execute_sync(self, sql: str, params: Sequence[Any]) -> int:
        cur = self._c.execute(sql, params)
        return int(cur.lastrowid or 0)

    async def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._executemany_sync, sql, list(seq))

    def _executemany_sync(self, sql: str, rows: list[Sequence[Any]]) -> None:
        self._c.executemany(sql, rows)

    async def transaction(self, fn) -> Any:
        """Run ``fn(conn)`` inside a single exclusive transaction.

        ``fn`` is a synchronous callable taking the sqlite3 connection. The
        whole callback runs under the lock in a worker thread, so it can issue
        multiple statements atomically (e.g. a state transition plus its event
        row).
        """
        async with self._lock:
            return await asyncio.to_thread(self._transaction_sync, fn)

    def _transaction_sync(self, fn) -> Any:
        try:
            self._c.execute("BEGIN IMMEDIATE")
            result = fn(self._c)
            self._c.execute("COMMIT")
            return result
        except Exception:
            self._c.execute("ROLLBACK")
            raise
