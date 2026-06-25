"""FastAPI application factory.

The lifespan creates the singleton :class:`MigrationService`, runs crash
recovery, and (if both providers are connected) launches the worker pool. On
shutdown it drains workers and closes the database and HTTP client.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from filemig.config import Settings, get_settings
from filemig.service import MigrationService
from filemig.web.routes import router


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service = MigrationService(settings)
        app.state.service = service
        await service.startup()
        try:
            yield
        finally:
            await service.shutdown()

    app = FastAPI(title="filemig", lifespan=lifespan)
    app.include_router(router)
    return app
