"""FastAPI application factory.

The lifespan creates the singleton :class:`MigrationService`, runs crash
recovery, and (if both providers are connected) launches the worker pool. On
shutdown it drains workers and closes the database and HTTP client.
"""

from __future__ import annotations

import secrets
from contextlib import asynccontextmanager
from hmac import compare_digest

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from filemig.config import Settings, get_settings
from filemig.service import MigrationService
from filemig.web.routes import router

CSRF_HEADER = "X-Filemig-CSRF"
_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


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
    app.state.csrf_token = secrets.token_urlsafe(32)

    @app.middleware("http")
    async def require_csrf_token(request: Request, call_next):
        if request.method in _UNSAFE_METHODS:
            supplied = request.headers.get(CSRF_HEADER, "")
            if not supplied or not compare_digest(
                supplied, request.app.state.csrf_token
            ):
                response = PlainTextResponse("invalid CSRF token", status_code=403)
            else:
                response = await call_next(request)
        else:
            response = await call_next(request)
        response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    # Combined with loopback-only binding, this rejects DNS-rebinding hostnames.
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=[settings.host])
    app.include_router(router)
    return app
