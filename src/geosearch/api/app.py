"""FastAPI application factory for the read-only GeoSearch Search API."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import Engine

from geosearch.api.config import (
    DEFAULT_DB_STATEMENT_TIMEOUT_MS,
    DEFAULT_HEALTH_CHECK_TIMEOUT_MS,
    ApiSettings,
    load_api_settings,
)
from geosearch.api.errors import register_exception_handlers
from geosearch.api.routes.health import router as health_router
from geosearch.api.routes.places import router as places_router
from geosearch.db.engine import create_db_engine


def create_app(
    *,
    settings: ApiSettings | None = None,
    engine: Engine | None = None,
    statement_timeout_ms: int | None = None,
    health_check_timeout_ms: int | None = None,
) -> FastAPI:
    """Create and configure the read-only GeoSearch FastAPI application.

    Design guarantees:
    - Does not run Alembic migrations on startup.
    - Does not ingest OSM files or mutate publication state.
    - Reuses a single SQLAlchemy Engine across requests (never creates an engine per request).
    - Disposes the engine cleanly on application shutdown when owned by the app.
    - When `engine` is injected (e.g. isolated test engine), the caller retains engine lifecycle
      ownership and `DATABASE_URL` is not read.
    """
    owns_engine = engine is None
    if engine is None:
        resolved_settings = settings if settings is not None else load_api_settings()
        resolved_engine = create_db_engine(
            resolved_settings.database_url,
            pool_size=resolved_settings.db_pool_size,
            max_overflow=resolved_settings.db_max_overflow,
            pool_timeout=resolved_settings.db_pool_timeout,
            connect_timeout=resolved_settings.db_connect_timeout,
        )
        resolved_statement_timeout = (
            statement_timeout_ms
            if statement_timeout_ms is not None
            else resolved_settings.db_statement_timeout_ms
        )
        resolved_health_timeout = (
            health_check_timeout_ms
            if health_check_timeout_ms is not None
            else resolved_settings.health_check_timeout_ms
        )
    else:
        resolved_engine = engine
        resolved_statement_timeout = (
            statement_timeout_ms
            if statement_timeout_ms is not None
            else (
                settings.db_statement_timeout_ms
                if settings is not None
                else DEFAULT_DB_STATEMENT_TIMEOUT_MS
            )
        )
        resolved_health_timeout = (
            health_check_timeout_ms
            if health_check_timeout_ms is not None
            else (
                settings.health_check_timeout_ms
                if settings is not None
                else DEFAULT_HEALTH_CHECK_TIMEOUT_MS
            )
        )

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if app_instance.state.owns_engine:
                app_instance.state.engine.dispose()

    app = FastAPI(
        title="GeoSearch API",
        version="0.1.0",
        description=(
            "Read-only REST API serving the currently published canonical OpenStreetMap "
            "POI dataset backed by PostgreSQL/PostGIS."
        ),
        lifespan=lifespan,
    )
    app.state.engine = resolved_engine
    app.state.owns_engine = owns_engine
    app.state.statement_timeout_ms = resolved_statement_timeout
    app.state.health_check_timeout_ms = resolved_health_timeout

    register_exception_handlers(app)
    app.include_router(health_router)
    app.include_router(places_router)

    return app
