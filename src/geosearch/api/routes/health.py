"""Health and readiness endpoint for the GeoSearch API."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError

from geosearch.api.dependencies import (
    get_engine,
    get_health_check_timeout_ms,
    read_committed_readonly,
)
from geosearch.api.schemas.common import HealthResponse
from geosearch.api.services.place_queries import check_database_health

logger = logging.getLogger(__name__)

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health and database readiness",
    description=(
        "Checks API process liveness and PostgreSQL database readiness using a "
        "lightweight read-only query. Returns 200 when healthy and 503 when the "
        "database dependency is unavailable."
    ),
    responses={
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": HealthResponse,
            "description": "Database dependency is unavailable or timed out.",
        }
    },
)
def get_health(
    engine: Engine = Depends(get_engine),
    timeout_ms: int = Depends(get_health_check_timeout_ms),
) -> HealthResponse | JSONResponse:
    """Verify lightweight read-only PostgreSQL connectivity without leaking internals."""
    try:
        with read_committed_readonly(engine, statement_timeout_ms=timeout_ms) as conn:
            healthy = check_database_health(conn)
    except SQLAlchemyError:
        logger.warning("Health check failed: database dependency unavailable.")
        healthy = False
    except Exception:
        logger.exception("Health check failed due to unexpected error.")
        healthy = False

    if not healthy:
        payload = HealthResponse(
            status="degraded",
            service="geosearch-api",
            database="unavailable",
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=payload.model_dump(),
        )

    return HealthResponse(
        status="ok",
        service="geosearch-api",
        database="ok",
    )
