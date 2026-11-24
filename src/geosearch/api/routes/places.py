"""Place search, nearby, and detail routes for the read-only GeoSearch REST API.

IMPORTANT ROUTE ORDERING:
`GET /api/v1/places/nearby` MUST remain registered BEFORE `GET /api/v1/places/{id}` so
FastAPI resolves `/nearby` to the geographic endpoint rather than matching `{id}`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request, status
from sqlalchemy import Connection, Engine

from geosearch.api.dependencies import (
    get_engine,
    get_statement_timeout_ms,
    read_committed_readonly,
    repeatable_read_readonly,
)
from geosearch.api.errors import PlaceNotFoundError
from geosearch.api.schemas.common import ErrorResponse
from geosearch.api.schemas.places import (
    NearbyPlacesResponse,
    PlaceCategory,
    PlaceDetail,
    PlaceListResponse,
)
from geosearch.api.services.place_queries import (
    DEFAULT_LIMIT,
    DEFAULT_PAGE,
    DEFAULT_RADIUS_M,
    MAX_LIMIT,
    MAX_PAGE,
    MAX_QUERY_LENGTH,
    MAX_RADIUS_M,
    find_nearby_active_places,
    get_active_place_by_id,
    list_active_places,
    normalize_categories,
    normalize_search_query,
    validate_nearby_parameters,
)
from geosearch.ingestion.validation import MAX_PG_INT64

router = APIRouter(prefix="/api/v1/places", tags=["places"])


def _get_between_queries_hook(request: Request) -> Callable[[Connection], None] | None:
    """Internal test-only hook stored on `app.state` (never exposed as an HTTP parameter)."""
    hook: Callable[[Connection], None] | None = getattr(
        request.app.state, "on_between_queries", None
    )
    return hook


@router.get(
    "",
    response_model=PlaceListResponse,
    summary="Search and list active canonical places",
    description=(
        "Returns a bounded, deterministically ordered page of active canonical places "
        "from the currently published dataset. Supports Vietnamese accent-insensitive "
        "name search (`q`), filtering by one or more approved MVP categories (`category`), "
        "and offset-based pagination (`page`, `limit`). Executes count and page queries "
        "within a single `REPEATABLE READ READ ONLY` transaction snapshot."
    ),
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "model": ErrorResponse,
            "description": "Invalid query parameter, unsupported category, or out-of-range pagination.",
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Database dependency is unavailable.",
        },
        status.HTTP_504_GATEWAY_TIMEOUT: {
            "model": ErrorResponse,
            "description": "Database query timed out.",
        },
    },
)
def search_places(
    request: Request,
    q: Annotated[
        str | None,
        Query(
            max_length=MAX_QUERY_LENGTH,
            description=(
                "Optional Vietnamese accent-insensitive substring search applied against "
                "normalized canonical place names (`name_search`)."
            ),
        ),
    ] = None,
    category: Annotated[
        list[PlaceCategory] | None,
        Query(
            description=(
                "Optional approved category filter. Repeat parameter to match any of "
                "multiple approved categories (`cafe`, `restaurant`, `atm`, `pharmacy`, "
                "`supermarket`)."
            ),
        ),
    ] = None,
    page: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_PAGE,
            description="1-based page number (default: 1).",
        ),
    ] = DEFAULT_PAGE,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_LIMIT,
            description="Maximum items per page (default: 20, max: 100).",
        ),
    ] = DEFAULT_LIMIT,
    engine: Engine = Depends(get_engine),
    statement_timeout_ms: int = Depends(get_statement_timeout_ms),
) -> PlaceListResponse:
    """List and search active canonical places inside one REPEATABLE READ READ ONLY snapshot."""
    normalized_q = normalize_search_query(q)
    normalized_categories = normalize_categories(category)
    with repeatable_read_readonly(engine, statement_timeout_ms=statement_timeout_ms) as conn:
        return list_active_places(
            conn,
            q=normalized_q,
            categories=normalized_categories,
            page=page,
            limit=limit,
            on_between_queries=_get_between_queries_hook(request),
        )


@router.get(
    "/nearby",
    response_model=NearbyPlacesResponse,
    summary="Find active canonical places within a geographic radius",
    description=(
        "Searches active canonical places within `radius_m` meters of the WGS84 point "
        "(`lon`, `lat`) using PostGIS `GEOGRAPHY(Point, 4326)` spheroidal `ST_DWithin` "
        "and `ST_Distance`. Results are sorted closest-first with deterministic `id` "
        "tie-breaking. Executes count and page queries inside one `REPEATABLE READ READ ONLY` "
        "transaction snapshot."
    ),
    responses={
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "model": ErrorResponse,
            "description": "Invalid coordinates, radius, category, or pagination parameter.",
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Database dependency is unavailable.",
        },
        status.HTTP_504_GATEWAY_TIMEOUT: {
            "model": ErrorResponse,
            "description": "Database query timed out.",
        },
    },
)
def search_nearby_places(
    request: Request,
    lon: Annotated[
        float,
        Query(
            ge=-180.0,
            le=180.0,
            allow_inf_nan=False,
            description="WGS84 query center longitude (X) in degrees within [-180, 180].",
        ),
    ],
    lat: Annotated[
        float,
        Query(
            ge=-90.0,
            le=90.0,
            allow_inf_nan=False,
            description="WGS84 query center latitude (Y) in degrees within [-90, 90].",
        ),
    ],
    radius_m: Annotated[
        float,
        Query(
            gt=0.0,
            le=MAX_RADIUS_M,
            allow_inf_nan=False,
            description=(
                "Geographic search radius in meters (default: 1000, max: 50000; "
                "WGS84 spheroidal distance)."
            ),
        ),
    ] = DEFAULT_RADIUS_M,
    q: Annotated[
        str | None,
        Query(
            max_length=MAX_QUERY_LENGTH,
            description="Optional Vietnamese accent-insensitive place name filter.",
        ),
    ] = None,
    category: Annotated[
        list[PlaceCategory] | None,
        Query(
            description=(
                "Optional approved category filter. Repeat parameter to match any of "
                "multiple approved categories (`cafe`, `restaurant`, `atm`, `pharmacy`, "
                "`supermarket`)."
            ),
        ),
    ] = None,
    page: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_PAGE,
            description="1-based page number (default: 1).",
        ),
    ] = DEFAULT_PAGE,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=MAX_LIMIT,
            description="Maximum items per page (default: 20, max: 100).",
        ),
    ] = DEFAULT_LIMIT,
    engine: Engine = Depends(get_engine),
    statement_timeout_ms: int = Depends(get_statement_timeout_ms),
) -> NearbyPlacesResponse:
    """Find active canonical places within radius using PostGIS WGS84 spheroid distance."""
    validated_lon, validated_lat, validated_radius = validate_nearby_parameters(
        longitude=lon,
        latitude=lat,
        radius_m=radius_m,
    )
    normalized_q = normalize_search_query(q)
    normalized_categories = normalize_categories(category)
    with repeatable_read_readonly(engine, statement_timeout_ms=statement_timeout_ms) as conn:
        return find_nearby_active_places(
            conn,
            longitude=validated_lon,
            latitude=validated_lat,
            radius_m=validated_radius,
            q=normalized_q,
            categories=normalized_categories,
            page=page,
            limit=limit,
            on_between_queries=_get_between_queries_hook(request),
        )


@router.get(
    "/{id}",
    response_model=PlaceDetail,
    summary="Retrieve active canonical place details by ID",
    description=(
        "Returns public details for a single active canonical place identified by its "
        "stable internal `places.id`. Returns 404 when the place does not exist, is "
        "inactive, or no publication is currently active. Executes as a single SQL "
        "statement under `READ COMMITTED READ ONLY`."
    ),
    responses={
        status.HTTP_404_NOT_FOUND: {
            "model": ErrorResponse,
            "description": "Active canonical place not found.",
        },
        status.HTTP_422_UNPROCESSABLE_CONTENT: {
            "model": ErrorResponse,
            "description": "Invalid place ID format or range.",
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "model": ErrorResponse,
            "description": "Database dependency is unavailable.",
        },
        status.HTTP_504_GATEWAY_TIMEOUT: {
            "model": ErrorResponse,
            "description": "Database query timed out.",
        },
    },
)
def get_place_detail(
    id: Annotated[
        int,
        Path(
            ge=1,
            le=MAX_PG_INT64,
            description="Stable internal canonical place ID (positive 64-bit integer).",
        ),
    ],
    engine: Engine = Depends(get_engine),
    statement_timeout_ms: int = Depends(get_statement_timeout_ms),
) -> PlaceDetail:
    """Retrieve one active place by stable internal ID in a single READ COMMITTED statement."""
    with read_committed_readonly(engine, statement_timeout_ms=statement_timeout_ms) as conn:
        place = get_active_place_by_id(conn, place_id=id)
    if place is None:
        raise PlaceNotFoundError()
    return place
