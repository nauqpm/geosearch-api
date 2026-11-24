"""Read-only database query services for the GeoSearch REST API.

All queries:
- Operate exclusively on active canonical places (`places.is_active = true`) paired with
  the single active publication slot (`import_runs.active_slot = 1 AND status = 'PUBLISHED'`).
- Use parameterized SQL expressions (never concatenating user input into SQL).
- Reuse M1 coordinate validation (`validate_wgs84_coordinates`) and M2 canonical
  Vietnamese normalization (`normalize_name_search`, `validate_category`).
- Never introduce writes, migrations, or unapproved database indexes.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from typing import Any, Final

from sqlalchemy import Connection, text

from geosearch.api.errors import InvalidQueryParameterError
from geosearch.api.schemas.common import Coordinates
from geosearch.api.schemas.places import (
    NearbyPlaceItem,
    NearbyPlacesResponse,
    PlaceDetail,
    PlaceListResponse,
    PlaceSummary,
)
from geosearch.db.types import InvalidCoordinateError, validate_wgs84_coordinates
from geosearch.ingestion.validation import (
    ValidationError as DomainValidationError,
    normalize_name_search,
    validate_category,
)

# Conservative implementation defaults (documented separately from approved architecture baseline).
DEFAULT_PAGE: Final[int] = 1
MAX_PAGE: Final[int] = 100_000
DEFAULT_LIMIT: Final[int] = 20
MAX_LIMIT: Final[int] = 100
MAX_QUERY_LENGTH: Final[int] = 200
DEFAULT_RADIUS_M: Final[float] = 1_000.0
MAX_RADIUS_M: Final[float] = 50_000.0


def check_database_health(conn: Connection) -> bool:
    """Run a lightweight, non-mutating PostgreSQL connectivity check."""
    result = conn.execute(text("SELECT 1")).scalar_one()
    return result == 1


def escape_like_literal(value: str, *, escape_char: str = "\\") -> str:
    """Escape SQL LIKE wildcard characters (`%`, `_`) and the escape character literally."""
    return (
        value.replace(escape_char, escape_char + escape_char)
        .replace("%", escape_char + "%")
        .replace("_", escape_char + "_")
    )


def normalize_search_query(raw_query: str | None) -> str | None:
    """Normalize an optional user search string using M2's canonical Vietnamese normalizer.

    Returns `None` when omitted, blank, or reduced to empty text after removing combining
    marks / invisible format characters (treated as no text filter).
    Rejects NUL characters and queries exceeding `MAX_QUERY_LENGTH`.
    """
    if raw_query is None:
        return None
    if len(raw_query) > MAX_QUERY_LENGTH:
        raise InvalidQueryParameterError(
            f"Search query must not exceed {MAX_QUERY_LENGTH} characters.",
            loc=["query", "q"],
            error_type="string_too_long",
        )
    try:
        normalized = normalize_name_search(raw_query)
    except DomainValidationError as exc:
        raise InvalidQueryParameterError(
            str(exc),
            loc=["query", "q"],
            error_type="value_error",
        ) from exc
    return normalized if normalized else None


def normalize_categories(categories: Sequence[str] | None) -> tuple[str, ...] | None:
    """Validate optional category filters strictly against the five approved MVP categories."""
    if not categories:
        return None
    validated: list[str] = []
    for idx, cat in enumerate(categories):
        try:
            validated.append(validate_category(cat))
        except DomainValidationError as exc:
            raise InvalidQueryParameterError(
                str(exc),
                loc=["query", "category", str(idx)],
                error_type="literal_error",
            ) from exc
    return tuple(sorted(set(validated)))


def _build_place_filter_clauses(
    *,
    normalized_q: str | None,
    normalized_categories: tuple[str, ...] | None,
) -> tuple[list[str], dict[str, Any]]:
    """Construct parameterized WHERE predicates for active canonical places."""
    clauses: list[str] = ["p.is_active = true"]
    params: dict[str, Any] = {}

    if normalized_q is not None:
        clauses.append(r"p.name_search LIKE :name_pattern ESCAPE '\'")
        params["name_pattern"] = f"%{escape_like_literal(normalized_q)}%"

    if normalized_categories is not None:
        clauses.append("p.category = ANY(:categories)")
        params["categories"] = list(normalized_categories)

    return clauses, params


def _row_to_place_summary(row: Any) -> PlaceSummary:
    return PlaceSummary(
        id=int(row.id),
        osm_type=str(row.osm_type),  # type: ignore[arg-type]
        osm_id=int(row.osm_id),
        name=row.name,
        category=str(row.category),  # type: ignore[arg-type]
        address=row.address,
        longitude=float(row.longitude),
        latitude=float(row.latitude),
    )


def get_active_place_by_id(
    conn: Connection,
    *,
    place_id: int,
) -> PlaceDetail | None:
    """Fetch one active canonical place by stable internal ID in a single SQL statement.

    Designed for execution under `READ COMMITTED, READ ONLY`: joins the single active
    publication slot (`import_runs.active_slot = 1 AND status = 'PUBLISHED'`) and requires
    `places.is_active = true`.
    """
    row = conn.execute(
        text(
            """
            SELECT
                p.id,
                p.osm_type,
                p.osm_id,
                p.name,
                p.category,
                p.address,
                ST_X(p.location::geometry) AS longitude,
                ST_Y(p.location::geometry) AS latitude,
                p.osm_version,
                p.osm_timestamp
            FROM places p
            INNER JOIN import_runs r
                ON r.active_slot = 1
               AND r.status = 'PUBLISHED'
            WHERE p.id = :place_id
              AND p.is_active = true
            """
        ),
        {"place_id": place_id},
    ).one_or_none()

    if row is None:
        return None

    return PlaceDetail(
        id=int(row.id),
        osm_type=str(row.osm_type),  # type: ignore[arg-type]
        osm_id=int(row.osm_id),
        name=row.name,
        category=str(row.category),  # type: ignore[arg-type]
        address=row.address,
        longitude=float(row.longitude),
        latitude=float(row.latitude),
        osm_version=int(row.osm_version) if row.osm_version is not None else None,
        osm_timestamp=row.osm_timestamp,
    )


def list_active_places(
    conn: Connection,
    *,
    q: str | None = None,
    categories: Sequence[str] | None = None,
    page: int = DEFAULT_PAGE,
    limit: int = DEFAULT_LIMIT,
    on_between_queries: Callable[[Connection], None] | None = None,
) -> PlaceListResponse:
    """List and search active canonical places with deterministic pagination.

    Executes two SQL statements (`COUNT(*)` and paginated `SELECT`) that MUST run inside
    one `REPEATABLE READ, READ ONLY` PostgreSQL transaction so `total` and `items` always
    reflect the exact same committed publication snapshot.
    """
    normalized_q = normalize_search_query(q)
    normalized_categories = normalize_categories(categories)
    clauses, params = _build_place_filter_clauses(
        normalized_q=normalized_q,
        normalized_categories=normalized_categories,
    )
    where_sql = " AND ".join(clauses)
    offset = (page - 1) * limit

    total = int(
        conn.execute(
            text(
                f"""
                SELECT count(*)
                FROM places p
                INNER JOIN import_runs r
                    ON r.active_slot = 1
                   AND r.status = 'PUBLISHED'
                WHERE {where_sql}
                """
            ),
            params,
        ).scalar_one()
    )

    if on_between_queries is not None:
        on_between_queries(conn)

    page_params = {**params, "limit": limit, "offset": offset}
    rows = conn.execute(
        text(
            f"""
            SELECT
                p.id,
                p.osm_type,
                p.osm_id,
                p.name,
                p.category,
                p.address,
                ST_X(p.location::geometry) AS longitude,
                ST_Y(p.location::geometry) AS latitude
            FROM places p
            INNER JOIN import_runs r
                ON r.active_slot = 1
               AND r.status = 'PUBLISHED'
            WHERE {where_sql}
            ORDER BY p.name_search ASC NULLS LAST, p.id ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        page_params,
    ).fetchall()

    items = [_row_to_place_summary(row) for row in rows]
    has_more = (offset + len(items)) < total

    return PlaceListResponse(
        items=items,
        total=total,
        page=page,
        limit=limit,
        has_more=has_more,
    )


def validate_nearby_parameters(
    *,
    longitude: float,
    latitude: float,
    radius_m: float,
) -> tuple[float, float, float]:
    """Validate WGS84 coordinates (reusing M1 validator) and positive bounded radius in meters."""
    try:
        validated_lon, validated_lat = validate_wgs84_coordinates(longitude, latitude)
    except InvalidCoordinateError as exc:
        raise InvalidQueryParameterError(
            str(exc),
            loc=["query", "coordinates"],
            error_type="value_error",
        ) from exc

    if isinstance(radius_m, bool) or not isinstance(radius_m, (int, float)):
        raise InvalidQueryParameterError(
            "radius_m must be a finite positive number in meters.",
            loc=["query", "radius_m"],
            error_type="float_type",
        )
    resolved_radius = float(radius_m)
    if not math.isfinite(resolved_radius) or resolved_radius <= 0.0:
        raise InvalidQueryParameterError(
            "radius_m must be finite and strictly greater than 0.",
            loc=["query", "radius_m"],
            error_type="greater_than",
        )
    if resolved_radius > MAX_RADIUS_M:
        raise InvalidQueryParameterError(
            f"radius_m must not exceed {MAX_RADIUS_M:g} meters.",
            loc=["query", "radius_m"],
            error_type="less_than_equal",
        )
    return validated_lon, validated_lat, resolved_radius


def find_nearby_active_places(
    conn: Connection,
    *,
    longitude: float,
    latitude: float,
    radius_m: float = DEFAULT_RADIUS_M,
    q: str | None = None,
    categories: Sequence[str] | None = None,
    page: int = DEFAULT_PAGE,
    limit: int = DEFAULT_LIMIT,
    on_between_queries: Callable[[Connection], None] | None = None,
) -> NearbyPlacesResponse:
    """Find active canonical places within `radius_m` meters using PostGIS WGS84 spheroid.

    Distance semantics:
    - Query point: `ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography` (X=lon, Y=lat).
    - Radius membership: `ST_DWithin(p.location, query_point, :radius_m, true)`.
    - Returned distance: `ST_Distance(p.location, query_point, true)` in meters (WGS84 spheroid).
    - Ordering: closest-first (`distance_m ASC`) with deterministic tie-breaking (`p.id ASC`).
    - Executes `COUNT(*)` and paginated `SELECT` inside one `REPEATABLE READ, READ ONLY`
      transaction snapshot so `total` and `items` always describe the exact same publication.
    """
    validated_lon, validated_lat, validated_radius = validate_nearby_parameters(
        longitude=longitude,
        latitude=latitude,
        radius_m=radius_m,
    )
    normalized_q = normalize_search_query(q)
    normalized_categories = normalize_categories(categories)

    clauses, params = _build_place_filter_clauses(
        normalized_q=normalized_q,
        normalized_categories=normalized_categories,
    )
    query_point_sql = "ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography"
    clauses.append(f"ST_DWithin(p.location, {query_point_sql}, :radius_m, true)")
    params.update(
        {
            "lon": validated_lon,
            "lat": validated_lat,
            "radius_m": validated_radius,
        }
    )
    where_sql = " AND ".join(clauses)
    offset = (page - 1) * limit

    total = int(
        conn.execute(
            text(
                f"""
                SELECT count(*)
                FROM places p
                INNER JOIN import_runs r
                    ON r.active_slot = 1
                   AND r.status = 'PUBLISHED'
                WHERE {where_sql}
                """
            ),
            params,
        ).scalar_one()
    )

    if on_between_queries is not None:
        on_between_queries(conn)

    page_params = {**params, "limit": limit, "offset": offset}
    rows = conn.execute(
        text(
            f"""
            SELECT
                p.id,
                p.osm_type,
                p.osm_id,
                p.name,
                p.category,
                p.address,
                ST_X(p.location::geometry) AS longitude,
                ST_Y(p.location::geometry) AS latitude,
                ST_Distance(p.location, {query_point_sql}, true) AS distance_m
            FROM places p
            INNER JOIN import_runs r
                ON r.active_slot = 1
               AND r.status = 'PUBLISHED'
            WHERE {where_sql}
            ORDER BY distance_m ASC, p.id ASC
            LIMIT :limit OFFSET :offset
            """
        ),
        page_params,
    ).fetchall()

    items = [
        NearbyPlaceItem(
            id=int(row.id),
            osm_type=str(row.osm_type),  # type: ignore[arg-type]
            osm_id=int(row.osm_id),
            name=row.name,
            category=str(row.category),  # type: ignore[arg-type]
            address=row.address,
            longitude=float(row.longitude),
            latitude=float(row.latitude),
            distance_m=float(row.distance_m),
        )
        for row in rows
    ]
    has_more = (offset + len(items)) < total

    return NearbyPlacesResponse(
        items=items,
        total=total,
        page=page,
        limit=limit,
        has_more=has_more,
        center=Coordinates(longitude=validated_lon, latitude=validated_lat),
        radius_m=validated_radius,
    )
