"""Lightweight PostGIS SQLAlchemy column types and WGS84 coordinate helpers."""

from __future__ import annotations

import math
from collections.abc import Callable
from typing import Any, Literal

from sqlalchemy import Connection, func, text
from sqlalchemy.dialects.postgresql.base import ischema_names
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.types import UserDefinedType


class InvalidCoordinateError(ValueError):
    """Raised when WGS84 longitude/latitude coordinates are non-finite or out of range."""


def validate_wgs84_coordinates(longitude: float, latitude: float) -> tuple[float, float]:
    """Validate finiteness and WGS84 bounds before constructing a PostGIS point."""
    if isinstance(longitude, bool) or isinstance(latitude, bool):
        raise InvalidCoordinateError("Coordinates must be numeric, not boolean.")

    lon = float(longitude)
    lat = float(latitude)

    if not math.isfinite(lon) or not math.isfinite(lat):
        raise InvalidCoordinateError(
            f"Coordinates must be finite numbers, got lon={longitude!r}, lat={latitude!r}."
        )
    if not (-180.0 <= lon <= 180.0):
        raise InvalidCoordinateError(
            f"Longitude must be within [-180.0, 180.0], got {lon!r}."
        )
    if not (-90.0 <= lat <= 90.0):
        raise InvalidCoordinateError(
            f"Latitude must be within [-90.0, 90.0], got {lat!r}."
        )
    return lon, lat


def _canonical_geom_type(geometry_type: str) -> str:
    normalized = geometry_type.strip().upper()
    mapping = {
        "POINT": "Point",
        "MULTIPOLYGON": "MultiPolygon",
        "POLYGON": "Polygon",
        "GEOMETRY": "Geometry",
    }
    return mapping.get(normalized, geometry_type.strip())


class _PostGISSpatialType(UserDefinedType[Any]):
    """Base SQLAlchemy UserDefinedType for PostGIS geometry and geography columns.

    When SQLAlchemy reflects an unconstrained PostGIS column such as ``geometry``,
    ``geometry(Point)``, or ``geography``, PostgreSQL omits the SRID modifier in
    ``format_type()``. Defaulting ``srid`` to ``0`` preserves the distinction
    between unconstrained SRID 0 and an explicit SRID 4326 typmod so schema
    drift detection flags missing or mismatched SRIDs.
    """

    cache_ok = True
    spatial_kind: str = ""

    def __init__(self, geometry_type: str = "Geometry", srid: int | str = 0) -> None:
        self.geometry_type = _canonical_geom_type(geometry_type)
        self.srid = int(srid)

    def get_col_spec(self, **kw: Any) -> str:
        if self.geometry_type == "Geometry" and self.srid == 0:
            return self.spatial_kind
        return f"{self.spatial_kind}({self.geometry_type},{self.srid})"

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.geometry_type!r}, {self.srid!r})"

    @property
    def spatial_signature(self) -> tuple[str, str, int]:
        """Normalized tuple used for schema reflection and Alembic drift comparison."""
        return (self.spatial_kind, self.geometry_type.upper(), self.srid)


class PostGISGeography(_PostGISSpatialType):
    """PostgreSQL/PostGIS GEOGRAPHY(GeometryType,SRID) column type."""

    cache_ok = True
    spatial_kind = "GEOGRAPHY"


class PostGISGeometry(_PostGISSpatialType):
    """PostgreSQL/PostGIS GEOMETRY(GeometryType,SRID) column type."""

    cache_ok = True
    spatial_kind = "GEOMETRY"


# Register PostGIS types so SQLAlchemy Inspector and Alembic reflect them cleanly.
ischema_names["geography"] = PostGISGeography
ischema_names["geometry"] = PostGISGeometry

# Fallback PostGIS extension tables/views in public schema for offline mode.
POSTGIS_IGNORED_TABLES: frozenset[str] = frozenset(
    {
        "spatial_ref_sys",
        "geography_columns",
        "geometry_columns",
        "raster_columns",
        "raster_overviews",
    }
)

POSTGIS_IGNORED_SCHEMAS: frozenset[str] = frozenset(
    {"tiger", "tiger_data", "topology"}
)

POSTGIS_IGNORED_TABLE_REFS: frozenset[tuple[str, str]] = frozenset(
    ("public", table_name) for table_name in POSTGIS_IGNORED_TABLES
)


def get_default_schema(connection: Connection) -> str:
    """Return the active default schema for the PostgreSQL connection."""
    schema = connection.execute(text("SELECT current_schema()")).scalar_one_or_none()
    return str(schema) if schema else "public"


def get_extension_managed_tables(connection: Connection) -> frozenset[tuple[str, str]]:
    """Return schema-qualified (schema, name) pairs owned by extensions or PostGIS helper schemas."""
    rows = connection.execute(
        text(
            """
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p', 'v', 'm')
              AND (
                  n.nspname = ANY(:ignored_schemas)
                  OR EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.objid = c.oid
                        AND d.deptype = 'e'
                  )
              )
            """
        ),
        {"ignored_schemas": list(POSTGIS_IGNORED_SCHEMAS)},
    ).fetchall()
    return frozenset((str(schema), str(relname)) for schema, relname in rows)


def get_visible_extension_managed_tables(connection: Connection) -> frozenset[str]:
    """Return unqualified relation names visible on search_path that are extension-owned.

    Using ``pg_table_is_visible(c.oid)`` ensures that a user table in an earlier
    schema on ``search_path`` (for example ``public.place``) shadows any same-named
    extension table in a later schema (such as ``tiger.place``) and therefore is
    not excluded from Alembic reflection or drift detection.
    """
    rows = connection.execute(
        text(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p', 'v', 'm')
              AND pg_table_is_visible(c.oid)
              AND (
                  n.nspname = ANY(:ignored_schemas)
                  OR EXISTS (
                      SELECT 1
                      FROM pg_depend d
                      WHERE d.objid = c.oid
                        AND d.deptype = 'e'
                  )
              )
            """
        ),
        {"ignored_schemas": list(POSTGIS_IGNORED_SCHEMAS)},
    ).scalars()
    return frozenset(str(relname) for relname in rows)


def make_alembic_include_object(
    ignored_tables: frozenset[tuple[str, str]] = POSTGIS_IGNORED_TABLE_REFS,
    *,
    visible_ignored_tables: frozenset[str] | None = None,
    default_schema: str = "public",
    ignored_schemas: frozenset[str] = POSTGIS_IGNORED_SCHEMAS,
) -> Callable[[Any, str | None, str, bool, Any], bool]:
    """Build an Alembic include_object hook scoped by schema ownership and visibility."""

    def _is_ignored_table(schema_attr: str | None, table_name: str | None) -> bool:
        if table_name is None:
            return False
        if schema_attr is not None:
            if schema_attr in ignored_schemas:
                return True
            return (schema_attr, table_name) in ignored_tables
        if visible_ignored_tables is not None:
            return table_name in visible_ignored_tables
        return (default_schema, table_name) in ignored_tables

    def _include_object(
        object_: Any,
        name: str | None,
        type_: str,
        reflected: bool,
        compare_to: Any,
    ) -> bool:
        if not reflected:
            return True
        if type_ == "table":
            schema_attr = getattr(object_, "schema", None)
            if _is_ignored_table(schema_attr, name):
                return False
        elif type_ == "index":
            table = getattr(object_, "table", None)
            if table is not None:
                schema_attr = getattr(table, "schema", None)
                table_name = getattr(table, "name", None)
                if _is_ignored_table(schema_attr, table_name):
                    return False
        return True

    return _include_object


alembic_include_object = make_alembic_include_object(POSTGIS_IGNORED_TABLE_REFS)


def alembic_render_item(
    type_: str,
    obj: Any,
    autogen_context: Any,
) -> str | Literal[False]:
    """Render custom PostGIS types in Alembic autogenerate with required imports."""
    if type_ == "type" and isinstance(obj, _PostGISSpatialType):
        class_name = type(obj).__name__
        autogen_context.imports.add(f"from geosearch.db.types import {class_name}")
        return f"{class_name}({obj.geometry_type!r}, {obj.srid!r})"
    return False


def alembic_compare_type(
    migration_context: Any,
    inspected_column: Any,
    metadata_column: Any,
    inspected_type: Any,
    metadata_type: Any,
) -> bool | None:
    """Compare PostGIS Geometry/Geography column types by canonical kind, type, and SRID."""
    if isinstance(inspected_type, _PostGISSpatialType) or isinstance(
        metadata_type, _PostGISSpatialType
    ):
        if type(inspected_type) is not type(metadata_type):
            return True
        return inspected_type.spatial_signature != metadata_type.spatial_signature
    return None


def make_wgs84_point_expr(longitude: float, latitude: float) -> ColumnElement[Any]:
    """Validate WGS84 coordinates and build ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography."""
    lon, lat = validate_wgs84_coordinates(longitude, latitude)
    return func.ST_SetSRID(func.ST_MakePoint(lon, lat), 4326).cast(
        PostGISGeography("Point", 4326)
    )
