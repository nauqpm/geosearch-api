"""Pydantic response schemas for canonical place detail, search, and nearby endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from geosearch.api.schemas.common import Coordinates

OsmType = Literal["node", "way", "relation"]
PlaceCategory = Literal["cafe", "restaurant", "atm", "pharmacy", "supermarket"]


class PlaceSummary(BaseModel):
    """Public summary representation of an active canonical POI.

    Coordinate order is explicit: `longitude` is WGS84 X in [-180, 180] and
    `latitude` is WGS84 Y in [-90, 90]. Internal fields (`raw_tags`, `name_search`,
    `is_active`, `last_seen_import_id`) are deliberately excluded.
    """

    model_config = ConfigDict(extra="forbid")

    id: int = Field(ge=1, description="Stable internal canonical place ID.")
    osm_type: OsmType = Field(description="OpenStreetMap object type.")
    osm_id: int = Field(ge=1, description="OpenStreetMap object identifier.")
    name: str | None = Field(
        default=None,
        description="Display name of the place (nullable only for unnamed ATMs).",
    )
    category: PlaceCategory = Field(
        description="Canonical MVP place category."
    )
    address: str | None = Field(
        default=None,
        description="Optional normalized address derived from OSM addr:* tags.",
    )
    longitude: float = Field(
        ge=-180.0,
        le=180.0,
        allow_inf_nan=False,
        description="WGS84 longitude (X) in degrees within [-180, 180].",
    )
    latitude: float = Field(
        ge=-90.0,
        le=90.0,
        allow_inf_nan=False,
        description="WGS84 latitude (Y) in degrees within [-90, 90].",
    )


class PlaceDetail(PlaceSummary):
    """Detailed public representation of an active canonical POI."""

    osm_version: int | None = Field(
        default=None,
        ge=1,
        description="Optional OpenStreetMap object version.",
    )
    osm_timestamp: datetime | None = Field(
        default=None,
        description="Optional OpenStreetMap last-modified UTC timestamp.",
    )


class PlaceListResponse(BaseModel):
    """Paginated collection of active canonical places matching search and filters."""

    model_config = ConfigDict(extra="forbid")

    items: list[PlaceSummary] = Field(description="Active places on the requested page.")
    total: int = Field(
        ge=0,
        description="Total matching active places in the observed publication snapshot.",
    )
    page: int = Field(ge=1, description="1-based page number.")
    limit: int = Field(ge=1, description="Maximum number of items per page.")
    has_more: bool = Field(
        description="True when additional matching items exist after this page."
    )


class NearbyPlaceItem(PlaceSummary):
    """Active canonical place returned from geographic nearby search with PostGIS distance."""

    distance_m: float = Field(
        ge=0.0,
        allow_inf_nan=False,
        description="WGS84 spheroidal distance in meters from the requested query center.",
    )


class NearbyPlacesResponse(BaseModel):
    """Paginated collection of active canonical places within a geographic radius."""

    model_config = ConfigDict(extra="forbid")

    items: list[NearbyPlaceItem] = Field(
        description="Active places within radius, ordered closest-first then by stable ID."
    )
    total: int = Field(
        ge=0,
        description="Total active places matching radius and filters in the observed snapshot.",
    )
    page: int = Field(ge=1, description="1-based page number.")
    limit: int = Field(ge=1, description="Maximum number of items per page.")
    has_more: bool = Field(
        description="True when additional matching nearby items exist after this page."
    )
    center: Coordinates = Field(description="Validated WGS84 query center point.")
    radius_m: float = Field(
        gt=0.0,
        allow_inf_nan=False,
        description="Validated geographic search radius in meters.",
    )
