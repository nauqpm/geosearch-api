"""Shared Pydantic schemas for error responses, coordinates, and service health."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ErrorDetailItem(BaseModel):
    """Sanitized validation error entry."""

    model_config = ConfigDict(extra="forbid")

    loc: list[str] = Field(description="Location path of the invalid parameter.")
    msg: str = Field(description="Human-readable validation message.")
    type: str = Field(description="Validation error category.")


class ErrorBody(BaseModel):
    """Structured public API error payload without internal details."""

    model_config = ConfigDict(extra="forbid")

    code: str = Field(description="Stable machine-readable error code.")
    message: str = Field(description="Safe human-readable error message.")
    details: list[ErrorDetailItem] | None = Field(
        default=None,
        description="Optional field-level validation details.",
    )


class ErrorResponse(BaseModel):
    """Consistent top-level JSON envelope for API errors."""

    model_config = ConfigDict(extra="forbid")

    error: ErrorBody


class Coordinates(BaseModel):
    """Explicit WGS84 geographic coordinates (longitude = X, latitude = Y)."""

    model_config = ConfigDict(extra="forbid")

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


class HealthResponse(BaseModel):
    """Service liveness and lightweight PostgreSQL readiness status."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ok", "degraded"] = Field(
        description="Overall health status ('ok' when database is reachable, 'degraded' otherwise)."
    )
    service: str = Field(description="Service identifier.")
    database: Literal["ok", "unavailable"] = Field(
        description="Lightweight read-only PostgreSQL readiness status."
    )
