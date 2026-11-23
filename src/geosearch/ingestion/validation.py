"""Deterministic Python validation and normalization helpers for OSM place data.

All validators enforce constraints compatible with the M1 PostgreSQL/PostGIS schema
while remaining independent of database sessions and HTTP layers.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Final

from geosearch.db.models import OSM_TYPES, PLACE_CATEGORIES
from geosearch.db.types import InvalidCoordinateError, validate_wgs84_coordinates
from geosearch.ingestion.models import CanonicalPlaceCandidate, OsmIdentity

MAX_PG_INT32: Final[int] = 2**31 - 1
MAX_PG_INT64: Final[int] = 2**63 - 1

_VALID_OSM_TYPES: Final[frozenset[str]] = frozenset(OSM_TYPES)
_VALID_CATEGORIES: Final[frozenset[str]] = frozenset(PLACE_CATEGORIES)


class ValidationError(ValueError):
    """Structured validation error carrying an issue code and diagnostic details."""

    def __init__(
        self,
        message: str,
        *,
        issue_code: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.issue_code = issue_code
        self.details = dict(details) if details is not None else {}


def _contains_nul(value: str) -> bool:
    return "\x00" in value


def validate_osm_identity(osm_type: Any, osm_id: Any) -> OsmIdentity:
    """Validate OSM object type and positive 64-bit integer ID."""
    if not isinstance(osm_type, str) or osm_type not in _VALID_OSM_TYPES:
        raise ValidationError(
            f"Unsupported or invalid OSM object type: {osm_type!r}.",
            issue_code="INVALID_OSM_IDENTITY",
            details={"raw_osm_type": repr(osm_type), "raw_osm_id": repr(osm_id)},
        )

    if isinstance(osm_id, bool) or not isinstance(osm_id, int):
        raise ValidationError(
            f"OSM ID must be a positive integer, got {osm_id!r}.",
            issue_code="INVALID_OSM_IDENTITY",
            details={"raw_osm_type": osm_type, "raw_osm_id": repr(osm_id)},
        )

    if osm_id <= 0 or osm_id > MAX_PG_INT64:
        raise ValidationError(
            f"OSM ID must be between 1 and {MAX_PG_INT64}, got {osm_id!r}.",
            issue_code="INVALID_OSM_IDENTITY",
            details={"raw_osm_type": osm_type, "raw_osm_id": osm_id},
        )

    return OsmIdentity(osm_type=osm_type, osm_id=osm_id)


def validate_category(category: Any) -> str:
    """Validate that a category belongs strictly to the five approved MVP categories."""
    if not isinstance(category, str) or category not in _VALID_CATEGORIES:
        raise ValidationError(
            f"Unsupported canonical category: {category!r}.",
            issue_code="INVALID_CATEGORY",
            details={"category": repr(category)},
        )
    return category


def normalize_name_search(raw_name: str) -> str:
    """Deterministically normalize Unicode text for accent-insensitive Vietnamese search.

    Steps:
    1. Validate Unicode text without embedded NUL characters.
    2. Apply Unicode casefolding.
    3. Decompose with NFD and remove Unicode non-spacing combining marks (category 'Mn').
    4. Convert Vietnamese 'đ'/'Đ' to ASCII 'd' (since 'đ' does not decompose in NFD).
    5. Collapse repeated whitespace and strip leading/trailing whitespace.
    """
    if not isinstance(raw_name, str) or _contains_nul(raw_name):
        raise ValidationError(
            "Name must be a valid Unicode string without NUL characters.",
            issue_code="INVALID_NAME",
            details={"raw_name_type": type(raw_name).__name__},
        )

    folded = raw_name.casefold()
    decomposed = unicodedata.normalize("NFD", folded)
    stripped_marks = "".join(
        ch for ch in decomposed if unicodedata.category(ch) not in ("Mn", "Cf")
    )
    vietnamese_normalized = stripped_marks.replace("đ", "d").replace("Đ", "d")
    return " ".join(vietnamese_normalized.split())


def validate_name(raw_name: Any, category: str) -> tuple[str | None, str | None]:
    """Validate and normalize place name according to category rules.

    - For 'atm', missing name (None) is valid and yields (None, None).
    - For all other approved categories, a non-blank name is required.
    - Explicitly provided names that are blank or become blank after normalization are rejected.
    - Original display name is preserved separately (outer whitespace trimmed); raw_tags retains
      the unmodified OSM value.
    """
    validated_category = validate_category(category)

    if raw_name is None:
        if validated_category == "atm":
            return None, None
        raise ValidationError(
            f"Category '{validated_category}' requires a non-blank name.",
            issue_code="MISSING_REQUIRED_NAME",
            details={"category": validated_category},
        )

    if not isinstance(raw_name, str) or _contains_nul(raw_name):
        raise ValidationError(
            f"Place name must be a valid string, got {type(raw_name).__name__}.",
            issue_code="INVALID_NAME",
            details={"category": validated_category, "raw_name_type": type(raw_name).__name__},
        )

    cleaned_name = raw_name.strip()
    if not cleaned_name:
        raise ValidationError(
            "Place name must not be blank or whitespace-only.",
            issue_code="BLANK_NAME",
            details={"category": validated_category},
        )

    normalized_search = normalize_name_search(cleaned_name)
    if not normalized_search:
        raise ValidationError(
            "Place name became blank after Unicode normalization.",
            issue_code="BLANK_NORMALIZED_NAME",
            details={"category": validated_category, "raw_name": raw_name},
        )

    return cleaned_name, normalized_search


def validate_coordinates(longitude: Any, latitude: Any) -> tuple[float, float]:
    """Validate numeric finiteness and WGS84 bounds by delegating to the M1 validator."""
    if longitude is None or latitude is None:
        raise ValidationError(
            f"Coordinates are required, got lon={longitude!r}, lat={latitude!r}.",
            issue_code="INVALID_COORDINATES",
            details={"longitude": repr(longitude), "latitude": repr(latitude)},
        )

    if (
        isinstance(longitude, bool)
        or isinstance(latitude, bool)
        or not isinstance(longitude, (int, float))
        or not isinstance(latitude, (int, float))
    ):
        raise ValidationError(
            f"Coordinates must be finite numeric values, got lon={longitude!r}, lat={latitude!r}.",
            issue_code="INVALID_COORDINATES",
            details={"longitude": repr(longitude), "latitude": repr(latitude)},
        )

    try:
        return validate_wgs84_coordinates(float(longitude), float(latitude))
    except InvalidCoordinateError as exc:
        raise ValidationError(
            str(exc),
            issue_code="INVALID_COORDINATES",
            details={"longitude": repr(longitude), "latitude": repr(latitude)},
        ) from exc


def validate_raw_tags(raw_tags: Any) -> dict[str, str]:
    """Validate and copy decoded OSM tags as a JSON-compatible string dictionary.

    - Rejects missing (None) or non-mapping values without substituting '{}'.
    - Requires non-empty string keys and string values without NUL bytes.
    - Preserves all unknown OSM tags untouched.
    """
    if not isinstance(raw_tags, Mapping):
        raise ValidationError(
            f"OSM raw_tags must be a dictionary/mapping, got {type(raw_tags).__name__}.",
            issue_code="INVALID_RAW_TAGS",
            details={"raw_tags_type": type(raw_tags).__name__},
        )

    validated: dict[str, str] = {}
    for key, value in raw_tags.items():
        if not isinstance(key, str) or not key or _contains_nul(key):
            raise ValidationError(
                f"Invalid OSM tag key: {key!r}.",
                issue_code="INVALID_RAW_TAGS",
                details={"invalid_key": repr(key)},
            )
        if not isinstance(value, str) or _contains_nul(value):
            raise ValidationError(
                f"Invalid OSM tag value for key {key!r}: expected str, got {type(value).__name__}.",
                issue_code="INVALID_RAW_TAGS",
                details={"key": key, "value_type": type(value).__name__},
            )
        validated[key] = value
    return validated


def normalize_address(raw_address: Any) -> str | None:
    """Normalize an explicit optional address string, converting blanks to None."""
    if raw_address is None:
        return None
    if not isinstance(raw_address, str) or _contains_nul(raw_address):
        raise ValidationError(
            f"Address must be a valid string or None, got {type(raw_address).__name__}.",
            issue_code="INVALID_ADDRESS",
            details={"raw_address_type": type(raw_address).__name__},
        )
    collapsed = " ".join(raw_address.split())
    return collapsed if collapsed else None


def extract_address_from_tags(raw_tags: Mapping[str, str]) -> str | None:
    """Derive an optional address strictly from present OSM addr:* tags.

    Design note (minimal lossless default pending an approved Vietnamese address rule):
    - If 'addr:full' is present and non-blank, use it directly (whitespace-collapsed).
    - Otherwise join only the non-blank structured OSM tags that are actually present
      ('addr:housenumber' + 'addr:street', then 'addr:ward'/'addr:subdistrict',
      'addr:district', 'addr:city', 'addr:province', 'addr:postcode').
    - Never fabricates missing components; returns None if all address tags are absent/blank.
    - All original addr:* tags remain preserved verbatim in raw_tags.
    """
    full_addr = normalize_address(raw_tags.get("addr:full"))
    if full_addr is not None:
        return full_addr

    housenumber = normalize_address(raw_tags.get("addr:housenumber"))
    street = normalize_address(raw_tags.get("addr:street"))
    street_line = " ".join(part for part in (housenumber, street) if part) or None

    parts: list[str] = []
    if street_line:
        parts.append(street_line)

    seen_lower: set[str] = {street_line.casefold()} if street_line else set()
    for tag_key in (
        "addr:ward",
        "addr:subdistrict",
        "addr:district",
        "addr:city",
        "addr:province",
        "addr:postcode",
    ):
        component = normalize_address(raw_tags.get(tag_key))
        if component is not None and component.casefold() not in seen_lower:
            parts.append(component)
            seen_lower.add(component.casefold())

    return ", ".join(parts) if parts else None


def validate_osm_metadata(
    *,
    osm_version: Any = None,
    osm_changeset: Any = None,
    osm_timestamp: Any = None,
) -> tuple[int | None, int | None, datetime | None]:
    """Validate optional OSM version, changeset, and timezone-aware timestamp."""
    validated_version: int | None = None
    if osm_version is not None:
        if (
            isinstance(osm_version, bool)
            or not isinstance(osm_version, int)
            or osm_version <= 0
            or osm_version > MAX_PG_INT32
        ):
            raise ValidationError(
                f"osm_version must be a positive 32-bit integer, got {osm_version!r}.",
                issue_code="INVALID_OSM_METADATA",
                details={"field": "osm_version", "value": repr(osm_version)},
            )
        validated_version = osm_version

    validated_changeset: int | None = None
    if osm_changeset is not None:
        if (
            isinstance(osm_changeset, bool)
            or not isinstance(osm_changeset, int)
            or osm_changeset <= 0
            or osm_changeset > MAX_PG_INT64
        ):
            raise ValidationError(
                f"osm_changeset must be a positive 64-bit integer, got {osm_changeset!r}.",
                issue_code="INVALID_OSM_METADATA",
                details={"field": "osm_changeset", "value": repr(osm_changeset)},
            )
        validated_changeset = osm_changeset

    validated_timestamp: datetime | None = None
    if osm_timestamp is not None:
        if not isinstance(osm_timestamp, datetime) or osm_timestamp.tzinfo is None:
            raise ValidationError(
                "osm_timestamp must be a timezone-aware datetime or None.",
                issue_code="INVALID_OSM_METADATA",
                details={"field": "osm_timestamp", "value": repr(osm_timestamp)},
            )
        validated_timestamp = osm_timestamp

    return validated_version, validated_changeset, validated_timestamp


def validate_source_provenance(source_provenance: Any) -> dict[str, Any]:
    """Validate optional pipeline/source provenance dictionary kept separate from raw_tags."""
    if source_provenance is None:
        return {}
    if not isinstance(source_provenance, Mapping):
        raise ValidationError(
            f"source_provenance must be a mapping, got {type(source_provenance).__name__}.",
            issue_code="INVALID_PROVENANCE",
            details={"source_provenance_type": type(source_provenance).__name__},
        )
    return dict(source_provenance)


def build_canonical_candidate(
    *,
    osm_type: Any,
    osm_id: Any,
    category: Any,
    raw_name: Any,
    longitude: Any,
    latitude: Any,
    raw_tags: Any,
    address: Any = None,
    osm_version: Any = None,
    osm_changeset: Any = None,
    osm_timestamp: Any = None,
    source_provenance: Any = None,
) -> CanonicalPlaceCandidate:
    """Validate all canonical fields and construct an immutable CanonicalPlaceCandidate."""
    identity = validate_osm_identity(osm_type, osm_id)
    validated_tags = validate_raw_tags(raw_tags)
    validated_category = validate_category(category)
    cleaned_name, name_search = validate_name(raw_name, validated_category)
    lon, lat = validate_coordinates(longitude, latitude)
    normalized_addr = (
        normalize_address(address)
        if address is not None
        else extract_address_from_tags(validated_tags)
    )
    version, changeset, timestamp = validate_osm_metadata(
        osm_version=osm_version,
        osm_changeset=osm_changeset,
        osm_timestamp=osm_timestamp,
    )
    provenance = validate_source_provenance(source_provenance)

    return CanonicalPlaceCandidate(
        osm_type=identity.osm_type,
        osm_id=identity.osm_id,
        name=cleaned_name,
        name_search=name_search,
        category=validated_category,
        address=normalized_addr,
        longitude=lon,
        latitude=lat,
        raw_tags=dict(sorted(validated_tags.items())),
        osm_version=version,
        osm_changeset=changeset,
        osm_timestamp=timestamp,
        source_provenance=provenance,
    )
