"""M2.1 unit tests for canonical data models and deterministic Python validation."""

from __future__ import annotations

import unicodedata
from datetime import datetime, timezone
from typing import Any

import pytest

from geosearch.db.models import OSM_TYPES, PLACE_CATEGORIES
from geosearch.ingestion import (
    OsmIdentity,
    ValidationError,
    build_canonical_candidate,
    extract_address_from_tags,
    normalize_address,
    normalize_name_search,
    validate_coordinates,
    validate_name,
    validate_osm_identity,
    validate_raw_tags,
)


@pytest.mark.parametrize(
    ("raw_name", "expected_search"),
    [
        ("Cà Phê Sữa Đá", "ca phe sua da"),
        ("Đặng Văn Lâm", "dang van lam"),
        ("  Quán   Cà Phê  ", "quan ca phe"),
        ("Phở Hòa Pasteur", "pho hoa pasteur"),
        ("ĐƯỜNG ĐIỆN BIÊN PHỦ", "duong dien bien phu"),
        ("đường đồng khởi\t\n  quận 1", "duong dong khoi quan 1"),
        ("Highlands Coffee 24/7", "highlands coffee 24/7"),
        ("Nhà Thuốc Long Châu", "nha thuoc long chau"),
        ("Siêu Thị Co.opmart", "sieu thi co.opmart"),
        ("Cà\u200bPhê\ufeff Đá", "caphe da"),
    ],
)
def test_vietnamese_name_search_normalization(raw_name: str, expected_search: str) -> None:
    """Vietnamese casefolding, combining-mark removal, đ/Đ -> d, and whitespace collapse."""
    assert normalize_name_search(raw_name) == expected_search


def test_name_search_nfc_and_nfd_produce_identical_output() -> None:
    """Equivalent NFC and NFD Unicode inputs produce byte-identical name_search strings."""
    nfc = unicodedata.normalize("NFC", "Cà Phê Sữa Đá")
    nfd = unicodedata.normalize("NFD", "Cà Phê Sữa Đá")
    assert nfc != nfd
    assert normalize_name_search(nfc) == normalize_name_search(nfd) == "ca phe sua da"


def test_named_and_unnamed_atm_behavior() -> None:
    """Unnamed ATM keeps name=None and name_search=None without placeholder; named ATM normalizes."""
    assert validate_name(None, "atm") == (None, None)
    assert validate_name("  ATM Vietcombank Đakao  ", "atm") == (
        "ATM Vietcombank Đakao",
        "atm vietcombank dakao",
    )

    candidate = build_canonical_candidate(
        osm_type="node",
        osm_id=101,
        category="atm",
        raw_name=None,
        longitude=106.7009,
        latitude=10.7769,
        raw_tags={"amenity": "atm", "operator": "Vietcombank"},
    )
    assert candidate.name is None
    assert candidate.name_search is None
    assert candidate.category == "atm"


@pytest.mark.parametrize("category", ["cafe", "restaurant", "pharmacy", "supermarket"])
def test_non_atm_categories_require_name(category: str) -> None:
    """All non-ATM approved categories reject missing (None) names."""
    with pytest.raises(ValidationError) as exc_info:
        validate_name(None, category)
    assert exc_info.value.issue_code == "MISSING_REQUIRED_NAME"


@pytest.mark.parametrize("category", PLACE_CATEGORIES)
@pytest.mark.parametrize(
    ("bad_name", "expected_code"),
    [
        ("", "BLANK_NAME"),
        ("   \t\n  ", "BLANK_NAME"),
        ("\u0301\u0302\u0323", "BLANK_NORMALIZED_NAME"),
        ("\u200b\ufeff\u200c", "BLANK_NORMALIZED_NAME"),
        ("a\x00b", "INVALID_NAME"),
        (123, "INVALID_NAME"),
    ],
)
def test_blank_and_malformed_names_rejected_for_all_categories(
    category: str,
    bad_name: Any,
    expected_code: str,
) -> None:
    """Explicitly supplied blank, combining-mark-only, or malformed names are rejected."""
    with pytest.raises(ValidationError) as exc_info:
        validate_name(bad_name, category)
    assert exc_info.value.issue_code == expected_code


def test_valid_osm_identities_and_distinct_types() -> None:
    """Supported OSM types validate and equal numeric IDs of different types stay distinct."""
    identities = [validate_osm_identity(osm_type, 123456) for osm_type in OSM_TYPES]
    assert identities == [
        OsmIdentity("node", 123456),
        OsmIdentity("way", 123456),
        OsmIdentity("relation", 123456),
    ]
    assert len(set(identities)) == 3


@pytest.mark.parametrize(
    ("osm_type", "osm_id"),
    [
        ("NODE", 1),
        ("area", 1),
        ("", 1),
        (None, 1),
        ("node", 0),
        ("node", -5),
        ("node", True),
        ("node", False),
        ("node", "123"),
        ("node", 12.5),
        ("node", None),
        ("node", 2**63),
    ],
)
def test_invalid_osm_identities_rejected(osm_type: Any, osm_id: Any) -> None:
    """Invalid, missing, or malformed OSM identities are rejected deterministically."""
    with pytest.raises(ValidationError) as exc_info:
        validate_osm_identity(osm_type, osm_id)
    assert exc_info.value.issue_code == "INVALID_OSM_IDENTITY"


@pytest.mark.parametrize(
    ("lon", "lat"),
    [
        (106.7009, 10.7769),
        (-180.0, -90.0),
        (180.0, 90.0),
        (0, 0),
    ],
)
def test_valid_wgs84_coordinates_preserve_lon_lat_order(lon: float, lat: float) -> None:
    """Valid WGS84 coordinates return (longitude, latitude) as floats."""
    validated_lon, validated_lat = validate_coordinates(lon, lat)
    assert validated_lon == float(lon)
    assert validated_lat == float(lat)


@pytest.mark.parametrize(
    ("bad_lon", "bad_lat"),
    [
        (180.0001, 10.0),
        (-180.0001, 10.0),
        (106.7, 90.0001),
        (106.7, -90.0001),
        (float("nan"), 10.0),
        (106.7, float("nan")),
        (float("inf"), 10.0),
        (106.7, float("-inf")),
        (None, 10.0),
        (106.7, None),
        (True, 10.0),
        (106.7, False),
        ("106.7", 10.0),
    ],
)
def test_invalid_coordinates_rejected(bad_lon: Any, bad_lat: Any) -> None:
    """Out-of-range, non-finite, missing, boolean, and non-numeric coordinates are rejected."""
    with pytest.raises(ValidationError) as exc_info:
        validate_coordinates(bad_lon, bad_lat)
    assert exc_info.value.issue_code == "INVALID_COORDINATES"


def test_raw_tags_preservation_and_provenance_separation() -> None:
    """Original OSM tags (including unknown tags) are preserved without mixing provenance."""
    source_tags = {
        "amenity": "cafe",
        "name": "  Cà Phê Sữa Đá  ",
        "cuisine": "vietnamese",
        "opening_hours": "07:00-22:00",
        "custom:wifi": "free",
    }
    candidate = build_canonical_candidate(
        osm_type="node",
        osm_id=5001,
        category="cafe",
        raw_name=source_tags["name"],
        longitude=106.7009,
        latitude=10.7769,
        raw_tags=source_tags,
        osm_version=3,
        osm_changeset=987654,
        osm_timestamp=datetime(2026, 1, 15, 8, 30, tzinfo=timezone.utc),
        source_provenance={"source_name": "fixture.osm", "extraction_scope": "node_only"},
    )
    # Mutating caller dict does not alter candidate raw_tags.
    source_tags["amenity"] = "mutated"

    assert candidate.name == "Cà Phê Sữa Đá"
    assert candidate.name_search == "ca phe sua da"
    assert candidate.raw_tags["amenity"] == "cafe"
    assert candidate.raw_tags["name"] == "  Cà Phê Sữa Đá  "
    assert candidate.raw_tags["custom:wifi"] == "free"
    assert list(candidate.raw_tags.keys()) == sorted(candidate.raw_tags.keys())
    assert "source_name" not in candidate.raw_tags
    assert candidate.source_provenance == {
        "source_name": "fixture.osm",
        "extraction_scope": "node_only",
    }


@pytest.mark.parametrize(
    "bad_tags",
    [
        None,
        [],
        ["amenity", "cafe"],
        "amenity=cafe",
        123,
        {"": "cafe"},
        {123: "cafe"},
        {"amenity": 123},
        {"amenity": None},
        {"amenity": "cafe\x00"},
    ],
)
def test_missing_or_malformed_raw_tags_rejected(bad_tags: Any) -> None:
    """Missing or malformed raw_tags are rejected and never silently replaced with {}."""
    with pytest.raises(ValidationError) as exc_info:
        validate_raw_tags(bad_tags)
    assert exc_info.value.issue_code == "INVALID_RAW_TAGS"


def test_nullable_address_normalization_and_tag_extraction() -> None:
    """Address is optional, blanks normalize to None, and no missing parts are fabricated."""
    assert normalize_address(None) is None
    assert normalize_address("") is None
    assert normalize_address("   \t\n ") is None
    assert normalize_address("  12  Lê Lợi\tQuận 1 ") == "12 Lê Lợi Quận 1"

    assert extract_address_from_tags({"amenity": "cafe"}) is None
    assert extract_address_from_tags({"addr:full": "   ", "addr:street": "  "}) is None
    assert (
        extract_address_from_tags(
            {
                "addr:full": "123 Nguyễn Huệ, Quận 1",
                "addr:street": "Ignored when addr:full present",
            }
        )
        == "123 Nguyễn Huệ, Quận 1"
    )
    assert (
        extract_address_from_tags(
            {
                "addr:housenumber": "15",
                "addr:street": "Lê Thánh Tôn",
                "addr:district": "Quận 1",
            }
        )
        == "15 Lê Thánh Tôn, Quận 1"
    )
    assert extract_address_from_tags({"addr:street": "Đồng Khởi"}) == "Đồng Khởi"


def test_invalid_osm_metadata_rejected() -> None:
    """Invalid osm_version, osm_changeset, or naive osm_timestamp are rejected."""
    for kwargs in (
        {"osm_version": 0},
        {"osm_version": -1},
        {"osm_version": True},
        {"osm_changeset": 0},
        {"osm_changeset": -9},
        {"osm_timestamp": datetime(2026, 1, 1, 12, 0)},  # naive datetime
    ):
        with pytest.raises(ValidationError) as exc_info:
            build_canonical_candidate(
                osm_type="node",
                osm_id=9001,
                category="cafe",
                raw_name="Cafe Test",
                longitude=106.7,
                latitude=10.7,
                raw_tags={"amenity": "cafe", "name": "Cafe Test"},
                **kwargs,
            )
        assert exc_info.value.issue_code == "INVALID_OSM_METADATA"
