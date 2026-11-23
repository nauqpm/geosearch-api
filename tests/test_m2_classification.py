"""M2.2 unit tests for deterministic OSM category mapping and tag normalization pipeline."""

from __future__ import annotations

import pytest

from geosearch.ingestion import (
    ClassificationStatus,
    NormalizationOutcomeKind,
    RawOsmObject,
    classify_osm_tags,
    normalize_osm_object,
)


@pytest.mark.parametrize(
    ("tags", "expected_category", "expected_name", "expected_name_search"),
    [
        (
            {"amenity": "cafe", "name": "Cà Phê Đá"},
            "cafe",
            "Cà Phê Đá",
            "ca phe da",
        ),
        (
            {"amenity": "restaurant", "name": "Quán Phở Đêm"},
            "restaurant",
            "Quán Phở Đêm",
            "quan pho dem",
        ),
        (
            {"amenity": "atm", "name": "ATM BIDV"},
            "atm",
            "ATM BIDV",
            "atm bidv",
        ),
        (
            {"amenity": "pharmacy", "name": "Nhà Thuốc An Khang"},
            "pharmacy",
            "Nhà Thuốc An Khang",
            "nha thuoc an khang",
        ),
        (
            {"shop": "supermarket", "name": "Siêu Thị WinMart"},
            "supermarket",
            "Siêu Thị WinMart",
            "sieu thi winmart",
        ),
    ],
)
def test_approved_five_categories_positive(
    tags: dict[str, str],
    expected_category: str,
    expected_name: str,
    expected_name_search: str,
) -> None:
    """All five approved OSM tag mappings classify and normalize deterministically."""
    classification = classify_osm_tags(tags)
    assert classification.status == ClassificationStatus.SUPPORTED
    assert classification.category == expected_category
    assert classification.matched_categories == (expected_category,)

    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=1001,
            tags=tags,
            longitude=106.7009,
            latitude=10.7769,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.VALID
    assert outcome.issue is None
    assert outcome.candidate is not None
    assert outcome.candidate.category == expected_category
    assert outcome.candidate.name == expected_name
    assert outcome.candidate.name_search == expected_name_search
    assert outcome.candidate.raw_tags == tags


@pytest.mark.parametrize(
    "tags",
    [
        {"amenity": "hospital", "name": "Bệnh Viện Chợ Rẫy"},
        {"amenity": "school", "name": "Trường THPT"},
        {"amenity": "bank", "name": "Vietcombank"},
        {"shop": "convenience", "name": "Circle K"},
        {"tourism": "hotel", "name": "Rex Hotel"},
        {"highway": "traffic_signals"},
        {"name": "Unnamed Tagless Feature"},
        {},
    ],
)
def test_unsupported_and_missing_category_tags_filtered_without_issue(
    tags: dict[str, str],
) -> None:
    """Unsupported amenity/shop tags or missing category tags filter cleanly with no error issue."""
    classification = classify_osm_tags(tags)
    assert classification.status == ClassificationStatus.UNSUPPORTED
    assert classification.category is None

    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=2001,
            tags=tags,
            longitude=106.7,
            latitude=10.7,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.UNSUPPORTED
    assert outcome.candidate is None
    assert outcome.issue is None


@pytest.mark.parametrize(
    ("tags", "expected_matched"),
    [
        (
            {"amenity": "cafe", "shop": "supermarket", "name": "Combo Store"},
            ("cafe", "supermarket"),
        ),
        (
            {"amenity": "atm", "shop": "supermarket", "name": "Mart ATM"},
            ("atm", "supermarket"),
        ),
        (
            {"amenity": "cafe;restaurant", "name": "Bistro Cafe"},
            ("cafe", "restaurant"),
        ),
        (
            {"amenity": "cafe;bakery", "name": "Cafe Bakery"},
            ("cafe",),
        ),
        (
            {"amenity": "cafe;", "name": "Trailing Semicolon Cafe"},
            ("cafe",),
        ),
        (
            {"amenity": ";cafe", "name": "Leading Semicolon Cafe"},
            ("cafe",),
        ),
        (
            {"amenity": "cafe;cafe", "name": "Repeated Semicolon Cafe"},
            ("cafe",),
        ),
        (
            {"shop": "supermarket;convenience", "name": "Mini Mart"},
            ("supermarket",),
        ),
        (
            {"shop": "supermarket;", "name": "Trailing Semicolon Mart"},
            ("supermarket",),
        ),
    ],
)
def test_conflicting_or_multivalue_category_tags_quarantined_deterministically(
    tags: dict[str, str],
    expected_matched: tuple[str, ...],
) -> None:
    """Conflicting or multi-value category tags never pick an arbitrary winner."""
    classification = classify_osm_tags(tags)
    assert classification.status == ClassificationStatus.AMBIGUOUS
    assert classification.category is None
    assert classification.matched_categories == expected_matched

    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=3001,
            tags=tags,
            longitude=106.7,
            latitude=10.7,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.AMBIGUOUS
    assert outcome.candidate is None
    assert outcome.issue is not None
    assert outcome.issue.issue_code == "AMBIGUOUS_CATEGORY"
    assert outcome.issue.stage == "CLASSIFY"
    assert outcome.issue.severity == "WARNING"
    assert outcome.issue.osm_type == "node"
    assert outcome.issue.osm_id == 3001


def test_missing_required_name_creates_traceable_issue() -> None:
    """Supported non-ATM POI without a name produces a traceable validation issue."""
    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=4001,
            tags={"amenity": "pharmacy", "dispensing": "yes"},
            longitude=106.7009,
            latitude=10.7769,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.INVALID
    assert outcome.candidate is None
    assert outcome.issue is not None
    assert outcome.issue.issue_code == "MISSING_REQUIRED_NAME"
    assert outcome.issue.stage == "VALIDATE"
    assert outcome.issue.severity == "ERROR"
    assert outcome.issue.osm_type == "node"
    assert outcome.issue.osm_id == 4001


def test_valid_unnamed_atm_accepted_and_retains_original_tags() -> None:
    """Unnamed ATM normalizes cleanly with name=None, name_search=None, and original tags kept."""
    tags = {
        "amenity": "atm",
        "operator": "Techcombank",
        "cash_in": "yes",
        "currency:VND": "yes",
    }
    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=4002,
            tags=tags,
            longitude=106.701,
            latitude=10.777,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.VALID
    assert outcome.candidate is not None
    assert outcome.candidate.category == "atm"
    assert outcome.candidate.name is None
    assert outcome.candidate.name_search is None
    assert outcome.candidate.raw_tags == tags


def test_invalid_osm_identity_on_supported_object_keeps_issue_identity_null() -> None:
    """Invalid OSM identity produces an issue with osm_type=None, osm_id=None to satisfy M1 CHECK."""
    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=-10,
            tags={"amenity": "cafe", "name": "Bad ID Cafe"},
            longitude=106.7,
            latitude=10.7,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.INVALID
    assert outcome.issue is not None
    assert outcome.issue.issue_code == "INVALID_OSM_IDENTITY"
    assert outcome.issue.osm_type is None
    assert outcome.issue.osm_id is None
    assert outcome.issue.details is not None
    assert outcome.issue.details["raw_osm_id"] == -10


def test_malformed_raw_tags_produces_invalid_issue() -> None:
    """Non-dict raw_tags produces a traceable INVALID_RAW_TAGS issue."""
    outcome = normalize_osm_object(
        RawOsmObject(
            osm_type="node",
            osm_id=4005,
            tags=["amenity=cafe"],
            longitude=106.7,
            latitude=10.7,
        )
    )
    assert outcome.kind == NormalizationOutcomeKind.INVALID
    assert outcome.issue is not None
    assert outcome.issue.issue_code == "INVALID_RAW_TAGS"
    assert outcome.issue.osm_type == "node"
    assert outcome.issue.osm_id == 4005


def test_deterministic_output_independent_of_dict_key_ordering() -> None:
    """Category classification and normalization are identical regardless of tag dict order."""
    tags_order_a = {
        "name": "Cà Phê Độc Lập",
        "amenity": "cafe",
        "addr:street": "Lê Duẩn",
        "addr:housenumber": "10",
        "wifi": "yes",
    }
    tags_order_b = {
        "wifi": "yes",
        "addr:housenumber": "10",
        "addr:street": "Lê Duẩn",
        "amenity": "cafe",
        "name": "Cà Phê Độc Lập",
    }

    out_a = normalize_osm_object(
        RawOsmObject("node", 5001, tags_order_a, 106.695, 10.779)
    )
    out_b = normalize_osm_object(
        RawOsmObject("node", 5001, tags_order_b, 106.695, 10.779)
    )
    assert out_a == out_b
    assert out_a.candidate is not None and out_b.candidate is not None
    assert list(out_a.candidate.raw_tags.items()) == list(out_b.candidate.raw_tags.items())

    # Also verify key-order independence for ambiguous tag conflicts.
    conflict_a = {"amenity": "cafe", "shop": "supermarket", "name": "Conflict"}
    conflict_b = {"name": "Conflict", "shop": "supermarket", "amenity": "cafe"}
    amb_a = normalize_osm_object(RawOsmObject("node", 5002, conflict_a, 106.7, 10.7))
    amb_b = normalize_osm_object(RawOsmObject("node", 5002, conflict_b, 106.7, 10.7))
    assert amb_a == amb_b
