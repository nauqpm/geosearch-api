"""M2.3 tests for streaming Pyosmium Node extraction on OSM XML and PBF fixtures."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path

import osmium
import pytest

from geosearch.ingestion import (
    EXTRACTION_SCOPE_NODE_ONLY,
    ExtractionStats,
    NormalizationOutcomeKind,
    OsmSourceError,
    compute_file_sha256,
    iter_osm_node_outcomes,
)

FIXTURE_OSM_XML = Path(__file__).resolve().parent / "fixtures" / "sample_synthetic_nodes.osm"


def test_stream_extract_xml_fixture_deterministic_and_complete() -> None:
    """Controlled OSM XML fixture streams valid candidates, issues, and exact stats."""
    stats = ExtractionStats()
    outcomes = list(iter_osm_node_outcomes(FIXTURE_OSM_XML, stats=stats))

    # 13 nodes total: 6 valid, 4 unsupported, 3 invalid/ambiguous.
    assert stats.total_nodes_seen == 13
    assert stats.unsupported_nodes_seen == 4
    assert stats.supported_nodes_seen == 9
    assert stats.valid_candidates == 6
    assert stats.invalid_candidates == 3
    assert stats.total_nodes_seen == stats.supported_nodes_seen + stats.unsupported_nodes_seen
    assert stats.supported_nodes_seen == stats.valid_candidates + stats.invalid_candidates
    assert stats.issue_counts_by_code == {
        "AMBIGUOUS_CATEGORY": 1,
        "BLANK_NAME": 1,
        "MISSING_REQUIRED_NAME": 1,
    }
    assert sum(stats.issue_counts_by_code.values()) == stats.invalid_candidates
    assert len(outcomes) == 9

    valid_candidates = [
        o.candidate for o in outcomes if o.kind == NormalizationOutcomeKind.VALID and o.candidate
    ]
    assert [c.osm_id for c in valid_candidates] == [1001, 1002, 1003, 1004, 1005, 1006]
    assert [c.category for c in valid_candidates] == [
        "cafe",
        "restaurant",
        "atm",
        "atm",
        "pharmacy",
        "supermarket",
    ]

    # Verify coordinate ordering (lon, lat), Vietnamese normalization, address, raw tags, provenance.
    cafe = valid_candidates[0]
    assert math.isclose(cafe.longitude, 106.7009, abs_tol=1e-7)
    assert math.isclose(cafe.latitude, 10.7769, abs_tol=1e-7)
    assert cafe.name == "Cà Phê Sữa Đá"
    assert cafe.name_search == "ca phe sua da"
    assert cafe.address == "12 Lê Lợi, Quận 1"
    assert cafe.raw_tags["cuisine"] == "coffee_shop"
    assert cafe.osm_version == 3
    assert cafe.osm_changeset == 5001
    assert cafe.osm_timestamp == datetime(2026, 1, 10, 8, 0, tzinfo=timezone.utc)
    assert cafe.source_provenance["extraction_scope"] == EXTRACTION_SCOPE_NODE_ONLY
    assert "extraction_scope" not in cafe.raw_tags

    unnamed_atm = valid_candidates[2]
    assert unnamed_atm.name is None
    assert unnamed_atm.name_search is None
    assert unnamed_atm.raw_tags == {"amenity": "atm", "operator": "Vietcombank"}

    # Re-running yields byte-for-byte identical deterministic output and counters.
    stats_repeat = ExtractionStats()
    outcomes_repeat = list(iter_osm_node_outcomes(FIXTURE_OSM_XML, stats=stats_repeat))
    assert outcomes_repeat == outcomes
    assert stats_repeat.to_dict() == stats.to_dict()


def test_stream_extract_real_pbf_including_missing_location_and_sentinel_metadata(
    work_dir: Path,
) -> None:
    """Real binary .osm.pbf fixture handles missing locations and absent OSM metadata safely."""
    pbf_path = work_dir / "synthetic_nodes.osm.pbf"
    writer = osmium.SimpleWriter(str(pbf_path))
    try:
        # Valid node without explicit version/changeset/timestamp (Pyosmium emits 0 / epoch).
        writer.add_node(
            osmium.osm.mutable.Node(
                id=10,
                location=(106.7009, 10.7769),
                tags={"amenity": "cafe", "name": "Đà Lạt Coffee"},
            )
        )
        # Supported node with missing location -> must emit INVALID_COORDINATES issue.
        writer.add_node(
            osmium.osm.mutable.Node(
                id=11,
                tags={"amenity": "pharmacy", "name": "Nhà Thuốc Không Tọa Độ"},
            )
        )
        # Unsupported node without location -> filtered silently without issue.
        writer.add_node(
            osmium.osm.mutable.Node(
                id=12,
                tags={"natural": "tree"},
            )
        )
        # Supported node with valid explicit metadata.
        writer.add_node(
            osmium.osm.mutable.Node(
                id=13,
                version=5,
                changeset=777,
                timestamp="2026-02-01T12:34:56Z",
                location=(106.6901, 10.7812),
                tags={"shop": "supermarket", "name": "Co.opmart"},
            )
        )
    finally:
        writer.close()

    stats = ExtractionStats()
    outcomes = list(iter_osm_node_outcomes(pbf_path, stats=stats))

    assert stats.total_nodes_seen == 4
    assert stats.unsupported_nodes_seen == 1
    assert stats.supported_nodes_seen == 3
    assert stats.valid_candidates == 2
    assert stats.invalid_candidates == 1
    assert stats.issue_counts_by_code == {"INVALID_COORDINATES": 1}

    first = outcomes[0]
    assert first.kind == NormalizationOutcomeKind.VALID
    assert first.candidate is not None
    assert first.candidate.osm_id == 10
    assert first.candidate.name_search == "da lat coffee"
    assert first.candidate.osm_version is None
    assert first.candidate.osm_changeset is None
    assert first.candidate.osm_timestamp is None

    second = outcomes[1]
    assert second.kind == NormalizationOutcomeKind.INVALID
    assert second.issue is not None
    assert second.issue.issue_code == "INVALID_COORDINATES"
    assert second.issue.osm_type == "node"
    assert second.issue.osm_id == 11

    third = outcomes[2]
    assert third.kind == NormalizationOutcomeKind.VALID
    assert third.candidate is not None
    assert third.candidate.osm_id == 13
    assert third.candidate.osm_version == 5
    assert third.candidate.osm_changeset == 777
    assert third.candidate.osm_timestamp == datetime(2026, 2, 1, 12, 34, 56, tzinfo=timezone.utc)


def test_file_checksum_deterministic_and_file_errors_handled_safely(work_dir: Path) -> None:
    """Checksum is reproducible; missing, empty, directory, and corrupt files raise OsmSourceError."""
    checksum_1 = compute_file_sha256(FIXTURE_OSM_XML)
    checksum_2 = compute_file_sha256(FIXTURE_OSM_XML)
    assert checksum_1 == checksum_2
    assert len(checksum_1) == 64

    with pytest.raises(OsmSourceError, match="does not exist"):
        list(iter_osm_node_outcomes(work_dir / "missing.osm.pbf"))

    with pytest.raises(OsmSourceError, match="not a regular file"):
        list(iter_osm_node_outcomes(work_dir))

    empty_file = work_dir / "empty.osm.pbf"
    empty_file.write_bytes(b"")
    with pytest.raises(OsmSourceError, match="empty"):
        list(iter_osm_node_outcomes(empty_file))

    corrupt_file = work_dir / "corrupt.osm.pbf"
    corrupt_file.write_bytes(b"not a valid pbf or osm xml stream at all")
    with pytest.raises(OsmSourceError):
        list(iter_osm_node_outcomes(corrupt_file))
