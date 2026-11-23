"""M1.4 integration tests for column, JSONB, nullability, and coordinate validation."""

from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError

from geosearch.db import (
    BOUNDARY_APPROVAL_STATUSES,
    PLACE_CATEGORIES,
    InvalidCoordinateError,
    make_wgs84_point_expr,
    validate_wgs84_coordinates,
)
from tests.helpers import (
    DEFAULT_APPROVED_AT,
    DEFAULT_VALIDATED_AT,
    DEFAULT_VALIDATION_EVIDENCE_JSON,
    VALID_MULTIPOLYGON_WKT,
    VALID_SHA256,
    assert_pg_error,
    insert_boundary_version,
    insert_import_run,
    insert_place,
)


def _seed_run(conn: Connection) -> int:
    bv_id = insert_boundary_version(conn)
    return insert_import_run(conn, bv_id)


@pytest.mark.parametrize("category", PLACE_CATEGORIES)
def test_valid_place_categories_accepted(migrated_conn: Connection, category: str) -> None:
    """All five approved MVP place categories are accepted."""
    run_id = _seed_run(migrated_conn)
    place_id = insert_place(
        migrated_conn,
        run_id,
        osm_id=100,
        category=category,
        name=f"Sample {category}",
        name_search=f"sample {category}",
    )
    assert place_id > 0


@pytest.mark.parametrize("invalid_category", ["bar", "hospital", "CAFE", "", "convenience"])
def test_invalid_place_categories_rejected(
    migrated_conn: Connection, invalid_category: str
) -> None:
    """Categories outside the approved five-category domain are rejected."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(migrated_conn, run_id, category=invalid_category)
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_category_domain")


@pytest.mark.parametrize("invalid_osm_type", ["area", "NODE", "point", ""])
def test_invalid_osm_type_rejected(migrated_conn: Connection, invalid_osm_type: str) -> None:
    """Only node, way, and relation are valid OSM object types."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(migrated_conn, run_id, osm_type=invalid_osm_type)
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_osm_type_domain")


@pytest.mark.parametrize("invalid_osm_id", [0, -1, -999999])
def test_non_positive_osm_id_rejected(migrated_conn: Connection, invalid_osm_id: int) -> None:
    """OSM IDs must be strictly positive integers."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(migrated_conn, run_id, osm_id=invalid_osm_id)
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_osm_id_positive")


@pytest.mark.parametrize("category", ["cafe", "restaurant", "pharmacy", "supermarket"])
def test_non_atm_requires_name(migrated_conn: Connection, category: str) -> None:
    """Non-ATM categories reject NULL name."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(
                migrated_conn,
                run_id,
                category=category,
                name=None,
                name_search=None,
            )
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_name_required_non_atm")


def test_atm_nullable_and_named_behavior(migrated_conn: Connection) -> None:
    """ATM allows NULL name + NULL name_search as well as populated name + name_search."""
    run_id = _seed_run(migrated_conn)
    unnamed_atm_id = insert_place(
        migrated_conn,
        run_id,
        osm_id=201,
        category="atm",
        name=None,
        name_search=None,
        raw_tags_json='{"amenity": "atm"}',
    )
    named_atm_id = insert_place(
        migrated_conn,
        run_id,
        osm_id=202,
        category="atm",
        name="Vietcombank ATM",
        name_search="vietcombank atm",
        raw_tags_json='{"amenity": "atm", "operator": "Vietcombank"}',
    )
    assert unnamed_atm_id != named_atm_id


@pytest.mark.parametrize("blank_name", ["", "   ", "\t\n"])
def test_blank_name_rejected(migrated_conn: Connection, blank_name: str) -> None:
    """Whitespace-only or empty place names are rejected."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(
                migrated_conn,
                run_id,
                category="cafe",
                name=blank_name,
                name_search="cafe",
            )
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_name_non_blank")


@pytest.mark.parametrize(
    ("category", "name", "name_search"),
    [
        ("cafe", "Highlands Coffee", None),
        ("cafe", "Highlands Coffee", "   "),
        ("atm", None, "vietcombank"),
        ("atm", "ACB ATM", None),
    ],
)
def test_name_search_nullability_consistency(
    migrated_conn: Connection,
    category: str,
    name: str | None,
    name_search: str | None,
) -> None:
    """name_search must be NULL iff name is NULL, and non-blank whenever name is present."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(
                migrated_conn,
                run_id,
                category=category,
                name=name,
                name_search=name_search,
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_places_name_search_consistency"
    )


def test_address_nullable_but_not_blank(migrated_conn: Connection) -> None:
    """NULL address is allowed; empty/whitespace-only address is rejected."""
    run_id = _seed_run(migrated_conn)
    place_id = insert_place(migrated_conn, run_id, osm_id=301, address=None)
    assert place_id > 0

    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(migrated_conn, run_id, osm_id=302, address="   ")
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_address_non_blank")


@pytest.mark.parametrize("invalid_json", ["[]", '["cafe"]', '"cafe"', "123", "true", "null"])
def test_places_raw_tags_rejects_non_object_jsonb(
    migrated_conn: Connection, invalid_json: str
) -> None:
    """raw_tags must be a JSONB object, rejecting arrays, scalars, and JSON null."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(migrated_conn, run_id, raw_tags_json=invalid_json)
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_raw_tags_json_object")


def test_secondary_tables_reject_non_object_jsonb(migrated_conn: Connection) -> None:
    """JSONB metadata columns in import_runs, import_issues, and duplicate_candidates reject arrays."""
    bv_id = insert_boundary_version(migrated_conn)

    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_import_run(migrated_conn, bv_id, publication_evidence_json='["not-object"]')
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_import_runs_publication_evidence_json_object",
    )

    run_id = insert_import_run(migrated_conn, bv_id)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO import_issues (
                        import_run_id, severity, stage, issue_code, message, details
                    )
                    VALUES (:run_id, 'WARNING', 'VALIDATE', 'BAD_TAG', 'bad tag', '["array"]'::jsonb)
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_import_issues_details_json_object"
    )

    p1 = insert_place(migrated_conn, run_id, osm_id=401)
    p2 = insert_place(migrated_conn, run_id, osm_id=402)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO duplicate_candidates (
                        import_run_id, place_a_id, place_b_id, detector_version, evidence
                    )
                    VALUES (:run_id, :a, :b, 'v1', '123'::jsonb)
                    """
                ),
                {"run_id": run_id, "a": min(p1, p2), "b": max(p1, p2)},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_duplicate_candidates_evidence_json_object",
    )


@pytest.mark.parametrize(
    ("column", "sql"),
    [
        (
            "osm_type",
            """
            INSERT INTO places (osm_type, osm_id, name, name_search, category, location, raw_tags, last_seen_import_id)
            VALUES (NULL, 501, 'A', 'a', 'cafe', ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography, '{}'::jsonb, :run_id)
            """,
        ),
        (
            "osm_id",
            """
            INSERT INTO places (osm_type, osm_id, name, name_search, category, location, raw_tags, last_seen_import_id)
            VALUES ('node', NULL, 'A', 'a', 'cafe', ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography, '{}'::jsonb, :run_id)
            """,
        ),
        (
            "category",
            """
            INSERT INTO places (osm_type, osm_id, name, name_search, category, location, raw_tags, last_seen_import_id)
            VALUES ('node', 502, 'A', 'a', NULL, ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography, '{}'::jsonb, :run_id)
            """,
        ),
        (
            "location",
            """
            INSERT INTO places (osm_type, osm_id, name, name_search, category, location, raw_tags, last_seen_import_id)
            VALUES ('node', 503, 'A', 'a', 'cafe', NULL, '{}'::jsonb, :run_id)
            """,
        ),
        (
            "raw_tags",
            """
            INSERT INTO places (osm_type, osm_id, name, name_search, category, location, raw_tags, last_seen_import_id)
            VALUES ('node', 504, 'A', 'a', 'cafe', ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography, NULL, :run_id)
            """,
        ),
        (
            "last_seen_import_id",
            """
            INSERT INTO places (osm_type, osm_id, name, name_search, category, location, raw_tags, last_seen_import_id)
            VALUES ('node', 505, 'A', 'a', 'cafe', ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography, '{}'::jsonb, NULL)
            """,
        ),
    ],
)
def test_places_required_columns_reject_null(
    migrated_conn: Connection, column: str, sql: str
) -> None:
    """Required NOT NULL columns on places reject NULL with SQLSTATE 23502."""
    run_id = _seed_run(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(text(sql), {"run_id": run_id})
    assert_pg_error(exc_info, sqlstate="23502", column_name=column)


@pytest.mark.parametrize(
    ("lon", "lat"),
    [
        (106.7009, 10.7769),
        (-180.0, -90.0),
        (180.0, 90.0),
        (0.0, 0.0),
    ],
)
def test_valid_wgs84_points_accepted(
    migrated_conn: Connection, lon: float, lat: float
) -> None:
    """Valid WGS84 points pass Python validation and persist cleanly in GEOGRAPHY(Point,4326)."""
    run_id = _seed_run(migrated_conn)
    validated_lon, validated_lat = validate_wgs84_coordinates(lon, lat)
    assert (validated_lon, validated_lat) == (lon, lat)

    point_expr = make_wgs84_point_expr(lon, lat)
    from sqlalchemy import insert

    from geosearch.db import Place

    place_id = migrated_conn.execute(
        insert(Place)
        .values(
            osm_type="node",
            osm_id=600,
            name="Valid Point Cafe",
            name_search="valid point cafe",
            category="cafe",
            location=point_expr,
            raw_tags={"amenity": "cafe"},
            last_seen_import_id=run_id,
        )
        .returning(Place.id)
    ).scalar_one()

    stored_lon, stored_lat = migrated_conn.execute(
        text("SELECT ST_X(location::geometry), ST_Y(location::geometry) FROM places WHERE id = :id"),
        {"id": place_id},
    ).one()
    assert math.isclose(stored_lon, lon, abs_tol=1e-9)
    assert math.isclose(stored_lat, lat, abs_tol=1e-9)


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
        (True, 10.0),
    ],
)
def test_python_layer_rejects_invalid_wgs84_coordinates(
    bad_lon: float, bad_lat: float
) -> None:
    """Python coordinate validation rejects non-finite and out-of-range coordinates before SQL construction."""
    with pytest.raises(InvalidCoordinateError):
        make_wgs84_point_expr(bad_lon, bad_lat)


def test_postgres_layer_rejects_empty_and_nan_points(migrated_conn: Connection) -> None:
    """PostgreSQL CHECK constraints reject POINT EMPTY and NaN coordinates directly at SQL level."""
    run_id = _seed_run(migrated_conn)

    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO places (
                        osm_type, osm_id, name, name_search, category,
                        location, raw_tags, last_seen_import_id
                    )
                    VALUES (
                        'node', 701, 'Empty Point', 'empty point', 'cafe',
                        ST_GeomFromText('POINT EMPTY', 4326)::geography,
                        '{}'::jsonb, :run_id
                    )
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_location_non_empty")

    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO places (
                        osm_type, osm_id, name, name_search, category,
                        location, raw_tags, last_seen_import_id
                    )
                    VALUES (
                        'node', 702, 'NaN Point', 'nan point', 'cafe',
                        ST_SetSRID(ST_MakePoint('NaN'::double precision, 10.0), 4326)::geography,
                        '{}'::jsonb, :run_id
                    )
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="ck_places_location_finite_wgs84")


def test_boundary_version_lifecycle_valid_states_and_default_draft(
    migrated_conn: Connection,
) -> None:
    """Approved DRAFT -> VALIDATED -> APPROVED states persist cleanly with expected default."""
    assert BOUNDARY_APPROVAL_STATUSES == ("DRAFT", "VALIDATED", "APPROVED")

    # Default status without explicit approval_status must be DRAFT.
    default_id, default_status = migrated_conn.execute(
        text(
            """
            INSERT INTO boundary_versions (version_code, geom, source_name, checksum_sha256)
            VALUES (
                'bv-default-draft',
                ST_GeomFromText(:geom_wkt, 4326),
                'synthetic-test-source',
                :sha
            )
            RETURNING id, approval_status
            """
        ),
        {"geom_wkt": VALID_MULTIPOLYGON_WKT, "sha": VALID_SHA256},
    ).one()
    assert default_id > 0
    assert default_status == "DRAFT"

    draft_id = insert_boundary_version(
        migrated_conn, version_code="bv-explicit-draft", approval_status="DRAFT"
    )
    validated_id = insert_boundary_version(
        migrated_conn, version_code="bv-explicit-validated", approval_status="VALIDATED"
    )
    approved_id = insert_boundary_version(
        migrated_conn, version_code="bv-explicit-approved", approval_status="APPROVED"
    )
    assert len({default_id, draft_id, validated_id, approved_id}) == 4


@pytest.mark.parametrize(
    "unapproved_status",
    ["PENDING", "REJECTED", "RETIRED", "draft", "approved", ""],
)
def test_boundary_version_rejects_unapproved_lifecycle_statuses(
    migrated_conn: Connection, unapproved_status: str
) -> None:
    """Alternative or unapproved boundary lifecycle statuses are rejected."""
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(migrated_conn, approval_status=unapproved_status)
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_boundary_versions_approval_status_domain",
    )


@pytest.mark.parametrize(
    (
        "status",
        "validated_by",
        "validated_at",
        "validation_evidence_json",
        "approved_by",
        "approved_at",
    ),
    [
        # DRAFT cannot carry premature validation or approval metadata.
        ("DRAFT", "validator@test", None, None, None, None),
        ("DRAFT", None, DEFAULT_VALIDATED_AT, None, None, None),
        ("DRAFT", None, None, DEFAULT_VALIDATION_EVIDENCE_JSON, None, None),
        ("DRAFT", None, None, None, "reviewer@test", None),
        ("DRAFT", None, None, None, None, DEFAULT_APPROVED_AT),
        # VALIDATED requires full validation provenance and forbids approval metadata.
        ("VALIDATED", None, DEFAULT_VALIDATED_AT, DEFAULT_VALIDATION_EVIDENCE_JSON, None, None),
        ("VALIDATED", "   ", DEFAULT_VALIDATED_AT, DEFAULT_VALIDATION_EVIDENCE_JSON, None, None),
        ("VALIDATED", "validator@test", None, DEFAULT_VALIDATION_EVIDENCE_JSON, None, None),
        ("VALIDATED", "validator@test", DEFAULT_VALIDATED_AT, None, None, None),
        (
            "VALIDATED",
            "validator@test",
            DEFAULT_VALIDATED_AT,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            "reviewer@test",
            None,
        ),
        (
            "VALIDATED",
            "validator@test",
            DEFAULT_VALIDATED_AT,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            None,
            DEFAULT_APPROVED_AT,
        ),
        # APPROVED requires BOTH validation provenance AND approval metadata (approved_at >= validated_at).
        (
            "APPROVED",
            None,
            DEFAULT_VALIDATED_AT,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            "reviewer@test",
            DEFAULT_APPROVED_AT,
        ),
        (
            "APPROVED",
            "validator@test",
            None,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            "reviewer@test",
            DEFAULT_APPROVED_AT,
        ),
        (
            "APPROVED",
            "validator@test",
            DEFAULT_VALIDATED_AT,
            None,
            "reviewer@test",
            DEFAULT_APPROVED_AT,
        ),
        (
            "APPROVED",
            "validator@test",
            DEFAULT_VALIDATED_AT,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            None,
            DEFAULT_APPROVED_AT,
        ),
        (
            "APPROVED",
            "validator@test",
            DEFAULT_VALIDATED_AT,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            "   ",
            DEFAULT_APPROVED_AT,
        ),
        (
            "APPROVED",
            "validator@test",
            DEFAULT_VALIDATED_AT,
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            "reviewer@test",
            None,
        ),
        (
            "APPROVED",
            "validator@test",
            datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc),
            DEFAULT_VALIDATION_EVIDENCE_JSON,
            "reviewer@test",
            datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        ),
    ],
)
def test_boundary_version_lifecycle_metadata_consistency_enforced(
    migrated_conn: Connection,
    status: str,
    validated_by: str | None,
    validated_at: datetime | None,
    validation_evidence_json: str | None,
    approved_by: str | None,
    approved_at: datetime | None,
) -> None:
    """Row-level metadata constraints enforce DRAFT, VALIDATED, and APPROVED invariants."""
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(
                migrated_conn,
                approval_status=status,
                validated_by=validated_by,
                validated_at=validated_at,
                validation_evidence_json=validation_evidence_json,
                approved_by=approved_by,
                approved_at=approved_at,
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_boundary_versions_lifecycle_metadata_consistency",
    )


@pytest.mark.parametrize("invalid_evidence", ["[]", '"ok"', "123", "true", "null"])
def test_boundary_version_validation_evidence_must_be_json_object(
    migrated_conn: Connection, invalid_evidence: str
) -> None:
    """validation_evidence must be a JSONB object whenever present."""
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(
                migrated_conn,
                approval_status="VALIDATED",
                validation_evidence_json=invalid_evidence,
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_boundary_versions_validation_evidence_json_object",
    )


def test_boundary_version_geometry_and_checksum_constraints(migrated_conn: Connection) -> None:
    """Invalid/empty boundary polygons and malformed checksums are rejected even for DRAFT rows."""
    # Self-intersecting bowtie polygon
    bowtie_wkt = "MULTIPOLYGON(((0 0, 10 10, 10 0, 0 10, 0 0)))"
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(migrated_conn, geom_wkt=bowtie_wkt, approval_status="DRAFT")
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_boundary_versions_geom_is_valid"
    )

    # Empty MultiPolygon
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(
                migrated_conn, geom_wkt="MULTIPOLYGON EMPTY", approval_status="DRAFT"
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_boundary_versions_geom_non_empty"
    )

    # Invalid SHA-256 format
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(
                migrated_conn, checksum_sha256="not-a-sha256", approval_status="DRAFT"
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_boundary_versions_checksum_sha256_format"
    )


def test_import_issues_osm_identity_pair_and_fk(migrated_conn: Connection) -> None:
    """import_issues requires OSM type and ID to be paired and enforces FK to import_runs."""
    run_id = _seed_run(migrated_conn)

    # Valid run-level issue without OSM identity
    issue_id = migrated_conn.execute(
        text(
            """
            INSERT INTO import_issues (import_run_id, severity, stage, issue_code, message)
            VALUES (:run_id, 'INFO', 'EXTRACT', 'RUN_START', 'Extraction started')
            RETURNING id
            """
        ),
        {"run_id": run_id},
    ).scalar_one()
    assert issue_id > 0

    # Partial OSM identity rejected
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO import_issues (
                        import_run_id, severity, stage, issue_code, message, osm_type, osm_id
                    )
                    VALUES (:run_id, 'ERROR', 'VALIDATE', 'MISSING_ID', 'Partial id', 'node', NULL)
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_import_issues_osm_identity_pair"
    )

    # Non-existent import_run_id rejected by FK
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO import_issues (
                        import_run_id, severity, stage, issue_code, message
                    )
                    VALUES (999999999, 'ERROR', 'VALIDATE', 'ORPHAN', 'Missing run')
                    """
                )
            )
    assert_pg_error(
        exc_info,
        sqlstate="23503",
        constraint_name="fk_import_issues_import_run_id_import_runs",
    )
