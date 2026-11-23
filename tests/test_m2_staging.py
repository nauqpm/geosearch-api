"""M2.4 PostgreSQL/PostGIS integration tests for deterministic staging, fingerprints, issues, and readiness."""

from __future__ import annotations

import math
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone

import pytest
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError

from geosearch.db import create_test_engine
from geosearch.ingestion import (
    EXTRACTION_SCOPE_NODE_ONLY,
    ExtractionStats,
    ImportRunStateError,
    NormalizationIssue,
    StagingReadinessError,
    begin_staging_run,
    build_canonical_candidate,
    compute_candidate_content_digest,
    compute_config_digest,
    compute_source_fingerprint,
    create_import_run,
    mark_extraction_completed,
    record_import_issue,
    stage_candidate_batch,
    verify_and_prepare_run,
)
from tests.helpers import (
    VALID_SHA256,
    assert_pg_error,
    insert_boundary_version,
    insert_import_run,
    upgrade_test_db_to_head,
)


def _make_cafe(
    *,
    osm_id: int = 1001,
    name: str = "Cà Phê Sữa Đá",
    lon: float = 106.7009,
    lat: float = 10.7769,
    extra_tags: dict[str, str] | None = None,
):
    tags = {"amenity": "cafe", "name": name}
    if extra_tags:
        tags.update(extra_tags)
    return build_canonical_candidate(
        osm_type="node",
        osm_id=osm_id,
        category="cafe",
        raw_name=name,
        longitude=lon,
        latitude=lat,
        raw_tags=tags,
        osm_version=2,
        osm_changeset=1234,
        osm_timestamp=datetime(2026, 1, 10, 8, 0, tzinfo=timezone.utc),
        source_provenance={"source_name": "synthetic.osm", "extraction_scope": "node_only"},
    )


def test_deterministic_source_fingerprint_and_content_digest() -> None:
    """Source fingerprints and candidate content digests are deterministic and sensitive to semantic inputs."""
    cfg_1 = compute_config_digest(apply_boundary_filter=True)
    cfg_2 = compute_config_digest(apply_boundary_filter=True)
    cfg_no_filter = compute_config_digest(apply_boundary_filter=False)
    assert cfg_1 == cfg_2
    assert cfg_1 != cfg_no_filter
    assert len(cfg_1) == 64

    fp_1 = compute_source_fingerprint(
        source_checksum_sha256="a" * 64,
        source_snapshot_id="vietnam-2026-01-01.osm.pbf",
        boundary_version_code="hcmc-synthetic-v1",
        boundary_checksum_sha256="b" * 64,
        config_digest=cfg_1,
    )
    fp_2 = compute_source_fingerprint(
        source_checksum_sha256="a" * 64,
        source_snapshot_id="vietnam-2026-01-01.osm.pbf",
        boundary_version_code="hcmc-synthetic-v1",
        boundary_checksum_sha256="b" * 64,
        config_digest=cfg_1,
    )
    assert fp_1 == fp_2
    assert len(fp_1) == 64

    # Changing any provenance component changes the fingerprint.
    fp_diff_source = compute_source_fingerprint(
        source_checksum_sha256="c" * 64,
        source_snapshot_id="vietnam-2026-01-01.osm.pbf",
        boundary_version_code="hcmc-synthetic-v1",
        boundary_checksum_sha256="b" * 64,
        config_digest=cfg_1,
    )
    fp_diff_boundary = compute_source_fingerprint(
        source_checksum_sha256="a" * 64,
        source_snapshot_id="vietnam-2026-01-01.osm.pbf",
        boundary_version_code="hcmc-synthetic-v2",
        boundary_checksum_sha256="b" * 64,
        config_digest=cfg_1,
    )
    assert fp_diff_source != fp_1
    assert fp_diff_boundary != fp_1

    c1 = _make_cafe(osm_id=1, extra_tags={"a": "1", "b": "2"})
    c2 = _make_cafe(osm_id=1, extra_tags={"b": "2", "a": "1"})
    c3 = _make_cafe(osm_id=1, name="Cà Phê Đen")
    assert compute_candidate_content_digest(c1) == compute_candidate_content_digest(c2)
    assert compute_candidate_content_digest(c1) != compute_candidate_content_digest(c3)


def test_staging_insert_retrieval_spatial_representation_and_readiness(
    migrated_conn: Connection,
) -> None:
    """Staging persists validated candidates with exact WGS84 coordinates and verifies readiness."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-stage-1")
    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    assert run.status == "PENDING"

    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256=run.source_checksum_sha256,
    )

    cafe = _make_cafe(osm_id=1001, lon=106.7009, lat=10.7769, extra_tags={"wifi": "yes"})
    atm_unnamed = build_canonical_candidate(
        osm_type="node",
        osm_id=1002,
        category="atm",
        raw_name=None,
        longitude=106.7050,
        latitude=10.7800,
        raw_tags={"amenity": "atm", "operator": "ACB"},
        source_provenance={"source_name": "fixture.osm"},
    )
    outside_cafe = _make_cafe(osm_id=1003, name="Hanoi Cafe", lon=105.8542, lat=21.0285)

    issue = NormalizationIssue(
        severity="ERROR",
        stage="VALIDATE",
        issue_code="MISSING_REQUIRED_NAME",
        message="Cafe missing required name",
        osm_type="node",
        osm_id=1004,
        details={"category": "cafe"},
    )

    batch_res = stage_candidate_batch(
        migrated_conn,
        import_run_id=run.id,
        candidates=[cafe, atm_unnamed, outside_cafe],
        issues=[issue],
        apply_boundary_filter=True,
    )
    assert batch_res.inserted == 2
    assert batch_res.outside_boundary == 1
    assert batch_res.duplicate_identical == 0
    assert batch_res.conflicts == 0
    assert batch_res.issues_recorded == 1

    # Before extraction EOF, readiness verification MUST fail.
    with pytest.raises(StagingReadinessError, match="EXTRACTED"):
        verify_and_prepare_run(migrated_conn, import_run_id=run.id)

    stats = ExtractionStats(
        total_nodes_seen=5,
        supported_nodes_seen=4,
        unsupported_nodes_seen=1,
        valid_candidates=3,
        invalid_candidates=1,
    )
    mark_extraction_completed(migrated_conn, import_run_id=run.id, stats=stats)

    # Reaching EOF alone leaves import_runs.status as PROCESSING, NOT PREPARED.
    status_at_eof = migrated_conn.execute(
        text("SELECT status FROM import_runs WHERE id = :id"),
        {"id": run.id},
    ).scalar_one()
    assert status_at_eof == "PROCESSING"

    # Explicit readiness check verifies counts and transitions run to PREPARED.
    report = verify_and_prepare_run(migrated_conn, import_run_id=run.id)
    assert report.staged_candidate_count == 2
    assert report.outside_boundary_count == 1
    assert report.issue_count == 1
    assert report.extraction_scope == EXTRACTION_SCOPE_NODE_ONLY

    row = migrated_conn.execute(
        text(
            """
            SELECT osm_type, osm_id, name, name_search, category,
                   ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat,
                   raw_tags, source_provenance
            FROM staging_place_candidates
            WHERE import_run_id = :run_id AND osm_id = 1001
            """
        ),
        {"run_id": run.id},
    ).one()
    assert row.osm_type == "node"
    assert row.name == "Cà Phê Sữa Đá"
    assert row.name_search == "ca phe sua da"
    assert math.isclose(row.lon, 106.7009, abs_tol=1e-9)
    assert math.isclose(row.lat, 10.7769, abs_tol=1e-9)
    assert row.raw_tags["wifi"] == "yes"
    assert "source_name" not in row.raw_tags
    assert row.source_provenance["source_name"] == "synthetic.osm"

    # Live places table must remain completely untouched by M2 staging.
    live_count = migrated_conn.execute(text("SELECT count(*) FROM places")).scalar_one()
    assert live_count == 0


def test_staging_uniqueness_within_run_and_isolation_across_runs(
    migrated_conn: Connection,
) -> None:
    """DB enforces uniqueness per run while allowing the same OSM identity across separate runs."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-iso")
    run_a = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    run_b = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    # Two independent runs over identical source have distinct IDs and equal fingerprints.
    assert run_a.id != run_b.id
    assert run_a.source_fingerprint == run_b.source_fingerprint

    for run in (run_a, run_b):
        begin_staging_run(
            migrated_conn,
            import_run_id=run.id,
            expected_source_fingerprint=run.source_fingerprint,
            expected_source_checksum_sha256=run.source_checksum_sha256,
        )

    cafe_a = _make_cafe(osm_id=5001, name="Cafe Run A")
    cafe_b = _make_cafe(osm_id=5001, name="Cafe Run B")

    stage_candidate_batch(migrated_conn, import_run_id=run_a.id, candidates=[cafe_a])
    stage_candidate_batch(migrated_conn, import_run_id=run_b.id, candidates=[cafe_b])

    # Run A and Run B are completely isolated.
    name_a = migrated_conn.execute(
        text(
            "SELECT name FROM staging_place_candidates "
            "WHERE import_run_id = :run_id AND osm_id = 5001"
        ),
        {"run_id": run_a.id},
    ).scalar_one()
    name_b = migrated_conn.execute(
        text(
            "SELECT name FROM staging_place_candidates "
            "WHERE import_run_id = :run_id AND osm_id = 5001"
        ),
        {"run_id": run_b.id},
    ).scalar_one()
    assert name_a == "Cafe Run A"
    assert name_b == "Cafe Run B"

    # Direct duplicate insert inside the same run is blocked by DB unique constraint.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_place_candidates (
                        import_run_id, osm_type, osm_id, name, name_search, category,
                        location, raw_tags, content_digest, source_provenance
                    )
                    VALUES (
                        :run_id, 'node', 5001, 'Dup', 'dup', 'cafe',
                        ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography,
                        '{}'::jsonb, repeat('b', 64), '{}'::jsonb
                    )
                    """
                ),
                {"run_id": run_a.id},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23505",
        constraint_name="uq_staging_place_candidates_run_osm_identity",
    )


def test_retry_identical_is_idempotent_and_conflict_is_quarantined_without_overwrite(
    migrated_conn: Connection,
) -> None:
    """Identical retry does not duplicate rows; conflicting identity preserves original and blocks readiness."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-retry")
    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256=run.source_checksum_sha256,
    )

    original = _make_cafe(osm_id=7001, name="Original Cafe")
    res_1 = stage_candidate_batch(migrated_conn, import_run_id=run.id, candidates=[original])
    assert res_1.inserted == 1

    # Retry with identical canonical content -> idempotent no-op.
    res_2 = stage_candidate_batch(migrated_conn, import_run_id=run.id, candidates=[original])
    assert res_2.inserted == 0
    assert res_2.duplicate_identical == 1
    assert res_2.conflicts == 0

    # Retry with conflicting content for the same OSM identity -> preserved, never overwritten.
    conflicting = _make_cafe(osm_id=7001, name="Mutated Cafe")
    res_3 = stage_candidate_batch(migrated_conn, import_run_id=run.id, candidates=[conflicting])
    assert res_3.inserted == 0
    assert res_3.conflicts == 1
    assert res_3.issues_recorded == 1

    stored_name = migrated_conn.execute(
        text(
            "SELECT name FROM staging_place_candidates "
            "WHERE import_run_id = :run_id AND osm_id = 7001"
        ),
        {"run_id": run.id},
    ).scalar_one()
    assert stored_name == "Original Cafe"

    issue_codes = migrated_conn.execute(
        text("SELECT issue_code FROM import_issues WHERE import_run_id = :run_id"),
        {"run_id": run.id},
    ).scalars().all()
    assert issue_codes == ["STAGING_CONFLICT"]

    # Even after EOF, a run with unresolved conflicts MUST NOT become PREPARED.
    mark_extraction_completed(
        migrated_conn,
        import_run_id=run.id,
        stats=ExtractionStats(
            total_nodes_seen=3,
            supported_nodes_seen=3,
            unsupported_nodes_seen=0,
            valid_candidates=3,
            invalid_candidates=0,
        ),
    )
    with pytest.raises(StagingReadinessError, match="conflict"):
        verify_and_prepare_run(migrated_conn, import_run_id=run.id)

    run_status = migrated_conn.execute(
        text("SELECT status FROM import_runs WHERE id = :run_id"),
        {"run_id": run.id},
    ).scalar_one()
    assert run_status == "PROCESSING"


def test_refuses_reusing_published_run_or_mismatched_resume_fingerprint(
    migrated_conn: Connection,
) -> None:
    """Published runs cannot be re-staged; resume rejects mismatched source fingerprint."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-guard")
    pub_run_id = insert_import_run(
        migrated_conn,
        bv_id,
        status="PUBLISHED",
        publication_operation_id=uuid.uuid4(),
        published_at=datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
        active_slot=1,
    )
    with pytest.raises(ImportRunStateError, match="PUBLISHED"):
        begin_staging_run(
            migrated_conn,
            import_run_id=pub_run_id,
            expected_source_fingerprint="fp-default-v1",
            expected_source_checksum_sha256=VALID_SHA256,
        )

    proc_run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=proc_run.id,
        expected_source_fingerprint=proc_run.source_fingerprint,
        expected_source_checksum_sha256=proc_run.source_checksum_sha256,
    )

    with pytest.raises(ImportRunStateError, match="fingerprint mismatch"):
        begin_staging_run(
            migrated_conn,
            import_run_id=proc_run.id,
            expected_source_fingerprint="different-fingerprint",
            expected_source_checksum_sha256=proc_run.source_checksum_sha256,
        )


def test_import_issue_recording_and_incomplete_count_drift_rejected(
    migrated_conn: Connection,
) -> None:
    """Standalone issue recording works; tampered/mismatched staging counts fail readiness."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-drift")
    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256=run.source_checksum_sha256,
    )
    stage_candidate_batch(migrated_conn, import_run_id=run.id, candidates=[_make_cafe(osm_id=8001)])
    issue_id = record_import_issue(
        migrated_conn,
        import_run_id=run.id,
        issue=NormalizationIssue(
            severity="INFO",
            stage="STAGE",
            issue_code="DIAGNOSTIC_NOTE",
            message="Diagnostic note recorded",
            details={"note": "ok"},
        ),
    )
    assert issue_id > 0

    mark_extraction_completed(
        migrated_conn,
        import_run_id=run.id,
        stats=ExtractionStats(
            total_nodes_seen=1,
            supported_nodes_seen=1,
            unsupported_nodes_seen=0,
            valid_candidates=1,
            invalid_candidates=0,
        ),
    )

    # Deliberately introduce staging count mismatch (delete staged row without updating checkpoint).
    migrated_conn.execute(
        text("DELETE FROM staging_place_candidates WHERE import_run_id = :run_id"),
        {"run_id": run.id},
    )
    with pytest.raises(StagingReadinessError, match="Staged row count mismatch"):
        verify_and_prepare_run(migrated_conn, import_run_id=run.id)


def test_failed_staging_transaction_rolls_back_cleanly_on_real_commit(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """A failed bounded staging transaction leaves zero partial writes across real COMMITs."""
    with disposable_database("m2_stage_rb") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-commit-rb")
                run = create_import_run(
                    conn,
                    boundary_version_id=bv_id,
                    source_name="fixture.osm",
                    source_checksum_sha256=VALID_SHA256,
                )
                begin_staging_run(
                    conn,
                    import_run_id=run.id,
                    expected_source_fingerprint=run.source_fingerprint,
                    expected_source_checksum_sha256=run.source_checksum_sha256,
                )

            # Batch 1 commits cleanly.
            with engine.begin() as conn:
                stage_candidate_batch(
                    conn,
                    import_run_id=run.id,
                    candidates=[_make_cafe(osm_id=9101)],
                )

            # Batch 2 fails mid-transaction -> must roll back completely.
            with pytest.raises(RuntimeError, match="Simulated batch crash"):
                with engine.begin() as conn:
                    stage_candidate_batch(
                        conn,
                        import_run_id=run.id,
                        candidates=[_make_cafe(osm_id=9102), _make_cafe(osm_id=9103)],
                    )
                    raise RuntimeError("Simulated batch crash before commit")

            # Verify on a fresh connection: only Batch 1 persisted, zero rows from Batch 2.
            with engine.connect() as conn:
                staged_ids = conn.execute(
                    text(
                        "SELECT osm_id FROM staging_place_candidates "
                        "WHERE import_run_id = :run_id ORDER BY osm_id"
                    ),
                    {"run_id": run.id},
                ).scalars().all()
                assert staged_ids == [9101]

                cp = conn.execute(
                    text(
                        "SELECT batches_committed, staged_candidate_count, staging_state "
                        "FROM staging_run_checkpoints WHERE import_run_id = :run_id"
                    ),
                    {"run_id": run.id},
                ).one()
                assert cp.batches_committed == 1
                assert cp.staged_candidate_count == 1
                assert cp.staging_state == "IN_PROGRESS"
        finally:
            engine.dispose()
