"""M3.1 publication preflight eligibility and 0003 PostgreSQL trigger guard integration tests."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import osmium
import pytest
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError

from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    EligibilityStatus,
    EligibilityTier,
    PublicationMode,
    register_publication_operation,
    validate_publication_eligibility,
)
from tests.helpers import (
    DEFAULT_APPROVED_AT,
    DEFAULT_VALIDATED_AT,
    VALID_SHA256,
    assert_pg_error,
    insert_boundary_version,
    insert_import_run,
    insert_place,
    upgrade_test_db_to_head,
)


def _write_pbf(path: Path, nodes: list[osmium.osm.mutable.Node]) -> Path:
    writer = osmium.SimpleWriter(str(path))
    try:
        for node in nodes:
            writer.add_node(node)
    finally:
        writer.close()
    return path


def _stage_prepared_node_run(
    conn: Connection,
    work_dir: Path,
    *,
    boundary_version_id: int,
    filename: str = "v1.osm.pbf",
    osm_ids: tuple[int, ...] = (101, 102),
) -> int:
    nodes = [
        osmium.osm.mutable.Node(
            id=oid,
            version=1,
            changeset=100,
            timestamp="2026-01-15T00:00:00Z",
            location=(106.7009 + idx * 0.001, 10.7769),
            tags={"amenity": "cafe", "name": f"Cà Phê {oid}"},
        )
        for idx, oid in enumerate(osm_ids)
    ]
    pbf = _write_pbf(work_dir / filename, nodes)
    report = ingest_osm_nodes_to_staging(
        conn,
        source_path=pbf,
        boundary_version_id=boundary_version_id,
        prepare_on_completion=True,
    )
    assert report.import_run.status == "PREPARED"
    return report.import_run.id


# ---------------------------------------------------------------------------
# M3.1 Preflight & Authorization Tiers
# ---------------------------------------------------------------------------


def test_m31_authoritative_mode_blocks_node_only_staging_ready_run(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Default AUTHORITATIVE mode rejects incomplete node_only runs as COVERAGE_INCOMPATIBLE."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-auth-block")
    run_id = _stage_prepared_node_run(migrated_conn, work_dir, boundary_version_id=bv_id)

    res = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_id,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.AUTHORITATIVE,
    )
    assert res.eligible is False
    assert res.status == EligibilityStatus.COVERAGE_INCOMPATIBLE
    assert res.tier == EligibilityTier.STAGING_READY
    assert res.extraction_scope == "node_only"
    assert res.coverage_restriction == "node_only_incomplete_osm_coverage"
    assert migrated_conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0


def test_m31_controlled_test_mode_distinguishes_approved_vs_draft_or_validated_boundary(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Synthetic DRAFT/VALIDATED boundaries are blocked; APPROVED synthetic passes controlled test."""
    bv_draft = insert_boundary_version(
        migrated_conn, version_code="bv-draft", approval_status="DRAFT"
    )
    bv_validated = insert_boundary_version(
        migrated_conn, version_code="bv-validated", approval_status="VALIDATED"
    )
    bv_approved = insert_boundary_version(
        migrated_conn, version_code="bv-approved", approval_status="APPROVED"
    )

    run_draft = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_draft, filename="draft.osm.pbf"
    )
    run_val = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_validated, filename="val.osm.pbf"
    )
    run_app = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_approved, filename="app.osm.pbf"
    )

    res_draft = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_draft,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_draft.eligible is False
    assert res_draft.status == EligibilityStatus.BOUNDARY_NOT_APPROVED
    assert res_draft.tier == EligibilityTier.STAGING_READY

    res_val = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_val,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_val.eligible is False
    assert res_val.status == EligibilityStatus.BOUNDARY_NOT_APPROVED
    assert res_val.tier == EligibilityTier.STAGING_READY

    res_app = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_app,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_app.eligible is True
    assert res_app.status == EligibilityStatus.ELIGIBLE
    assert res_app.tier == EligibilityTier.ELIGIBLE_FOR_CONTROLLED_TEST_PUBLICATION
    assert res_app.is_first_publication is True
    assert res_app.staged_candidate_count == 2
    assert migrated_conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0


def test_m31_blocks_node_only_publication_over_active_full_coverage_dataset(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """A node_only run cannot replace an active full-coverage / authoritative publication."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-full-active")
    insert_import_run(
        migrated_conn,
        bv_id,
        status="PUBLISHED",
        publication_operation_id=uuid.uuid4(),
        published_at=datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
        publication_evidence_json=(
            '{"publication_mode": "AUTHORITATIVE", "extraction_scope": "full_osm"}'
        ),
        active_slot=1,
    )
    run_node = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_id, filename="node_only.osm.pbf"
    )

    res = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_node,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res.eligible is False
    assert res.status == EligibilityStatus.COVERAGE_INCOMPATIBLE
    assert any("incompatible coverage" in r.lower() for r in res.reasons)


@pytest.mark.parametrize("bad_status", ["PENDING", "PROCESSING", "FAILED", "ABORTED"])
def test_m31_rejects_non_prepared_run_statuses(
    migrated_conn: Connection,
    bad_status: str,
) -> None:
    """Runs not in PREPARED status fail preflight with NOT_PREPARED."""
    bv_id = insert_boundary_version(migrated_conn, version_code=f"bv-{bad_status.lower()}")
    run_id = insert_import_run(migrated_conn, bv_id, status=bad_status)

    res = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_id,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res.eligible is False
    assert res.status == EligibilityStatus.NOT_PREPARED


def test_m31_rejects_tampered_staging_digest_and_provenance_mismatch(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Direct post-PREPARED tampering is blocked by DB trigger; defense-in-depth catches corrupted digests/provenance."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-tamper")
    run_id = _stage_prepared_node_run(migrated_conn, work_dir, boundary_version_id=bv_id)

    # 0. Direct UPDATE on PREPARED staging is blocked at DB layer by 0004 trigger.
    with pytest.raises(IntegrityError) as exc_trg:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    "UPDATE staging_place_candidates SET name = 'Direct Tamper', "
                    "name_search = 'direct tamper' WHERE import_run_id = :run_id AND osm_id = 101"
                ),
                {"run_id": run_id},
            )
    assert_pg_error(exc_trg, sqlstate="23514", constraint_name="trg_prepared_staging_immutable")

    # 1. Defense-in-depth: even if staging content were corrupted before trigger enforcement,
    #    validate_publication_eligibility rejects mismatched content_digest -> INVALID_STAGING.
    with migrated_conn.begin_nested() as sp:
        migrated_conn.execute(
            text(
                "ALTER TABLE staging_place_candidates "
                "DISABLE TRIGGER trg_staging_place_candidates_immutability"
            )
        )
        migrated_conn.execute(
            text(
                "UPDATE staging_place_candidates SET name = 'Tampered Cafe', "
                "name_search = 'tampered cafe' WHERE import_run_id = :run_id AND osm_id = 101"
            ),
            {"run_id": run_id},
        )
        migrated_conn.execute(
            text(
                "ALTER TABLE staging_place_candidates "
                "ENABLE TRIGGER trg_staging_place_candidates_immutability"
            )
        )
        res = validate_publication_eligibility(
            migrated_conn,
            import_run_id=run_id,
            publication_operation_id=uuid.uuid4(),
            publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
        )
        assert res.eligible is False
        assert res.status == EligibilityStatus.INVALID_STAGING
        assert any("content_digest" in r for r in res.reasons)
        sp.rollback()

    # 2. Defense-in-depth: checkpoint source_fingerprint mismatch -> SOURCE_PROVENANCE_MISMATCH.
    with migrated_conn.begin_nested() as sp:
        migrated_conn.execute(
            text(
                "ALTER TABLE staging_run_checkpoints "
                "DISABLE TRIGGER trg_staging_run_checkpoints_immutability"
            )
        )
        migrated_conn.execute(
            text(
                "UPDATE staging_run_checkpoints SET source_fingerprint = 'tampered-fp' "
                "WHERE import_run_id = :run_id"
            ),
            {"run_id": run_id},
        )
        migrated_conn.execute(
            text(
                "ALTER TABLE staging_run_checkpoints "
                "ENABLE TRIGGER trg_staging_run_checkpoints_immutability"
            )
        )
        res = validate_publication_eligibility(
            migrated_conn,
            import_run_id=run_id,
            publication_operation_id=uuid.uuid4(),
            publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
        )
        assert res.eligible is False
        assert res.status == EligibilityStatus.SOURCE_PROVENANCE_MISMATCH
        sp.rollback()

    # 3. Missing recovery artifact when replacement publication requires one -> MISSING_RECOVERY_ARTIFACT.
    with migrated_conn.begin_nested() as sp:
        insert_import_run(
            migrated_conn,
            bv_id,
            status="PUBLISHED",
            publication_operation_id=uuid.uuid4(),
            published_at=datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            publication_evidence_json=(
                '{"publication_mode": "CONTROLLED_TEST_NODE_ONLY", "extraction_scope": "node_only"}'
            ),
            active_slot=1,
        )
        res = validate_publication_eligibility(
            migrated_conn,
            import_run_id=run_id,
            publication_operation_id=uuid.uuid4(),
            publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            require_recovery_artifact_for_replacement=True,
            recovery_artifact_dir=None,
        )
        assert res.eligible is False
        assert res.status == EligibilityStatus.MISSING_RECOVERY_ARTIFACT
        sp.rollback()


def test_m31_operation_uuid_ownership_conflict_and_already_published_detection(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Validates nil UUID rejection, conflicting ownership, stale baseline, and ALREADY_PUBLISHED."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-ops")
    run_1 = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_id, filename="r1.osm.pbf"
    )
    run_2 = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_id, filename="r2.osm.pbf"
    )

    # Nil UUID rejected.
    res_nil = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_1,
        publication_operation_id="00000000-0000-0000-0000-000000000000",
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_nil.status == EligibilityStatus.OTHER_BLOCKING_FAILURE

    # Register op_1 on run_1.
    op_1 = uuid.uuid4()
    op_2 = uuid.uuid4()
    register_publication_operation(
        migrated_conn, import_run_id=run_1, publication_operation_id=op_1
    )

    # Same UUID on run_1 while still PREPARED -> still ELIGIBLE (retry after pre-commit failure).
    res_same = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_1,
        publication_operation_id=op_1,
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_same.eligible is True
    assert res_same.status == EligibilityStatus.ELIGIBLE

    # Different UUID on run_1 -> PUBLICATION_CONFLICT.
    res_diff_op = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_1,
        publication_operation_id=op_2,
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_diff_op.status == EligibilityStatus.PUBLICATION_CONFLICT

    # Same UUID on run_2 -> PUBLICATION_CONFLICT.
    res_other_run = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_2,
        publication_operation_id=op_1,
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_other_run.status == EligibilityStatus.PUBLICATION_CONFLICT

    # Stale expected_active_run_id -> PUBLICATION_CONFLICT.
    res_stale = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_2,
        publication_operation_id=op_2,
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
        expected_active_run_id=999999,
    )
    assert res_stale.status == EligibilityStatus.PUBLICATION_CONFLICT

    # Published run queried with its own UUID -> ALREADY_PUBLISHED.
    op_pub = uuid.uuid4()
    pub_run = insert_import_run(
        migrated_conn,
        bv_id,
        status="PUBLISHED",
        publication_operation_id=op_pub,
        published_at=datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
        active_slot=1,
    )
    res_already = validate_publication_eligibility(
        migrated_conn,
        import_run_id=pub_run,
        publication_operation_id=op_pub,
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res_already.status == EligibilityStatus.ALREADY_PUBLISHED
    assert res_already.existing_operation_evidence is not None
    assert res_already.existing_operation_evidence.is_committed_publication is True


def test_m31_authoritative_boundary_evidence_and_orphaned_active_places_rejected(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Authoritative boundary evidence rejects synthetic checks; orphaned active places block first publish."""
    from geosearch.publication.eligibility import _is_authoritative_boundary_evidence

    ok_synth, reasons_synth = _is_authoritative_boundary_evidence(
        {"method": "synthetic-check", "points_verified": 4}
    )
    assert ok_synth is False
    assert reasons_synth

    ok_auth, reasons_auth = _is_authoritative_boundary_evidence(
        {
            "verification_class": "authoritative_external",
            "authoritative_verification": True,
            "external_reference_source": "official-hcmc-2025-gazette",
        }
    )
    assert ok_auth is True
    assert reasons_auth == []

    # Orphaned active place with zero active_slot=1 runs must fail closed.
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-orphan")
    run_id = _stage_prepared_node_run(
        migrated_conn, work_dir, boundary_version_id=bv_id, filename="orphan.osm.pbf"
    )
    insert_place(migrated_conn, run_id, osm_id=99901, is_active=True)

    res = validate_publication_eligibility(
        migrated_conn,
        import_run_id=run_id,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    assert res.eligible is False
    assert res.status == EligibilityStatus.OTHER_BLOCKING_FAILURE
    assert any("active place" in r.lower() for r in res.reasons)


# ---------------------------------------------------------------------------
# Migration 0003 PostgreSQL Trigger Guards
# ---------------------------------------------------------------------------


def test_m3_db_trigger_boundary_immutability_and_monotonic_lifecycle(
    migrated_conn: Connection,
) -> None:
    """Trigger enforces boundary_versions immutability and monotonic DRAFT->VALIDATED->APPROVED."""
    bv_id = insert_boundary_version(
        migrated_conn, version_code="bv-trg-mono", approval_status="DRAFT"
    )

    # Immutable geom cannot be modified.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    "UPDATE boundary_versions SET geom = ST_GeomFromText("
                    "'MULTIPOLYGON(((106.5 10.6, 106.9 10.6, 106.9 11.0, 106.5 11.0, 106.5 10.6)))', "
                    "4326) WHERE id = :id"
                ),
                {"id": bv_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="trg_boundary_versions_immutable_provenance"
    )

    # Valid forward transition DRAFT -> VALIDATED succeeds.
    migrated_conn.execute(
        text(
            """
            UPDATE boundary_versions
            SET approval_status = 'VALIDATED',
                validated_by = 'geo-qa@test',
                validated_at = :val_at,
                validation_evidence = '{"method": "synthetic-check"}'::jsonb
            WHERE id = :id
            """
        ),
        {"id": bv_id, "val_at": DEFAULT_VALIDATED_AT},
    )

    # Backward transition VALIDATED -> DRAFT is blocked.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    UPDATE boundary_versions
                    SET approval_status = 'DRAFT',
                        validated_by = NULL,
                        validated_at = NULL,
                        validation_evidence = NULL
                    WHERE id = :id
                    """
                ),
                {"id": bv_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="trg_boundary_versions_monotonic_lifecycle"
    )

    # Valid forward transition VALIDATED -> APPROVED succeeds.
    migrated_conn.execute(
        text(
            """
            UPDATE boundary_versions
            SET approval_status = 'APPROVED',
                approved_by = 'lead@test',
                approved_at = :app_at
            WHERE id = :id
            """
        ),
        {"id": bv_id, "app_at": DEFAULT_APPROVED_AT},
    )

    # Backward transition APPROVED -> VALIDATED is blocked.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    UPDATE boundary_versions
                    SET approval_status = 'VALIDATED',
                        approved_by = NULL,
                        approved_at = NULL
                    WHERE id = :id
                    """
                ),
                {"id": bv_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="trg_boundary_versions_monotonic_lifecycle"
    )


def test_m3_db_triggers_import_runs_places_and_duplicate_candidates_immutability(
    migrated_conn: Connection,
) -> None:
    """DB triggers enforce PUBLISHED->APPROVED boundary, immutable op UUID, places identity, and dup evidence."""
    bv_draft = insert_boundary_version(
        migrated_conn, version_code="bv-trg-draft", approval_status="DRAFT"
    )
    bv_approved = insert_boundary_version(
        migrated_conn, version_code="bv-trg-app", approval_status="APPROVED"
    )
    t1 = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(hours=1)

    # Cross-table guard: PUBLISHED run cannot reference DRAFT boundary.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_import_run(
                migrated_conn,
                bv_draft,
                status="PUBLISHED",
                publication_operation_id=uuid.uuid4(),
                published_at=t1,
                active_slot=1,
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="trg_import_runs_published_requires_approved_boundary",
    )

    # Operation UUID immutability once registered on PREPARED run.
    op_1 = uuid.uuid4()
    run_id = insert_import_run(
        migrated_conn,
        bv_approved,
        status="PREPARED",
        publication_operation_id=op_1,
    )
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    "UPDATE import_runs SET publication_operation_id = :new_op WHERE id = :id"
                ),
                {"new_op": uuid.uuid4(), "id": run_id},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="trg_import_runs_immutable_publication_operation_id",
    )

    # PUBLISHED cannot transition back to PROCESSING.
    migrated_conn.execute(
        text(
            "UPDATE import_runs SET status = 'PUBLISHED', active_slot = 1, "
            "published_at = :t1 WHERE id = :id"
        ),
        {"t1": t1, "id": run_id},
    )
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    "UPDATE import_runs SET status = 'PROCESSING', active_slot = NULL, "
                    "published_at = NULL WHERE id = :id"
                ),
                {"id": run_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="trg_import_runs_immutable_published_at"
    )

    # SUPERSEDED run cannot be mutated or reactivated.
    migrated_conn.execute(
        text(
            "UPDATE import_runs SET status = 'SUPERSEDED', active_slot = NULL, "
            "superseded_at = :t2 WHERE id = :id"
        ),
        {"t2": t2, "id": run_id},
    )
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text("UPDATE import_runs SET publication_evidence = '{}'::jsonb WHERE id = :id"),
                {"id": run_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="trg_import_runs_immutable_superseded"
    )

    # places OSM identity immutability.
    p1 = insert_place(migrated_conn, run_id, osm_id=501)
    p2 = insert_place(migrated_conn, run_id, osm_id=502)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text("UPDATE places SET osm_id = 999999 WHERE id = :id"),
                {"id": p1},
            )
    assert_pg_error(exc_info, sqlstate="23514", constraint_name="trg_places_immutable_identity")

    # duplicate_candidates immutability on UPDATE.
    low, high = min(p1, p2), max(p1, p2)
    migrated_conn.execute(
        text(
            """
            INSERT INTO duplicate_candidates (
                import_run_id, place_a_id, place_b_id, detector_version, evidence
            )
            VALUES (:run_id, :a, :b, 'det-v1', '{"score": 0.9}'::jsonb)
            """
        ),
        {"run_id": run_id, "a": low, "b": high},
    )
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    "UPDATE duplicate_candidates SET evidence = '{\"score\": 0.1}'::jsonb "
                    "WHERE import_run_id = :run_id"
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="trg_duplicate_candidates_immutable"
    )
