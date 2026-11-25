"""M1.4 integration tests for uniqueness, import_runs active_slot, and duplicate_candidates."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError

from geosearch.db import create_test_engine
from tests.helpers import (
    assert_pg_error,
    insert_boundary_version,
    insert_import_run,
    insert_place,
    upgrade_test_db_to_head,
)


def test_duplicate_osm_identity_rejected(migrated_conn: Connection) -> None:
    """Duplicate (osm_type, osm_id) in places is rejected by uq_places_osm_identity."""
    bv_id = insert_boundary_version(migrated_conn)
    run_id = insert_import_run(migrated_conn, bv_id)

    insert_place(migrated_conn, run_id, osm_type="node", osm_id=42001)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_place(migrated_conn, run_id, osm_type="node", osm_id=42001)
    assert_pg_error(exc_info, sqlstate="23505", constraint_name="uq_places_osm_identity")


def test_duplicate_boundary_version_code_rejected(migrated_conn: Connection) -> None:
    """Duplicate version_code in boundary_versions is rejected by uq_boundary_versions_version_code."""
    insert_boundary_version(migrated_conn, version_code="dup-bv-code-v1")
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_boundary_version(migrated_conn, version_code="dup-bv-code-v1")
    assert_pg_error(
        exc_info,
        sqlstate="23505",
        constraint_name="uq_boundary_versions_version_code",
    )


def test_same_osm_id_with_different_osm_types_permitted(migrated_conn: Connection) -> None:
    """Same numeric osm_id across node, way, and relation is permitted as distinct identities."""
    bv_id = insert_boundary_version(migrated_conn)
    run_id = insert_import_run(migrated_conn, bv_id)

    id_node = insert_place(migrated_conn, run_id, osm_type="node", osm_id=55555)
    id_way = insert_place(migrated_conn, run_id, osm_type="way", osm_id=55555)
    id_rel = insert_place(migrated_conn, run_id, osm_type="relation", osm_id=55555)
    assert len({id_node, id_way, id_rel}) == 3


def test_import_runs_allows_reusing_source_fingerprint(migrated_conn: Connection) -> None:
    """Source fingerprint is not unique; re-importing the same source creates distinct run IDs."""
    bv_id = insert_boundary_version(migrated_conn)
    run_1 = insert_import_run(migrated_conn, bv_id, source_fingerprint="same-fp-2026")
    run_2 = insert_import_run(migrated_conn, bv_id, source_fingerprint="same-fp-2026")
    assert run_1 != run_2


def test_duplicate_publication_operation_uuid_rejected(migrated_conn: Connection) -> None:
    """Duplicate non-null publication_operation_id is rejected while multiple NULLs are allowed."""
    bv_id = insert_boundary_version(migrated_conn)
    # Multiple NULL publication_operation_ids are allowed.
    insert_import_run(migrated_conn, bv_id, status="PREPARED", publication_operation_id=None)
    insert_import_run(migrated_conn, bv_id, status="PREPARED", publication_operation_id=None)

    op_uuid = uuid.uuid4()
    insert_import_run(migrated_conn, bv_id, status="PREPARED", publication_operation_id=op_uuid)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_import_run(
                migrated_conn, bv_id, status="PREPARED", publication_operation_id=op_uuid
            )
    assert_pg_error(
        exc_info,
        sqlstate="23505",
        constraint_name="uq_import_runs_publication_operation_id",
    )


def test_active_slot_zero_one_and_two_rejected(migrated_conn: Connection) -> None:
    """Zero active publications and one active slot are allowed; two active slots are rejected."""
    bv_id = insert_boundary_version(migrated_conn)
    now = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)

    # Zero active publications initially is allowed.
    insert_import_run(migrated_conn, bv_id, status="PREPARED", active_slot=None)
    active_count = migrated_conn.execute(
        text("SELECT count(*) FROM import_runs WHERE active_slot = 1")
    ).scalar_one()
    assert active_count == 0

    # One active slot is allowed.
    insert_import_run(
        migrated_conn,
        bv_id,
        status="PUBLISHED",
        publication_operation_id=uuid.uuid4(),
        published_at=now,
        active_slot=1,
    )

    # A second active slot simultaneously is rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_import_run(
                migrated_conn,
                bv_id,
                status="PUBLISHED",
                publication_operation_id=uuid.uuid4(),
                published_at=now + timedelta(minutes=5),
                active_slot=1,
            )
    assert_pg_error(exc_info, sqlstate="23505", constraint_name="uq_import_runs_active_slot")


@pytest.mark.parametrize(
    ("status", "active_slot", "pub_uuid", "pub_at", "sup_at", "expected_constraint"),
    [
        ("PREPARED", 1, uuid.uuid4(), None, None, "ck_import_runs_active_slot_status"),
        ("PROCESSING", 1, None, None, None, "ck_import_runs_active_slot_status"),
        ("FAILED", 1, None, None, None, "ck_import_runs_active_slot_status"),
        (
            "SUPERSEDED",
            1,
            uuid.uuid4(),
            datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 2, 1, 11, 0, tzinfo=timezone.utc),
            "ck_import_runs_active_slot_status",
        ),
        (
            "PUBLISHED",
            None,
            uuid.uuid4(),
            datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            None,
            "ck_import_runs_active_slot_status",
        ),
        (
            "PUBLISHED",
            2,
            uuid.uuid4(),
            datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            None,
            "ck_import_runs_active_slot_value",
        ),
        (
            "PUBLISHED",
            1,
            None,
            datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            None,
            "ck_import_runs_publication_lifecycle_evidence",
        ),
        (
            "PUBLISHED",
            1,
            uuid.uuid4(),
            None,
            None,
            "ck_import_runs_publication_lifecycle_evidence",
        ),
        (
            "SUPERSEDED",
            None,
            uuid.uuid4(),
            datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            None,
            "ck_import_runs_publication_lifecycle_evidence",
        ),
        (
            "SUPERSEDED",
            None,
            uuid.uuid4(),
            datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc),
            datetime(2026, 2, 1, 9, 0, tzinfo=timezone.utc),
            "ck_import_runs_publication_lifecycle_evidence",
        ),
    ],
)
def test_invalid_active_slot_and_publication_lifecycle_rejected(
    migrated_conn: Connection,
    status: str,
    active_slot: int | None,
    pub_uuid: uuid.UUID | None,
    pub_at: datetime | None,
    sup_at: datetime | None,
    expected_constraint: str,
) -> None:
    """Invalid active_slot/status and lifecycle timestamp combinations are rejected."""
    bv_id = insert_boundary_version(migrated_conn)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            insert_import_run(
                migrated_conn,
                bv_id,
                status=status,
                active_slot=active_slot,
                publication_operation_id=pub_uuid,
                published_at=pub_at,
                superseded_at=sup_at,
            )
    assert_pg_error(exc_info, sqlstate="23514", constraint_name=expected_constraint)


def test_superseded_publication_evidence_preserved_across_commit(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """Verify schema supports transitioning PUBLISHED -> SUPERSEDED with durable evidence on real COMMIT."""
    with disposable_database("runs_commit") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            op_v1 = uuid.uuid4()
            op_v2 = uuid.uuid4()
            t1 = datetime(2026, 3, 1, 10, 0, tzinfo=timezone.utc)
            t2 = datetime(2026, 3, 2, 10, 0, tzinfo=timezone.utc)

            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn)
                run_v1 = insert_import_run(
                    conn,
                    bv_id,
                    status="PUBLISHED",
                    publication_operation_id=op_v1,
                    published_at=t1,
                    publication_evidence_json='{"active_place_count": 10}',
                    active_slot=1,
                )

            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE import_runs
                        SET status = 'SUPERSEDED',
                            active_slot = NULL,
                            superseded_at = :superseded_at
                        WHERE id = :run_v1
                        """
                    ),
                    {"superseded_at": t2, "run_v1": run_v1},
                )
                run_v2 = insert_import_run(
                    conn,
                    bv_id,
                    status="PUBLISHED",
                    publication_operation_id=op_v2,
                    published_at=t2,
                    publication_evidence_json='{"active_place_count": 12}',
                    active_slot=1,
                )

            # Inspect in a separate connection after COMMIT.
            with engine.connect() as conn:
                row_v1 = conn.execute(
                    text(
                        """
                        SELECT status, active_slot, publication_operation_id,
                               published_at, superseded_at, publication_evidence
                        FROM import_runs
                        WHERE id = :id
                        """
                    ),
                    {"id": run_v1},
                ).one()
                assert row_v1.status == "SUPERSEDED"
                assert row_v1.active_slot is None
                assert row_v1.publication_operation_id == op_v1
                assert row_v1.published_at == t1
                assert row_v1.superseded_at == t2
                assert row_v1.publication_evidence == {"active_place_count": 10}

                active_ids = conn.execute(
                    text("SELECT id FROM import_runs WHERE active_slot = 1")
                ).scalars().all()
                assert active_ids == [run_v2]
        finally:
            engine.dispose()


def test_duplicate_candidates_canonical_order_and_per_run_uniqueness(
    migrated_conn: Connection,
) -> None:
    """Enforce place_a_id < place_b_id, uniqueness within one run, and coexistence across runs."""
    bv_id = insert_boundary_version(migrated_conn)
    run_1 = insert_import_run(migrated_conn, bv_id, source_fingerprint="fp-1")
    run_2 = insert_import_run(migrated_conn, bv_id, source_fingerprint="fp-2")

    p1 = insert_place(migrated_conn, run_1, osm_id=9001)
    p2 = insert_place(migrated_conn, run_1, osm_id=9002)
    low_id, high_id = min(p1, p2), max(p1, p2)

    # Canonical pair accepted in run_1.
    migrated_conn.execute(
        text(
            """
            INSERT INTO duplicate_candidates (
                import_run_id, place_a_id, place_b_id, detector_version,
                confidence_score, distance_meters, evidence
            )
            VALUES (:run_id, :a, :b, 'det-v1', 0.9125, 14.2, '{"reason": "proximity"}'::jsonb)
            """
        ),
        {"run_id": run_1, "a": low_id, "b": high_id},
    )

    # Duplicate pair within the same run rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO duplicate_candidates (
                        import_run_id, place_a_id, place_b_id, detector_version, evidence
                    )
                    VALUES (:run_id, :a, :b, 'det-v1', '{"reason": "retry"}'::jsonb)
                    """
                ),
                {"run_id": run_1, "a": low_id, "b": high_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23505", constraint_name="uq_duplicate_candidates_run_pair"
    )

    # Same pair in a different run is allowed.
    cand_run2 = migrated_conn.execute(
        text(
            """
            INSERT INTO duplicate_candidates (
                import_run_id, place_a_id, place_b_id, detector_version, evidence
            )
            VALUES (:run_id, :a, :b, 'det-v1', '{"reason": "second run"}'::jsonb)
            RETURNING id
            """
        ),
        {"run_id": run_2, "a": low_id, "b": high_id},
    ).scalar_one()
    assert cand_run2 > 0

    # Reversed pair rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO duplicate_candidates (
                        import_run_id, place_a_id, place_b_id, detector_version, evidence
                    )
                    VALUES (:run_id, :a, :b, 'det-v1', '{}'::jsonb)
                    """
                ),
                {"run_id": run_2, "a": high_id, "b": low_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_duplicate_candidates_canonical_pair_order"
    )

    # Self-pair rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO duplicate_candidates (
                        import_run_id, place_a_id, place_b_id, detector_version, evidence
                    )
                    VALUES (:run_id, :a, :a, 'det-v1', '{}'::jsonb)
                    """
                ),
                {"run_id": run_2, "a": low_id},
            )
    assert_pg_error(
        exc_info, sqlstate="23514", constraint_name="ck_duplicate_candidates_canonical_pair_order"
    )


def test_foreign_keys_restrict_historical_deletes(migrated_conn: Connection) -> None:
    """ON DELETE RESTRICT protects boundary_versions, import_runs, and places from cascading deletes."""
    bv_id = insert_boundary_version(migrated_conn)
    run_id = insert_import_run(migrated_conn, bv_id)
    p1 = insert_place(migrated_conn, run_id, osm_id=9101)
    p2 = insert_place(migrated_conn, run_id, osm_id=9102)
    migrated_conn.execute(
        text(
            """
            INSERT INTO duplicate_candidates (
                import_run_id, place_a_id, place_b_id, detector_version, evidence
            )
            VALUES (:run_id, :a, :b, 'det-v1', '{}'::jsonb)
            """
        ),
        {"run_id": run_id, "a": min(p1, p2), "b": max(p1, p2)},
    )

    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text("DELETE FROM boundary_versions WHERE id = :id"), {"id": bv_id}
            )
    assert_pg_error(
        exc_info,
        sqlstate="23503",
        constraint_name="fk_import_runs_boundary_version_id_boundary_versions",
    )

    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(text("DELETE FROM places WHERE id = :id"), {"id": min(p1, p2)})
    assert_pg_error(
        exc_info,
        sqlstate="23503",
        constraint_name="fk_duplicate_candidates_place_a_id_places",
    )
