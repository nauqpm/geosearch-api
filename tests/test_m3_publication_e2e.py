"""M3.3 & M3.4 end-to-end atomic publication, stable identity, reactivation, scope & duplicate tests.

NOTE: All boundaries and OSM fixtures here are synthetic engineering verification fixtures
run in isolated disposable PostgreSQL/PostGIS databases under explicit
`PublicationMode.CONTROLLED_TEST_NODE_ONLY`. They do NOT constitute authoritative
production deployment approval.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from decimal import Decimal
from pathlib import Path

import osmium
import pytest
from sqlalchemy import text

from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    DuplicateCandidateInput,
    DuplicateDetectionState,
    EligibilityStatus,
    PublicationConflictError,
    PublicationFaultHooks,
    PublicationInvariantError,
    PublicationMode,
    PublicationOutcomeStatus,
    StagedDuplicateCandidate,
    publish_prepared_run,
    resolve_unknown_commit_outcome,
)
from tests.helpers import insert_boundary_version, insert_place, upgrade_test_db_to_head

COMPLEX_MULTIPOLYGON_WKT = (
    "MULTIPOLYGON("
    "((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 3 7, 7 7, 7 3, 3 3)),"
    "((20 0, 24 0, 24 4, 20 4, 20 0))"
    ")"
)


def _write_pbf(path: Path, nodes: list[osmium.osm.mutable.Node]) -> Path:
    writer = osmium.SimpleWriter(str(path))
    try:
        for node in nodes:
            writer.add_node(node)
    finally:
        writer.close()
    return path


def _node(
    osm_id: int,
    *,
    name: str,
    lon: float = 106.7009,
    lat: float = 10.7769,
    category_tag: tuple[str, str] = ("amenity", "cafe"),
    version: int = 1,
    changeset: int = 100,
) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=version,
        changeset=changeset,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags={category_tag[0]: category_tag[1], "name": name},
    )


def test_m33_controlled_v1_v2_v3_reactivation_stable_ids_and_provenance(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """V1(A,B,C) -> V2(A,B,D) -> V3(A,C,D) preserves stable IDs, updates last_seen_import_id, retains history."""
    with disposable_database("m33_v1_v2_v3") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m33-seq")

            # V1: A(101), B(102), C(103)
            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    _node(101, name="Cà Phê A", lon=106.701, lat=10.777, version=1),
                    _node(102, name="Quán Phở B", lon=106.702, lat=10.778, category_tag=("amenity", "restaurant"), version=1),
                    _node(103, name="Nhà Thuốc C", lon=106.703, lat=10.779, category_tag=("amenity", "pharmacy"), version=1),
                ],
            )
            # V2: A(101 unchanged!), B(102 updated!), D(104 new!) — C(103) absent -> deactivated!
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    _node(101, name="Cà Phê A", lon=106.701, lat=10.777, version=1),
                    _node(102, name="Quán Phở B Mới", lon=106.7025, lat=10.7785, category_tag=("amenity", "restaurant"), version=2),
                    _node(104, name="Siêu Thị D", lon=106.704, lat=10.780, category_tag=("shop", "supermarket"), version=1),
                ],
            )
            # V3: A(101), C(103 reactivated!), D(104) — B(102) deactivated!
            pbf_v3 = _write_pbf(
                work_dir / "v3.osm.pbf",
                [
                    _node(101, name="Cà Phê A", lon=106.701, lat=10.777, version=1),
                    _node(103, name="Nhà Thuốc C Trở Lại", lon=106.703, lat=10.779, category_tag=("amenity", "pharmacy"), version=3),
                    _node(104, name="Siêu Thị D", lon=106.704, lat=10.780, category_tag=("shop", "supermarket"), version=1),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)
            ing_v3 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v3, boundary_version_id=bv_id)

            op_v1 = uuid.uuid4()
            res_v1 = publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=op_v1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )
            assert res_v1.outcome == PublicationOutcomeStatus.PUBLISHED
            assert res_v1.previous_active_run_id is None
            assert res_v1.recovery_artifact is None
            assert res_v1.active_place_count == 3
            assert res_v1.inserted_place_count == 3
            assert res_v1.duplicate_detection_state == DuplicateDetectionState.NOT_EXECUTED

            with engine.connect() as conn:
                v1_ids = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                }
                assert set(v1_ids.keys()) == {101, 102, 103}

            # Publish V2 over V1 with automatic pre-publication recovery artifact.
            op_v2 = uuid.uuid4()
            artifact_root = work_dir / "recovery_artifacts"
            res_v2 = publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=op_v2,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=ing_v1.import_run.id,
                recovery_artifact_root=artifact_root,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.VERIFIED_EMPTY,
                    detector_version="det-empty-v1",
                ),
            )
            assert res_v2.outcome == PublicationOutcomeStatus.PUBLISHED
            assert res_v2.previous_active_run_id == ing_v1.import_run.id
            assert res_v2.recovery_artifact is not None
            assert res_v2.recovery_artifact.source_import_run_id == ing_v1.import_run.id
            assert res_v2.active_place_count == 3
            assert res_v2.inserted_place_count == 1
            assert res_v2.updated_place_count == 2
            assert res_v2.deactivated_place_count == 1
            assert res_v2.duplicate_detection_state == DuplicateDetectionState.VERIFIED_EMPTY

            with engine.connect() as conn:
                rows = {
                    int(r.osm_id): r
                    for r in conn.execute(
                        text(
                            "SELECT id, osm_id, name, name_search, is_active, last_seen_import_id "
                            "FROM places"
                        )
                    ).fetchall()
                }
                assert set(rows.keys()) == {101, 102, 103, 104}
                # A & B preserve exact stable places.id; A updates last_seen_import_id even though unchanged!
                assert rows[101].id == v1_ids[101]
                assert rows[101].is_active is True
                assert rows[101].last_seen_import_id == ing_v2.import_run.id

                assert rows[102].id == v1_ids[102]
                assert rows[102].name == "Quán Phở B Mới"
                assert rows[102].name_search == "quan pho b moi"
                assert rows[102].is_active is True
                assert rows[102].last_seen_import_id == ing_v2.import_run.id

                # C is soft-deactivated (retained historically with V1 provenance).
                assert rows[103].id == v1_ids[103]
                assert rows[103].is_active is False
                assert rows[103].last_seen_import_id == ing_v1.import_run.id

                # D is newly inserted and active.
                assert rows[104].is_active is True
                assert rows[104].last_seen_import_id == ing_v2.import_run.id

                # V1 is SUPERSEDED with original evidence intact; V2 owns active_slot = 1.
                run_v1_row = conn.execute(
                    text(
                        "SELECT status, active_slot, publication_operation_id, published_at, "
                        "superseded_at, publication_evidence FROM import_runs WHERE id = :id"
                    ),
                    {"id": ing_v1.import_run.id},
                ).one()
                assert run_v1_row.status == "SUPERSEDED"
                assert run_v1_row.active_slot is None
                assert run_v1_row.publication_operation_id == op_v1
                assert run_v1_row.published_at is not None
                assert run_v1_row.superseded_at >= run_v1_row.published_at
                assert run_v1_row.publication_evidence["superseded_by_run_id"] == ing_v2.import_run.id
                assert run_v1_row.publication_evidence["active_place_count"] == 3

                active_slots = conn.execute(
                    text("SELECT id FROM import_runs WHERE active_slot = 1")
                ).scalars().all()
                assert active_slots == [ing_v2.import_run.id]

            # Publish V3: reactivate C(103) and verify ORIGINAL stable places.id is preserved!
            op_v3 = uuid.uuid4()
            res_v3 = publish_prepared_run(
                engine,
                import_run_id=ing_v3.import_run.id,
                publication_operation_id=op_v3,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=ing_v2.import_run.id,
                recovery_artifact_root=artifact_root,
            )
            assert res_v3.reactivated_place_count == 1
            assert res_v3.deactivated_place_count == 1

            with engine.connect() as conn:
                c_row = conn.execute(
                    text(
                        "SELECT id, name, is_active, last_seen_import_id "
                        "FROM places WHERE osm_type = 'node' AND osm_id = 103"
                    )
                ).one()
                assert c_row.id == v1_ids[103]
                assert c_row.is_active is True
                assert c_row.name == "Nhà Thuốc C Trở Lại"
                assert c_row.last_seen_import_id == ing_v3.import_run.id
        finally:
            engine.dispose()


def test_m33_node_only_publication_rejects_mixed_coverage_and_reconciliation_preserves_non_nodes(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Mixed-coverage active DB blocks node_only publication; reconciliation helper preserves Way/Relation."""
    from geosearch.publication import PublicationEligibilityError, reconcile_place_membership

    with disposable_database("m33_scope_iso") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m33-scope")

            pbf_v1 = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, name="Node V1")])
            pbf_v2 = _write_pbf(work_dir / "v2.osm.pbf", [_node(102, name="Node V2")])

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            # Seed pre-existing active way and relation places creating an incompatible mixed-coverage state.
            with engine.begin() as conn:
                way_id = insert_place(
                    conn, ing_v1.import_run.id, osm_type="way", osm_id=50001, name="Way Mall"
                )
                rel_id = insert_place(
                    conn, ing_v1.import_run.id, osm_type="relation", osm_id=60001, name="Rel Plaza"
                )

            # 1. Engine-level safety: node_only replacement MUST fail closed over mixed-coverage active DB!
            with pytest.raises(PublicationEligibilityError) as exc_cov:
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=work_dir / "artifacts",
                )
            assert exc_cov.value.status == EligibilityStatus.COVERAGE_INCOMPATIBLE

            # 2. Defense-in-depth at reconciliation helper level: even if invoked directly,
            #    CONTROLLED_TEST_NODE_ONLY reconciliation NEVER deactivates way or relation rows.
            with engine.begin() as conn:
                deactivated = reconcile_place_membership(
                    conn,
                    import_run_id=ing_v2.import_run.id,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
                assert deactivated == 1  # Only node 101 deactivated
                states = {
                    (str(r.osm_type), int(r.osm_id)): (int(r.id), bool(r.is_active))
                    for r in conn.execute(
                        text("SELECT osm_type, osm_id, id, is_active FROM places")
                    ).fetchall()
                }
                assert states[("node", 101)][1] is False
                assert states[("way", 50001)] == (way_id, True)
                assert states[("relation", 60001)] == (rel_id, True)
        finally:
            engine.dispose()


def test_m33_complex_multipolygon_spatial_publication_st_covers(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Complex MultiPolygon (interior, outer edge, vertex, hole, hole edge, island, outside) publishes cleanly."""
    with disposable_database("m33_spatial") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(
                    conn, version_code="bv-m33-complex", geom_wkt=COMPLEX_MULTIPOLYGON_WKT
                )

            pbf = _write_pbf(
                work_dir / "complex.osm.pbf",
                [
                    _node(1, name="Interior", lon=1.5, lat=5.0),
                    _node(2, name="Outer Edge", lon=0.0, lat=5.0),
                    _node(3, name="Outer Vertex", lon=10.0, lat=10.0),
                    _node(4, name="Hole Boundary", lon=3.0, lat=5.0),
                    _node(5, name="Island Interior", lon=22.0, lat=2.0),
                    _node(6, name="Hole Interior Outside", lon=5.0, lat=5.0),
                    _node(7, name="Outside West", lon=-1.0, lat=5.0),
                ],
            )
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            res = publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )
            assert res.active_place_count == 5

            with engine.connect() as conn:
                active_ids = conn.execute(
                    text("SELECT osm_id FROM places WHERE is_active = true ORDER BY osm_id")
                ).scalars().all()
                assert active_ids == [1, 2, 3, 4, 5]
        finally:
            engine.dispose()


def test_m34_duplicate_candidates_resolution_ordering_coexistence_and_historical_retention(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Duplicate candidates canonicalize place_a_id < place_b_id, coexist across runs, survive deactivation."""
    with disposable_database("m34_dups") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m34-dups")

            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    _node(301, name="Cà Phê Twin 1", lon=106.701, lat=10.777),
                    _node(302, name="Cà Phê Twin 2", lon=106.70105, lat=10.77705),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    _node(301, name="Cà Phê Twin 1", lon=106.701, lat=10.777),
                    _node(303, name="Cà Phê Other", lon=106.705, lat=10.780),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

            # Invalid self-pair rolls back entire publication!
            with pytest.raises(PublicationInvariantError, match="self-pair"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v1.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    duplicate_candidates=DuplicateCandidateInput(
                        state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                        detector_version="det-v1",
                        candidates=(
                            StagedDuplicateCandidate(
                                osm_type_a="node",
                                osm_id_a=301,
                                osm_type_b="node",
                                osm_id_b=301,
                                detector_version="det-v1",
                                evidence={"bad": True},
                            ),
                        ),
                    ),
                )
            with engine.connect() as conn:
                assert conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0

            # Now publish cleanly with reversed input order (302, 301) + duplicate identical pair in input.
            op_v1 = uuid.uuid4()
            # Retrieve the operation UUID registered on ing_v1 by the rolled-back attempt above.
            with engine.connect() as conn:
                registered_op_v1 = conn.execute(
                    text("SELECT publication_operation_id FROM import_runs WHERE id = :id"),
                    {"id": ing_v1.import_run.id},
                ).scalar_one()
            assert registered_op_v1 is not None

            res_v1 = publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=registered_op_v1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-v1",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=302,
                            osm_type_b="node",
                            osm_id_b=301,
                            detector_version="det-v1",
                            confidence_score=Decimal("0.9250"),
                            distance_meters=7.8,
                            evidence={"rule": "name_and_distance"},
                        ),
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=301,
                            osm_type_b="node",
                            osm_id_b=302,
                            detector_version="det-v1",
                            confidence_score=Decimal("0.9250"),
                            distance_meters=7.8,
                            evidence={"rule": "name_and_distance"},
                        ),
                    ),
                ),
            )
            assert res_v1.active_place_count == 2  # Both POIs kept, NO auto-merge!
            assert res_v1.duplicate_candidate_count == 1

            # Publish V2 which deactivates node 302; historical duplicate candidate from V1 MUST remain!
            # Also record a duplicate candidate for V2 referencing active pair (301, 303) AND verify
            # idempotent retry of resolve_and_record_duplicate_candidates does not duplicate/overwrite.
            res_v2 = publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                recovery_artifact_root=work_dir / "artifacts",
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-v2",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=301,
                            osm_type_b="node",
                            osm_id_b=303,
                            detector_version="det-v2",
                            evidence={"rule": "v2_pair"},
                        ),
                    ),
                ),
            )
            assert res_v2.duplicate_candidate_count == 1

            # Calling resolver again on V1 with IDENTICAL evidence is idempotent (0 inserted, 1 preserved).
            # Calling resolver again with CONFLICTING evidence MUST raise PublicationInvariantError
            # while preserving existing immutable evidence untouched!
            from geosearch.publication import resolve_and_record_duplicate_candidates

            with engine.begin() as conn:
                replay_ok = resolve_and_record_duplicate_candidates(
                    conn,
                    import_run_id=ing_v1.import_run.id,
                    duplicate_input=DuplicateCandidateInput(
                        state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                        detector_version="det-v1",
                        candidates=(
                            StagedDuplicateCandidate(
                                osm_type_a="node",
                                osm_id_a=301,
                                osm_type_b="node",
                                osm_id_b=302,
                                detector_version="det-v1",
                                confidence_score=Decimal("0.9250"),
                                distance_meters=7.8,
                                evidence={"rule": "name_and_distance"},
                            ),
                        ),
                    ),
                )
                assert replay_ok.inserted_count == 0
                assert replay_ok.existing_preserved_count == 1

            with pytest.raises(PublicationInvariantError, match="Conflicting duplicate candidate evidence"):
                with engine.begin() as conn:
                    resolve_and_record_duplicate_candidates(
                        conn,
                        import_run_id=ing_v1.import_run.id,
                        duplicate_input=DuplicateCandidateInput(
                            state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                            detector_version="det-v1-replay",
                            candidates=(
                                StagedDuplicateCandidate(
                                    osm_type_a="node",
                                    osm_id_a=301,
                                    osm_type_b="node",
                                    osm_id_b=302,
                                    detector_version="det-v1-replay",
                                    evidence={"rule": "attempted_overwrite"},
                                ),
                            ),
                        ),
                    )

            with engine.connect() as conn:
                dup_rows = conn.execute(
                    text(
                        "SELECT import_run_id, place_a_id, place_b_id, detector_version, evidence "
                        "FROM duplicate_candidates ORDER BY import_run_id"
                    )
                ).fetchall()
                assert len(dup_rows) == 2
                assert dup_rows[0].import_run_id == ing_v1.import_run.id
                assert dup_rows[0].place_a_id < dup_rows[0].place_b_id
                assert dup_rows[0].detector_version == "det-v1"
                assert dup_rows[0].evidence == {"rule": "name_and_distance"}
                assert dup_rows[1].import_run_id == ing_v2.import_run.id
                assert dup_rows[1].place_a_id < dup_rows[1].place_b_id
                assert dup_rows[1].detector_version == "det-v2"
                assert dup_rows[1].evidence == {"rule": "v2_pair"}
        finally:
            engine.dispose()


def test_m34_idempotent_retry_cases_a_b_c_d(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Verifies Cases A (pre-commit retry), B (post-commit retry), C (lost ack), D (conflicting UUID)."""
    with disposable_database("m34_retry") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m34-retry")

            pbf_1 = _write_pbf(work_dir / "r1.osm.pbf", [_node(401, name="Cafe Retry 1")])
            pbf_2 = _write_pbf(work_dir / "r2.osm.pbf", [_node(402, name="Cafe Retry 2")])
            ing_1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_1, boundary_version_id=bv_id)
            ing_2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_2, boundary_version_id=bv_id)

            op_1 = uuid.uuid4()

            # Case A: Pre-COMMIT failure rolls back live changes; durable evidence shows NOT committed.
            def _fail_before_commit(_conn) -> None:
                raise RuntimeError("Simulated crash before COMMIT")

            with pytest.raises(RuntimeError, match="Simulated crash before COMMIT"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_1.import_run.id,
                    publication_operation_id=op_1,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    fault_hooks=PublicationFaultHooks(on_before_commit=_fail_before_commit),
                )

            ev_before = resolve_unknown_commit_outcome(
                engine, publication_operation_id=op_1, import_run_id=ing_1.import_run.id
            )
            assert ev_before is not None
            assert ev_before.status == "PREPARED"
            assert ev_before.is_committed_publication is False

            # Case D: Using op_1 on a DIFFERENT run is rejected with PUBLICATION_CONFLICT.
            with pytest.raises(PublicationConflictError) as exc_conflict:
                publish_prepared_run(
                    engine,
                    import_run_id=ing_2.import_run.id,
                    publication_operation_id=op_1,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
            assert exc_conflict.value.status == EligibilityStatus.PUBLICATION_CONFLICT

            # Case C: Retry same (ing_1, op_1), COMMIT succeeds, then ack is lost!
            def _lose_ack(_run_id: int, _op: uuid.UUID) -> None:
                raise ConnectionResetError("Simulated lost COMMIT acknowledgement")

            with pytest.raises(ConnectionResetError, match="lost COMMIT acknowledgement"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_1.import_run.id,
                    publication_operation_id=op_1,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    fault_hooks=PublicationFaultHooks(on_after_commit_before_ack=_lose_ack),
                )

            # Inspect durable evidence -> confirms COMMIT succeeded!
            ev_after = resolve_unknown_commit_outcome(
                engine, publication_operation_id=op_1, import_run_id=ing_1.import_run.id
            )
            assert ev_after is not None
            assert ev_after.status == "PUBLISHED"
            assert ev_after.is_committed_publication is True
            assert ev_after.published_at is not None

            # Case B: Retrying with same op_1 returns ALREADY_PUBLISHED without mutating published_at.
            retry_res = publish_prepared_run(
                engine,
                import_run_id=ing_1.import_run.id,
                publication_operation_id=op_1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )
            assert retry_res.outcome == PublicationOutcomeStatus.ALREADY_PUBLISHED
            assert retry_res.already_committed is True
            assert retry_res.published_at == ev_after.published_at
        finally:
            engine.dispose()
