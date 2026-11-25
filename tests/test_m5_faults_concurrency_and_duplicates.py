"""M5 AC4, AC5 & AC6 tests: Fault injection, stale approval race, API snapshot isolation, and duplicates."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from decimal import Decimal
from pathlib import Path
from typing import Any

import osmium
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError

from geosearch.api import create_app
from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    DuplicateCandidateInput,
    DuplicateDetectionState,
    PublicationConflictError,
    PublicationFaultHooks,
    PublicationInvariantError,
    PublicationMode,
    PublicationOutcomeStatus,
    RecoveryArtifactRecord,
    RecoveryFaultHooks,
    StagedDuplicateCandidate,
    build_recovery_approval_from_artifact,
    execute_controlled_recovery,
    prepare_recovery_artifact,
    publish_prepared_run,
)
from tests.helpers import assert_pg_error, insert_boundary_version, upgrade_test_db_to_head


class _InjectedFault(RuntimeError):
    """Deterministic test fault injected at a specific recovery/publication checkpoint."""


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
    lon: float = 106.701,
    lat: float = 10.777,
    category_tag: tuple[str, str] = ("amenity", "cafe"),
    version: int = 1,
) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=version,
        changeset=100 + version,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags={category_tag[0]: category_tag[1], "name": name},
    )


def _setup_v1_v2(
    engine: Engine,
    work_dir: Path,
) -> tuple[int, int, int, RecoveryArtifactRecord]:
    """Publish V1 (3 places), capture V1 artifact, then publish V2 (5 places)."""
    with engine.begin() as conn:
        bv_id = insert_boundary_version(conn, version_code="bv-m5-rel")

    pbf_v1 = _write_pbf(
        work_dir / "v1.osm.pbf",
        [
            _node(301, name="Rel Cafe 1", lon=106.700, lat=10.780),
            _node(302, name="Rel Cafe 2 V1", lon=106.701, lat=10.780),
            _node(303, name="Rel Cafe 3 V1", lon=106.702, lat=10.780),
        ],
    )
    pbf_v2 = _write_pbf(
        work_dir / "v2.osm.pbf",
        [
            _node(301, name="Rel Cafe 1", lon=106.700, lat=10.780),
            _node(302, name="Rel Cafe 2 V2", lon=106.701, lat=10.780, version=2),
            _node(304, name="Rel Cafe 4 V2", lon=106.703, lat=10.780),
            _node(305, name="Rel Cafe 5 V2", lon=106.704, lat=10.780),
            _node(306, name="Rel Cafe 6 V2", lon=106.705, lat=10.780),
        ],
    )
    ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
    ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

    publish_prepared_run(
        engine,
        import_run_id=ing_v1.import_run.id,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
        expected_active_run_id=None,
    )
    v1_artifact = prepare_recovery_artifact(
        engine, artifact_root=work_dir / "v1_artifacts", artifact_name="v1_source"
    )
    assert v1_artifact is not None

    publish_prepared_run(
        engine,
        import_run_id=ing_v2.import_run.id,
        publication_operation_id=uuid.uuid4(),
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
        expected_active_run_id=ing_v1.import_run.id,
        recovery_artifact_dir=v1_artifact.artifact_dir,
    )
    return bv_id, ing_v1.import_run.id, ing_v2.import_run.id, v1_artifact


def _assert_v2_intact(engine: Engine, *, v2_run_id: int) -> None:
    with engine.connect() as conn:
        active = conn.execute(
            text("SELECT id, status FROM import_runs WHERE active_slot = 1")
        ).fetchall()
        assert len(active) == 1
        assert int(active[0].id) == v2_run_id
        assert active[0].status == "PUBLISHED"

        places = {
            int(r.osm_id): (r.name, bool(r.is_active), int(r.last_seen_import_id))
            for r in conn.execute(
                text("SELECT osm_id, name, is_active, last_seen_import_id FROM places")
            ).fetchall()
        }
    assert {oid for oid, (_, active_flag, _) in places.items() if active_flag} == {
        301,
        302,
        304,
        305,
        306,
    }
    assert places[302][0] == "Rel Cafe 2 V2"
    assert places[303][1] is False


# ===========================================================================
# AC4: Pre-COMMIT Fault Injection & Stale Approval Rejection Under Lock
# ===========================================================================


@pytest.mark.parametrize(
    "checkpoint_name",
    [
        "on_before_prepare_commit",
        "on_after_prepare_before_publish",
        "on_before_lock",
        "on_after_lock",
        "on_after_revalidation",
        "on_before_upsert",
        "on_during_upsert",
        "on_after_upsert",
        "on_during_reconciliation",
        "on_after_deactivation",
        "on_during_duplicate_resolution",
        "on_during_pre_commit_verification",
        "on_during_active_slot_switch",
        "on_before_commit",
    ],
)
def test_m5_ac4_pre_commit_fault_injection_rolls_back_cleanly(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    checkpoint_name: str,
) -> None:
    """AC4: Any pre-COMMIT fault during recovery preparation or publication leaves V2 intact."""
    with disposable_database(f"m5_ac4_{checkpoint_name[:12]}") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            _bv_id, _v1_id, v2_id, v1_artifact = _setup_v1_v2(engine, work_dir)

            def _raise_fault(*_args: Any, **_kwargs: Any) -> None:
                raise _InjectedFault(f"Injected fault at {checkpoint_name}")

            if checkpoint_name in ("on_before_prepare_commit", "on_after_prepare_before_publish"):
                fault_hooks = RecoveryFaultHooks(**{checkpoint_name: _raise_fault})
            else:
                fault_hooks = RecoveryFaultHooks(
                    publication_hooks=PublicationFaultHooks(**{checkpoint_name: _raise_fault})
                )

            op_rec = uuid.uuid4()
            approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_rec,
                expected_active_run_id=v2_id,
                approved_by="ops-lead@geosearch",
                approval_reason=f"Fault test {checkpoint_name}",
            )

            with pytest.raises(_InjectedFault, match=checkpoint_name):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_rec,
                    approval=approval,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                    upsert_batch_size=1,
                    fault_hooks=fault_hooks,
                )

            # Live dataset and active slot remain strictly unchanged on V2.
            _assert_v2_intact(engine, v2_run_id=v2_id)
        finally:
            engine.dispose()


def test_m5_ac4_stale_approval_rejected_under_publication_lock_after_concurrent_v3(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC4: If V3 commits after recovery preflight, stale recovery expecting V2 is rejected under lock."""
    with disposable_database("m5_ac4_stale") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            bv_id, _v1_id, v2_id, v1_artifact = _setup_v1_v2(engine, work_dir)

            pbf_v3 = _write_pbf(
                work_dir / "v3.osm.pbf",
                [
                    _node(301, name="Rel Cafe 1 V3", lon=106.700, lat=10.780, version=3),
                    _node(309, name="Rel Cafe 9 V3", lon=106.709, lat=10.780),
                ],
            )
            ing_v3 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v3, boundary_version_id=bv_id)

            recovery_before_lock = threading.Event()
            v3_committed = threading.Event()
            rec_errors: list[BaseException] = []

            op_rec = uuid.uuid4()
            approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_rec,
                expected_active_run_id=v2_id,
                approved_by="ops-lead@geosearch",
                approval_reason="INC-STALE-LOCK",
            )

            def _run_recovery() -> None:
                try:
                    def _pause_before_lock() -> None:
                        recovery_before_lock.set()
                        assert v3_committed.wait(timeout=10.0)

                    execute_controlled_recovery(
                        engine,
                        source_artifact_dir=v1_artifact.artifact_dir,
                        publication_operation_id=op_rec,
                        approval=approval,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        protection_artifact_root=work_dir / "v2_prot",
                        fault_hooks=RecoveryFaultHooks(
                            publication_hooks=PublicationFaultHooks(
                                on_before_lock=_pause_before_lock
                            )
                        ),
                    )
                except BaseException as exc:
                    rec_errors.append(exc)

            t_rec = threading.Thread(target=_run_recovery)
            t_rec.start()
            # Wait deterministically until recovery passed preflight and paused right before lock.
            assert recovery_before_lock.wait(timeout=10.0)

            # Concurrently commit V3 over V2 while recovery is waiting before lock!
            op_v3 = uuid.uuid4()
            pub_v3 = publish_prepared_run(
                engine,
                import_run_id=ing_v3.import_run.id,
                publication_operation_id=op_v3,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=v2_id,
                recovery_artifact_root=work_dir / "v3_prot",
            )
            assert pub_v3.outcome == PublicationOutcomeStatus.PUBLISHED

            # Release recovery -> acquires advisory lock -> sees active run is V3 != V2 -> rejects!
            v3_committed.set()
            t_rec.join(timeout=10.0)

            assert len(rec_errors) == 1
            assert isinstance(rec_errors[0], PublicationConflictError)

            # Newer active run V3 and its content remain completely intact.
            with engine.connect() as conn:
                active = conn.execute(
                    text("SELECT id, publication_operation_id FROM import_runs WHERE active_slot = 1")
                ).one()
                active_places = {
                    int(r.osm_id): r.name
                    for r in conn.execute(
                        text("SELECT osm_id, name FROM places WHERE is_active = true")
                    ).fetchall()
                }
            assert int(active.id) == ing_v3.import_run.id
            assert active.publication_operation_id == op_v3
            assert active_places == {301: "Rel Cafe 1 V3", 309: "Rel Cafe 9 V3"}
        finally:
            engine.dispose()


# ===========================================================================
# AC5: Concurrent API Readers See Consistent Snapshots During Recovery
# ===========================================================================


def test_m5_ac5_uncommitted_recovery_is_invisible_to_concurrent_api_readers(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC5: API readers do not block or observe uncommitted recovery writes prior to COMMIT."""
    with disposable_database("m5_ac5_uncomm") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            _bv_id, _v1_id, v2_id, v1_artifact = _setup_v1_v2(engine, work_dir)
            with engine.connect() as conn:
                ids_by_osm = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                }

            rec_before_commit = threading.Event()
            release_commit = threading.Event()
            rec_errors: list[BaseException] = []

            op_rec = uuid.uuid4()
            approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_rec,
                expected_active_run_id=v2_id,
                approved_by="ops-lead@geosearch",
                approval_reason="INC-CONC-UNCOMM",
            )

            def _run_recovery() -> None:
                try:
                    def _pause_before_commit(_conn: Connection) -> None:
                        rec_before_commit.set()
                        assert release_commit.wait(timeout=10.0)

                    execute_controlled_recovery(
                        engine,
                        source_artifact_dir=v1_artifact.artifact_dir,
                        publication_operation_id=op_rec,
                        approval=approval,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        protection_artifact_root=work_dir / "v2_prot",
                        fault_hooks=RecoveryFaultHooks(
                            publication_hooks=PublicationFaultHooks(
                                on_before_commit=_pause_before_commit
                            )
                        ),
                    )
                except BaseException as exc:  # pragma: no cover
                    rec_errors.append(exc)

            app = create_app(engine=engine)
            with TestClient(app) as client:
                t_rec = threading.Thread(target=_run_recovery)
                t_rec.start()
                assert rec_before_commit.wait(timeout=10.0)

                # Recovery has executed UPSERT, deactivation, and active-slot switch uncommitted.
                # Concurrent HTTP readers MUST see ONLY committed V2!
                r_list = client.get("/api/v1/places", params={"limit": 20})
                assert r_list.status_code == 200
                body_during = r_list.json()
                assert body_during["total"] == 5
                assert {it["osm_id"]: it["name"] for it in body_during["items"]} == {
                    301: "Rel Cafe 1",
                    302: "Rel Cafe 2 V2",
                    304: "Rel Cafe 4 V2",
                    305: "Rel Cafe 5 V2",
                    306: "Rel Cafe 6 V2",
                }
                # V1-only 303 is still invisible (404); V2-only 304 is still visible (200).
                assert client.get(f"/api/v1/places/{ids_by_osm[303]}").status_code == 404
                assert client.get(f"/api/v1/places/{ids_by_osm[304]}").status_code == 200

                # Release recovery COMMIT.
                release_commit.set()
                t_rec.join(timeout=10.0)
                assert not rec_errors, rec_errors[0]

                # Immediately after COMMIT, API serves complete restored V1 atomically!
                r_after = client.get("/api/v1/places", params={"limit": 20})
                assert r_after.status_code == 200
                body_after = r_after.json()
                assert body_after["total"] == 3
                assert {it["osm_id"]: it["name"] for it in body_after["items"]} == {
                    301: "Rel Cafe 1",
                    302: "Rel Cafe 2 V1",
                    303: "Rel Cafe 3 V1",
                }
                assert client.get(f"/api/v1/places/{ids_by_osm[303]}").status_code == 200
                assert client.get(f"/api/v1/places/{ids_by_osm[304]}").status_code == 404
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    ("endpoint", "params"),
    [
        ("/api/v1/places", {"page": 1, "limit": 10}),
        ("/api/v1/places/nearby", {"lon": 106.700, "lat": 10.780, "radius_m": 5000, "limit": 10}),
    ],
)
def test_m5_ac5_multi_query_response_never_mixes_v2_total_with_recovered_v1_items(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    endpoint: str,
    params: dict[str, Any],
) -> None:
    """AC5: Recovery COMMIT between COUNT(*) and SELECT items never mixes V2 and V1 in one response."""
    with disposable_database("m5_ac5_rr") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            _bv_id, _v1_id, v2_id, v1_artifact = _setup_v1_v2(engine, work_dir)

            count_done = threading.Event()
            recovery_committed = threading.Event()
            holder: dict[str, Any] = {}

            app = create_app(engine=engine)

            def _pause_between_queries(_conn: Connection) -> None:
                count_done.set()
                assert recovery_committed.wait(timeout=10.0)

            app.state.on_between_queries = _pause_between_queries

            with TestClient(app) as client:
                def _call_api() -> None:
                    try:
                        holder["resp"] = client.get(endpoint, params=params)
                    except BaseException as exc:  # pragma: no cover
                        holder["err"] = exc

                t_reader = threading.Thread(target=_call_api)
                t_reader.start()

                # Wait deterministically until COUNT(*) has executed inside REPEATABLE READ READ ONLY.
                assert count_done.wait(timeout=10.0)

                # Commit full V2 -> V1 recovery on an independent connection mid-request!
                op_rec = uuid.uuid4()
                rec_res = execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_rec,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_rec,
                        expected_active_run_id=v2_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-RR-SNAP",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )
                assert rec_res.outcome == PublicationOutcomeStatus.PUBLISHED

                # Release SELECT items inside the in-flight HTTP request.
                recovery_committed.set()
                t_reader.join(timeout=10.0)

                assert "err" not in holder, holder.get("err")
                in_flight = holder["resp"]
                assert in_flight.status_code == 200
                body_v2 = in_flight.json()
                # Both total (5) AND all 5 items MUST come consistently from V2!
                assert body_v2["total"] == 5
                assert len(body_v2["items"]) == 5
                assert {it["osm_id"]: it["name"] for it in body_v2["items"]} == {
                    301: "Rel Cafe 1",
                    302: "Rel Cafe 2 V2",
                    304: "Rel Cafe 4 V2",
                    305: "Rel Cafe 5 V2",
                    306: "Rel Cafe 6 V2",
                }

                # Next HTTP request opens a fresh snapshot and sees strictly recovered V1 (3 items)!
                app.state.on_between_queries = None
                next_resp = client.get(endpoint, params=params)
                assert next_resp.status_code == 200
                body_v1 = next_resp.json()
                assert body_v1["total"] == 3
                assert len(body_v1["items"]) == 3
                assert {it["osm_id"]: it["name"] for it in body_v1["items"]} == {
                    301: "Rel Cafe 1",
                    302: "Rel Cafe 2 V1",
                    303: "Rel Cafe 3 V1",
                }
        finally:
            engine.dispose()


# ===========================================================================
# AC6: Duplicate Candidate Invariants, Immutability & Historical Preservation
# ===========================================================================


def test_m5_ac6_duplicate_evidence_preserved_across_supersession_recovery_retry_and_deactivation(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC6: Pair ordering, per-run uniqueness, immutability, retry, history, and no auto-merge."""
    with disposable_database("m5_ac6_dups") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m5-ac6")

            # Two distinct OSM nodes representing a suspected duplicate cafe in V1 (501 & 502)
            # plus a V2-only duplicate pair (501 & 503) where 503 is later deactivated on recovery.
            pbf_v1 = _write_pbf(
                work_dir / "dup_v1.osm.pbf",
                [
                    _node(501, name="Cà Phê Trùng", lon=106.70100, lat=10.77700),
                    _node(502, name="Cà Phê Trùng", lon=106.70105, lat=10.77705),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "dup_v2.osm.pbf",
                [
                    _node(501, name="Cà Phê Trùng", lon=106.70100, lat=10.77700),
                    _node(502, name="Cà Phê Trùng", lon=106.70105, lat=10.77705),
                    _node(503, name="Cà Phê Trùng V2", lon=106.70108, lat=10.77708),
                ],
            )
            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

            # Publish V1 with reversed input order (502, 501) -> must canonicalize place_a_id < place_b_id.
            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-m5-v1",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=502,
                            osm_type_b="node",
                            osm_id_b=501,
                            detector_version="det-m5-v1",
                            confidence_score=Decimal("0.9500"),
                            distance_meters=7.8,
                            evidence={"reason": "v1_same_name_proximity"},
                        ),
                    ),
                ),
            )
            v1_artifact = prepare_recovery_artifact(
                engine, artifact_root=work_dir / "v1_art", artifact_name="v1_with_dups"
            )
            assert v1_artifact is not None
            assert v1_artifact.duplicate_candidate_count == 1

            # Publish V2 with BOTH (501, 502) coexisting under V2 AND (501, 503) involving V2-only 503.
            publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=ing_v1.import_run.id,
                recovery_artifact_dir=v1_artifact.artifact_dir,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-m5-v2",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=501,
                            osm_type_b="node",
                            osm_id_b=502,
                            detector_version="det-m5-v2",
                            confidence_score=Decimal("0.9400"),
                            distance_meters=7.8,
                            evidence={"reason": "v2_pair_501_502"},
                        ),
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=503,
                            osm_type_b="node",
                            osm_id_b=501,
                            detector_version="det-m5-v2",
                            confidence_score=Decimal("0.8800"),
                            distance_meters=11.2,
                            evidence={"reason": "v2_pair_501_503"},
                        ),
                    ),
                ),
            )

            # Recover V1: first fail pre-COMMIT after duplicate resolution, then retry cleanly!
            op_rec = uuid.uuid4()
            approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_rec,
                expected_active_run_id=ing_v2.import_run.id,
                approved_by="ops-lead@geosearch",
                approval_reason="INC-DUP-RECOVERY",
            )
            rec_dup_input = DuplicateCandidateInput(
                state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                detector_version="det-m5-rec",
                candidates=(
                    StagedDuplicateCandidate(
                        osm_type_a="node",
                        osm_id_a=502,
                        osm_type_b="node",
                        osm_id_b=501,
                        detector_version="det-m5-rec",
                        confidence_score=Decimal("0.9500"),
                        distance_meters=7.8,
                        evidence={"reason": "recovery_verified_pair"},
                    ),
                ),
            )

            def _fail_before_commit(_conn: Connection) -> None:
                raise _InjectedFault("Rollback recovery with duplicates")

            with pytest.raises(_InjectedFault):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_rec,
                    approval=approval,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot_1",
                    duplicate_candidates=rec_dup_input,
                    fault_hooks=RecoveryFaultHooks(
                        publication_hooks=PublicationFaultHooks(
                            on_before_commit=_fail_before_commit
                        )
                    ),
                )

            # Retry with exact same op_rec succeeds and records exactly 1 pair for recovery run.
            rec_res = execute_controlled_recovery(
                engine,
                source_artifact_dir=v1_artifact.artifact_dir,
                publication_operation_id=op_rec,
                approval=approval,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                protection_artifact_root=work_dir / "v2_prot_2",
                duplicate_candidates=rec_dup_input,
            )
            assert rec_res.outcome == PublicationOutcomeStatus.PUBLISHED
            rec_run_id = rec_res.recovery_import_run_id

            # Inspect canonical places & all historical duplicate_candidates rows.
            with engine.connect() as conn:
                places = {
                    int(r.osm_id): (int(r.id), bool(r.is_active))
                    for r in conn.execute(
                        text("SELECT osm_id, id, is_active FROM places ORDER BY osm_id")
                    ).fetchall()
                }
                dup_rows = conn.execute(
                    text(
                        """
                        SELECT id, import_run_id, place_a_id, place_b_id,
                               detector_version, confidence_score, distance_meters, evidence
                        FROM duplicate_candidates
                        ORDER BY import_run_id, place_a_id, place_b_id
                        """
                    )
                ).fetchall()

            # Both distinct OSM identities 501 & 502 remain separate active POIs (never merged);
            # 503 is deactivated softly.
            assert places[501][1] is True
            assert places[502][1] is True
            assert places[501][0] != places[502][0]
            assert places[503][1] is False

            # All 4 historical duplicate_candidates rows coexist across V1 (1), V2 (2), and Recovery (1),
            # including V2's pair referencing now-inactive place 503!
            assert len(dup_rows) == 4
            by_run: dict[int, list[Any]] = {}
            for r in dup_rows:
                assert int(r.place_a_id) < int(r.place_b_id)
                by_run.setdefault(int(r.import_run_id), []).append(r)

            assert len(by_run[ing_v1.import_run.id]) == 1
            assert by_run[ing_v1.import_run.id][0].evidence == {"reason": "v1_same_name_proximity"}
            assert len(by_run[ing_v2.import_run.id]) == 2
            assert {r.evidence["reason"] for r in by_run[ing_v2.import_run.id]} == {
                "v2_pair_501_502",
                "v2_pair_501_503",
            }
            assert len(by_run[rec_run_id]) == 1
            assert by_run[rec_run_id][0].evidence == {"reason": "recovery_verified_pair"}

            # Immutability: PostgreSQL trigger rejects UPDATE on historical duplicate_candidates.
            with engine.connect() as conn:
                with conn.begin():
                    with pytest.raises(IntegrityError) as exc_imm:
                        conn.execute(
                            text(
                                "UPDATE duplicate_candidates SET evidence = '{\"tampered\": true}'::jsonb "
                                "WHERE id = :did"
                            ),
                            {"did": int(dup_rows[0].id)},
                        )
                    assert_pg_error(
                        exc_imm,
                        sqlstate="23514",
                        constraint_name="trg_duplicate_candidates_immutable",
                    )

            # Uniqueness: inserting duplicate (import_run_id, place_a_id, place_b_id) rejected by DB.
            with engine.connect() as conn:
                with conn.begin():
                    with pytest.raises(IntegrityError) as exc_uq:
                        conn.execute(
                            text(
                                """
                                INSERT INTO duplicate_candidates (
                                    import_run_id, place_a_id, place_b_id, detector_version, evidence
                                )
                                VALUES (:rid, :pa, :pb, 'det-dup', '{}'::jsonb)
                                """
                            ),
                            {
                                "rid": rec_run_id,
                                "pa": int(by_run[rec_run_id][0].place_a_id),
                                "pb": int(by_run[rec_run_id][0].place_b_id),
                            },
                        )
                    assert_pg_error(
                        exc_uq,
                        sqlstate="23505",
                        constraint_name="uq_duplicate_candidates_run_pair",
                    )

            # Search API returns BOTH distinct OSM POIs (501 & 502) separately — never merged!
            app = create_app(engine=engine)
            with TestClient(app) as client:
                resp = client.get("/api/v1/places", params={"q": "ca phe trung"})
                assert resp.status_code == 200
                body = resp.json()
                assert body["total"] == 2
                assert {it["osm_id"] for it in body["items"]} == {501, 502}

            # Replay safety inside a run: re-invoking resolver with identical evidence preserves
            # the existing row; conflicting evidence for the same pair is rejected fail-closed!
            from geosearch.publication import resolve_and_record_duplicate_candidates

            with engine.connect() as conn:
                with conn.begin():
                    replay_ok = resolve_and_record_duplicate_candidates(
                        conn,
                        import_run_id=rec_run_id,
                        duplicate_input=rec_dup_input,
                    )
                    assert replay_ok.inserted_count == 0
                    assert replay_ok.existing_preserved_count == 1

                    with pytest.raises(
                        PublicationInvariantError, match="Conflicting duplicate candidate evidence"
                    ):
                        resolve_and_record_duplicate_candidates(
                            conn,
                            import_run_id=rec_run_id,
                            duplicate_input=DuplicateCandidateInput(
                                state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                                detector_version="det-m5-rec",
                                candidates=(
                                    StagedDuplicateCandidate(
                                        osm_type_a="node",
                                        osm_id_a=501,
                                        osm_type_b="node",
                                        osm_id_b=502,
                                        detector_version="det-m5-rec",
                                        confidence_score=Decimal("0.5000"),
                                        distance_meters=7.8,
                                        evidence={"reason": "conflicting_replay_evidence"},
                                    ),
                                ),
                            ),
                        )
        finally:
            engine.dispose()


def test_m5_ac6_default_recovery_records_not_executed_and_preserves_v1_v2_duplicate_history_only(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC6 (Option A): Default recovery records NOT_EXECUTED without copying V1/V2 duplicate pairs."""
    with disposable_database("m5_ac6_opt_a") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m5-ac6-opta")

            pbf_v1 = _write_pbf(
                work_dir / "opta_v1.osm.pbf",
                [
                    _node(601, name="Cà Phê Option A", lon=106.70100, lat=10.77700),
                    _node(602, name="Cà Phê Option A", lon=106.70104, lat=10.77704),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "opta_v2.osm.pbf",
                [
                    _node(601, name="Cà Phê Option A", lon=106.70100, lat=10.77700),
                    _node(602, name="Cà Phê Option A", lon=106.70104, lat=10.77704),
                    _node(603, name="Cà Phê Option A V2", lon=106.70108, lat=10.77708),
                ],
            )
            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-v1",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=602,
                            osm_type_b="node",
                            osm_id_b=601,
                            detector_version="det-v1",
                            confidence_score=Decimal("0.9200"),
                            distance_meters=6.2,
                            evidence={"reason": "v1_original_evidence"},
                        ),
                    ),
                ),
            )
            v1_artifact = prepare_recovery_artifact(
                engine, artifact_root=work_dir / "v1_art", artifact_name="v1_opta"
            )
            assert v1_artifact is not None
            assert v1_artifact.duplicate_candidate_count == 1

            publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=ing_v1.import_run.id,
                recovery_artifact_dir=v1_artifact.artifact_dir,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-v2",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=603,
                            osm_type_b="node",
                            osm_id_b=601,
                            detector_version="det-v2",
                            confidence_score=Decimal("0.8500"),
                            distance_meters=12.0,
                            evidence={"reason": "v2_original_evidence"},
                        ),
                    ),
                ),
            )

            # Default recovery (no duplicate_candidates supplied): Option A requires NOT_EXECUTED,
            # zero copied pairs on the recovery run, and full preservation on V1 and V2 runs.
            op_rec = uuid.uuid4()
            rec_res = execute_controlled_recovery(
                engine,
                source_artifact_dir=v1_artifact.artifact_dir,
                publication_operation_id=op_rec,
                approval=build_recovery_approval_from_artifact(
                    v1_artifact,
                    publication_operation_id=op_rec,
                    expected_active_run_id=ing_v2.import_run.id,
                    approved_by="ops-lead@geosearch",
                    approval_reason="INC-OPTION-A",
                ),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                protection_artifact_root=work_dir / "v2_prot",
            )
            assert rec_res.outcome == PublicationOutcomeStatus.PUBLISHED
            assert (
                rec_res.publication_result.duplicate_detection_state
                == DuplicateDetectionState.NOT_EXECUTED
            )
            assert rec_res.publication_result.duplicate_candidate_count == 0
            assert (
                rec_res.publication_result.publication_evidence["duplicate_candidates"]["state"]
                == "NOT_EXECUTED"
            )
            assert (
                rec_res.publication_result.publication_evidence["duplicate_candidates"][
                    "resolved_pair_count"
                ]
                == 0
            )

            with engine.connect() as conn:
                rec_dups = conn.execute(
                    text("SELECT id FROM duplicate_candidates WHERE import_run_id = :rid"),
                    {"rid": rec_res.recovery_import_run_id},
                ).fetchall()
                v1_dups = conn.execute(
                    text(
                        "SELECT place_a_id, place_b_id, evidence "
                        "FROM duplicate_candidates WHERE import_run_id = :rid"
                    ),
                    {"rid": ing_v1.import_run.id},
                ).fetchall()
                v2_dups = conn.execute(
                    text(
                        "SELECT place_a_id, place_b_id, evidence "
                        "FROM duplicate_candidates WHERE import_run_id = :rid"
                    ),
                    {"rid": ing_v2.import_run.id},
                ).fetchall()
                places = {
                    int(r.osm_id): (int(r.id), bool(r.is_active))
                    for r in conn.execute(
                        text("SELECT osm_id, id, is_active FROM places ORDER BY osm_id")
                    ).fetchall()
                }

            assert rec_dups == []
            assert len(v1_dups) == 1
            assert int(v1_dups[0].place_a_id) < int(v1_dups[0].place_b_id)
            assert v1_dups[0].evidence == {"reason": "v1_original_evidence"}
            assert len(v2_dups) == 1
            assert int(v2_dups[0].place_a_id) < int(v2_dups[0].place_b_id)
            assert v2_dups[0].evidence == {"reason": "v2_original_evidence"}

            # Distinct OSM identities remain separate canonical POIs (never merged); 603 deactivated.
            assert places[601][1] is True
            assert places[602][1] is True
            assert places[601][0] != places[602][0]
            assert places[603][1] is False
        finally:
            engine.dispose()
