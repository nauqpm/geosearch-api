"""M3.5 fault-injection (all 13 checkpoints), deterministic concurrency, and reader isolation tests."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import osmium
import pytest
from sqlalchemy import Connection, Engine, text

from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    DuplicateCandidateInput,
    DuplicateDetectionState,
    EligibilityStatus,
    PublicationConflictError,
    PublicationFaultHooks,
    PublicationMode,
    PublicationOutcomeStatus,
    StagedDuplicateCandidate,
    publish_prepared_run,
    read_active_place_identities_single_statement,
    read_publication_snapshot_in_tx,
    repeatable_read_snapshot,
    resolve_unknown_commit_outcome,
)
from tests.helpers import insert_boundary_version, upgrade_test_db_to_head


class _InjectedFault(RuntimeError):
    """Deterministic fault raised at a specific publication transaction checkpoint."""


def _write_pbf(path: Path, nodes: list[osmium.osm.mutable.Node]) -> Path:
    writer = osmium.SimpleWriter(str(path))
    try:
        for node in nodes:
            writer.add_node(node)
    finally:
        writer.close()
    return path


def _node(osm_id: int, name: str, lon: float = 106.701, lat: float = 10.777) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=1,
        changeset=100,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags={"amenity": "cafe", "name": name},
    )


def _setup_v1_published_and_v2_prepared(
    engine: Engine,
    work_dir: Path,
) -> tuple[int, int, uuid.UUID, dict[int, int]]:
    """Publish V1(A=101, B=102, C=103) and stage PREPARED V2(A=101, B=102 updated, D=104)."""
    with engine.begin() as conn:
        bv_id = insert_boundary_version(conn, version_code="bv-m35-fi")

    pbf_v1 = _write_pbf(
        work_dir / "v1.osm.pbf",
        [
            _node(101, "Cafe A V1", 106.701, 10.777),
            _node(102, "Cafe B V1", 106.702, 10.778),
            _node(103, "Cafe C V1", 106.703, 10.779),
        ],
    )
    pbf_v2 = _write_pbf(
        work_dir / "v2.osm.pbf",
        [
            _node(101, "Cafe A V1", 106.701, 10.777),
            _node(102, "Cafe B V2", 106.702, 10.778),
            _node(104, "Cafe D V2", 106.704, 10.780),
        ],
    )
    ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
    ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

    op_v1 = uuid.uuid4()
    publish_prepared_run(
        engine,
        import_run_id=ing_v1.import_run.id,
        publication_operation_id=op_v1,
        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
    )
    with engine.connect() as conn:
        v1_ids = {
            int(r.osm_id): int(r.id)
            for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
        }
    return ing_v1.import_run.id, ing_v2.import_run.id, op_v1, v1_ids


def _assert_v1_intact(
    engine: Engine,
    *,
    v1_run_id: int,
    v2_run_id: int,
    op_v1: uuid.UUID,
    v1_ids: dict[int, int],
) -> None:
    """Assert zero partial V2 writes survived rollback and V1 remains completely active."""
    with engine.connect() as conn:
        active_runs = conn.execute(
            text("SELECT id, status, publication_operation_id FROM import_runs WHERE active_slot = 1")
        ).fetchall()
        assert len(active_runs) == 1
        assert active_runs[0].id == v1_run_id
        assert active_runs[0].status == "PUBLISHED"
        assert active_runs[0].publication_operation_id == op_v1

        v2_row = conn.execute(
            text("SELECT status, active_slot, published_at FROM import_runs WHERE id = :id"),
            {"id": v2_run_id},
        ).one()
        assert v2_row.status == "PREPARED"
        assert v2_row.active_slot is None
        assert v2_row.published_at is None

        places = {
            int(r.osm_id): r
            for r in conn.execute(
                text("SELECT id, osm_id, name, is_active, last_seen_import_id FROM places")
            ).fetchall()
        }
        assert set(places.keys()) == {101, 102, 103}
        assert places[101].id == v1_ids[101] and places[101].is_active is True
        assert places[102].id == v1_ids[102] and places[102].name == "Cafe B V1" and places[102].is_active is True
        assert places[103].id == v1_ids[103] and places[103].is_active is True
        assert all(p.last_seen_import_id == v1_run_id for p in places.values())

        v2_dups = conn.execute(
            text("SELECT count(*) FROM duplicate_candidates WHERE import_run_id = :id"),
            {"id": v2_run_id},
        ).scalar_one()
        assert v2_dups == 0


# ---------------------------------------------------------------------------
# M3.5 Fault Injection: All 12 Pre-COMMIT Checkpoints + Checkpoint 13 Post-COMMIT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "checkpoint_name",
    [
        "1_before_lock",
        "2_after_lock",
        "3_after_revalidation",
        "4_before_upsert",
        "5_during_upsert",
        "6_after_upsert",
        "7_during_reconciliation",
        "8_after_deactivation",
        "9_during_duplicate_resolution",
        "10_during_pre_commit_verification",
        "11_during_active_slot_switch",
        "12_before_commit",
    ],
)
def test_m35_fault_injection_pre_commit_checkpoints_roll_back_atomically_and_allow_retry(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    checkpoint_name: str,
) -> None:
    """Every pre-COMMIT fault rolls back live state completely and permits safe retry with same UUID."""
    with disposable_database(f"m35_fi_{checkpoint_name.split('_')[0]}") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            v1_id, v2_id, op_v1, v1_ids = _setup_v1_published_and_v2_prepared(engine, work_dir)
            op_v2 = uuid.uuid4()
            artifact_root = work_dir / "artifacts"

            def _boom(*_args: Any) -> None:
                raise _InjectedFault(f"Fault injected at {checkpoint_name}")

            def _boom_mid_upsert(_conn: Connection, batch_idx: int) -> None:
                if batch_idx == 2:
                    raise _InjectedFault("Fault injected mid-UPSERT after batch 1")

            hook_map: dict[str, PublicationFaultHooks] = {
                "1_before_lock": PublicationFaultHooks(on_before_lock=_boom),
                "2_after_lock": PublicationFaultHooks(on_after_lock=_boom),
                "3_after_revalidation": PublicationFaultHooks(on_after_revalidation=_boom),
                "4_before_upsert": PublicationFaultHooks(on_before_upsert=_boom),
                "5_during_upsert": PublicationFaultHooks(on_during_upsert=_boom_mid_upsert),
                "6_after_upsert": PublicationFaultHooks(on_after_upsert=_boom),
                "7_during_reconciliation": PublicationFaultHooks(on_during_reconciliation=_boom),
                "8_after_deactivation": PublicationFaultHooks(on_after_deactivation=_boom),
                "9_during_duplicate_resolution": PublicationFaultHooks(
                    on_during_duplicate_resolution=_boom
                ),
                "10_during_pre_commit_verification": PublicationFaultHooks(
                    on_during_pre_commit_verification=_boom
                ),
                "11_during_active_slot_switch": PublicationFaultHooks(
                    on_during_active_slot_switch=_boom
                ),
                "12_before_commit": PublicationFaultHooks(on_before_commit=_boom),
            }

            dup_in = DuplicateCandidateInput(
                state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                detector_version="det-fi-v1",
                candidates=(
                    StagedDuplicateCandidate(
                        osm_type_a="node",
                        osm_id_a=101,
                        osm_type_b="node",
                        osm_id_b=102,
                        detector_version="det-fi-v1",
                        evidence={"checkpoint": checkpoint_name},
                    ),
                ),
            )

            with pytest.raises(_InjectedFault):
                publish_prepared_run(
                    engine,
                    import_run_id=v2_id,
                    publication_operation_id=op_v2,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=v1_id,
                    recovery_artifact_root=artifact_root,
                    duplicate_candidates=dup_in,
                    upsert_batch_size=1,
                    fault_hooks=hook_map[checkpoint_name],
                )

            # Assert zero partial writes survived!
            _assert_v1_intact(
                engine, v1_run_id=v1_id, v2_run_id=v2_id, op_v1=op_v1, v1_ids=v1_ids
            )

            # Safe retry with the EXACT SAME publication_operation_id succeeds!
            retry_res = publish_prepared_run(
                engine,
                import_run_id=v2_id,
                publication_operation_id=op_v2,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=v1_id,
                recovery_artifact_root=artifact_root,
                duplicate_candidates=dup_in,
            )
            assert retry_res.outcome == PublicationOutcomeStatus.PUBLISHED
            assert retry_res.active_place_count == 3
            assert retry_res.duplicate_candidate_count == 1
        finally:
            engine.dispose()


def test_m35_fault_injection_checkpoint_13_after_commit_before_ack(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Checkpoint 13: Failure AFTER COMMIT keeps committed V2 durable; retry returns ALREADY_PUBLISHED."""
    with disposable_database("m35_fi_13") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            v1_id, v2_id, _op_v1, v1_ids = _setup_v1_published_and_v2_prepared(engine, work_dir)
            op_v2 = uuid.uuid4()

            def _lose_ack(_run_id: int, _op: uuid.UUID) -> None:
                raise _InjectedFault("Simulated network drop after COMMIT")

            with pytest.raises(_InjectedFault, match="after COMMIT"):
                publish_prepared_run(
                    engine,
                    import_run_id=v2_id,
                    publication_operation_id=op_v2,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=v1_id,
                    recovery_artifact_root=work_dir / "artifacts",
                    fault_hooks=PublicationFaultHooks(on_after_commit_before_ack=_lose_ack),
                )

            # Durable evidence confirms V2 committed (must NOT be rolled back!).
            ev = resolve_unknown_commit_outcome(
                engine, publication_operation_id=op_v2, import_run_id=v2_id
            )
            assert ev is not None
            assert ev.status == "PUBLISHED"
            assert ev.active_slot == 1
            assert ev.is_committed_publication is True

            # Retry with same op_v2 resolves idempotently to ALREADY_PUBLISHED.
            retry_res = publish_prepared_run(
                engine,
                import_run_id=v2_id,
                publication_operation_id=op_v2,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=v1_id,
            )
            assert retry_res.outcome == PublicationOutcomeStatus.ALREADY_PUBLISHED
            assert retry_res.already_committed is True
            assert retry_res.published_at == ev.published_at

            # Stable IDs preserved in committed V2.
            with engine.connect() as conn:
                ids_now = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(
                        text("SELECT osm_id, id FROM places WHERE is_active = true")
                    ).fetchall()
                }
                assert set(ids_now.keys()) == {101, 102, 104}
                assert ids_now[101] == v1_ids[101]
                assert ids_now[102] == v1_ids[102]
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# M3.5 Deterministic Concurrency Tests (Multiple PostgreSQL Connections)
# ---------------------------------------------------------------------------


def _wait_for_pg_lock_waiter(engine: Engine, *, query_fragment: str, timeout: float = 5.0) -> None:
    """Deterministically poll pg_stat_activity until a worker blocks on a PostgreSQL Lock."""
    deadline = threading.Event()
    steps = int(timeout / 0.01)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as observer:
        for _ in range(steps):
            waiting = observer.execute(
                text(
                    """
                    SELECT count(*)
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE :pattern
                    """
                ),
                {"pattern": f"%{query_fragment}%"},
            ).scalar_one()
            if int(waiting) >= 1:
                return
            deadline.wait(0.01)
    raise AssertionError(f"Timed out waiting for PostgreSQL Lock waiter matching {query_fragment!r}.")


def test_m35_concurrent_two_workers_same_run_same_uuid_idempotent(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Two workers racing to publish the SAME run with the SAME UUID: one commits, one gets ALREADY_PUBLISHED."""
    with disposable_database("m35_same_run") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m35-same")
            pbf = _write_pbf(work_dir / "r1.osm.pbf", [_node(501, "Cafe Race Same")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            op_uuid = uuid.uuid4()

            w1_holds_lock = threading.Event()
            w2_started = threading.Event()
            release_w1 = threading.Event()
            results: dict[str, Any] = {}

            def _worker1() -> None:
                try:
                    def _hold_after_lock(_conn: Connection) -> None:
                        w1_holds_lock.set()
                        assert release_w1.wait(timeout=10.0)

                    results["w1"] = publish_prepared_run(
                        engine,
                        import_run_id=ing.import_run.id,
                        publication_operation_id=op_uuid,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        fault_hooks=PublicationFaultHooks(on_after_lock=_hold_after_lock),
                    )
                except BaseException as exc:  # pragma: no cover
                    results["w1_err"] = exc

            def _worker2() -> None:
                assert w1_holds_lock.wait(timeout=10.0)
                w2_started.set()
                try:
                    results["w2"] = publish_prepared_run(
                        engine,
                        import_run_id=ing.import_run.id,
                        publication_operation_id=op_uuid,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )
                except BaseException as exc:  # pragma: no cover
                    results["w2_err"] = exc

            t1 = threading.Thread(target=_worker1)
            t2 = threading.Thread(target=_worker2)
            t1.start()
            t2.start()

            assert w1_holds_lock.wait(timeout=10.0)
            assert w2_started.wait(timeout=10.0)
            _wait_for_pg_lock_waiter(engine, query_fragment="pg_advisory_xact_lock")
            release_w1.set()

            t1.join(timeout=10.0)
            t2.join(timeout=10.0)

            assert "w1_err" not in results, results.get("w1_err")
            assert "w2_err" not in results, results.get("w2_err")
            assert results["w1"].outcome == PublicationOutcomeStatus.PUBLISHED
            assert results["w2"].outcome == PublicationOutcomeStatus.ALREADY_PUBLISHED
            assert results["w2"].already_committed is True
            assert results["w1"].published_at == results["w2"].published_at
        finally:
            engine.dispose()


def test_m35_concurrent_registration_race_independent_uuids_same_run(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Two workers racing with INDEPENDENT UUIDs on the SAME run: one wins registration, other gets CONFLICT."""
    with disposable_database("m35_reg_race") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m35-reg")
            pbf = _write_pbf(work_dir / "r1.osm.pbf", [_node(511, "Cafe Reg Race")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            op_1 = uuid.uuid4()
            op_2 = uuid.uuid4()

            w1_in_reg = threading.Event()
            release_w1 = threading.Event()
            outcomes: dict[str, Any] = {}

            def _worker1() -> None:
                try:
                    def _hold_reg(_conn: Connection) -> None:
                        w1_in_reg.set()
                        assert release_w1.wait(timeout=10.0)

                    outcomes["w1"] = publish_prepared_run(
                        engine,
                        import_run_id=ing.import_run.id,
                        publication_operation_id=op_1,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        fault_hooks=PublicationFaultHooks(on_during_registration=_hold_reg),
                    )
                except BaseException as exc:  # pragma: no cover
                    outcomes["w1_err"] = exc

            def _worker2() -> None:
                assert w1_in_reg.wait(timeout=10.0)
                try:
                    outcomes["w2"] = publish_prepared_run(
                        engine,
                        import_run_id=ing.import_run.id,
                        publication_operation_id=op_2,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )
                except BaseException as exc:
                    outcomes["w2_err"] = exc

            t1 = threading.Thread(target=_worker1)
            t2 = threading.Thread(target=_worker2)
            t1.start()
            t2.start()

            assert w1_in_reg.wait(timeout=10.0)
            _wait_for_pg_lock_waiter(engine, query_fragment="FROM import_runs")
            release_w1.set()

            t1.join(timeout=10.0)
            t2.join(timeout=10.0)

            assert "w1_err" not in outcomes, outcomes.get("w1_err")
            assert outcomes["w1"].outcome == PublicationOutcomeStatus.PUBLISHED
            assert isinstance(outcomes.get("w2_err"), PublicationConflictError)
            assert outcomes["w2_err"].status == EligibilityStatus.PUBLICATION_CONFLICT
        finally:
            engine.dispose()


def test_m35_concurrent_two_different_runs_same_baseline_prevents_stale_overwrite(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Two workers publishing DIFFERENT runs against V1 baseline: winner commits, loser fails closed."""
    with disposable_database("m35_diff_runs") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m35-diff")

            pbf_v1 = _write_pbf(work_dir / "v1.osm.pbf", [_node(601, "V1 Base")])
            pbf_v2a = _write_pbf(work_dir / "v2a.osm.pbf", [_node(602, "V2A Winner")])
            pbf_v2b = _write_pbf(work_dir / "v2b.osm.pbf", [_node(603, "V2B Stale Loser")])

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2a = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2a, boundary_version_id=bv_id)
            ing_v2b = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2b, boundary_version_id=bv_id)

            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            w1_holds_lock = threading.Event()
            release_w1 = threading.Event()
            outcomes: dict[str, Any] = {}

            def _worker_a() -> None:
                try:
                    def _hold_lock(_conn: Connection) -> None:
                        w1_holds_lock.set()
                        assert release_w1.wait(timeout=10.0)

                    outcomes["a"] = publish_prepared_run(
                        engine,
                        import_run_id=ing_v2a.import_run.id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        expected_active_run_id=ing_v1.import_run.id,
                        recovery_artifact_root=work_dir / "artifacts_a",
                        fault_hooks=PublicationFaultHooks(on_after_lock=_hold_lock),
                    )
                except BaseException as exc:  # pragma: no cover
                    outcomes["a_err"] = exc

            def _worker_b() -> None:
                assert w1_holds_lock.wait(timeout=10.0)
                try:
                    outcomes["b"] = publish_prepared_run(
                        engine,
                        import_run_id=ing_v2b.import_run.id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        expected_active_run_id=ing_v1.import_run.id,
                        recovery_artifact_root=work_dir / "artifacts_b",
                    )
                except BaseException as exc:
                    outcomes["b_err"] = exc

            ta = threading.Thread(target=_worker_a)
            tb = threading.Thread(target=_worker_b)
            ta.start()
            tb.start()

            assert w1_holds_lock.wait(timeout=10.0)
            _wait_for_pg_lock_waiter(engine, query_fragment="pg_advisory_xact_lock")
            release_w1.set()

            ta.join(timeout=10.0)
            tb.join(timeout=10.0)

            assert "a_err" not in outcomes, outcomes.get("a_err")
            assert outcomes["a"].outcome == PublicationOutcomeStatus.PUBLISHED
            assert isinstance(outcomes.get("b_err"), PublicationConflictError)
            assert outcomes["b_err"].status == EligibilityStatus.PUBLICATION_CONFLICT

            # Verify V2A is active and V2B did NOT overwrite it!
            with engine.connect() as conn:
                active_run_id = conn.execute(
                    text("SELECT id FROM import_runs WHERE active_slot = 1")
                ).scalar_one()
                assert active_run_id == ing_v2a.import_run.id
                active_osm_ids = conn.execute(
                    text("SELECT osm_id FROM places WHERE is_active = true ORDER BY osm_id")
                ).scalars().all()
                assert active_osm_ids == [602]
        finally:
            engine.dispose()


def test_m35_concurrent_duplicate_uuid_across_different_runs_rejected(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Two workers attempting to use the SAME UUID on TWO DIFFERENT runs: only one succeeds."""
    with disposable_database("m35_dup_uuid") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m35-dupuuid")

            pbf_1 = _write_pbf(work_dir / "r1.osm.pbf", [_node(701, "Run 1")])
            pbf_2 = _write_pbf(work_dir / "r2.osm.pbf", [_node(702, "Run 2")])
            ing_1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_1, boundary_version_id=bv_id)
            ing_2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_2, boundary_version_id=bv_id)
            shared_uuid = uuid.uuid4()

            barrier = threading.Barrier(2, timeout=10.0)
            results: list[tuple[str, Any]] = []
            lock = threading.Lock()

            def _run(label: str, run_id: int) -> None:
                barrier.wait()
                try:
                    res = publish_prepared_run(
                        engine,
                        import_run_id=run_id,
                        publication_operation_id=shared_uuid,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )
                    with lock:
                        results.append((label, res))
                except BaseException as exc:
                    with lock:
                        results.append((label, exc))

            t1 = threading.Thread(target=_run, args=("r1", ing_1.import_run.id))
            t2 = threading.Thread(target=_run, args=("r2", ing_2.import_run.id))
            t1.start()
            t2.start()
            t1.join(timeout=10.0)
            t2.join(timeout=10.0)

            successes = [val for _, val in results if not isinstance(val, BaseException)]
            failures = [val for _, val in results if isinstance(val, BaseException)]
            assert len(successes) == 1
            assert len(failures) == 1
            assert isinstance(failures[0], PublicationConflictError)
            assert failures[0].status == EligibilityStatus.PUBLICATION_CONFLICT
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# M3.5 Reader Isolation Tests (READ COMMITTED & REPEATABLE READ READ ONLY)
# ---------------------------------------------------------------------------


def test_m35_reader_snapshot_isolation_during_and_after_atomic_publication(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Verifies single-statement READ COMMITTED and multi-statement REPEATABLE READ READ ONLY isolation."""
    with disposable_database("m35_readers") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            v1_id, v2_id, _op_v1, v1_ids = _setup_v1_published_and_v2_prepared(engine, work_dir)

            writer_ready_to_commit = threading.Event()
            allow_commit = threading.Event()
            writer_err: list[BaseException] = []

            def _publish_v2() -> None:
                try:
                    def _pause_before_commit(_conn: Connection) -> None:
                        writer_ready_to_commit.set()
                        assert allow_commit.wait(timeout=10.0)

                    publish_prepared_run(
                        engine,
                        import_run_id=v2_id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        expected_active_run_id=v1_id,
                        recovery_artifact_root=work_dir / "artifacts",
                        fault_hooks=PublicationFaultHooks(on_before_commit=_pause_before_commit),
                    )
                except BaseException as exc:  # pragma: no cover
                    writer_err.append(exc)

            t_pub = threading.Thread(target=_publish_v2)
            t_pub.start()

            # Wait until V2 has finished UPSERT, deactivation, verification, and slot switch
            # inside its uncommitted transaction (immediately before COMMIT).
            assert writer_ready_to_commit.wait(timeout=10.0)

            # 1. Single-statement READ COMMITTED reader sees ONLY V1 (zero uncommitted V2 leaks!).
            rc_during = read_active_place_identities_single_statement(engine)
            assert [(t, oid, rid) for t, oid, _pid, rid in rc_during] == [
                ("node", 101, v1_id),
                ("node", 102, v1_id),
                ("node", 103, v1_id),
            ]

            # 2. Multi-statement REPEATABLE READ READ ONLY reader started BEFORE V2 COMMIT:
            #    Statement 1 runs before V2 commits; Statement 2 runs AFTER V2 commits —
            #    BOTH statements MUST observe the exact same consistent V1 snapshot!
            with repeatable_read_snapshot(engine) as rr_conn:
                snap_before = read_publication_snapshot_in_tx(rr_conn)
                assert snap_before.active_run_id == v1_id
                assert [oid for _t, oid, _pid in snap_before.active_place_identities] == [101, 102, 103]
                assert snap_before.active_place_names[("node", 102)] == "Cafe B V1"

                # Release V2 to COMMIT while rr_conn transaction stays open!
                allow_commit.set()
                t_pub.join(timeout=10.0)
                assert not writer_err, writer_err[0]

                # Second read inside the SAME REPEATABLE READ transaction still sees V1 consistently!
                snap_after_commit_same_tx = read_publication_snapshot_in_tx(rr_conn)
                assert snap_after_commit_same_tx == snap_before

            # 3. A NEW transaction after COMMIT sees complete V2 atomically!
            rc_after = read_active_place_identities_single_statement(engine)
            assert [(t, oid, rid) for t, oid, _pid, rid in rc_after] == [
                ("node", 101, v2_id),
                ("node", 102, v2_id),
                ("node", 104, v2_id),
            ]
            with repeatable_read_snapshot(engine) as rr_new:
                snap_v2 = read_publication_snapshot_in_tx(rr_new)
                assert snap_v2.active_run_id == v2_id
                assert [oid for _t, oid, _pid in snap_v2.active_place_identities] == [101, 102, 104]
                assert snap_v2.active_place_names[("node", 102)] == "Cafe B V2"
                assert dict((oid, pid) for _t, oid, pid in snap_v2.active_place_identities)[101] == v1_ids[101]
        finally:
            engine.dispose()


def test_m35_stale_preflight_caught_by_lock_revalidation_and_failed_publish_invisible_to_readers(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Tampering staging after preflight is rejected under lock; failed publish never leaks to readers."""
    from geosearch.publication import PublicationEligibilityError

    with disposable_database("m35_reval_lock") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            v1_id, v2_id, _op_v1, _v1_ids = _setup_v1_published_and_v2_prepared(engine, work_dir)

            # Tamper a staged candidate row AFTER preflight passed, right before acquiring the lock
            # (using controlled trigger bypass on disposable DB to verify Python lock-revalidation defense-in-depth).
            def _tamper_between_preflight_and_lock() -> None:
                with engine.begin() as tamper_conn:
                    tamper_conn.execute(
                        text(
                            "ALTER TABLE staging_place_candidates "
                            "DISABLE TRIGGER trg_staging_place_candidates_immutability"
                        )
                    )
                    tamper_conn.execute(
                        text(
                            "UPDATE staging_place_candidates "
                            "SET name = 'Tampered After Preflight', "
                            "    name_search = 'tampered after preflight' "
                            "WHERE import_run_id = :run_id AND osm_id = 104"
                        ),
                        {"run_id": v2_id},
                    )
                    tamper_conn.execute(
                        text(
                            "ALTER TABLE staging_place_candidates "
                            "ENABLE TRIGGER trg_staging_place_candidates_immutability"
                        )
                    )

            with pytest.raises(PublicationEligibilityError) as exc_info:
                publish_prepared_run(
                    engine,
                    import_run_id=v2_id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=v1_id,
                    recovery_artifact_root=work_dir / "artifacts",
                    fault_hooks=PublicationFaultHooks(
                        on_before_lock=_tamper_between_preflight_and_lock
                    ),
                )
            assert exc_info.value.status == EligibilityStatus.INVALID_STAGING

            # Readers still see clean V1 with zero leakage from the rejected publication.
            rc = read_active_place_identities_single_statement(engine)
            assert [(t, oid, rid) for t, oid, _pid, rid in rc] == [
                ("node", 101, v1_id),
                ("node", 102, v1_id),
                ("node", 103, v1_id),
            ]
        finally:
            engine.dispose()
