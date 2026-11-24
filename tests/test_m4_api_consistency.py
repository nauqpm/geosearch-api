"""M4.4 & M4.5 tests (Groups F & G): Read consistency, publication concurrency, and DB safety."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import osmium
import psycopg
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, event, text
from sqlalchemy.exc import DBAPIError

from geosearch.api import create_app
from geosearch.api.dependencies import read_committed_readonly, repeatable_read_readonly
from geosearch.db import (
    UnsafeDatabaseConfigError,
    create_db_engine,
    create_test_engine,
    get_test_database_url,
)
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    PublicationFaultHooks,
    PublicationMode,
    publish_prepared_run,
)
from tests.helpers import (
    assert_pg_error,
    insert_boundary_version,
    insert_import_run,
    insert_place,
    upgrade_test_db_to_head,
)


class _InjectedPublicationFault(RuntimeError):
    """Controlled test exception to trigger deterministic pre-COMMIT rollback."""


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
    lon: float = 106.7000,
    lat: float = 10.7800,
    category_tag: tuple[str, str] = ("amenity", "cafe"),
    version: int = 1,
) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=version,
        changeset=100,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags={category_tag[0]: category_tag[1], "name": name},
    )


def test_m44_api_publication_concurrency_uncommitted_commit_and_rollback(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """API readers never see uncommitted V2, observe atomic COMMIT, and never see rolled-back V3."""
    with disposable_database("m44_pub_conc") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m44-conc")

            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    _node(101, name="Cafe A V1", lon=106.700, lat=10.780),
                    _node(102, name="Cafe B V1", lon=106.701, lat=10.780),
                    _node(103, name="Cafe C V1", lon=106.702, lat=10.780),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    _node(101, name="Cafe A V1", lon=106.700, lat=10.780),
                    _node(102, name="Cafe B V2", lon=106.701, lat=10.780, version=2),
                    _node(104, name="Cafe D V2", lon=106.703, lat=10.780),
                ],
            )
            pbf_v3 = _write_pbf(
                work_dir / "v3.osm.pbf",
                [
                    _node(101, name="Cafe A V3 Rolled Back", lon=106.700, lat=10.780, version=3),
                    _node(105, name="Cafe E V3 Rolled Back", lon=106.704, lat=10.780),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)
            ing_v3 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v3, boundary_version_id=bv_id)

            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )

            with engine.connect() as conn:
                v1_ids = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                }

            app = create_app(engine=engine)
            with TestClient(app) as client:
                writer_before_commit = threading.Event()
                release_commit = threading.Event()
                writer_errors: list[BaseException] = []

                def _publish_v2() -> None:
                    try:
                        def _pause(_conn: Connection) -> None:
                            writer_before_commit.set()
                            assert release_commit.wait(timeout=10.0)

                        publish_prepared_run(
                            engine,
                            import_run_id=ing_v2.import_run.id,
                            publication_operation_id=uuid.uuid4(),
                            publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                            expected_active_run_id=ing_v1.import_run.id,
                            recovery_artifact_root=work_dir / "artifacts",
                            fault_hooks=PublicationFaultHooks(on_before_commit=_pause),
                        )
                    except BaseException as exc:  # pragma: no cover
                        writer_errors.append(exc)

                t_pub = threading.Thread(target=_publish_v2)
                t_pub.start()
                assert writer_before_commit.wait(timeout=10.0)

                # While V2 holds the publication lock with uncommitted UPSERT/deactivation/slot-switch,
                # API readers do NOT block and observe ONLY committed V1!
                r_list_during = client.get("/api/v1/places")
                assert r_list_during.status_code == 200
                list_during = r_list_during.json()
                assert list_during["total"] == 3
                assert {it["osm_id"]: it["name"] for it in list_during["items"]} == {
                    101: "Cafe A V1",
                    102: "Cafe B V1",
                    103: "Cafe C V1",
                }

                r_nearby_during = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 106.700, "lat": 10.780, "radius_m": 2000},
                )
                assert r_nearby_during.status_code == 200
                assert [it["osm_id"] for it in r_nearby_during.json()["items"]] == [101, 102, 103]

                r_detail_103_during = client.get(f"/api/v1/places/{v1_ids[103]}")
                assert r_detail_103_during.status_code == 200
                assert r_detail_103_during.json()["name"] == "Cafe C V1"

                # Release V2 COMMIT.
                release_commit.set()
                t_pub.join(timeout=10.0)
                assert not writer_errors, writer_errors[0]

                # Immediately after COMMIT, API sees complete V2 atomically with preserved stable IDs.
                r_list_after = client.get("/api/v1/places")
                assert r_list_after.status_code == 200
                list_after = r_list_after.json()
                assert list_after["total"] == 3
                assert {it["osm_id"]: it["name"] for it in list_after["items"]} == {
                    101: "Cafe A V1",
                    102: "Cafe B V2",
                    104: "Cafe D V2",
                }
                assert client.get(f"/api/v1/places/{v1_ids[103]}").status_code == 404
                r_101_after = client.get(f"/api/v1/places/{v1_ids[101]}")
                assert r_101_after.status_code == 200
                assert r_101_after.json()["id"] == v1_ids[101]

                # Rolled-back V3 publication leaves V2 completely intact and never leaks.
                def _fail_before_commit(_conn: Connection) -> None:
                    raise _InjectedPublicationFault("Deterministic V3 rollback")

                with pytest.raises(_InjectedPublicationFault):
                    publish_prepared_run(
                        engine,
                        import_run_id=ing_v3.import_run.id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        expected_active_run_id=ing_v2.import_run.id,
                        recovery_artifact_root=work_dir / "artifacts",
                        fault_hooks=PublicationFaultHooks(on_before_commit=_fail_before_commit),
                    )

                r_after_rollback = client.get("/api/v1/places")
                assert r_after_rollback.status_code == 200
                assert {it["osm_id"]: it["name"] for it in r_after_rollback.json()["items"]} == {
                    101: "Cafe A V1",
                    102: "Cafe B V2",
                    104: "Cafe D V2",
                }
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    ("endpoint", "params"),
    [
        ("/api/v1/places", {"page": 1, "limit": 10}),
        ("/api/v1/places/nearby", {"lon": 106.700, "lat": 10.780, "radius_m": 5000, "limit": 10}),
    ],
)
def test_m44_multi_statement_response_repeatable_read_snapshot_across_live_commit(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    endpoint: str,
    params: dict[str, Any],
) -> None:
    """COUNT(*) and SELECT items never mix V1 total with V2 items when V2 commits mid-request."""
    with disposable_database("m44_rr_snap") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m44-rr")

            # V1 has 3 places; V2 has 5 places (drops 103, updates 102, adds 104, 105, 106).
            pbf_v1 = _write_pbf(
                work_dir / "rr_v1.osm.pbf",
                [
                    _node(101, name="Snap Cafe 1", lon=106.700, lat=10.780),
                    _node(102, name="Snap Cafe 2 V1", lon=106.701, lat=10.780),
                    _node(103, name="Snap Cafe 3 V1", lon=106.702, lat=10.780),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "rr_v2.osm.pbf",
                [
                    _node(101, name="Snap Cafe 1", lon=106.700, lat=10.780),
                    _node(102, name="Snap Cafe 2 V2", lon=106.701, lat=10.780, version=2),
                    _node(104, name="Snap Cafe 4 V2", lon=106.703, lat=10.780),
                    _node(105, name="Snap Cafe 5 V2", lon=106.704, lat=10.780),
                    _node(106, name="Snap Cafe 6 V2", lon=106.705, lat=10.780),
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

            count_completed = threading.Event()
            v2_committed = threading.Event()
            response_holder: dict[str, Any] = {}

            app = create_app(engine=engine)

            def _pause_between_count_and_items(_conn: Connection) -> None:
                count_completed.set()
                assert v2_committed.wait(timeout=10.0)

            app.state.on_between_queries = _pause_between_count_and_items

            with TestClient(app) as client:
                def _run_http_request() -> None:
                    try:
                        response_holder["resp"] = client.get(endpoint, params=params)
                    except BaseException as exc:  # pragma: no cover
                        response_holder["err"] = exc

                t_req = threading.Thread(target=_run_http_request)
                t_req.start()

                # Wait deterministically until Statement 1 (COUNT(*)) has finished in the request's tx.
                assert count_completed.wait(timeout=10.0)

                # Commit V2 completely on an independent connection BEFORE Statement 2 runs!
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=ing_v1.import_run.id,
                    recovery_artifact_root=work_dir / "artifacts",
                )

                # Release Statement 2 inside the in-flight HTTP request.
                v2_committed.set()
                t_req.join(timeout=10.0)

                assert "err" not in response_holder, response_holder.get("err")
                in_flight_resp = response_holder["resp"]
                assert in_flight_resp.status_code == 200
                body_v1 = in_flight_resp.json()
                # Both total AND items MUST come consistently from V1 (3 items, V1 name for 102)!
                assert body_v1["total"] == 3
                assert len(body_v1["items"]) == 3
                assert {it["osm_id"]: it["name"] for it in body_v1["items"]} == {
                    101: "Snap Cafe 1",
                    102: "Snap Cafe 2 V1",
                    103: "Snap Cafe 3 V1",
                }

                # Remove test pause hook; a subsequent HTTP request sees complete V2!
                app.state.on_between_queries = None
                next_resp = client.get(endpoint, params=params)
                assert next_resp.status_code == 200
                body_v2 = next_resp.json()
                assert body_v2["total"] == 5
                assert len(body_v2["items"]) == 5
                assert {it["osm_id"]: it["name"] for it in body_v2["items"]} == {
                    101: "Snap Cafe 1",
                    102: "Snap Cafe 2 V2",
                    104: "Snap Cafe 4 V2",
                    105: "Snap Cafe 5 V2",
                    106: "Snap Cafe 6 V2",
                }
        finally:
            engine.dispose()


def test_m44_postgresql_isolation_levels_readonly_transactions_and_write_methods_blocked(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Transactions enforce READ COMMITTED/REPEATABLE READ + READ ONLY; HTTP write verbs return 405."""
    with disposable_database("m44_readonly") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=3, max_overflow=0)
        try:
            # 1. Verify PostgreSQL enforces READ COMMITTED + READ ONLY.
            with read_committed_readonly(engine, statement_timeout_ms=2000) as conn:
                iso = conn.execute(text("SHOW transaction_isolation")).scalar_one()
                ro = conn.execute(text("SHOW transaction_read_only")).scalar_one()
                assert iso == "read committed"
                assert ro == "on"
                with pytest.raises(DBAPIError) as exc_info:
                    conn.execute(
                        text(
                            "INSERT INTO boundary_versions (version_code, geom, source_name, checksum_sha256) "
                            "VALUES ('nope', ST_GeomFromText('MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))', 4326), 'x', repeat('a', 64))"
                        )
                    )
                assert_pg_error(exc_info, sqlstate="25006")

            # 2. Verify PostgreSQL enforces REPEATABLE READ + READ ONLY.
            with repeatable_read_readonly(engine, statement_timeout_ms=2000) as conn:
                iso = conn.execute(text("SHOW transaction_isolation")).scalar_one()
                ro = conn.execute(text("SHOW transaction_read_only")).scalar_one()
                assert iso == "repeatable read"
                assert ro == "on"
                with pytest.raises(DBAPIError) as exc_info:
                    conn.execute(text("DELETE FROM places"))
                assert_pg_error(exc_info, sqlstate="25006")

            # 3. Publish V1 and verify GET/write endpoints never alter M3 publication metadata or places.
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m44-ro")
            pbf = _write_pbf(work_dir / "ro.osm.pbf", [_node(801, name="ReadOnly Cafe")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )

            with engine.connect() as conn:
                before_runs = conn.execute(
                    text(
                        "SELECT id, status, active_slot, publication_operation_id, published_at, publication_evidence "
                        "FROM import_runs ORDER BY id"
                    )
                ).fetchall()
                before_places = conn.execute(
                    text("SELECT id, osm_type, osm_id, name, is_active, updated_at FROM places ORDER BY id")
                ).fetchall()

            app = create_app(engine=engine)
            with TestClient(app) as client:
                assert client.get("/health").status_code == 200
                assert client.get("/api/v1/places").status_code == 200
                assert client.get(
                    "/api/v1/places/nearby", params={"lon": 106.7, "lat": 10.78}
                ).status_code == 200
                assert client.get(f"/api/v1/places/{before_places[0].id}").status_code == 200

                # No mutating HTTP methods exist on public routes.
                for method in ("post", "put", "patch", "delete"):
                    for path in (
                        "/health",
                        "/api/v1/places",
                        "/api/v1/places/nearby",
                        f"/api/v1/places/{before_places[0].id}",
                    ):
                        resp = getattr(client, method)(path)
                        assert resp.status_code == 405, f"{method.upper()} {path}"
                        assert resp.json()["error"]["code"] == "METHOD_NOT_ALLOWED"

            with engine.connect() as conn:
                after_runs = conn.execute(
                    text(
                        "SELECT id, status, active_slot, publication_operation_id, published_at, publication_evidence "
                        "FROM import_runs ORDER BY id"
                    )
                ).fetchall()
                after_places = conn.execute(
                    text("SELECT id, osm_type, osm_id, name, is_active, updated_at FROM places ORDER BY id")
                ).fetchall()
            assert after_runs == before_runs
            assert after_places == before_places
            assert engine.pool.checkedout() == 0
        finally:
            engine.dispose()


def test_m44_database_unavailability_timeout_unexpected_error_and_pool_cleanup(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Verifies safe 503/504/500 responses without secret leakage and zero connection pool leakage."""
    # 1. Database unavailable -> 503 DATABASE_UNAVAILABLE without leaking credentials or SQL.
    secret_pw = "secret_api_db_pw_777"
    dead_engine = create_db_engine(
        f"postgresql+psycopg://dead_user:{secret_pw}@127.0.0.1:1/dead_db",
        pool_size=1,
        max_overflow=0,
        connect_timeout=1,
    )
    try:
        dead_app = create_app(engine=dead_engine, statement_timeout_ms=500)
        with TestClient(dead_app) as client:
            for path, params in [
                ("/api/v1/places", None),
                ("/api/v1/places/nearby", {"lon": 106.7, "lat": 10.78}),
                ("/api/v1/places/1", None),
            ]:
                r = client.get(path, params=params)
                assert r.status_code == 503
                assert r.json() == {
                    "error": {
                        "code": "DATABASE_UNAVAILABLE",
                        "message": "Database service is temporarily unavailable.",
                    }
                }
                assert secret_pw not in r.text
                assert "dead_user" not in r.text
                assert "SELECT" not in r.text
    finally:
        dead_engine.dispose()

    # 2. Real PostgreSQL statement_timeout -> 504 DATABASE_TIMEOUT + clean pool recovery.
    #    Unexpected server exception -> 500 INTERNAL_SERVER_ERROR without leaking traceback.
    with disposable_database("m44_err_safety") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m44-err")
            pbf = _write_pbf(work_dir / "err.osm.pbf", [_node(901, name="Timeout Cafe")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )

            app = create_app(engine=engine, statement_timeout_ms=100)

            def _trigger_pg_timeout(conn: Connection) -> None:
                conn.execute(text("SELECT pg_sleep(0.5)"))

            app.state.on_between_queries = _trigger_pg_timeout
            with TestClient(app, raise_server_exceptions=False) as client:
                r_timeout = client.get("/api/v1/places")
                assert r_timeout.status_code == 504
                assert r_timeout.json() == {
                    "error": {
                        "code": "DATABASE_TIMEOUT",
                        "message": "Database query timed out.",
                    }
                }
                assert "pg_sleep" not in r_timeout.text
                assert "statement_timeout" not in r_timeout.text

                # Pool recovers cleanly and subsequent requests succeed with zero leaked connections.
                app.state.on_between_queries = None
                r_ok = client.get("/api/v1/places")
                assert r_ok.status_code == 200
                assert r_ok.json()["total"] == 1

                # Unexpected runtime error returns masked 500 JSON without leaking internal text.
                def _boom(_conn: Connection) -> None:
                    raise RuntimeError("secret_internal_exception_marker_abc")

                app.state.on_between_queries = _boom
                r_500 = client.get("/api/v1/places")
                assert r_500.status_code == 500
                assert r_500.json() == {
                    "error": {
                        "code": "INTERNAL_SERVER_ERROR",
                        "message": "An unexpected internal error occurred.",
                    }
                }
                assert "secret_internal_exception_marker_abc" not in r_500.text
                assert "Traceback" not in r_500.text

            assert engine.pool.checkedout() == 0
        finally:
            engine.dispose()


def test_m44_test_database_identity_guards_remain_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """M1 fail-closed TEST_DATABASE_URL guards remain active and unaffected by M4."""
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    with pytest.raises(UnsafeDatabaseConfigError, match="TEST_DATABASE_URL"):
        get_test_database_url()

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_dev",
    )
    monkeypatch.setenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_test",
    )
    with pytest.raises(UnsafeDatabaseConfigError, match="same PostgreSQL server"):
        get_test_database_url()

    monkeypatch.setenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://user:test_pw@127.0.0.1:55433/geosearch_prod",
    )
    with pytest.raises(UnsafeDatabaseConfigError, match="unrecognized database"):
        get_test_database_url()


def test_m47_ac1_validation_rejects_invalid_requests_before_database_access() -> None:
    """AC1: Invalid requests across list, nearby, and detail return 422 with zero DB connect attempts."""
    secret_pw = "ac1_secret_pw_never_leak"
    engine = create_db_engine(
        f"postgresql+psycopg://ac1_user:{secret_pw}@127.0.0.1:1/geosearch_unreachable",
        pool_size=1,
        max_overflow=0,
        connect_timeout=1,
    )
    connect_attempts = 0
    original_connect = engine.connect

    def _counting_connect(*args: Any, **kwargs: Any) -> Any:
        nonlocal connect_attempts
        connect_attempts += 1
        return original_connect(*args, **kwargs)

    @event.listens_for(engine, "do_connect")
    def _fail_fast_on_connect(_dialect: Any, _conn_rec: Any, _cargs: Any, _cparams: Any) -> None:
        raise psycopg.OperationalError("Simulated database outage during AC1 test")

    engine.connect = _counting_connect  # type: ignore[method-assign]
    try:
        app = create_app(engine=engine, statement_timeout_ms=500)
        invalid_cases: list[tuple[str, dict[str, Any] | list[tuple[str, str]] | None]] = [
            # List endpoint: invalid pagination, category, oversized q, NUL byte in q.
            ("/api/v1/places", {"page": 0}),
            ("/api/v1/places", {"page": -2}),
            ("/api/v1/places", {"page": "abc"}),
            ("/api/v1/places", {"limit": 0}),
            ("/api/v1/places", {"limit": 101}),
            ("/api/v1/places", {"category": "bar"}),
            ("/api/v1/places", [("category", "cafe"), ("category", "invalid")]),
            ("/api/v1/places", {"q": "x" * 201}),
            ("/api/v1/places", {"q": "bad\x00query"}),
            # Nearby endpoint: non-finite/out-of-range coordinates, invalid radius, category, pagination, NUL q.
            ("/api/v1/places/nearby", {"lon": "nan", "lat": 10.78}),
            ("/api/v1/places/nearby", {"lon": "inf", "lat": 10.78}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": "-inf"}),
            ("/api/v1/places/nearby", {"lon": 180.1, "lat": 10.78}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": -90.1}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "radius_m": 0}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "radius_m": "nan"}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "radius_m": 50000.1}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "category": "invalid"}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "page": 0}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "limit": 101}),
            ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "q": "bad\x00nearby"}),
            # Detail endpoint: zero, negative, non-integer, and BIGINT overflow IDs.
            ("/api/v1/places/0", None),
            ("/api/v1/places/-1", None),
            ("/api/v1/places/not-an-int", None),
            (f"/api/v1/places/{2**63}", None),
        ]

        with TestClient(app) as client:
            for path, params in invalid_cases:
                resp = client.get(path, params=params)
                assert resp.status_code == 422, (
                    f"Expected 422 for {path} params={params!r}, got {resp.status_code}: {resp.text}"
                )
                body = resp.json()
                assert body["error"]["code"] == "VALIDATION_ERROR"
                assert isinstance(body["error"]["details"], list)
                assert len(body["error"]["details"]) >= 1
                assert connect_attempts == 0, (
                    f"Invalid request {path} params={params!r} attempted DB connection ({connect_attempts})"
                )

            # Valid-request controls across all three place endpoints must attempt DB connection
            # and return documented 503 DATABASE_UNAVAILABLE without leaking secrets.
            valid_controls: list[tuple[str, dict[str, Any] | None]] = [
                ("/api/v1/places", None),
                ("/api/v1/places/nearby", {"lon": 106.70, "lat": 10.78, "radius_m": 1000}),
                ("/api/v1/places/1", None),
            ]
            for expected_attempts, (path, params) in enumerate(valid_controls, start=1):
                resp = client.get(path, params=params)
                assert resp.status_code == 503
                assert resp.json() == {
                    "error": {
                        "code": "DATABASE_UNAVAILABLE",
                        "message": "Database service is temporarily unavailable.",
                    }
                }
                assert secret_pw not in resp.text
                assert "ac1_user" not in resp.text
                assert connect_attempts == expected_attempts
    finally:
        engine.dispose()


def test_m47_ac3_no_active_publication_returns_empty_and_hides_orphan_active_place(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """AC3: Pre-publication nearby is empty, and an orphan active place without active_slot=1 is hidden."""
    with disposable_database("m47_ac3_no_pub") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            app = create_app(engine=engine)
            with TestClient(app) as client:
                # Before any publication or rows exist: nearby returns 200 with empty items and total=0.
                r_pre_nearby = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 106.7009, "lat": 10.7769, "radius_m": 5000},
                )
                assert r_pre_nearby.status_code == 200
                pre_body = r_pre_nearby.json()
                assert pre_body["items"] == []
                assert pre_body["total"] == 0
                assert pre_body["has_more"] is False

                # Insert a schema-valid places(is_active=true) row tied to an unpublished PREPARED run
                # (zero rows in import_runs hold active_slot=1; all M1/M3 constraints remain intact).
                with engine.begin() as conn:
                    bv_id = insert_boundary_version(conn, version_code="bv-m47-ac3")
                    run_id = insert_import_run(conn, bv_id, status="PREPARED", active_slot=None)
                    orphan_place_id = insert_place(
                        conn,
                        run_id,
                        osm_id=7701,
                        name="Orphan Active Cafe",
                        name_search="orphan active cafe",
                        category="cafe",
                        lon=106.7009,
                        lat=10.7769,
                        is_active=True,
                    )

                with engine.connect() as conn:
                    active_pub_count = conn.execute(
                        text("SELECT count(*) FROM import_runs WHERE active_slot = 1")
                    ).scalar_one()
                    orphan_is_active = conn.execute(
                        text("SELECT is_active FROM places WHERE id = :pid"),
                        {"pid": orphan_place_id},
                    ).scalar_one()
                    assert active_pub_count == 0
                    assert orphan_is_active is True

                # List, nearby, and detail must all hide the orphan row when no publication is active.
                r_list = client.get("/api/v1/places")
                assert r_list.status_code == 200
                assert r_list.json()["items"] == []
                assert r_list.json()["total"] == 0

                r_nearby = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 106.7009, "lat": 10.7769, "radius_m": 5000},
                )
                assert r_nearby.status_code == 200
                assert r_nearby.json()["items"] == []
                assert r_nearby.json()["total"] == 0

                r_detail = client.get(f"/api/v1/places/{orphan_place_id}")
                assert r_detail.status_code == 404
                assert r_detail.json() == {
                    "error": {"code": "PLACE_NOT_FOUND", "message": "Place not found."}
                }
        finally:
            engine.dispose()
