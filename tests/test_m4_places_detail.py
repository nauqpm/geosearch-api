"""M4.2 tests (Group B): GET /api/v1/places/{id} detail, active semantics, and schema hygiene."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import osmium
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from geosearch.api import create_app
from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import PublicationMode, PublicationOutcomeStatus, publish_prepared_run
from tests.helpers import insert_boundary_version, upgrade_test_db_to_head


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
    tags: dict[str, str],
    lon: float = 106.7009,
    lat: float = 10.7769,
    version: int = 1,
    changeset: int = 100,
) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=version,
        changeset=changeset,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags=tags,
    )


def test_m42_place_detail_active_unnamed_atm_inactive_staging_only_and_no_leak(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Active detail returns exact public schema; unnamed ATM keeps nulls; inactive & staging-only return 404."""
    with disposable_database("m42_detail") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m42-detail")

            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    _node(
                        1001,
                        lon=106.7011,
                        lat=10.7771,
                        version=3,
                        tags={
                            "amenity": "cafe",
                            "name": "Cà Phê Sài Gòn",
                            "addr:housenumber": "12",
                            "addr:street": "Lê Lợi",
                            "addr:district": "Quận 1",
                            "secret_internal_tag": "must-not-appear",
                        },
                    ),
                    _node(
                        1002,
                        lon=106.7022,
                        lat=10.7782,
                        version=1,
                        tags={"amenity": "atm"},  # Unnamed ATM with no address
                    ),
                    _node(
                        1003,
                        lon=106.7033,
                        lat=10.7793,
                        version=1,
                        tags={"amenity": "pharmacy", "name": "Nhà Thuốc Cũ"},
                    ),
                ],
            )
            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)

            app = create_app(engine=engine)
            with TestClient(app) as client:
                # Before publication: staging-only rows are never exposed (even by guessing ID 1).
                resp_pre = client.get("/api/v1/places/1")
                assert resp_pre.status_code == 404
                assert resp_pre.json() == {
                    "error": {"code": "PLACE_NOT_FOUND", "message": "Place not found."}
                }

                # Publish V1 via controlled test publication.
                res_v1 = publish_prepared_run(
                    engine,
                    import_run_id=ing_v1.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=None,
                )
                assert res_v1.outcome == PublicationOutcomeStatus.PUBLISHED

                with engine.connect() as conn:
                    ids_by_osm = {
                        int(r.osm_id): int(r.id)
                        for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                    }

                # Active named cafe with address.
                resp_cafe = client.get(f"/api/v1/places/{ids_by_osm[1001]}")
                assert resp_cafe.status_code == 200
                body_cafe = resp_cafe.json()
                assert set(body_cafe.keys()) == {
                    "id",
                    "osm_type",
                    "osm_id",
                    "name",
                    "category",
                    "address",
                    "longitude",
                    "latitude",
                    "osm_version",
                    "osm_timestamp",
                }
                assert body_cafe["id"] == ids_by_osm[1001]
                assert body_cafe["osm_type"] == "node"
                assert body_cafe["osm_id"] == 1001
                assert body_cafe["name"] == "Cà Phê Sài Gòn"
                assert body_cafe["category"] == "cafe"
                assert body_cafe["address"] == "12 Lê Lợi, Quận 1"
                assert body_cafe["longitude"] == pytest.approx(106.7011, abs=1e-6)
                assert body_cafe["latitude"] == pytest.approx(10.7771, abs=1e-6)
                assert body_cafe["osm_version"] == 3
                assert body_cafe["osm_timestamp"] is not None
                # Internal columns must NEVER leak.
                for forbidden in (
                    "raw_tags",
                    "name_search",
                    "is_active",
                    "last_seen_import_id",
                    "osm_changeset",
                    "created_at",
                    "updated_at",
                    "secret_internal_tag",
                ):
                    assert forbidden not in resp_cafe.text

                # Active unnamed ATM preserves null name and null address.
                resp_atm = client.get(f"/api/v1/places/{ids_by_osm[1002]}")
                assert resp_atm.status_code == 200
                body_atm = resp_atm.json()
                assert body_atm["id"] == ids_by_osm[1002]
                assert body_atm["category"] == "atm"
                assert body_atm["name"] is None
                assert body_atm["address"] is None

                # Nonexistent ID returns 404.
                resp_missing = client.get("/api/v1/places/999999")
                assert resp_missing.status_code == 404
                assert resp_missing.json()["error"]["code"] == "PLACE_NOT_FOUND"

                # Publish V2 dropping 1003 (deactivated) and adding 1004.
                pbf_v2 = _write_pbf(
                    work_dir / "v2.osm.pbf",
                    [
                        _node(
                            1001,
                            lon=106.7011,
                            lat=10.7771,
                            version=4,
                            tags={"amenity": "cafe", "name": "Cà Phê Sài Gòn V2"},
                        ),
                        _node(1002, lon=106.7022, lat=10.7782, tags={"amenity": "atm"}),
                        _node(
                            1004,
                            lon=106.7044,
                            lat=10.7804,
                            tags={"shop": "supermarket", "name": "Siêu Thị Mới"},
                        ),
                    ],
                )
                ing_v2 = ingest_osm_nodes_to_staging(
                    engine, source_path=pbf_v2, boundary_version_id=bv_id
                )
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=ing_v1.import_run.id,
                    recovery_artifact_root=work_dir / "artifacts",
                )

                # Deactivated 1003 now returns 404.
                resp_deactivated = client.get(f"/api/v1/places/{ids_by_osm[1003]}")
                assert resp_deactivated.status_code == 404

                # Stable ID preserved for 1001 with updated name.
                resp_v2_cafe = client.get(f"/api/v1/places/{ids_by_osm[1001]}")
                assert resp_v2_cafe.status_code == 200
                assert resp_v2_cafe.json()["name"] == "Cà Phê Sài Gòn V2"
                assert resp_v2_cafe.json()["osm_version"] == 4

                # Publish V3 reactivating 1003 — stable internal ID is preserved and visible again!
                pbf_v3 = _write_pbf(
                    work_dir / "v3.osm.pbf",
                    [
                        _node(1001, lon=106.7011, lat=10.7771, tags={"amenity": "cafe", "name": "Cà Phê Sài Gòn V2"}),
                        _node(1003, lon=106.7033, lat=10.7793, version=2, tags={"amenity": "pharmacy", "name": "Nhà Thuốc Tái Hoạt Động"}),
                    ],
                )
                ing_v3 = ingest_osm_nodes_to_staging(
                    engine, source_path=pbf_v3, boundary_version_id=bv_id
                )
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v3.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=ing_v2.import_run.id,
                    recovery_artifact_root=work_dir / "artifacts",
                )

                resp_reactivated = client.get(f"/api/v1/places/{ids_by_osm[1003]}")
                assert resp_reactivated.status_code == 200
                assert resp_reactivated.json()["id"] == ids_by_osm[1003]
                assert resp_reactivated.json()["name"] == "Nhà Thuốc Tái Hoạt Động"
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    "raw_id",
    ["0", "-1", "-999", "abc", "1.5", str(2**63)],
)
def test_m42_place_detail_rejects_invalid_ids_with_422(
    migrated_test_engine,
    raw_id: str,
) -> None:
    """Zero, negative, non-integer, and BIGINT-overflow IDs return structured 422 validation errors."""
    app = create_app(engine=migrated_test_engine)
    with TestClient(app) as client:
        resp = client.get(f"/api/v1/places/{raw_id}")
        assert resp.status_code == 422
        body = resp.json()
        assert body["error"]["code"] == "VALIDATION_ERROR"
        assert isinstance(body["error"]["details"], list)
        assert len(body["error"]["details"]) >= 1


def test_m47_ac2_staged_unpublished_v2_remains_invisible_while_v1_active(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC2: A PREPARED unpublished V2 run in staging never leaks into public API while V1 is active."""
    with disposable_database("m47_ac2_staged") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m47-ac2")

            pbf_v1 = _write_pbf(
                work_dir / "ac2_v1.osm.pbf",
                [
                    _node(1001, lon=106.701, lat=10.777, tags={"amenity": "cafe", "name": "Active Cafe V1"}),
                    _node(1002, lon=106.702, lat=10.778, tags={"amenity": "pharmacy", "name": "Active Pharmacy V1"}),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "ac2_v2.osm.pbf",
                [
                    _node(1001, lon=106.701, lat=10.777, tags={"amenity": "cafe", "name": "Active Cafe V1"}),
                    _node(1002, lon=106.702, lat=10.778, tags={"amenity": "pharmacy", "name": "Active Pharmacy V1"}),
                    _node(1005, lon=106.7015, lat=10.7775, tags={"amenity": "cafe", "name": "Staged Only Cafe V2"}),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            res_v1 = publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )
            assert res_v1.outcome == PublicationOutcomeStatus.PUBLISHED

            # Stage and verify V2 as PREPARED, but do NOT publish it.
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

            with engine.connect() as conn:
                v2_run = conn.execute(
                    text("SELECT status, active_slot FROM import_runs WHERE id = :run_id"),
                    {"run_id": ing_v2.import_run.id},
                ).one()
                assert v2_run.status == "PREPARED"
                assert v2_run.active_slot is None

                staged_v2_osm_ids = set(
                    conn.execute(
                        text("SELECT osm_id FROM staging_place_candidates WHERE import_run_id = :run_id"),
                        {"run_id": ing_v2.import_run.id},
                    ).scalars().all()
                )
                assert staged_v2_osm_ids == {1001, 1002, 1005}

                # Staged-only identity 1005 has NO row and NO internal places.id in canonical places.
                live_ids_by_osm = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                }
                assert set(live_ids_by_osm.keys()) == {1001, 1002}
                assert 1005 not in live_ids_by_osm

            app = create_app(engine=engine)
            with TestClient(app) as client:
                r_list = client.get("/api/v1/places")
                assert r_list.status_code == 200
                list_body = r_list.json()
                assert list_body["total"] == 2
                assert {item["osm_id"] for item in list_body["items"]} == {1001, 1002}
                assert "Staged Only Cafe V2" not in r_list.text

                r_nearby = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 106.701, "lat": 10.777, "radius_m": 5000},
                )
                assert r_nearby.status_code == 200
                nearby_body = r_nearby.json()
                assert nearby_body["total"] == 2
                assert {item["osm_id"] for item in nearby_body["items"]} == {1001, 1002}
                assert "Staged Only Cafe V2" not in r_nearby.text

                r_detail = client.get(f"/api/v1/places/{live_ids_by_osm[1001]}")
                assert r_detail.status_code == 200
                assert r_detail.json()["id"] == live_ids_by_osm[1001]
                assert r_detail.json()["osm_id"] == 1001
                assert r_detail.json()["name"] == "Active Cafe V1"
        finally:
            engine.dispose()
