"""M5 AC2 & AC3 tests: End-to-end V1->V2->Restore V1 recovery, provenance, history, and retries."""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import osmium
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Connection, Engine, text

from geosearch.api import create_app
from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    LostCommitAcknowledgementError,
    PublicationConflictError,
    PublicationFaultHooks,
    PublicationMode,
    PublicationOutcomeStatus,
    RecoveryFaultHooks,
    build_recovery_approval_from_artifact,
    execute_controlled_recovery,
    prepare_recovery_artifact,
    publish_prepared_run,
    resolve_unknown_commit_outcome,
    verify_recovery_artifact,
)
from tests.helpers import insert_boundary_version, upgrade_test_db_to_head


class _InjectedRecoveryFault(RuntimeError):
    """Deterministic test fault injected prior to recovery COMMIT."""


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
    name: str | None,
    lon: float,
    lat: float,
    category_tag: tuple[str, str] = ("amenity", "cafe"),
    version: int = 1,
    changeset: int = 100,
    address: str | None = None,
    extra_tags: dict[str, str] | None = None,
) -> osmium.osm.mutable.Node:
    tags: dict[str, str] = {category_tag[0]: category_tag[1]}
    if name is not None:
        tags["name"] = name
    if address is not None:
        tags["addr:full"] = address
    if extra_tags:
        tags.update(extra_tags)
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=version,
        changeset=changeset,
        timestamp=f"2026-01-{10 + version:02d}T00:00:00Z",
        location=(lon, lat),
        tags=tags,
    )


def _dir_tree_hashes(directory: Path) -> dict[str, str]:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.iterdir())
        if p.is_file()
    }


def _fetch_all_places_by_osm_id(engine: Engine) -> dict[int, dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    id,
                    osm_type,
                    osm_id,
                    name,
                    name_search,
                    category,
                    address,
                    ST_X(location::geometry) AS lon,
                    ST_Y(location::geometry) AS lat,
                    raw_tags,
                    last_seen_import_id,
                    is_active,
                    osm_version,
                    osm_changeset,
                    osm_timestamp
                FROM places
                ORDER BY osm_id
                """
            )
        ).fetchall()
    return {
        int(r.osm_id): {
            "id": int(r.id),
            "osm_type": str(r.osm_type),
            "osm_id": int(r.osm_id),
            "name": r.name,
            "name_search": r.name_search,
            "category": str(r.category),
            "address": r.address,
            "lon": float(r.lon),
            "lat": float(r.lat),
            "raw_tags": dict(r.raw_tags),
            "last_seen_import_id": int(r.last_seen_import_id),
            "is_active": bool(r.is_active),
            "osm_version": int(r.osm_version) if r.osm_version is not None else None,
            "osm_changeset": int(r.osm_changeset) if r.osm_changeset is not None else None,
            "osm_timestamp": r.osm_timestamp,
        }
        for r in rows
    }


def test_m5_ac2_restore_v1_while_v2_active_restores_canonical_content_boundary_and_ids(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC2: Publish V1 -> Publish V2 -> Restore V1 verifies all 4 POI cases, boundary, and artifacts."""
    with disposable_database("m5_ac2_e2e") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            # Distinct APPROVED boundaries for V1 and V2 so active boundary restoration is proven.
            bv_v1_wkt = "MULTIPOLYGON(((106.60 10.70, 106.80 10.70, 106.80 10.90, 106.60 10.90, 106.60 10.70)))"
            bv_v2_wkt = "MULTIPOLYGON(((106.55 10.65, 106.85 10.65, 106.85 10.95, 106.55 10.95, 106.55 10.65)))"
            with engine.begin() as conn:
                bv_v1 = insert_boundary_version(
                    conn,
                    version_code="bv-m5-v1",
                    geom_wkt=bv_v1_wkt,
                    checksum_sha256="1" * 64,
                )
                bv_v2 = insert_boundary_version(
                    conn,
                    version_code="bv-m5-v2",
                    geom_wkt=bv_v2_wkt,
                    checksum_sha256="2" * 64,
                )

            # V1 dataset:
            # - 101: Unchanged between V1 and V2
            # - 102: Changed in V2 (name, address, coordinates, tags, version)
            # - 103: V1-only (deactivated in V2, reactivated in Recovery)
            # - 105: Unnamed ATM in V1 (unchanged in V2)
            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    _node(
                        101,
                        name="Cà Phê Không Đổi",
                        lon=106.7001,
                        lat=10.7701,
                        address="10 Đồng Khởi",
                        extra_tags={"wifi": "yes"},
                    ),
                    _node(
                        102,
                        name="Quán Phở Bản V1",
                        lon=106.7010,
                        lat=10.7710,
                        category_tag=("amenity", "restaurant"),
                        version=1,
                        changeset=101,
                        address="20 Lê Lợi",
                        extra_tags={"cuisine": "vietnamese"},
                    ),
                    _node(
                        103,
                        name="Nhà Thuốc Chỉ V1",
                        lon=106.7020,
                        lat=10.7720,
                        category_tag=("amenity", "pharmacy"),
                        version=1,
                        changeset=102,
                        address="30 Pasteur",
                    ),
                    _node(
                        105,
                        name=None,
                        lon=106.7025,
                        lat=10.7725,
                        category_tag=("amenity", "atm"),
                        version=1,
                        changeset=103,
                    ),
                ],
            )
            # V2 dataset:
            # - 101: identical to V1
            # - 102: mutated in V2
            # - 104: V2-only (must be deactivated softly on V1 restore)
            # - 105: identical unnamed ATM
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    _node(
                        101,
                        name="Cà Phê Không Đổi",
                        lon=106.7001,
                        lat=10.7701,
                        address="10 Đồng Khởi",
                        extra_tags={"wifi": "yes"},
                    ),
                    _node(
                        102,
                        name="Quán Phở Bị Lỗi V2",
                        lon=106.7099,
                        lat=10.7799,
                        category_tag=("amenity", "restaurant"),
                        version=2,
                        changeset=202,
                        address="999 Đường Mới",
                        extra_tags={"cuisine": "fusion", "bad_import": "true"},
                    ),
                    _node(
                        104,
                        name="Siêu Thị Chỉ V2",
                        lon=106.7040,
                        lat=10.7740,
                        category_tag=("shop", "supermarket"),
                        version=1,
                        changeset=204,
                        address="40 Nguyễn Huệ",
                    ),
                    _node(
                        105,
                        name=None,
                        lon=106.7025,
                        lat=10.7725,
                        category_tag=("amenity", "atm"),
                        version=1,
                        changeset=103,
                    ),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_v1)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_v2)

            # 1. Publish V1 and capture immutable V1 artifact.
            op_v1 = uuid.uuid4()
            pub_v1 = publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=op_v1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )
            assert pub_v1.active_place_count == 4
            v1_places_original = _fetch_all_places_by_osm_id(engine)
            assert set(v1_places_original.keys()) == {101, 102, 103, 105}

            v1_artifact = prepare_recovery_artifact(
                engine,
                artifact_root=work_dir / "v1_artifacts",
                artifact_name="v1_immutable_source",
            )
            assert v1_artifact is not None
            v1_artifact_hashes_before = _dir_tree_hashes(v1_artifact.artifact_dir)

            # 2. Publish V2 over V1 (using V1 artifact as pre-publication protection).
            op_v2 = uuid.uuid4()
            pub_v2 = publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=op_v2,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=ing_v1.import_run.id,
                recovery_artifact_dir=v1_artifact.artifact_dir,
            )
            assert pub_v2.active_place_count == 4
            v2_places = _fetch_all_places_by_osm_id(engine)
            assert v2_places[103]["is_active"] is False
            assert v2_places[104]["is_active"] is True
            assert v2_places[102]["name"] == "Quán Phở Bị Lỗi V2"

            # 3. Manually approve restoring V1 artifact while V2 is active.
            op_recovery = uuid.uuid4()
            approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_recovery,
                expected_active_run_id=ing_v2.import_run.id,
                approved_by="oncall-sre@geosearch",
                approval_reason="INC-2026-042: Roll back corrupt V2 restaurant update",
            )

            # 4. Execute controlled recovery (V1 source artifact + auto V2 protection artifact).
            v2_protection_root = work_dir / "v2_protection_artifacts"
            rec_res = execute_controlled_recovery(
                engine,
                source_artifact_dir=v1_artifact.artifact_dir,
                publication_operation_id=op_recovery,
                approval=approval,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                protection_artifact_root=v2_protection_root,
            )

            assert rec_res.outcome == PublicationOutcomeStatus.PUBLISHED
            assert rec_res.already_committed is False
            assert rec_res.reused_prepared_run is False
            rec_run_id = rec_res.recovery_import_run_id
            assert rec_run_id not in (ing_v1.import_run.id, ing_v2.import_run.id)
            assert rec_res.publication_operation_id == op_recovery

            # Distinct roles of V1 source artifact and V2 protection artifact verified!
            assert rec_res.source_artifact.source_import_run_id == ing_v1.import_run.id
            assert rec_res.protection_artifact is not None
            assert rec_res.protection_artifact.source_import_run_id == ing_v2.import_run.id
            assert rec_res.protection_artifact.artifact_dir != v1_artifact.artifact_dir
            verify_recovery_artifact(
                rec_res.protection_artifact.artifact_dir,
                expected_source_run_id=ing_v2.import_run.id,
            )
            # Source V1 artifact preserved byte-for-byte untouched.
            assert _dir_tree_hashes(v1_artifact.artifact_dir) == v1_artifact_hashes_before

            # 5. Verify complete live places state after real COMMIT.
            after = _fetch_all_places_by_osm_id(engine)
            # No hard deletions: all 5 historical OSM identities remain in `places`.
            assert set(after.keys()) == {101, 102, 103, 104, 105}

            active_osm_ids = {oid for oid, row in after.items() if row["is_active"]}
            assert active_osm_ids == {101, 102, 103, 105}

            # Every restored V1 POI (including unchanged 101 & 105, changed 102, reactivated 103)
            # preserves its exact stable places.id, canonical V1 content, and sets
            # last_seen_import_id = rec_run_id.
            for oid in (101, 102, 103, 105):
                orig = v1_places_original[oid]
                restored = after[oid]
                assert restored["id"] == orig["id"]
                assert restored["is_active"] is True
                assert restored["last_seen_import_id"] == rec_run_id
                assert restored["name"] == orig["name"]
                assert restored["name_search"] == orig["name_search"]
                assert restored["category"] == orig["category"]
                assert restored["address"] == orig["address"]
                assert restored["lon"] == orig["lon"]
                assert restored["lat"] == orig["lat"]
                assert restored["raw_tags"] == orig["raw_tags"]
                assert restored["osm_version"] == orig["osm_version"]
                assert restored["osm_changeset"] == orig["osm_changeset"]
                assert restored["osm_timestamp"] == orig["osm_timestamp"]

            # V2-only POI (104) is softly deactivated with stable ID and V2 last_seen_import_id.
            assert after[104]["id"] == v2_places[104]["id"]
            assert after[104]["is_active"] is False
            assert after[104]["last_seen_import_id"] == ing_v2.import_run.id

            # Verify active run, active boundary (restored to bv_v1!), and historical runs.
            with engine.connect() as conn:
                runs = {
                    int(r.id): r
                    for r in conn.execute(
                        text(
                            """
                            SELECT id, run_type, status, active_slot, boundary_version_id,
                                   publication_operation_id, published_at, superseded_at,
                                   publication_evidence
                            FROM import_runs
                            ORDER BY id
                            """
                        )
                    ).fetchall()
                }
            assert len(runs) == 3
            assert [rid for rid, r in runs.items() if r.active_slot == 1] == [rec_run_id]

            r_v1 = runs[ing_v1.import_run.id]
            r_v2 = runs[ing_v2.import_run.id]
            r_rec = runs[rec_run_id]

            assert r_v1.status == "SUPERSEDED"
            assert r_v1.active_slot is None
            assert r_v1.publication_operation_id == op_v1
            assert r_v1.published_at == pub_v1.published_at
            assert r_v1.superseded_at is not None

            assert r_v2.status == "SUPERSEDED"
            assert r_v2.active_slot is None
            assert r_v2.publication_operation_id == op_v2
            assert r_v2.published_at == pub_v2.published_at
            assert r_v2.superseded_at is not None

            assert r_rec.run_type == "RECOVERY"
            assert r_rec.status == "PUBLISHED"
            assert r_rec.active_slot == 1
            assert r_rec.boundary_version_id == bv_v1
            assert r_rec.publication_operation_id == op_recovery
            assert r_rec.published_at is not None
            assert r_rec.superseded_at is None
            assert r_rec.publication_evidence["recovery"]["source_artifact"]["source_import_run_id"] == ing_v1.import_run.id
            assert r_rec.publication_evidence["recovery"]["approval"]["approved_by"] == "oncall-sre@geosearch"
            assert r_rec.publication_evidence["recovery_artifact"]["source_import_run_id"] == ing_v2.import_run.id

            # Verify read-only Search API serves restored V1 content and hides deactivated V2-only POI.
            app = create_app(engine=engine)
            with TestClient(app) as client:
                list_resp = client.get("/api/v1/places", params={"limit": 20})
                assert list_resp.status_code == 200
                list_body = list_resp.json()
                assert list_body["total"] == 4
                assert {it["osm_id"]: it["name"] for it in list_body["items"]} == {
                    101: "Cà Phê Không Đổi",
                    102: "Quán Phở Bản V1",
                    103: "Nhà Thuốc Chỉ V1",
                    105: None,
                }
                assert client.get(f"/api/v1/places/{after[103]['id']}").status_code == 200
                assert client.get(f"/api/v1/places/{after[104]['id']}").status_code == 404
        finally:
            engine.dispose()


def test_m5_ac3_recovery_retry_anti_repurposing_lost_commit_ack_and_retry_after_supersession(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC3: Normal retry, UUID anti-repurposing, lost COMMIT ack resolution, and post-supersession retry."""
    with disposable_database("m5_ac3_retry") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m5-ac3")

            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    _node(201, name="Cafe V1 A", lon=106.701, lat=10.777),
                    _node(202, name="Cafe V1 B", lon=106.702, lat=10.778),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    _node(201, name="Cafe V2 A", lon=106.701, lat=10.777, version=2),
                    _node(203, name="Cafe V2 C", lon=106.703, lat=10.779),
                ],
            )
            pbf_v3 = _write_pbf(
                work_dir / "v3.osm.pbf",
                [
                    _node(201, name="Cafe V3 A", lon=106.701, lat=10.777, version=3),
                    _node(204, name="Cafe V3 D", lon=106.704, lat=10.780),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)
            ing_v3 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v3, boundary_version_id=bv_id)

            op_v1 = uuid.uuid4()
            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=op_v1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )
            v1_artifact = prepare_recovery_artifact(
                engine, artifact_root=work_dir / "artifacts", artifact_name="v1_src"
            )
            assert v1_artifact is not None

            op_v2 = uuid.uuid4()
            publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=op_v2,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=ing_v1.import_run.id,
                recovery_artifact_dir=v1_artifact.artifact_dir,
            )

            # 1. Refusing to repurpose an existing IMPORT operation UUID for recovery.
            with pytest.raises(PublicationConflictError, match="non-RECOVERY ImportRun"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_v1,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_v1,
                        expected_active_run_id=ing_v2.import_run.id,
                        approved_by="ops-a",
                        approval_reason="bad reuse",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "prot",
                )

            # 2. Recovery Attempt 1 prepares recovery run, then fails pre-COMMIT.
            op_rec = uuid.uuid4()
            approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_rec,
                expected_active_run_id=ing_v2.import_run.id,
                approved_by="ops-lead@geosearch",
                approval_reason="INC-RETRY-1",
            )

            def _fail_before_commit(_conn: Connection) -> None:
                raise _InjectedRecoveryFault("Simulated crash before recovery COMMIT")

            with pytest.raises(_InjectedRecoveryFault):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_rec,
                    approval=approval,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "prot_attempt1",
                    fault_hooks=RecoveryFaultHooks(
                        publication_hooks=PublicationFaultHooks(
                            on_before_commit=_fail_before_commit
                        )
                    ),
                )

            # Exactly one PREPARED RECOVERY run exists bound to op_rec; V2 still active.
            with engine.connect() as conn:
                rec_runs = conn.execute(
                    text("SELECT id, status FROM import_runs WHERE run_type = 'RECOVERY'")
                ).fetchall()
                active_id = conn.execute(
                    text("SELECT id FROM import_runs WHERE active_slot = 1")
                ).scalar_one()
            assert len(rec_runs) == 1
            assert rec_runs[0].status == "PREPARED"
            prepared_rec_id = int(rec_runs[0].id)
            assert int(active_id) == ing_v2.import_run.id

            # 3. Attempting to repurpose op_rec with a DIFFERENT approval (different approved_by) fails!
            mismatched_approval = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_rec,
                expected_active_run_id=ing_v2.import_run.id,
                approved_by="different-approver@geosearch",
                approval_reason="INC-RETRY-1",
                approved_at=approval.approved_at,
            )
            with pytest.raises(PublicationConflictError, match="Refusing to repurpose"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_rec,
                    approval=mismatched_approval,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "prot_repurpose",
                )

            # 4. Retry Attempt 2 with exact same op_rec & approval reuses the PREPARED run,
            #    COMMITS durably, then loses COMMIT acknowledgement!
            def _lose_ack(run_id: int, op_uuid: uuid.UUID) -> None:
                raise LostCommitAcknowledgementError(
                    "Simulated network drop after COMMIT",
                    import_run_id=run_id,
                    publication_operation_id=op_uuid,
                )

            with pytest.raises(LostCommitAcknowledgementError) as exc_ack:
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_rec,
                    approval=approval,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "prot_attempt2",
                    fault_hooks=RecoveryFaultHooks(
                        publication_hooks=PublicationFaultHooks(
                            on_after_commit_before_ack=_lose_ack
                        )
                    ),
                )
            assert exc_ack.value.import_run_id == prepared_rec_id
            assert exc_ack.value.publication_operation_id == op_rec

            # Still only ONE RECOVERY run exists — retry did not create a second run!
            with engine.connect() as conn:
                rec_run_count = int(
                    conn.execute(
                        text("SELECT count(*) FROM import_runs WHERE run_type = 'RECOVERY'")
                    ).scalar_one()
                )
            assert rec_run_count == 1

            # 5. Resolve unknown COMMIT outcome via durable operation UUID and published_at evidence.
            durable_ev = resolve_unknown_commit_outcome(
                engine, publication_operation_id=op_rec, import_run_id=prepared_rec_id
            )
            assert durable_ev is not None
            assert durable_ev.is_committed_publication is True
            assert durable_ev.status == "PUBLISHED"
            assert durable_ev.active_slot == 1
            assert durable_ev.published_at is not None
            assert durable_ev.publication_evidence is not None
            assert durable_ev.publication_evidence["recovery"]["source_artifact"]["source_import_run_id"] == ing_v1.import_run.id

            # 6. Retrying committed recovery returns ALREADY_PUBLISHED without republishing.
            retry_committed = execute_controlled_recovery(
                engine,
                source_artifact_dir=v1_artifact.artifact_dir,
                publication_operation_id=op_rec,
                approval=approval,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                protection_artifact_root=work_dir / "prot_retry_committed",
            )
            assert retry_committed.outcome == PublicationOutcomeStatus.ALREADY_PUBLISHED
            assert retry_committed.already_committed is True
            assert retry_committed.recovery_import_run_id == prepared_rec_id
            assert retry_committed.publication_result.published_at == durable_ev.published_at

            # 7. Supersede the recovery run with V3, then retry op_rec again ->
            #    MUST NOT reactivate the superseded recovery run!
            op_v3 = uuid.uuid4()
            pub_v3 = publish_prepared_run(
                engine,
                import_run_id=ing_v3.import_run.id,
                publication_operation_id=op_v3,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=prepared_rec_id,
                recovery_artifact_root=work_dir / "prot_v3",
            )
            assert pub_v3.outcome == PublicationOutcomeStatus.PUBLISHED

            retry_after_supersession = execute_controlled_recovery(
                engine,
                source_artifact_dir=v1_artifact.artifact_dir,
                publication_operation_id=op_rec,
                approval=approval,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                protection_artifact_root=work_dir / "prot_retry_superseded",
            )
            assert retry_after_supersession.outcome == PublicationOutcomeStatus.ALREADY_PUBLISHED
            assert retry_after_supersession.already_committed is True
            assert retry_after_supersession.recovery_import_run_id == prepared_rec_id

            # V3 remains strictly active; V1, V2, and RECOVERY run all remain SUPERSEDED with
            # original UUIDs and published_at preserved!
            with engine.connect() as conn:
                active_rows = conn.execute(
                    text("SELECT id, publication_operation_id FROM import_runs WHERE active_slot = 1")
                ).fetchall()
                rec_row = conn.execute(
                    text(
                        "SELECT status, active_slot, publication_operation_id, published_at, "
                        "superseded_at FROM import_runs WHERE id = :rid"
                    ),
                    {"rid": prepared_rec_id},
                ).one()
            assert len(active_rows) == 1
            assert int(active_rows[0].id) == ing_v3.import_run.id
            assert active_rows[0].publication_operation_id == op_v3
            assert rec_row.status == "SUPERSEDED"
            assert rec_row.active_slot is None
            assert rec_row.publication_operation_id == op_rec
            assert rec_row.published_at == durable_ev.published_at
            assert rec_row.superseded_at is not None

            live_v3 = _fetch_all_places_by_osm_id(engine)
            assert {oid for oid, p in live_v3.items() if p["is_active"]} == {201, 204}
            assert live_v3[201]["name"] == "Cafe V3 A"
        finally:
            engine.dispose()
