"""M5 AC1 tests: Source artifact verification, TOCTOU protection, and mandatory manual approval."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import stat
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import osmium
import pytest
from sqlalchemy import Engine, text

from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    MANIFEST_CHECKSUM_FILENAME,
    MANIFEST_FILENAME,
    PLACES_SNAPSHOT_FILENAME,
    EligibilityStatus,
    PublicationConflictError,
    PublicationEligibilityError,
    PublicationMode,
    RecoveryApproval,
    RecoveryApprovalError,
    RecoveryArtifactError,
    RecoveryArtifactRecord,
    RecoveryFaultHooks,
    build_recovery_approval_from_artifact,
    execute_controlled_recovery,
    prepare_recovery_artifact,
    prepare_recovery_run,
    publish_prepared_run,
    verify_historical_source_artifact,
)
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
    name: str,
    lon: float = 106.701,
    lat: float = 10.777,
    category_tag: tuple[str, str] = ("amenity", "cafe"),
    version: int = 1,
    extra_tags: dict[str, str] | None = None,
) -> osmium.osm.mutable.Node:
    tags = {category_tag[0]: category_tag[1], "name": name, **(extra_tags or {})}
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=version,
        changeset=100 + version,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags=tags,
    )


def _make_writable(path: Path) -> None:
    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)


def _copy_writable_artifact(src_dir: Path, dst_dir: Path) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=False)
    for child in src_dir.iterdir():
        target = dst_dir / child.name
        shutil.copyfile(child, target)
        _make_writable(target)
    return dst_dir


def _recompute_manifest_digests(artifact_dir: Path, manifest: dict[str, Any]) -> None:
    core = {
        "artifact_format": manifest["artifact_format"],
        "source_publication": manifest["source_publication"],
        "boundary_version": manifest["boundary_version"],
        "membership_summary": manifest["membership_summary"],
        "files": manifest["files"],
    }
    manifest["content_sha256"] = hashlib.sha256(
        json.dumps(core, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    manifest_bytes = json.dumps(
        manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    (artifact_dir / MANIFEST_FILENAME).write_bytes(manifest_bytes)
    (artifact_dir / MANIFEST_CHECKSUM_FILENAME).write_bytes(
        f"{manifest_sha}  {MANIFEST_FILENAME}\n".encode("utf-8")
    )


def _snapshot_live_state(engine: Engine) -> dict[str, Any]:
    with engine.connect() as conn:
        active_runs = [
            (int(r.id), str(r.status), str(r.publication_operation_id))
            for r in conn.execute(
                text(
                    "SELECT id, status, publication_operation_id "
                    "FROM import_runs WHERE active_slot = 1 ORDER BY id"
                )
            ).fetchall()
        ]
        all_runs = [
            (int(r.id), str(r.run_type), str(r.status))
            for r in conn.execute(
                text("SELECT id, run_type, status FROM import_runs ORDER BY id")
            ).fetchall()
        ]
        places = [
            (
                int(r.id),
                str(r.osm_type),
                int(r.osm_id),
                r.name,
                str(r.category),
                bool(r.is_active),
                int(r.last_seen_import_id),
            )
            for r in conn.execute(
                text(
                    "SELECT id, osm_type, osm_id, name, category, is_active, last_seen_import_id "
                    "FROM places ORDER BY id"
                )
            ).fetchall()
        ]
    return {"active_runs": active_runs, "all_runs": all_runs, "places": places}


def _setup_v1_and_v2_active(
    engine: Engine,
    work_dir: Path,
) -> tuple[int, int, int, RecoveryArtifactRecord]:
    """Publish V1, export V1 immutable artifact, then publish V2 as active."""
    with engine.begin() as conn:
        bv_id = insert_boundary_version(conn, version_code="bv-m5-ac1")

    pbf_v1 = _write_pbf(
        work_dir / "v1.osm.pbf",
        [
            _node(101, name="Cà Phê Một", lon=106.701, lat=10.777),
            _node(102, name="Quán Phở Hai", lon=106.702, lat=10.778, category_tag=("amenity", "restaurant")),
        ],
    )
    pbf_v2 = _write_pbf(
        work_dir / "v2.osm.pbf",
        [
            _node(101, name="Cà Phê Một V2", lon=106.701, lat=10.777, version=2),
            _node(103, name="Nhà Thuốc Ba", lon=106.703, lat=10.779, category_tag=("amenity", "pharmacy")),
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
        engine,
        artifact_root=work_dir / "v1_artifacts",
        artifact_name="v1_source",
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


def test_m5_ac1_rejects_missing_tampered_unsupported_and_mismatched_artifacts(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC1: Corrupt, missing, unsupported, or provenance-mismatched artifacts are rejected cleanly."""
    with disposable_database("m5_ac1_art") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            _bv_id, v1_run_id, v2_run_id, v1_artifact = _setup_v1_and_v2_active(engine, work_dir)
            baseline_state = _snapshot_live_state(engine)

            # Valid superseded V1 artifact passes historical verification cleanly.
            with engine.connect() as conn:
                verified = verify_historical_source_artifact(
                    conn,
                    artifact_dir=v1_artifact.artifact_dir,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
            assert verified.record.source_import_run_id == v1_run_id
            assert verified.source_run_status_in_db == "SUPERSEDED"
            assert len(verified.places) == 2

            # 1. Missing artifact directory.
            op_missing = uuid.uuid4()
            approval_missing = build_recovery_approval_from_artifact(
                v1_artifact,
                publication_operation_id=op_missing,
                expected_active_run_id=v2_run_id,
                approved_by="ops-lead@geosearch",
                approval_reason="INC-001",
            )
            with pytest.raises(RecoveryArtifactError, match="does not exist"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=work_dir / "does_not_exist",
                    publication_operation_id=op_missing,
                    approval=approval_missing,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 2. Tampered places_snapshot.jsonl content (checksum mismatch).
            tampered_places_dir = _copy_writable_artifact(
                v1_artifact.artifact_dir, work_dir / "tampered_places"
            )
            (tampered_places_dir / PLACES_SNAPSHOT_FILENAME).write_text(
                (tampered_places_dir / PLACES_SNAPSHOT_FILENAME)
                .read_text(encoding="utf-8")
                .replace("Cà Phê Một", "Cà Phê Giả"),
                encoding="utf-8",
            )
            op_tamper = uuid.uuid4()
            with pytest.raises(RecoveryArtifactError, match="checksum or size mismatch"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=tampered_places_dir,
                    publication_operation_id=op_tamper,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_tamper,
                        expected_active_run_id=v2_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-002",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 3. Unsupported artifact_format (even with recomputed manifest.sha256).
            bad_fmt_dir = _copy_writable_artifact(v1_artifact.artifact_dir, work_dir / "bad_fmt")
            manifest_fmt = json.loads((bad_fmt_dir / MANIFEST_FILENAME).read_text(encoding="utf-8"))
            manifest_fmt["artifact_format"] = "geosearch.recovery_artifact.v999"
            _recompute_manifest_digests(bad_fmt_dir, manifest_fmt)
            with engine.connect() as conn:
                with pytest.raises(RecoveryArtifactError, match="Unsupported recovery artifact format"):
                    verify_historical_source_artifact(
                        conn,
                        artifact_dir=bad_fmt_dir,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )

            # 4. Recomputed-checksum manifest with forged source_fingerprint mismatch vs DB history.
            forged_prov_dir = _copy_writable_artifact(
                v1_artifact.artifact_dir, work_dir / "forged_prov"
            )
            manifest_prov = json.loads(
                (forged_prov_dir / MANIFEST_FILENAME).read_text(encoding="utf-8")
            )
            manifest_prov["source_publication"]["source_fingerprint"] = "f" * 64
            _recompute_manifest_digests(forged_prov_dir, manifest_prov)
            with engine.connect() as conn:
                with pytest.raises(
                    RecoveryArtifactError, match="historical provenance mismatch"
                ):
                    verify_historical_source_artifact(
                        conn,
                        artifact_dir=forged_prov_dir,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )

            # 5. Recomputed-checksum manifest referencing a non-APPROVED (DRAFT) boundary.
            with engine.begin() as conn:
                draft_bv_id = insert_boundary_version(
                    conn, version_code="bv-m5-draft", approval_status="DRAFT"
                )
            draft_bv_dir = _copy_writable_artifact(v1_artifact.artifact_dir, work_dir / "draft_bv")
            manifest_bv = json.loads((draft_bv_dir / MANIFEST_FILENAME).read_text(encoding="utf-8"))
            manifest_bv["boundary_version"]["id"] = draft_bv_id
            _recompute_manifest_digests(draft_bv_dir, manifest_bv)
            with engine.connect() as conn:
                with pytest.raises(RecoveryArtifactError, match="boundary_version.id does not match"):
                    verify_historical_source_artifact(
                        conn,
                        artifact_dir=draft_bv_dir,
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )

            # 6. AUTHORITATIVE mode blocks node_only / synthetic boundary artifact fail-closed.
            op_auth = uuid.uuid4()
            with pytest.raises(PublicationEligibilityError) as exc_auth:
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_auth,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_auth,
                        expected_active_run_id=v2_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-AUTH",
                    ),
                    publication_mode=PublicationMode.AUTHORITATIVE,
                    protection_artifact_root=work_dir / "v2_prot",
                )
            assert exc_auth.value.status == EligibilityStatus.COVERAGE_INCOMPATIBLE

            # 7. TOCTOU tampering between verification and staging is detected and rejected.
            toctou_dir = _copy_writable_artifact(v1_artifact.artifact_dir, work_dir / "toctou_src")
            op_toctou = uuid.uuid4()

            def _tamper_after_verify(src_dir: Path) -> None:
                snap = src_dir / PLACES_SNAPSHOT_FILENAME
                snap.write_bytes(snap.read_bytes() + b"\n")

            with pytest.raises(
                RecoveryArtifactError, match="changed on disk between verification and staging"
            ):
                prepare_recovery_run(
                    engine,
                    source_artifact_dir=toctou_dir,
                    publication_operation_id=op_toctou,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_toctou,
                        expected_active_run_id=v2_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-TOCTOU",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    fault_hooks=RecoveryFaultHooks(
                        on_after_source_verification=_tamper_after_verify
                    ),
                )

            # Prove NONE of the rejected attempts mutated active run, import_runs, or live places.
            assert _snapshot_live_state(engine) == baseline_state
        finally:
            engine.dispose()


def test_m5_ac1_rejects_missing_malformed_and_mismatched_manual_approval(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC1: Missing, naive-timestamp, blank, or mismatched manual approval is rejected fail-closed."""
    with disposable_database("m5_ac1_appr") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            _bv_id, v1_run_id, v2_run_id, v1_artifact = _setup_v1_and_v2_active(engine, work_dir)
            baseline_state = _snapshot_live_state(engine)

            op_id = uuid.uuid4()
            now_utc = datetime.now(timezone.utc)

            # 1. Missing approval (None).
            with pytest.raises(RecoveryApprovalError, match="requires explicit traceable manual"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=None,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 2. Blank approved_by.
            with pytest.raises(RecoveryApprovalError, match="approved_by"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=RecoveryApproval(
                        approved_by="   ",
                        approved_at=now_utc,
                        approval_reason="INC-101",
                        publication_operation_id=op_id,
                        source_import_run_id=v1_run_id,
                        source_artifact_manifest_sha256=v1_artifact.manifest_sha256,
                        source_artifact_content_sha256=v1_artifact.content_sha256,
                        expected_active_run_id=v2_run_id,
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 3. Naive (non-timezone-aware) approved_at.
            with pytest.raises(RecoveryApprovalError, match="timezone-aware"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=RecoveryApproval(
                        approved_by="ops-lead@geosearch",
                        approved_at=datetime(2026, 11, 24, 12, 0, 0),
                        approval_reason="INC-102",
                        publication_operation_id=op_id,
                        source_import_run_id=v1_run_id,
                        source_artifact_manifest_sha256=v1_artifact.manifest_sha256,
                        source_artifact_content_sha256=v1_artifact.content_sha256,
                        expected_active_run_id=v2_run_id,
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 4. Approval bound to a different publication_operation_id.
            with pytest.raises(RecoveryApprovalError, match="publication_operation_id"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=uuid.uuid4(),
                        expected_active_run_id=v2_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-103",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 5. Approval bound to a different source_artifact_content_sha256.
            with pytest.raises(RecoveryApprovalError, match="content_sha256"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=RecoveryApproval(
                        approved_by="ops-lead@geosearch",
                        approved_at=now_utc,
                        approval_reason="INC-104",
                        publication_operation_id=op_id,
                        source_import_run_id=v1_run_id,
                        source_artifact_manifest_sha256=v1_artifact.manifest_sha256,
                        source_artifact_content_sha256="0" * 64,
                        expected_active_run_id=v2_run_id,
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 6. Approval expecting wrong active run (V1 instead of V2).
            with pytest.raises(PublicationConflictError, match="active-run mismatch"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_id,
                        expected_active_run_id=v1_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-105",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_root=work_dir / "v2_prot",
                )

            # 7. Accidentally supplying V1 artifact as V2 protection artifact is rejected
            #    (both same path AND a copied directory at a different path).
            with pytest.raises(RecoveryArtifactError, match="must refer to distinct artifacts"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_id,
                        expected_active_run_id=v2_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-106",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_dir=v1_artifact.artifact_dir,
                )

            # Prove NONE of the 7 rejected pre-prepare attempts created any run or changed live state.
            assert _snapshot_live_state(engine) == baseline_state

            copied_v1_as_prot = _copy_writable_artifact(
                v1_artifact.artifact_dir, work_dir / "v1_copy_used_as_v2_prot"
            )
            op_id_copy = uuid.uuid4()
            with pytest.raises(RecoveryArtifactError, match="baseline run mismatch"):
                execute_controlled_recovery(
                    engine,
                    source_artifact_dir=v1_artifact.artifact_dir,
                    publication_operation_id=op_id_copy,
                    approval=build_recovery_approval_from_artifact(
                        v1_artifact,
                        publication_operation_id=op_id_copy,
                        expected_active_run_id=v2_run_id,
                        approved_by="ops-lead@geosearch",
                        approval_reason="INC-107",
                    ),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    protection_artifact_dir=copied_v1_as_prot,
                )

            # Prove live state and active run (V2) are completely untouched.
            after_state = _snapshot_live_state(engine)
            assert after_state["active_runs"] == baseline_state["active_runs"]
            assert after_state["places"] == baseline_state["places"]
        finally:
            engine.dispose()
