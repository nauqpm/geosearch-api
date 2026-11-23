"""M3.2 immutable pre-publication recovery artifact tests on PostgreSQL/PostGIS."""

from __future__ import annotations

import json
import os
import stat
import threading
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import osmium
import pytest
from sqlalchemy import Connection, text

from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import (
    MANIFEST_CHECKSUM_FILENAME,
    MANIFEST_FILENAME,
    PLACES_SNAPSHOT_FILENAME,
    DuplicateCandidateInput,
    DuplicateDetectionState,
    EligibilityStatus,
    PublicationEligibilityError,
    PublicationMode,
    RecoveryArtifactError,
    StagedDuplicateCandidate,
    prepare_recovery_artifact,
    publish_prepared_run,
    verify_artifact_matches_live_baseline,
    verify_recovery_artifact,
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


def _make_writable(path: Path) -> None:
    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)


def test_m32_first_publication_returns_none_and_does_not_fabricate_artifact(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """When no active publication exists, prepare_recovery_artifact returns None cleanly."""
    with disposable_database("m32_first") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            artifact_root = work_dir / "artifacts"
            artifact = prepare_recovery_artifact(engine, artifact_root=artifact_root)
            assert artifact is None
            # No temp or finalized artifact directories left behind.
            assert list(artifact_root.iterdir()) == []
        finally:
            engine.dispose()


def test_m32_recovery_artifact_consistency_corruption_rejection_and_overwrite_protection(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Validates complete snapshot content, checksums, corruption detection, and immutability."""
    with disposable_database("m32_verify") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m32-v1")

            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    osmium.osm.mutable.Node(
                        id=1001,
                        version=1,
                        changeset=10,
                        timestamp="2026-01-10T00:00:00Z",
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "Cà Phê A"},
                    ),
                    osmium.osm.mutable.Node(
                        id=1002,
                        version=1,
                        changeset=11,
                        timestamp="2026-01-10T00:00:00Z",
                        location=(106.7010, 10.7770),
                        tags={"amenity": "cafe", "name": "Cà Phê A Copy"},
                    ),
                ],
            )
            ing_v1 = ingest_osm_nodes_to_staging(
                engine, source_path=pbf_v1, boundary_version_id=bv_id
            )
            op_v1 = uuid.uuid4()
            pub_v1 = publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=op_v1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                duplicate_candidates=DuplicateCandidateInput(
                    state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                    detector_version="det-v1",
                    candidates=(
                        StagedDuplicateCandidate(
                            osm_type_a="node",
                            osm_id_a=1002,
                            osm_type_b="node",
                            osm_id_b=1001,
                            detector_version="det-v1",
                            evidence={"reason": "proximity"},
                        ),
                    ),
                ),
            )
            assert pub_v1.active_place_count == 2
            assert pub_v1.duplicate_candidate_count == 1

            artifact_root = work_dir / "artifacts"
            artifact = prepare_recovery_artifact(
                engine,
                artifact_root=artifact_root,
                artifact_name="baseline_v1",
            )
            assert artifact is not None
            assert artifact.source_import_run_id == ing_v1.import_run.id
            assert artifact.source_publication_operation_id == op_v1
            assert artifact.source_boundary_version_id == bv_id
            assert artifact.active_place_count == 2
            assert artifact.duplicate_candidate_count == 1

            # Reconstructible full place content + stable place_id present in JSONL.
            place_lines = [
                json.loads(line)
                for line in artifact.places_snapshot_path.read_text(encoding="utf-8").splitlines()
            ]
            assert [r["osm_id"] for r in place_lines] == [1001, 1002]
            assert all(isinstance(r["place_id"], int) and r["place_id"] > 0 for r in place_lines)
            assert all(r["location_ewkb_hex"] for r in place_lines)

            # Overwriting existing finalized artifact name is refused.
            with pytest.raises(RecoveryArtifactError, match="Refusing to overwrite"):
                prepare_recovery_artifact(
                    engine,
                    artifact_root=artifact_root,
                    artifact_name="baseline_v1",
                )

            # Live baseline verification passes while V1 is still active.
            with engine.connect() as conn:
                verify_artifact_matches_live_baseline(conn, artifact=artifact)

            # Baseline mismatch check rejects wrong expected_source_run_id.
            with pytest.raises(RecoveryArtifactError, match="baseline run mismatch"):
                verify_recovery_artifact(artifact.artifact_dir, expected_source_run_id=999999)

            # Failed preparation before finalization leaves NO incomplete artifact behind.
            def _abort_before_finalize(_tmp_dir: Path) -> None:
                raise RuntimeError("Simulated disk/process crash before finalize")

            with pytest.raises(RuntimeError, match="Simulated disk/process crash"):
                prepare_recovery_artifact(
                    engine,
                    artifact_root=artifact_root,
                    artifact_name="aborted_v1",
                    on_before_finalize=_abort_before_finalize,
                )
            assert not (artifact_root / "aborted_v1").exists()
            assert not any(p.name.startswith(".tmp_recovery_") for p in artifact_root.iterdir())

            # Truncated / corrupted snapshot blocks replacement publication.
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    osmium.osm.mutable.Node(
                        id=1001,
                        version=2,
                        changeset=20,
                        timestamp="2026-01-12T00:00:00Z",
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "Cà Phê A Updated"},
                    )
                ],
            )
            ing_v2 = ingest_osm_nodes_to_staging(
                engine, source_path=pbf_v2, boundary_version_id=bv_id
            )

            # Missing artifact blocks replacement publication.
            with pytest.raises(PublicationEligibilityError) as exc_missing:
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
            assert exc_missing.value.status == EligibilityStatus.MISSING_RECOVERY_ARTIFACT

            # Truncate places_snapshot.jsonl and verify rejection.
            _make_writable(artifact.places_snapshot_path)
            original_bytes = artifact.places_snapshot_path.read_bytes()
            artifact.places_snapshot_path.write_bytes(original_bytes[:40])
            with pytest.raises(RecoveryArtifactError, match="checksum or size mismatch"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_dir=artifact.artifact_dir,
                )

            # Restore snapshot, corrupt manifest.json, verify manifest checksum mismatch.
            artifact.places_snapshot_path.write_bytes(original_bytes)
            _make_writable(artifact.manifest_path)
            artifact.manifest_path.write_bytes(artifact.manifest_path.read_bytes() + b" ")
            with pytest.raises(RecoveryArtifactError, match="manifest.json checksum mismatch"):
                verify_recovery_artifact(artifact.artifact_dir)
        finally:
            engine.dispose()


def test_m32_repeatable_read_snapshot_consistency_and_stale_artifact_rejection(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Artifact sees one consistent REPEATABLE READ snapshot; stale artifact is rejected under lock."""
    with disposable_database("m32_stale") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m32-stale")

            pbf_v1 = _write_pbf(
                work_dir / "v1.osm.pbf",
                [
                    osmium.osm.mutable.Node(
                        id=2001,
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "V1 Cafe"},
                    ),
                ],
            )
            pbf_v2 = _write_pbf(
                work_dir / "v2.osm.pbf",
                [
                    osmium.osm.mutable.Node(
                        id=2001,
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "V2 Cafe"},
                    ),
                    osmium.osm.mutable.Node(
                        id=2002,
                        location=(106.7019, 10.7779),
                        tags={"amenity": "cafe", "name": "V2 Extra"},
                    ),
                ],
            )
            pbf_v3 = _write_pbf(
                work_dir / "v3.osm.pbf",
                [
                    osmium.osm.mutable.Node(
                        id=2001,
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "V3 Cafe"},
                    ),
                ],
            )

            ing_v1 = ingest_osm_nodes_to_staging(
                engine, source_path=pbf_v1, boundary_version_id=bv_id
            )
            ing_v2 = ingest_osm_nodes_to_staging(
                engine, source_path=pbf_v2, boundary_version_id=bv_id
            )
            ing_v3 = ingest_osm_nodes_to_staging(
                engine, source_path=pbf_v3, boundary_version_id=bv_id
            )

            # Publish V1 first.
            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            # Open REPEATABLE READ snapshot of V1, pause while V2 commits concurrently on another
            # connection, then finish exporting V1's consistent snapshot.
            snapshot_opened = threading.Event()
            v2_committed = threading.Event()
            artifact_root = work_dir / "artifacts"
            exported: dict[str, object] = {}

            def _export_worker() -> None:
                def _on_open(_conn: Connection) -> None:
                    snapshot_opened.set()
                    assert v2_committed.wait(timeout=10.0)

                exported["artifact"] = prepare_recovery_artifact(
                    engine,
                    artifact_root=artifact_root,
                    artifact_name="v1_during_v2",
                    on_snapshot_opened=_on_open,
                )

            t = threading.Thread(target=_export_worker)
            t.start()
            assert snapshot_opened.wait(timeout=10.0)

            # Commit V2 concurrently while V1 artifact export is paused mid-snapshot.
            publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                recovery_artifact_root=work_dir / "v2_artifacts",
            )
            v2_committed.set()
            t.join(timeout=10.0)

            v1_artifact = exported["artifact"]
            assert v1_artifact is not None
            # Consistent REPEATABLE READ snapshot captured ONLY V1 (1 place), NOT V2's 2 places!
            assert v1_artifact.source_import_run_id == ing_v1.import_run.id
            assert v1_artifact.active_place_count == 1

            # Attempting to publish V3 using the stale V1 artifact MUST fail closed!
            with pytest.raises(RecoveryArtifactError, match="baseline run mismatch"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v3.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_dir=v1_artifact.artifact_dir,
                )

            # And live baseline check under lock explicitly rejects it as stale.
            with engine.connect() as conn:
                with pytest.raises(RecoveryArtifactError, match="Stale recovery artifact"):
                    verify_artifact_matches_live_baseline(conn, artifact=v1_artifact)
        finally:
            engine.dispose()
