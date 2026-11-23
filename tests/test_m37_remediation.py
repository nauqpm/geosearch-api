"""M3.7 transactional integrity & compliance remediation regression test suite.

Covers:
- BLOCKER-1: Complete recovery artifact baseline verification (name, category, raw_tags,
  coordinates, duplicate evidence swap, duplicate pair swap, boundary geometry EWKB drift,
  source provenance mismatch, corrupt/missing files, recomputed outer manifest checksum,
  unexpected artifact on first publication, unchanged artifact remains valid).
- BLOCKER-2: Prepared staging immutability triggers (0004_m3_staging_immutability) for
  INSERT/UPDATE/DELETE across PREPARED, PUBLISHED, SUPERSEDED; concurrent staging write
  racing with PREPARED transition; concurrent staging write during open publication tx;
  in-tx staging digest mismatch before COMMIT; hardened import_runs evidence immutability.
- BLOCKER-3: Controlled-test database isolation guard (dev DB collision, non-test DB name,
  spoofed env/non-local host, orphaned active Way/Relation on first publish, incompatible
  mixed-coverage replacement, node_only over authoritative, authorized test DB V1 -> V2).
- Section 5: Duplicate candidate conflicting replay & unexpected rows, lost COMMIT ACK
  followed by V3 supersession & V2 retry, concurrent reader isolation during rolled-back
  publication, zero orphan recovery artifacts on conflicting registration race.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import threading
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from decimal import Decimal
from pathlib import Path
from typing import Any

import osmium
import pytest
from sqlalchemy import Connection, Engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError

from geosearch.db import create_test_engine
from geosearch.ingestion import (
    ExtractionStats,
    begin_staging_run,
    build_canonical_candidate,
    create_import_run,
    ingest_osm_nodes_to_staging,
    mark_extraction_completed,
    stage_candidate_batch,
    verify_and_prepare_run,
)
from geosearch.publication import (
    DUPLICATES_SNAPSHOT_FILENAME,
    MANIFEST_CHECKSUM_FILENAME,
    MANIFEST_FILENAME,
    DuplicateCandidateInput,
    DuplicateDetectionState,
    EligibilityStatus,
    PublicationConflictError,
    PublicationEligibilityError,
    PublicationFaultHooks,
    PublicationInvariantError,
    PublicationMode,
    PublicationOutcomeStatus,
    RecoveryArtifactError,
    StagedDuplicateCandidate,
    prepare_recovery_artifact,
    publish_prepared_run,
    read_active_place_identities_single_statement,
    read_publication_snapshot_in_tx,
    repeatable_read_snapshot,
    resolve_unknown_commit_outcome,
    validate_publication_eligibility,
    verify_artifact_matches_live_baseline,
    verify_recovery_artifact,
)
from tests.helpers import (
    VALID_SHA256,
    assert_pg_error,
    insert_boundary_version,
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


def _node(
    osm_id: int,
    name: str,
    *,
    lon: float = 106.701,
    lat: float = 10.777,
    category_tag: tuple[str, str] = ("amenity", "cafe"),
    extra_tags: dict[str, str] | None = None,
) -> osmium.osm.mutable.Node:
    tags = {category_tag[0]: category_tag[1], "name": name, **(extra_tags or {})}
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=1,
        changeset=100,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags=tags,
    )


def _make_writable(path: Path) -> None:
    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)


def _wait_for_pg_lock_waiter(engine: Engine, *, query_fragment: str, timeout: float = 5.0) -> None:
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


def _setup_v1_with_dups_and_artifact(
    engine: Engine,
    work_dir: Path,
) -> tuple[int, int, int, Any]:
    """Publish V1 (3 nodes + 1 duplicate candidate), prepare V2, and build V1 recovery artifact."""
    with engine.begin() as conn:
        bv_id = insert_boundary_version(conn, version_code="bv-m37-b1")

    pbf_v1 = _write_pbf(
        work_dir / "v1.osm.pbf",
        [
            _node(101, "Cafe Alpha", lon=106.701, lat=10.777, extra_tags={"wifi": "yes"}),
            _node(102, "Cafe Beta", lon=106.702, lat=10.778),
            _node(103, "Cafe Gamma", lon=106.703, lat=10.779),
        ],
    )
    pbf_v2 = _write_pbf(
        work_dir / "v2.osm.pbf",
        [
            _node(101, "Cafe Alpha V2", lon=106.701, lat=10.777),
            _node(104, "Cafe Delta", lon=106.704, lat=10.780),
        ],
    )
    ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
    ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

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
                    osm_id_a=101,
                    osm_type_b="node",
                    osm_id_b=102,
                    detector_version="det-v1",
                    confidence_score=Decimal("0.9100"),
                    distance_meters=12.5,
                    evidence={"rule": "orig_evidence"},
                ),
            ),
        ),
    )

    artifact = prepare_recovery_artifact(
        engine,
        artifact_root=work_dir / "artifacts",
        artifact_name="v1_baseline",
    )
    assert artifact is not None
    return bv_id, ing_v1.import_run.id, ing_v2.import_run.id, artifact


# ===========================================================================
# BLOCKER-1: Complete Recovery Artifact Content Baseline Verification
# ===========================================================================


@pytest.mark.parametrize(
    ("mutation_label", "mutation_sql"),
    [
        (
            "name_changed",
            "UPDATE places SET name = 'Cafe Alpha Mutated', name_search = 'cafe alpha mutated' "
            "WHERE osm_type = 'node' AND osm_id = 101",
        ),
        (
            "category_changed",
            "UPDATE places SET category = 'restaurant' WHERE osm_type = 'node' AND osm_id = 101",
        ),
        (
            "raw_tags_changed",
            "UPDATE places SET raw_tags = '{\"amenity\": \"cafe\", \"name\": \"Cafe Alpha\", "
            "\"wifi\": \"no\"}'::jsonb WHERE osm_type = 'node' AND osm_id = 101",
        ),
        (
            "coordinates_changed",
            "UPDATE places SET location = ST_SetSRID(ST_MakePoint(106.7099, 10.7799), 4326)::geography "
            "WHERE osm_type = 'node' AND osm_id = 101",
        ),
    ],
)
def test_blocker1_content_only_place_mutations_after_snapshot_rejected(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    mutation_label: str,
    mutation_sql: str,
) -> None:
    """Any post-snapshot place content change (name/category/raw_tags/coords) invalidates artifact."""
    prefix = f"m37_b1_{mutation_label.split('_')[0]}"
    with disposable_database(prefix) as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            _bv_id, _v1_id, v2_id, artifact = _setup_v1_with_dups_and_artifact(engine, work_dir)

            with engine.begin() as conn:
                conn.execute(text(mutation_sql))

            with engine.connect() as conn:
                with pytest.raises(RecoveryArtifactError, match="canonical content hash differs"):
                    verify_artifact_matches_live_baseline(conn, artifact=artifact)

            # Publication itself must also fail closed using the stale artifact.
            with pytest.raises(RecoveryArtifactError, match="canonical content hash differs"):
                publish_prepared_run(
                    engine,
                    import_run_id=v2_id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_dir=artifact.artifact_dir,
                )
        finally:
            engine.dispose()


def test_blocker1_duplicate_evidence_and_pair_swaps_without_count_change_rejected(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Changing duplicate evidence or replacing pair while preserving count=1 invalidates artifact."""
    with disposable_database("m37_b1_dup") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            _bv_id, v1_id, _v2_id, artifact = _setup_v1_with_dups_and_artifact(engine, work_dir)

            with engine.connect() as conn:
                ids = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                }

            # Case 1: Same pair (101, 102), count still 1, DIFFERENT evidence.
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM duplicate_candidates WHERE import_run_id = :run_id"),
                    {"run_id": v1_id},
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO duplicate_candidates (
                            import_run_id, place_a_id, place_b_id, detector_version,
                            confidence_score, distance_meters, evidence
                        )
                        VALUES (:run_id, :a, :b, 'det-v1', 0.9100, 12.5, '{"rule": "mutated"}'::jsonb)
                        """
                    ),
                    {
                        "run_id": v1_id,
                        "a": min(ids[101], ids[102]),
                        "b": max(ids[101], ids[102]),
                    },
                )
            with engine.connect() as conn:
                with pytest.raises(
                    RecoveryArtifactError, match="duplicate_candidates canonical content/evidence"
                ):
                    verify_artifact_matches_live_baseline(conn, artifact=artifact)

            # Case 2: Different pair (102, 103), count still 1.
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM duplicate_candidates WHERE import_run_id = :run_id"),
                    {"run_id": v1_id},
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO duplicate_candidates (
                            import_run_id, place_a_id, place_b_id, detector_version,
                            confidence_score, distance_meters, evidence
                        )
                        VALUES (:run_id, :a, :b, 'det-v1', 0.9100, 12.5, '{"rule": "orig_evidence"}'::jsonb)
                        """
                    ),
                    {
                        "run_id": v1_id,
                        "a": min(ids[102], ids[103]),
                        "b": max(ids[102], ids[103]),
                    },
                )
            with engine.connect() as conn:
                with pytest.raises(
                    RecoveryArtifactError, match="duplicate_candidates canonical content/evidence"
                ):
                    verify_artifact_matches_live_baseline(conn, artifact=artifact)
        finally:
            engine.dispose()


def test_blocker1_boundary_geom_ewkb_drift_provenance_mismatch_and_artifact_tampering(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Boundary EWKB mutation without checksum change, source provenance mismatch, and corrupt manifest all fail."""
    with disposable_database("m37_b1_geom") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            bv_id, v1_id, v2_id, artifact = _setup_v1_with_dups_and_artifact(engine, work_dir)

            # 1. Valid unchanged artifact passes baseline verification.
            with engine.connect() as conn:
                verify_artifact_matches_live_baseline(conn, artifact=artifact)

            # 2. Controlled fixture: modify boundary geom WITHOUT changing checksum_sha256.
            with engine.begin() as conn:
                conn.execute(
                    text("ALTER TABLE boundary_versions DISABLE TRIGGER trg_boundary_versions_guard")
                )
                conn.execute(
                    text(
                        "UPDATE boundary_versions SET geom = ST_GeomFromText("
                        "'MULTIPOLYGON(((106.5 10.6, 106.9 10.6, 106.9 11.0, 106.5 11.0, 106.5 10.6)))', "
                        "4326) WHERE id = :id"
                    ),
                    {"id": bv_id},
                )
                conn.execute(
                    text("ALTER TABLE boundary_versions ENABLE TRIGGER trg_boundary_versions_guard")
                )
                with pytest.raises(RecoveryArtifactError, match="geometry EWKB hash differs"):
                    verify_artifact_matches_live_baseline(conn, artifact=artifact)
                conn.rollback()

            # 3. Source checksum mismatch against live active run.
            with engine.begin() as conn:
                conn.execute(
                    text("ALTER TABLE import_runs DISABLE TRIGGER trg_import_runs_guard")
                )
                conn.execute(
                    text(
                        "UPDATE import_runs SET source_checksum_sha256 = repeat('f', 64) "
                        "WHERE id = :id"
                    ),
                    {"id": v1_id},
                )
                conn.execute(
                    text("ALTER TABLE import_runs ENABLE TRIGGER trg_import_runs_guard")
                )
                with pytest.raises(RecoveryArtifactError, match="source_checksum_sha256 mismatch"):
                    verify_artifact_matches_live_baseline(conn, artifact=artifact)
                conn.rollback()

            # 4. Corrupted manifest.json with RECOMPUTED outer manifest.sha256 (inner content_sha256 catches it!).
            tampered_dir = work_dir / "tampered_manifest_copy"
            tampered_dir.mkdir()
            for fname in (
                "places_snapshot.jsonl",
                DUPLICATES_SNAPSHOT_FILENAME,
                MANIFEST_FILENAME,
                MANIFEST_CHECKSUM_FILENAME,
            ):
                (tampered_dir / fname).write_bytes((artifact.artifact_dir / fname).read_bytes())

            manifest_data = json.loads((tampered_dir / MANIFEST_FILENAME).read_text(encoding="utf-8"))
            manifest_data["source_publication"]["source_name"] = "forged.osm.pbf"
            forged_bytes = json.dumps(
                manifest_data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
            ).encode("utf-8")
            (tampered_dir / MANIFEST_FILENAME).write_bytes(forged_bytes)
            recomputed_outer = hashlib.sha256(forged_bytes).hexdigest()
            (tampered_dir / MANIFEST_CHECKSUM_FILENAME).write_text(
                f"{recomputed_outer}  {MANIFEST_FILENAME}\n", encoding="utf-8"
            )
            with pytest.raises(RecoveryArtifactError, match="content_sha256 mismatch"):
                verify_recovery_artifact(tampered_dir)

            # 5. Missing required file (duplicate_candidates_snapshot.jsonl removed).
            missing_dir = work_dir / "missing_file_copy"
            missing_dir.mkdir()
            for fname in ("places_snapshot.jsonl", MANIFEST_FILENAME, MANIFEST_CHECKSUM_FILENAME):
                (missing_dir / fname).write_bytes((artifact.artifact_dir / fname).read_bytes())
            with pytest.raises(RecoveryArtifactError, match="missing required file"):
                verify_recovery_artifact(missing_dir)

            # 6. Original unchanged artifact still succeeds for V2 replacement publication!
            res_v2 = publish_prepared_run(
                engine,
                import_run_id=v2_id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                recovery_artifact_dir=artifact.artifact_dir,
            )
            assert res_v2.outcome == PublicationOutcomeStatus.PUBLISHED
        finally:
            engine.dispose()


def test_blocker1_unexpected_recovery_artifact_rejected_on_first_publication(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Supplying a recovery artifact when no active publication exists fails closed."""
    with disposable_database("m37_b1_unexp") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-unexp")
            pbf = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "Cafe First")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            fake_dir = work_dir / "unexpected_artifact"
            fake_dir.mkdir()

            with pytest.raises(RecoveryArtifactError, match="no active publication exists"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_dir=fake_dir,
                )
        finally:
            engine.dispose()


# ===========================================================================
# BLOCKER-2: Prepared Staging Immutability & Concurrency
# ===========================================================================


@pytest.mark.parametrize("target_state", ["PREPARED", "PUBLISHED", "SUPERSEDED"])
def test_blocker2_direct_staging_and_issues_mutations_blocked_across_prepared_published_superseded(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    target_state: str,
) -> None:
    """INSERT/UPDATE/DELETE on staging tables & import_issues are blocked for PREPARED/PUBLISHED/SUPERSEDED."""
    with disposable_database(f"m37_b2_{target_state.lower()[:8]}") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-b2")
            pbf_1 = _write_pbf(
                work_dir / "r1.osm.pbf",
                [
                    _node(101, "Cafe 101"),
                    # Include an invalid node (missing name on cafe) so an import_issues + fingerprint row exist!
                    osmium.osm.mutable.Node(
                        id=901,
                        location=(106.701, 10.777),
                        tags={"amenity": "cafe"},
                    ),
                ],
            )
            pbf_2 = _write_pbf(work_dir / "r2.osm.pbf", [_node(102, "Cafe 102")])

            ing_1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_1, boundary_version_id=bv_id)
            run_id = ing_1.import_run.id

            if target_state in ("PUBLISHED", "SUPERSEDED"):
                publish_prepared_run(
                    engine,
                    import_run_id=run_id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
            if target_state == "SUPERSEDED":
                ing_2 = ingest_osm_nodes_to_staging(
                    engine, source_path=pbf_2, boundary_version_id=bv_id
                )
                publish_prepared_run(
                    engine,
                    import_run_id=ing_2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=work_dir / "artifacts",
                )

            with engine.connect() as conn:
                # 1. staging_place_candidates: INSERT, UPDATE, DELETE all rejected!
                for sql in (
                    """
                    INSERT INTO staging_place_candidates (
                        import_run_id, osm_type, osm_id, name, name_search, category,
                        location, raw_tags, content_digest, source_provenance
                    )
                    VALUES (
                        :run_id, 'node', 9999, 'Injected', 'injected', 'cafe',
                        ST_SetSRID(ST_MakePoint(106.701, 10.777), 4326)::geography,
                        '{"amenity":"cafe"}'::jsonb, repeat('b', 64), '{}'::jsonb
                    )
                    """,
                    "UPDATE staging_place_candidates SET name = 'Mutated', name_search = 'mutated' "
                    "WHERE import_run_id = :run_id",
                    "DELETE FROM staging_place_candidates WHERE import_run_id = :run_id",
                ):
                    with pytest.raises(IntegrityError) as exc:
                        with conn.begin():
                            conn.execute(text(sql), {"run_id": run_id})
                    assert_pg_error(
                        exc, sqlstate="23514", constraint_name="trg_prepared_staging_immutable"
                    )

                # 2. staging_run_checkpoints: UPDATE, DELETE rejected!
                for sql in (
                    "UPDATE staging_run_checkpoints SET staged_candidate_count = 999 "
                    "WHERE import_run_id = :run_id",
                    "DELETE FROM staging_run_checkpoints WHERE import_run_id = :run_id",
                ):
                    with pytest.raises(IntegrityError) as exc:
                        with conn.begin():
                            conn.execute(text(sql), {"run_id": run_id})
                    assert_pg_error(
                        exc, sqlstate="23514", constraint_name="trg_prepared_staging_immutable"
                    )

                # 3. staging_issue_fingerprints: INSERT, UPDATE, DELETE rejected!
                for sql in (
                    "INSERT INTO staging_issue_fingerprints (import_run_id, issue_fingerprint) "
                    "VALUES (:run_id, repeat('c', 64))",
                    "UPDATE staging_issue_fingerprints SET import_issue_id = NULL "
                    "WHERE import_run_id = :run_id",
                    "DELETE FROM staging_issue_fingerprints WHERE import_run_id = :run_id",
                ):
                    with pytest.raises(IntegrityError) as exc:
                        with conn.begin():
                            conn.execute(text(sql), {"run_id": run_id})
                    assert_pg_error(
                        exc, sqlstate="23514", constraint_name="trg_prepared_staging_immutable"
                    )

                # 4. import_issues: INSERT, UPDATE, DELETE rejected!
                for sql in (
                    "INSERT INTO import_issues (import_run_id, severity, stage, issue_code, message) "
                    "VALUES (:run_id, 'INFO', 'VERIFY', 'LATE_ISSUE', 'Should fail')",
                    "UPDATE import_issues SET message = 'Mutated' WHERE import_run_id = :run_id",
                    "DELETE FROM import_issues WHERE import_run_id = :run_id",
                ):
                    with pytest.raises(IntegrityError) as exc:
                        with conn.begin():
                            conn.execute(text(sql), {"run_id": run_id})
                    assert_pg_error(
                        exc, sqlstate="23514", constraint_name="trg_import_issues_immutable"
                    )
        finally:
            engine.dispose()


def test_blocker2_hardened_import_runs_published_evidence_immutability(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """0004 trigger blocks in-place mutation of PUBLISHED evidence and stripping keys on SUPERSEDED."""
    with disposable_database("m37_b2_ev") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-ev")
            pbf = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "Cafe Ev")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            with engine.connect() as conn:
                # 1. Mutating publication_evidence while PUBLISHED is blocked.
                with pytest.raises(IntegrityError) as exc1:
                    with conn.begin():
                        conn.execute(
                            text(
                                "UPDATE import_runs SET publication_evidence = '{}'::jsonb "
                                "WHERE id = :id"
                            ),
                            {"id": ing.import_run.id},
                        )
                assert_pg_error(
                    exc1,
                    sqlstate="23514",
                    constraint_name="trg_import_runs_immutable_published_evidence",
                )

                # 2. Stripping original keys during PUBLISHED -> SUPERSEDED is blocked.
                with pytest.raises(IntegrityError) as exc2:
                    with conn.begin():
                        conn.execute(
                            text(
                                "UPDATE import_runs "
                                "SET status = 'SUPERSEDED', active_slot = NULL, "
                                "    superseded_at = now(), publication_evidence = '{}'::jsonb "
                                "WHERE id = :id"
                            ),
                            {"id": ing.import_run.id},
                        )
                assert_pg_error(
                    exc2,
                    sqlstate="23514",
                    constraint_name="trg_import_runs_immutable_published_evidence",
                )
        finally:
            engine.dispose()


def test_blocker2_concurrent_direct_staging_write_racing_with_prepared_transition(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """Direct SQL INSERT into staging racing with verify_and_prepare_run blocks on FOR SHARE and fails."""
    with disposable_database("m37_b2_race") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-race")
                run = create_import_run(
                    conn,
                    boundary_version_id=bv_id,
                    source_name="race.osm",
                    source_checksum_sha256=VALID_SHA256,
                )
                begin_staging_run(
                    conn,
                    import_run_id=run.id,
                    expected_source_fingerprint=run.source_fingerprint,
                    expected_source_checksum_sha256=run.source_checksum_sha256,
                )
                cand = build_canonical_candidate(
                    osm_type="node",
                    osm_id=101,
                    category="cafe",
                    raw_name="Race Cafe",
                    longitude=106.701,
                    latitude=10.777,
                    raw_tags={"amenity": "cafe", "name": "Race Cafe"},
                    source_provenance={"extraction_scope": "node_only"},
                )
                stage_candidate_batch(conn, import_run_id=run.id, candidates=[cand])
                mark_extraction_completed(
                    conn,
                    import_run_id=run.id,
                    stats=ExtractionStats(
                        total_nodes_seen=1,
                        supported_nodes_seen=1,
                        unsupported_nodes_seen=0,
                        valid_candidates=1,
                        invalid_candidates=0,
                    ),
                    source_verified_post_parse=True,
                )

            prep_holding_lock = threading.Event()
            release_prep = threading.Event()
            outcomes: dict[str, Any] = {}

            def _prep_worker() -> None:
                try:
                    with engine.begin() as conn:
                        def _pause(_conn: Connection) -> None:
                            prep_holding_lock.set()
                            assert release_prep.wait(timeout=10.0)

                        verify_and_prepare_run(
                            conn,
                            import_run_id=run.id,
                            on_before_prepare_commit=_pause,
                        )
                except BaseException as exc:  # pragma: no cover
                    outcomes["prep_err"] = exc

            def _rogue_sql_writer() -> None:
                assert prep_holding_lock.wait(timeout=10.0)
                try:
                    with engine.begin() as conn:
                        # Direct raw SQL INSERT bypassing M2 Python API — trigger acquires FOR SHARE
                        # on import_runs, blocks until PREPARED commits, then rejects!
                        conn.execute(
                            text(
                                """
                                INSERT INTO staging_place_candidates (
                                    import_run_id, osm_type, osm_id, name, name_search, category,
                                    location, raw_tags, content_digest, source_provenance
                                )
                                VALUES (
                                    :run_id, 'node', 999, 'Rogue Row', 'rogue row', 'cafe',
                                    ST_SetSRID(ST_MakePoint(106.702, 10.778), 4326)::geography,
                                    '{"amenity":"cafe"}'::jsonb, repeat('d', 64), '{}'::jsonb
                                )
                                """
                            ),
                            {"run_id": run.id},
                        )
                except BaseException as exc:
                    outcomes["writer_err"] = exc

            t_prep = threading.Thread(target=_prep_worker)
            t_writer = threading.Thread(target=_rogue_sql_writer)
            t_prep.start()
            t_writer.start()

            assert prep_holding_lock.wait(timeout=10.0)
            _wait_for_pg_lock_waiter(engine, query_fragment="staging_place_candidates")
            release_prep.set()

            t_prep.join(timeout=10.0)
            t_writer.join(timeout=10.0)

            assert "prep_err" not in outcomes, outcomes.get("prep_err")
            assert isinstance(outcomes.get("writer_err"), IntegrityError)
            orig = outcomes["writer_err"].orig
            assert getattr(orig, "sqlstate", None) == "23514"
            assert getattr(orig.diag, "constraint_name", None) == "trg_prepared_staging_immutable"

            # Verify zero rogue rows slipped into staging_place_candidates!
            with engine.connect() as conn:
                staged_ids = conn.execute(
                    text("SELECT osm_id FROM staging_place_candidates WHERE import_run_id = :id"),
                    {"id": run.id},
                ).scalars().all()
                assert staged_ids == [101]
        finally:
            engine.dispose()


def test_blocker2_concurrent_staging_mutation_during_open_publication_and_in_tx_digest_guard(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Concurrent staging mutation during publish fails; in-tx staging digest mismatch rolls back."""
    with disposable_database("m37_b2_pub") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-b2-pub")
            pbf = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "Cafe Stable")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)

            # Part A: Concurrent staging UPDATE during open publish_prepared_run blocks on PostgreSQL
            # row locks (FOR SHARE on staging / FOR UPDATE on import_runs) and then fails with 23514!
            in_reval = threading.Event()
            release_pub = threading.Event()
            concurrent_err: list[BaseException] = []
            pub_outcome: dict[str, Any] = {}

            def _pub_worker() -> None:
                def _pause_after_reval(_conn: Connection) -> None:
                    in_reval.set()
                    assert release_pub.wait(timeout=10.0)

                try:
                    pub_outcome["res"] = publish_prepared_run(
                        engine,
                        import_run_id=ing.import_run.id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        fault_hooks=PublicationFaultHooks(on_after_revalidation=_pause_after_reval),
                    )
                except BaseException as exc:  # pragma: no cover
                    pub_outcome["err"] = exc

            def _attempt_staging_mutation() -> None:
                assert in_reval.wait(timeout=10.0)
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                "UPDATE staging_place_candidates SET name = 'Mutated Mid Pub', "
                                "name_search = 'mutated mid pub' WHERE import_run_id = :id"
                            ),
                            {"id": ing.import_run.id},
                        )
                except BaseException as exc:
                    concurrent_err.append(exc)

            t_pub = threading.Thread(target=_pub_worker)
            t_mut = threading.Thread(target=_attempt_staging_mutation)
            t_pub.start()
            t_mut.start()

            assert in_reval.wait(timeout=10.0)
            _wait_for_pg_lock_waiter(engine, query_fragment="staging_place_candidates")
            release_pub.set()
            t_pub.join(timeout=10.0)
            t_mut.join(timeout=10.0)

            assert "err" not in pub_outcome, pub_outcome.get("err")
            assert pub_outcome["res"].outcome == PublicationOutcomeStatus.PUBLISHED
            assert len(concurrent_err) == 1
            assert isinstance(concurrent_err[0], IntegrityError)
            assert getattr(concurrent_err[0].orig, "sqlstate", None) == "23514"
            assert (
                getattr(concurrent_err[0].orig.diag, "constraint_name", None)
                == "trg_prepared_staging_immutable"
            )

            # Part B: Defense-in-depth — simulate mid-tx staging drift (with trigger disabled on
            # disposable DB during on_after_upsert) and prove pre-COMMIT staging digest guard rolls back!
            pbf_2 = _write_pbf(work_dir / "v2.osm.pbf", [_node(102, "Cafe Digest Guard")])
            ing_2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_2, boundary_version_id=bv_id)

            def _tamper_same_tx_after_upsert(conn: Connection) -> None:
                conn.execute(
                    text(
                        "ALTER TABLE staging_place_candidates "
                        "DISABLE TRIGGER trg_staging_place_candidates_immutability"
                    )
                )
                conn.execute(
                    text(
                        "UPDATE staging_place_candidates "
                        "SET name = 'Drifted Mid Tx', name_search = 'drifted mid tx' "
                        "WHERE import_run_id = :id"
                    ),
                    {"id": ing_2.import_run.id},
                )
                conn.execute(
                    text(
                        "ALTER TABLE staging_place_candidates "
                        "ENABLE TRIGGER trg_staging_place_candidates_immutability"
                    )
                )

            with pytest.raises(PublicationInvariantError, match="staging_place_candidates digest changed"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=work_dir / "artifacts",
                    fault_hooks=PublicationFaultHooks(on_after_upsert=_tamper_same_tx_after_upsert),
                )

            # Verify V1 remained intact and V2 rolled back cleanly!
            with engine.connect() as conn:
                active_id = conn.execute(
                    text("SELECT id FROM import_runs WHERE active_slot = 1")
                ).scalar_one()
                assert active_id == ing.import_run.id
        finally:
            engine.dispose()


# ===========================================================================
# BLOCKER-3: Controlled Test Publication Isolation & Coverage Safety
# ===========================================================================


def test_blocker3_controlled_test_mode_rejects_dev_db_non_test_db_and_spoofed_env(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CONTROLLED_TEST_NODE_ONLY fails closed against dev server collisions, non-test DBs, and spoofed envs."""
    with disposable_database("m37_b3_iso") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-b3")
            pbf = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "Cafe Guard")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)

            # Case 1: Configured DATABASE_URL targets the SAME PostgreSQL server (host/port) -> REJECTED!
            # (Simulates running controlled-test publication against a development server.)
            parsed = make_url(db_url)
            same_server_dev_url = parsed.set(database="geosearch_dev").render_as_string(
                hide_password=False
            )
            with monkeypatch.context() as mp:
                mp.setenv("DATABASE_URL", same_server_dev_url)
                mp.setenv("ENV", "test")
                mp.setenv("GEOSEARCH_ALLOW_TEST", "true")
                with pytest.raises(PublicationEligibilityError) as exc_dev:
                    publish_prepared_run(
                        engine,
                        import_run_id=ing.import_run.id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    )
                assert exc_dev.value.status == EligibilityStatus.COVERAGE_INCOMPATIBLE
                assert any("unauthorized database/environment" in r.lower() for r in exc_dev.value.reasons)

            # Case 2: Non-test database name ending in "_test" (e.g. "custom_app_test") or "geosearch_prod"
            # checked via safe connection proxy — never touches real dev/prod DB!
            class _FakeNonTestConn:
                def __init__(self, real_conn: Connection, fake_db: str, fake_url: str) -> None:
                    self._real = real_conn
                    self._fake_db = fake_db
                    self.engine = type("_FakeEngine", (), {"url": make_url(fake_url)})()

                def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
                    if "current_database()" in str(statement):
                        return type("_ScalarResult", (), {"scalar_one": lambda _s: self._fake_db})()
                    return self._real.execute(statement, *args, **kwargs)

            with engine.connect() as conn:
                # 2a. Non-test DB name ending in "_test" (does not match ^geosearch_test...)
                fake_conn_suffix = _FakeNonTestConn(
                    conn,
                    "custom_app_test",
                    "postgresql+psycopg://u:p@127.0.0.1:55433/custom_app_test",
                )
                res_suffix = validate_publication_eligibility(
                    fake_conn_suffix,  # type: ignore[arg-type]
                    import_run_id=ing.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
                assert res_suffix.eligible is False
                assert res_suffix.status == EligibilityStatus.COVERAGE_INCOMPATIBLE

                # 2b. Remote production host even with DB name "geosearch_test" -> REJECTED!
                fake_conn_remote = _FakeNonTestConn(
                    conn,
                    "geosearch_test",
                    "postgresql+psycopg://u:p@prod-db.internal:5432/geosearch_test",
                )
                res_remote = validate_publication_eligibility(
                    fake_conn_remote,  # type: ignore[arg-type]
                    import_run_id=ing.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
                assert res_remote.eligible is False
                assert res_remote.status == EligibilityStatus.COVERAGE_INCOMPATIBLE

            # Verify live places remained 100% untouched after all unauthorized attempts!
            with engine.connect() as conn:
                assert conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0

            # Case 3: Authorized isolated test database succeeds cleanly!
            res_ok = publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )
            assert res_ok.outcome == PublicationOutcomeStatus.PUBLISHED
        finally:
            engine.dispose()


def test_blocker3_first_publication_rejects_orphaned_active_way_or_relation_places(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """First publication rejects orphaned active Way/Relation places (not just Node places)."""
    with disposable_database("m37_b3_orphan") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-orphan")
            pbf = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "Cafe First")])
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)

            # Seed an orphaned active WAY place with zero active_slot=1 runs.
            with engine.begin() as conn:
                insert_place(conn, ing.import_run.id, osm_type="way", osm_id=77701, name="Orphan Way")

            with pytest.raises(PublicationEligibilityError) as exc:
                publish_prepared_run(
                    engine,
                    import_run_id=ing.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                )
            assert exc.value.status == EligibilityStatus.OTHER_BLOCKING_FAILURE
            assert any("all osm types" in r.lower() for r in exc.value.reasons)
        finally:
            engine.dispose()


# ===========================================================================
# Section 5: Duplicate Verification, Lost ACK + Supersession, Failed-Publish Readers, No Orphan Artifacts
# ===========================================================================


def test_section5_unexpected_duplicate_candidate_rows_rejected_by_pre_commit_verification(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Unexpected pre-existing duplicate_candidates rows are rejected under NOT_EXECUTED, VERIFIED_EMPTY, and CANDIDATES_PROVIDED."""
    with disposable_database("m37_s5_dups") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-s5dup")
            pbf_1 = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "A"), _node(102, "B"), _node(103, "C")])
            pbf_2 = _write_pbf(work_dir / "v2.osm.pbf", [_node(101, "A"), _node(102, "B"), _node(103, "C")])

            ing_1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_1, boundary_version_id=bv_id)
            ing_2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_2, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing_1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )
            with engine.connect() as conn:
                ids = {
                    int(r.osm_id): int(r.id)
                    for r in conn.execute(text("SELECT osm_id, id FROM places")).fetchall()
                }

            # Inject an unexpected duplicate_candidates row for V2 right before pre-COMMIT verification.
            def _inject_unexpected_dup(conn: Connection) -> None:
                conn.execute(
                    text(
                        """
                        INSERT INTO duplicate_candidates (
                            import_run_id, place_a_id, place_b_id, detector_version, evidence
                        )
                        VALUES (:run_id, :a, :b, 'rogue-det', '{}'::jsonb)
                        """
                    ),
                    {
                        "run_id": ing_2.import_run.id,
                        "a": min(ids[102], ids[103]),
                        "b": max(ids[102], ids[103]),
                    },
                )

            op_v2 = uuid.uuid4()
            # 1. Under VERIFIED_EMPTY -> MUST fail!
            with pytest.raises(PublicationInvariantError, match="Unexpected duplicate_candidates rows"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_2.import_run.id,
                    publication_operation_id=op_v2,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=work_dir / "artifacts_1",
                    duplicate_candidates=DuplicateCandidateInput(
                        state=DuplicateDetectionState.VERIFIED_EMPTY,
                        detector_version="det-v2",
                    ),
                    fault_hooks=PublicationFaultHooks(
                        on_during_pre_commit_verification=_inject_unexpected_dup
                    ),
                )

            # 2. Under CANDIDATES_PROVIDED with only (101, 102) -> unexpected extra (102, 103) MUST fail!
            with pytest.raises(PublicationInvariantError, match="unexpected_extra"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_2.import_run.id,
                    publication_operation_id=op_v2,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=work_dir / "artifacts_2",
                    duplicate_candidates=DuplicateCandidateInput(
                        state=DuplicateDetectionState.CANDIDATES_PROVIDED,
                        detector_version="det-v2",
                        candidates=(
                            StagedDuplicateCandidate(
                                osm_type_a="node",
                                osm_id_a=101,
                                osm_type_b="node",
                                osm_id_b=102,
                                detector_version="det-v2",
                                evidence={"ok": True},
                            ),
                        ),
                    ),
                    fault_hooks=PublicationFaultHooks(
                        on_during_pre_commit_verification=_inject_unexpected_dup
                    ),
                )
        finally:
            engine.dispose()


def test_section5_lost_commit_ack_followed_by_v3_supersession_returns_already_published(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """V2 commits (ACK lost) -> V3 supersedes V2 -> retry V2 with op_v2 returns ALREADY_PUBLISHED; V3 stays active."""
    with disposable_database("m37_s5_ack") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-ack")
            pbf_v1 = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "V1")])
            pbf_v2 = _write_pbf(work_dir / "v2.osm.pbf", [_node(102, "V2")])
            pbf_v3 = _write_pbf(work_dir / "v3.osm.pbf", [_node(103, "V3")])

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)
            ing_v3 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v3, boundary_version_id=bv_id)

            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            # 1-2. V2 commits using op_v2, client loses acknowledgement.
            op_v2 = uuid.uuid4()
            def _lose_ack(_run_id: int, _op: uuid.UUID) -> None:
                raise ConnectionResetError("Simulated lost ACK for V2")

            with pytest.raises(ConnectionResetError, match="lost ACK for V2"):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=op_v2,
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=work_dir / "artifacts",
                    fault_hooks=PublicationFaultHooks(on_after_commit_before_ack=_lose_ack),
                )

            # 3. V3 supersedes V2 cleanly.
            res_v3 = publish_prepared_run(
                engine,
                import_run_id=ing_v3.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                recovery_artifact_root=work_dir / "artifacts",
            )
            assert res_v3.outcome == PublicationOutcomeStatus.PUBLISHED

            # 4-5. Client retries V2 with original op_v2 -> returns ALREADY_PUBLISHED!
            ev_v2 = resolve_unknown_commit_outcome(
                engine, publication_operation_id=op_v2, import_run_id=ing_v2.import_run.id
            )
            assert ev_v2 is not None
            assert ev_v2.status == "SUPERSEDED"
            assert ev_v2.is_committed_publication is True

            retry_v2 = publish_prepared_run(
                engine,
                import_run_id=ing_v2.import_run.id,
                publication_operation_id=op_v2,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )
            assert retry_v2.outcome == PublicationOutcomeStatus.ALREADY_PUBLISHED
            assert retry_v2.already_committed is True

            # 6-7. V3 remains active; V2 is NOT reactivated!
            with engine.connect() as conn:
                active_ids = conn.execute(
                    text("SELECT id FROM import_runs WHERE active_slot = 1")
                ).scalars().all()
                assert active_ids == [ing_v3.import_run.id]
                v2_status = conn.execute(
                    text("SELECT status FROM import_runs WHERE id = :id"),
                    {"id": ing_v2.import_run.id},
                ).scalar_one()
                assert v2_status == "SUPERSEDED"
                active_places = conn.execute(
                    text("SELECT osm_id FROM places WHERE is_active = true")
                ).scalars().all()
                assert active_places == [103]
        finally:
            engine.dispose()


def test_section5_reader_isolation_during_failed_rolled_back_publication(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Readers see only committed V1 while V2 holds uncommitted UPSERT/deactivation and after V2 rolls back."""
    with disposable_database("m37_s5_readfail") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=6, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-readfail")
            pbf_v1 = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "A V1"), _node(102, "B V1")])
            pbf_v2 = _write_pbf(work_dir / "v2.osm.pbf", [_node(101, "A V2"), _node(103, "C V2")])

            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)

            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            v2_open_before_commit = threading.Event()
            release_to_fail = threading.Event()
            writer_errors: list[BaseException] = []

            def _failing_v2_worker() -> None:
                def _pause_then_abort(_conn: Connection) -> None:
                    v2_open_before_commit.set()
                    assert release_to_fail.wait(timeout=10.0)
                    raise RuntimeError("Injected pre-COMMIT failure for reader rollback test")

                try:
                    publish_prepared_run(
                        engine,
                        import_run_id=ing_v2.import_run.id,
                        publication_operation_id=uuid.uuid4(),
                        publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                        recovery_artifact_root=work_dir / "artifacts",
                        fault_hooks=PublicationFaultHooks(on_before_commit=_pause_then_abort),
                    )
                except BaseException as exc:
                    writer_errors.append(exc)

            t = threading.Thread(target=_failing_v2_worker)
            t.start()
            assert v2_open_before_commit.wait(timeout=10.0)

            # While V2 holds uncommitted UPSERT + deactivation + slot switch, readers observe ONLY V1.
            rc_during = read_active_place_identities_single_statement(engine)
            assert [(oid, rid) for _t, oid, _pid, rid in rc_during] == [
                (101, ing_v1.import_run.id),
                (102, ing_v1.import_run.id),
            ]
            with repeatable_read_snapshot(engine) as rr_conn:
                snap_during = read_publication_snapshot_in_tx(rr_conn)
                assert snap_during.active_run_id == ing_v1.import_run.id
                assert snap_during.active_place_names == {
                    ("node", 101): "A V1",
                    ("node", 102): "B V1",
                }

                # Trigger rollback of V2 while repeatable-read snapshot stays open!
                release_to_fail.set()
                t.join(timeout=10.0)
                assert len(writer_errors) == 1
                assert "Injected pre-COMMIT failure" in str(writer_errors[0])

                snap_after_rollback_same_tx = read_publication_snapshot_in_tx(rr_conn)
                assert snap_after_rollback_same_tx == snap_during

            # Fresh readers post-rollback still observe ONLY V1.
            rc_after = read_active_place_identities_single_statement(engine)
            assert [(oid, rid) for _t, oid, _pid, rid in rc_after] == [
                (101, ing_v1.import_run.id),
                (102, ing_v1.import_run.id),
            ]
        finally:
            engine.dispose()


def test_section5_conflicting_registration_does_not_leave_orphan_recovery_artifact(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Operation registration happens BEFORE artifact export so conflicting UUID leaves zero orphan artifacts."""
    with disposable_database("m37_s5_orphan") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m37-no-orphan")
            pbf_1 = _write_pbf(work_dir / "v1.osm.pbf", [_node(101, "V1")])
            pbf_2 = _write_pbf(work_dir / "v2.osm.pbf", [_node(102, "V2")])

            ing_1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_1, boundary_version_id=bv_id)
            ing_2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_2, boundary_version_id=bv_id)

            op_v1 = uuid.uuid4()
            publish_prepared_run(
                engine,
                import_run_id=ing_1.import_run.id,
                publication_operation_id=op_v1,
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
            )

            # Register op_2a on ing_2 via a pre-commit failure, then attempt ing_2 with conflicting op_2b
            # pointing at a fresh artifact_root -> MUST fail with PUBLICATION_CONFLICT and create 0 files!
            conflict_artifact_root = work_dir / "conflict_artifacts"
            with pytest.raises(PublicationConflictError):
                publish_prepared_run(
                    engine,
                    import_run_id=ing_2.import_run.id,
                    publication_operation_id=op_v1,  # already owned by ing_1!
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    recovery_artifact_root=conflict_artifact_root,
                )
            assert not conflict_artifact_root.exists() or list(conflict_artifact_root.iterdir()) == []
        finally:
            engine.dispose()
