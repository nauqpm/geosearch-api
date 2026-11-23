"""M2.6 data integrity, source TOCTOU, strict readiness, concurrency, and SQL constraint tests."""

from __future__ import annotations

import shutil
import threading
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from pathlib import Path

import osmium
import pytest
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError

from geosearch.db import create_test_engine
from geosearch.ingestion import (
    ExtractionStats,
    ImportRunStateError,
    NormalizationIssue,
    OsmSourceError,
    StagingReadinessError,
    VerifiedSourceSnapshot,
    begin_staging_run,
    build_canonical_candidate,
    compute_file_sha256,
    compute_issue_fingerprint,
    create_import_run,
    ingest_osm_nodes_to_staging,
    mark_extraction_completed,
    mark_staging_failed,
    record_import_issue,
    stage_candidate_batch,
    verify_and_prepare_run,
)
from tests.helpers import (
    VALID_SHA256,
    assert_pg_error,
    insert_boundary_version,
    insert_import_run,
    upgrade_test_db_to_head,
)

FIXTURE_OSM_XML = Path(__file__).resolve().parent / "fixtures" / "sample_synthetic_nodes.osm"

# Complex synthetic MultiPolygon (SRID 4326) with hole and island (NOT real HCMC geography).
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


def _make_cafe(
    *,
    osm_id: int = 1001,
    name: str = "Cà Phê Test",
    lon: float = 106.7009,
    lat: float = 10.7769,
):
    return build_canonical_candidate(
        osm_type="node",
        osm_id=osm_id,
        category="cafe",
        raw_name=name,
        longitude=lon,
        latitude=lat,
        raw_tags={"amenity": "cafe", "name": name},
        source_provenance={"source_name": "synthetic.osm", "extraction_scope": "node_only"},
    )


# ---------------------------------------------------------------------------
# BLOCKER-1: Retry idempotency, partial commit resume, changed diagnostics
# ---------------------------------------------------------------------------


def test_blocker1_retry_after_partial_batch_commit_is_idempotent_and_non_inflating(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """Partial commit failure followed by resume yields exact unique rows, issues, and pass counters."""
    with disposable_database("m26_partial_retry") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m26-partial")

            def _fail_batch_2(batch_index: int, _conn: Connection) -> None:
                if batch_index == 2:
                    raise RuntimeError("Transient crash on batch 2")

            with pytest.raises(RuntimeError, match="Transient crash on batch 2"):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=FIXTURE_OSM_XML,
                    boundary_version_id=bv_id,
                    batch_size=2,
                    on_before_batch_commit=_fail_batch_2,
                )

            with engine.connect() as conn:
                run_id = int(conn.execute(text("SELECT id FROM import_runs")).scalar_one())
                pre_ids = conn.execute(
                    text(
                        "SELECT osm_id FROM staging_place_candidates "
                        "WHERE import_run_id = :id ORDER BY osm_id"
                    ),
                    {"id": run_id},
                ).scalars().all()
                assert pre_ids == [1001, 1002]

            # Resume same run without error: replays Batch 1 identically and inserts Batches 2..N.
            resumed = ingest_osm_nodes_to_staging(
                engine,
                source_path=FIXTURE_OSM_XML,
                boundary_version_id=bv_id,
                import_run_id=run_id,
                batch_size=2,
                prepare_on_completion=True,
            )
            assert resumed.import_run.status == "PREPARED"
            assert resumed.staged_candidates == 4  # newly inserted in pass 2
            assert resumed.duplicate_identical == 2  # replayed from pass 1
            assert resumed.issues_recorded == 3
            assert resumed.duplicate_issues_skipped == 0
            assert resumed.readiness_report is not None
            assert resumed.readiness_report.staged_candidate_count == 6
            assert resumed.readiness_report.pass_inserted_count == 4
            assert resumed.readiness_report.duplicate_identical_count == 2
            assert resumed.readiness_report.issue_count == 3
            assert resumed.readiness_report.resume_count == 1
            assert resumed.readiness_report.current_pass_number == 2
            assert resumed.readiness_report.stale_staged_count == 0

            with engine.connect() as conn:
                assert (
                    conn.execute(
                        text(
                            "SELECT count(*) FROM staging_place_candidates "
                            "WHERE import_run_id = :id"
                        ),
                        {"id": run_id},
                    ).scalar_one()
                    == 6
                )
                assert (
                    conn.execute(
                        text("SELECT count(*) FROM import_issues WHERE import_run_id = :id"),
                        {"id": run_id},
                    ).scalar_one()
                    == 3
                )
        finally:
            engine.dispose()


def test_blocker1_nullable_osm_identity_issue_idempotency_and_changed_diagnostics(
    migrated_conn: Connection,
) -> None:
    """Identical NULL-identity issues deduplicate; changed diagnostics record a new unique issue."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-m26-issues")
    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256=run.source_checksum_sha256,
    )

    null_id_issue = NormalizationIssue(
        severity="ERROR",
        stage="VALIDATE",
        issue_code="INVALID_OSM_IDENTITY",
        message="OSM ID must be positive",
        osm_type=None,
        osm_id=None,
        details={"raw_osm_type": "node", "raw_osm_id": -7},
    )
    id_1 = record_import_issue(migrated_conn, import_run_id=run.id, issue=null_id_issue)
    id_2 = record_import_issue(migrated_conn, import_run_id=run.id, issue=null_id_issue)
    assert id_1 == id_2

    # Changed diagnostic details must produce a new distinct persisted issue.
    changed_issue = NormalizationIssue(
        severity="ERROR",
        stage="VALIDATE",
        issue_code="INVALID_OSM_IDENTITY",
        message="OSM ID must be positive",
        osm_type=None,
        osm_id=None,
        details={"raw_osm_type": "node", "raw_osm_id": -99},
    )
    id_3 = record_import_issue(migrated_conn, import_run_id=run.id, issue=changed_issue)
    assert id_3 != id_1

    # Calling mark_staging_failed twice with identical SOURCE_INTEGRITY_ERROR does not duplicate.
    mark_staging_failed(
        migrated_conn,
        import_run_id=run.id,
        error_message="Checksum mismatch",
        issue_code="SOURCE_INTEGRITY_ERROR",
        details={"reason": "modified"},
    )
    mark_staging_failed(
        migrated_conn,
        import_run_id=run.id,
        error_message="Checksum mismatch",
        issue_code="SOURCE_INTEGRITY_ERROR",
        details={"reason": "modified"},
    )

    total_issues = migrated_conn.execute(
        text("SELECT count(*) FROM import_issues WHERE import_run_id = :id"),
        {"id": run.id},
    ).scalar_one()
    cp_issue_count = migrated_conn.execute(
        text("SELECT issue_count FROM staging_run_checkpoints WHERE import_run_id = :id"),
        {"id": run.id},
    ).scalar_one()
    assert total_issues == 3
    assert cp_issue_count == 3


# ---------------------------------------------------------------------------
# BLOCKER-2: Source integrity, isolated snapshot, TOCTOU, truncation
# ---------------------------------------------------------------------------


def test_blocker2_source_modified_between_checksum_and_parse_is_rejected(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Modifying the source file after checksum fails closed with SOURCE_INTEGRITY_ERROR."""
    source_copy = work_dir / "mutable_source.osm"
    shutil.copyfile(FIXTURE_OSM_XML, source_copy)

    with disposable_database("m26_toctou_mod") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m26-toctou")

            snapshot_seen: list[Path] = []

            def _mutate_original_before_parse(snapshot: VerifiedSourceSnapshot) -> None:
                snapshot_seen.append(snapshot.snapshot_path)
                # Mutate the original source file right after checksum/snapshot creation.
                source_copy.write_text(
                    source_copy.read_text(encoding="utf-8") + "\n<!-- mutated -->\n",
                    encoding="utf-8",
                )

            with pytest.raises(OsmSourceError, match="modified, replaced, or truncated"):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=source_copy,
                    boundary_version_id=bv_id,
                    on_before_parse=_mutate_original_before_parse,
                )

            # Temporary isolated snapshot must be cleaned up on exit.
            assert len(snapshot_seen) == 1
            assert not snapshot_seen[0].exists()

            with engine.connect() as conn:
                run_id, status = conn.execute(
                    text("SELECT id, status FROM import_runs")
                ).one()
                assert status == "FAILED"
                issue_codes = conn.execute(
                    text("SELECT issue_code FROM import_issues WHERE import_run_id = :id"),
                    {"id": run_id},
                ).scalars().all()
                assert issue_codes == ["SOURCE_INTEGRITY_ERROR"]
                with pytest.raises(StagingReadinessError):
                    verify_and_prepare_run(conn, import_run_id=run_id)
        finally:
            engine.dispose()


def test_blocker2_source_replaced_with_other_valid_file_and_clean_truncation_rejected(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Replacing source between checksum/parse OR cleanly truncated parseable file vs approved SHA fails."""
    full_pbf = _write_pbf(
        work_dir / "approved_full.osm.pbf",
        [
            osmium.osm.mutable.Node(
                id=101,
                location=(106.7009, 10.7769),
                tags={"amenity": "cafe", "name": "Cafe A"},
            ),
            osmium.osm.mutable.Node(
                id=102,
                location=(106.7019, 10.7779),
                tags={"amenity": "cafe", "name": "Cafe B"},
            ),
        ],
    )
    approved_sha = compute_file_sha256(full_pbf)

    # Cleanly truncated (still valid PBF!) containing only 1 node instead of 2.
    cleanly_truncated_pbf = _write_pbf(
        work_dir / "truncated_valid.osm.pbf",
        [
            osmium.osm.mutable.Node(
                id=101,
                location=(106.7009, 10.7769),
                tags={"amenity": "cafe", "name": "Cafe A"},
            ),
        ],
    )

    with disposable_database("m26_toctou_swap") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m26-swap")

            # Case 1: Cleanly truncated parseable file differs from approved checksum.
            with pytest.raises(OsmSourceError, match="checksum mismatch"):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=cleanly_truncated_pbf,
                    boundary_version_id=bv_id,
                    expected_source_checksum_sha256=approved_sha,
                )

            # Case 2: Source file replaced with another valid PBF right before parse.
            swappable = work_dir / "swappable.osm.pbf"
            shutil.copyfile(full_pbf, swappable)

            def _replace_with_truncated(_snapshot: VerifiedSourceSnapshot) -> None:
                shutil.copyfile(cleanly_truncated_pbf, swappable)

            with pytest.raises(OsmSourceError, match="modified, replaced, or truncated"):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=swappable,
                    boundary_version_id=bv_id,
                    on_before_parse=_replace_with_truncated,
                )

            with engine.connect() as conn:
                statuses = conn.execute(
                    text("SELECT status FROM import_runs ORDER BY id")
                ).scalars().all()
                assert statuses == ["FAILED", "FAILED"]
                codes = conn.execute(
                    text("SELECT issue_code FROM import_issues ORDER BY id")
                ).scalars().all()
                assert codes == ["SOURCE_INTEGRITY_ERROR", "SOURCE_INTEGRITY_ERROR"]
        finally:
            engine.dispose()


def test_blocker2_midstream_truncated_xml_fails_safely_and_blocks_partial_batches(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Mid-stream truncated OSM XML fails safely; already-committed Batch 1 cannot become PREPARED."""
    full_xml = FIXTURE_OSM_XML.read_text(encoding="utf-8")
    # Truncate midway after the first 3 nodes so Batch 1 (batch_size=2) commits before EOF error.
    cut_index = full_xml.index('id="1004"')
    truncated_xml = work_dir / "midstream_truncated.osm"
    truncated_xml.write_text(full_xml[:cut_index], encoding="utf-8")

    with disposable_database("m26_mid_trunc") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m26-midtrunc")

            with pytest.raises(OsmSourceError):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=truncated_xml,
                    boundary_version_id=bv_id,
                    batch_size=2,
                )

            with engine.connect() as conn:
                run_id, status = conn.execute(
                    text("SELECT id, status FROM import_runs")
                ).one()
                assert status == "FAILED"
                staged_count = conn.execute(
                    text(
                        "SELECT count(*) FROM staging_place_candidates "
                        "WHERE import_run_id = :id"
                    ),
                    {"id": run_id},
                ).scalar_one()
                # Small truncated XML fails inside libosmium's first read buffer before staging.
                assert staged_count == 0
                issue_codes = conn.execute(
                    text("SELECT issue_code FROM import_issues WHERE import_run_id = :id"),
                    {"id": run_id},
                ).scalars().all()
                assert issue_codes == ["SOURCE_INTEGRITY_ERROR"]
                with pytest.raises(StagingReadinessError):
                    verify_and_prepare_run(conn, import_run_id=run_id)
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# BLOCKER-3: Boundary association, ST_Covers, exact accounting, empty staging
# ---------------------------------------------------------------------------


def test_blocker3_mismatched_boundary_id_and_unfiltered_outside_points_rejected(
    migrated_conn: Connection,
) -> None:
    """Mismatched boundary_version_id is rejected; unfiltered out-of-boundary rows fail readiness."""
    bv_1 = insert_boundary_version(migrated_conn, version_code="bv-m26-1")
    bv_2 = insert_boundary_version(migrated_conn, version_code="bv-m26-2")

    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_1,
        source_name="fixture.osm",
        source_checksum_sha256=VALID_SHA256,
        apply_boundary_filter=False,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256=run.source_checksum_sha256,
    )

    # Attempting to override boundary_version_id with a different boundary fails closed.
    with pytest.raises(ImportRunStateError, match="Mismatched boundary_version_id"):
        stage_candidate_batch(
            migrated_conn,
            import_run_id=run.id,
            candidates=[_make_cafe(osm_id=1)],
            boundary_version_id=bv_2,
        )

    # Staging out-of-boundary point with apply_boundary_filter=False MUST fail readiness ST_Covers!
    inside = _make_cafe(osm_id=10, lon=106.7009, lat=10.7769)
    outside = _make_cafe(osm_id=11, name="Outside Hanoi Cafe", lon=105.8542, lat=21.0285)
    stage_candidate_batch(
        migrated_conn,
        import_run_id=run.id,
        candidates=[inside, outside],
        apply_boundary_filter=False,
    )
    mark_extraction_completed(
        migrated_conn,
        import_run_id=run.id,
        stats=ExtractionStats(
            total_nodes_seen=2,
            supported_nodes_seen=2,
            unsupported_nodes_seen=0,
            valid_candidates=2,
            invalid_candidates=0,
        ),
        source_verified_post_parse=True,
    )

    with pytest.raises(StagingReadinessError, match="ST_Covers"):
        verify_and_prepare_run(migrated_conn, import_run_id=run.id)


def test_blocker3_complex_multipolygon_st_covers_all_spatial_cases(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Ingest verifies ST_Covers for interior, outer edge, vertex, hole, hole edge, island, outside."""
    bv_id = insert_boundary_version(
        migrated_conn,
        version_code="bv-m26-complex",
        geom_wkt=COMPLEX_MULTIPOLYGON_WKT,
    )
    pbf = _write_pbf(
        work_dir / "complex_spatial.osm.pbf",
        [
            # Covered (True):
            osmium.osm.mutable.Node(
                id=1, location=(1.5, 5.0), tags={"amenity": "cafe", "name": "Interior"}
            ),
            osmium.osm.mutable.Node(
                id=2, location=(0.0, 5.0), tags={"amenity": "cafe", "name": "Outer Edge"}
            ),
            osmium.osm.mutable.Node(
                id=3, location=(10.0, 10.0), tags={"amenity": "cafe", "name": "Outer Vertex"}
            ),
            osmium.osm.mutable.Node(
                id=4, location=(3.0, 5.0), tags={"amenity": "cafe", "name": "Hole Boundary Edge"}
            ),
            osmium.osm.mutable.Node(
                id=5, location=(22.0, 2.0), tags={"amenity": "cafe", "name": "Island Interior"}
            ),
            osmium.osm.mutable.Node(
                id=6, location=(20.0, 2.0), tags={"amenity": "cafe", "name": "Island Edge"}
            ),
            # Outside (False):
            osmium.osm.mutable.Node(
                id=7, location=(5.0, 5.0), tags={"amenity": "cafe", "name": "Hole Interior"}
            ),
            osmium.osm.mutable.Node(
                id=8, location=(15.0, 5.0), tags={"amenity": "cafe", "name": "Between Main Island"}
            ),
            osmium.osm.mutable.Node(
                id=9, location=(-1.0, 5.0), tags={"amenity": "cafe", "name": "Outside West"}
            ),
        ],
    )

    report = ingest_osm_nodes_to_staging(
        migrated_conn,
        source_path=pbf,
        boundary_version_id=bv_id,
        batch_size=4,
        apply_boundary_filter=True,
        prepare_on_completion=True,
    )
    assert report.import_run.status == "PREPARED"
    assert report.staged_candidates == 6
    assert report.outside_boundary == 3
    assert report.readiness_report is not None
    assert report.readiness_report.boundary_filter_verified is True
    assert report.readiness_report.uncovered_staged_count == 0
    assert report.readiness_report.extraction_scope == "node_only"

    staged_ids = migrated_conn.execute(
        text(
            "SELECT osm_id FROM staging_place_candidates "
            "WHERE import_run_id = :id ORDER BY osm_id"
        ),
        {"id": report.import_run.id},
    ).scalars().all()
    assert staged_ids == [1, 2, 3, 4, 5, 6]


def test_blocker3_empty_staging_stale_rows_and_unverified_post_parse_rejected(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Empty staging, stale/extra rows, truncated counters, and unverified source all fail readiness."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-m26-empty")

    # 1. Empty staging (all POIs outside boundary) MUST fail closed.
    empty_pbf = _write_pbf(
        work_dir / "outside_only.osm.pbf",
        [
            osmium.osm.mutable.Node(
                id=501,
                location=(105.8542, 21.0285),
                tags={"amenity": "cafe", "name": "Hanoi Outside"},
            )
        ],
    )
    with pytest.raises(StagingReadinessError, match="empty"):
        ingest_osm_nodes_to_staging(
            migrated_conn,
            source_path=empty_pbf,
            boundary_version_id=bv_id,
        )

    # 2. Stale/extra staging row not seen in final pass MUST fail readiness.
    run_stale = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="stale.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run_stale.id,
        expected_source_fingerprint=run_stale.source_fingerprint,
        expected_source_checksum_sha256=run_stale.source_checksum_sha256,
    )
    # Pass 1 stages two rows (last_seen_pass=1).
    stage_candidate_batch(
        migrated_conn,
        import_run_id=run_stale.id,
        candidates=[_make_cafe(osm_id=601), _make_cafe(osm_id=602)],
    )
    # Pass 2 resumes (current_pass_number=2) but only replays osm_id=601 (osm_id=602 stays stale!).
    begin_staging_run(
        migrated_conn,
        import_run_id=run_stale.id,
        expected_source_fingerprint=run_stale.source_fingerprint,
        expected_source_checksum_sha256=run_stale.source_checksum_sha256,
    )
    stage_candidate_batch(
        migrated_conn,
        import_run_id=run_stale.id,
        candidates=[_make_cafe(osm_id=601)],
    )
    mark_extraction_completed(
        migrated_conn,
        import_run_id=run_stale.id,
        stats=ExtractionStats(
            total_nodes_seen=1,
            supported_nodes_seen=1,
            unsupported_nodes_seen=0,
            valid_candidates=1,
            invalid_candidates=0,
        ),
        source_verified_post_parse=True,
    )
    with pytest.raises(StagingReadinessError, match="stale/unverified"):
        verify_and_prepare_run(migrated_conn, import_run_id=run_stale.id)

    # 3. Truncated/mismatched pass counters rejected by mark_extraction_completed.
    run_trunc = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="trunc.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run_trunc.id,
        expected_source_fingerprint=run_trunc.source_fingerprint,
        expected_source_checksum_sha256=run_trunc.source_checksum_sha256,
    )
    stage_candidate_batch(
        migrated_conn,
        import_run_id=run_trunc.id,
        candidates=[_make_cafe(osm_id=701)],
    )
    with pytest.raises(ImportRunStateError, match="accounting mismatch"):
        mark_extraction_completed(
            migrated_conn,
            import_run_id=run_trunc.id,
            stats=ExtractionStats(
                total_nodes_seen=5,
                supported_nodes_seen=5,
                unsupported_nodes_seen=0,
                valid_candidates=5,
                invalid_candidates=0,
            ),
        )

    # 4. source_verified_post_parse=False rejected at readiness.
    mark_extraction_completed(
        migrated_conn,
        import_run_id=run_trunc.id,
        stats=ExtractionStats(
            total_nodes_seen=1,
            supported_nodes_seen=1,
            unsupported_nodes_seen=0,
            valid_candidates=1,
            invalid_candidates=0,
        ),
        source_verified_post_parse=False,
    )
    with pytest.raises(StagingReadinessError, match="source_verified_post_parse"):
        verify_and_prepare_run(migrated_conn, import_run_id=run_trunc.id)

    # 5. Missing checkpoint rejected at readiness.
    no_cp_run = insert_import_run(migrated_conn, bv_id, status="PROCESSING")
    with pytest.raises(StagingReadinessError, match="Missing staging_run_checkpoints"):
        verify_and_prepare_run(migrated_conn, import_run_id=no_cp_run)


# ---------------------------------------------------------------------------
# Section 5: Deterministic concurrency test (FOR UPDATE serialization)
# ---------------------------------------------------------------------------


def test_concurrent_staging_writer_blocked_during_verify_and_prepare_run(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """A concurrent staging writer cannot slip rows in between verification and PREPARED commit."""
    with disposable_database("m26_concur") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=4, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m26-concur")
                run = create_import_run(
                    conn,
                    boundary_version_id=bv_id,
                    source_name="concur.osm",
                    source_checksum_sha256=VALID_SHA256,
                )
                begin_staging_run(
                    conn,
                    import_run_id=run.id,
                    expected_source_fingerprint=run.source_fingerprint,
                    expected_source_checksum_sha256=run.source_checksum_sha256,
                )
                stage_candidate_batch(
                    conn,
                    import_run_id=run.id,
                    candidates=[_make_cafe(osm_id=8001)],
                )
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

            verifier_holding_lock = threading.Event()
            writer_started = threading.Event()
            release_verifier = threading.Event()

            writer_outcome: dict[str, BaseException | None] = {"error": None}
            verifier_outcome: dict[str, BaseException | None] = {"error": None}

            def _verifier_worker() -> None:
                try:
                    with engine.begin() as conn:

                        def _sync_hook(_conn: Connection) -> None:
                            verifier_holding_lock.set()
                            assert writer_started.wait(timeout=5.0)
                            # Allow deterministic wait until writer is observed blocked in pg_locks
                            # (driven by release_verifier from main thread).
                            assert release_verifier.wait(timeout=5.0)

                        verify_and_prepare_run(
                            conn,
                            import_run_id=run.id,
                            on_before_prepare_commit=_sync_hook,
                        )
                except BaseException as exc:  # pragma: no cover - asserted below
                    verifier_outcome["error"] = exc

            def _staging_writer_worker() -> None:
                assert verifier_holding_lock.wait(timeout=5.0)
                writer_started.set()
                try:
                    with engine.begin() as conn:
                        # Attempts to inject a late batch while verifier holds FOR UPDATE lock.
                        stage_candidate_batch(
                            conn,
                            import_run_id=run.id,
                            candidates=[_make_cafe(osm_id=8002)],
                        )
                except BaseException as exc:
                    writer_outcome["error"] = exc

            t_verifier = threading.Thread(target=_verifier_worker)
            t_writer = threading.Thread(target=_staging_writer_worker)
            t_verifier.start()
            t_writer.start()

            assert verifier_holding_lock.wait(timeout=5.0)
            assert writer_started.wait(timeout=5.0)

            # Deterministically wait until PostgreSQL reports the writer waiting on the row lock.
            with engine.connect() as observer:
                for _ in range(200):
                    waiting = observer.execute(
                        text(
                            """
                            SELECT count(*)
                            FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND wait_event_type = 'Lock'
                              AND query LIKE '%FROM import_runs%'
                            """
                        )
                    ).scalar_one()
                    if waiting >= 1:
                        break
                    threading.Event().wait(0.01)

            release_verifier.set()
            t_verifier.join(timeout=10.0)
            t_writer.join(timeout=10.0)

            assert verifier_outcome["error"] is None
            assert isinstance(writer_outcome["error"], ImportRunStateError)
            assert "PREPARED" in str(writer_outcome["error"])

            with engine.connect() as conn:
                status = conn.execute(
                    text("SELECT status FROM import_runs WHERE id = :id"),
                    {"id": run.id},
                ).scalar_one()
                assert status == "PREPARED"
                staged_ids = conn.execute(
                    text(
                        "SELECT osm_id FROM staging_place_candidates "
                        "WHERE import_run_id = :id ORDER BY osm_id"
                    ),
                    {"id": run.id},
                ).scalars().all()
                assert staged_ids == [8001]
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# Section 8: All non-stageable run statuses & direct PostgreSQL CHECK tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status",
    ["PREPARED", "PUBLISHED", "SUPERSEDED", "FAILED", "ABORTED"],
)
def test_all_non_stageable_import_run_statuses_rejected(
    migrated_conn: Connection,
    status: str,
) -> None:
    """begin_staging_run rejects PREPARED, PUBLISHED, SUPERSEDED, FAILED, and ABORTED runs."""
    bv_id = insert_boundary_version(migrated_conn, version_code=f"bv-nonstage-{status.lower()}")
    now = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)
    kwargs: dict[str, object] = {"status": status}
    if status == "PUBLISHED":
        kwargs.update(
            publication_operation_id=uuid.uuid4(),
            published_at=now,
            active_slot=1,
        )
    elif status == "SUPERSEDED":
        kwargs.update(
            publication_operation_id=uuid.uuid4(),
            published_at=now,
            superseded_at=now,
            active_slot=None,
        )

    run_id = insert_import_run(migrated_conn, bv_id, **kwargs)  # type: ignore[arg-type]
    with pytest.raises(ImportRunStateError, match=status):
        begin_staging_run(
            migrated_conn,
            import_run_id=run_id,
            expected_source_fingerprint="fp-default-v1",
            expected_source_checksum_sha256=VALID_SHA256,
        )


def test_resume_rejects_mismatched_source_checksum(migrated_conn: Connection) -> None:
    """Resuming a PROCESSING run with a different source_checksum_sha256 fails closed."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-m26-badsha")
    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="fixture.osm",
        source_checksum_sha256="a" * 64,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256="a" * 64,
    )
    with pytest.raises(ImportRunStateError, match="checksum mismatch"):
        begin_staging_run(
            migrated_conn,
            import_run_id=run.id,
            expected_source_fingerprint=run.source_fingerprint,
            expected_source_checksum_sha256="b" * 64,
        )


def test_direct_postgres_check_constraints_on_staging_tables(
    migrated_conn: Connection,
) -> None:
    """Direct SQL negative tests prove PostgreSQL CHECK constraints enforce staging invariants."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-m26-sqlck")
    run_id = insert_import_run(migrated_conn, bv_id, status="PROCESSING")

    # 1. staging_place_candidates: invalid content_digest format rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_place_candidates (
                        import_run_id, osm_type, osm_id, name, name_search, category,
                        location, raw_tags, content_digest, source_provenance
                    )
                    VALUES (
                        :run_id, 'node', 1, 'Cafe', 'cafe', 'cafe',
                        ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography,
                        '{}'::jsonb, 'not-sha256', '{}'::jsonb
                    )
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_place_candidates_content_digest_format",
    )

    # 2. staging_place_candidates: last_seen_pass <= 0 rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_place_candidates (
                        import_run_id, osm_type, osm_id, name, name_search, category,
                        location, raw_tags, content_digest, source_provenance, last_seen_pass
                    )
                    VALUES (
                        :run_id, 'node', 2, 'Cafe', 'cafe', 'cafe',
                        ST_SetSRID(ST_MakePoint(106.7, 10.7), 4326)::geography,
                        '{}'::jsonb, repeat('a', 64), '{}'::jsonb, 0
                    )
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_place_candidates_last_seen_pass_positive",
    )

    # 3. staging_issue_fingerprints: malformed SHA-256 rejected.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_issue_fingerprints (import_run_id, issue_fingerprint)
                    VALUES (:run_id, 'bad-fp')
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_issue_fingerprints_fingerprint_format",
    )

    # 4. staging_run_checkpoints: VERIFIED without source_verified_post_parse=true rejected.
    now = datetime(2026, 2, 1, 10, 0, tzinfo=timezone.utc)
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_run_checkpoints (
                        import_run_id, staging_state, source_fingerprint, source_checksum_sha256,
                        source_verified_post_parse, total_nodes_seen, supported_nodes_seen,
                        unsupported_nodes_seen, valid_candidates_emitted, invalid_candidates_emitted,
                        pass_inserted_count, staged_candidate_count,
                        extraction_completed_at, verified_at, readiness_evidence
                    )
                    VALUES (
                        :run_id, 'VERIFIED', 'fp-1', repeat('a', 64),
                        false, 1, 1, 0, 1, 0,
                        1, 1, :now, :now, '{"ok": true}'::jsonb
                    )
                    """
                ),
                {"run_id": run_id, "now": now},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_run_checkpoints_lifecycle_consistency",
    )

    # 5. staging_run_checkpoints: VERIFIED with 0 staged rows rejected at DB CHECK layer.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_run_checkpoints (
                        import_run_id, staging_state, source_fingerprint, source_checksum_sha256,
                        source_verified_post_parse, total_nodes_seen, supported_nodes_seen,
                        unsupported_nodes_seen, valid_candidates_emitted, invalid_candidates_emitted,
                        pass_inserted_count, staged_candidate_count,
                        extraction_completed_at, verified_at, readiness_evidence
                    )
                    VALUES (
                        :run_id, 'VERIFIED', 'fp-1', repeat('a', 64),
                        true, 0, 0, 0, 0, 0,
                        0, 0, :now, :now, '{"ok": true}'::jsonb
                    )
                    """
                ),
                {"run_id": run_id, "now": now},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_run_checkpoints_lifecycle_consistency",
    )

    # 6. staging_run_checkpoints: EXTRACTED with inconsistent pass accounting rejected at DB layer.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_run_checkpoints (
                        import_run_id, staging_state, source_fingerprint, source_checksum_sha256,
                        total_nodes_seen, supported_nodes_seen, unsupported_nodes_seen,
                        valid_candidates_emitted, invalid_candidates_emitted,
                        pass_inserted_count, staged_candidate_count, extraction_completed_at
                    )
                    VALUES (
                        :run_id, 'EXTRACTED', 'fp-1', repeat('a', 64),
                        5, 5, 0, 5, 0,
                        2, 2, :now
                    )
                    """
                ),
                {"run_id": run_id, "now": now},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_run_checkpoints_extracted_pass_accounting",
    )

    # 7. staging_run_checkpoints: unapproved extraction_scope rejected at DB layer.
    with pytest.raises(IntegrityError) as exc_info:
        with migrated_conn.begin_nested():
            migrated_conn.execute(
                text(
                    """
                    INSERT INTO staging_run_checkpoints (
                        import_run_id, extraction_scope, source_fingerprint, source_checksum_sha256
                    )
                    VALUES (:run_id, 'full_osm', 'fp-1', repeat('a', 64))
                    """
                ),
                {"run_id": run_id},
            )
    assert_pg_error(
        exc_info,
        sqlstate="23514",
        constraint_name="ck_staging_run_checkpoints_extraction_scope_domain",
    )


# ---------------------------------------------------------------------------
# Final Issue Identity Verification: distinct identical-payload malformed occurrences
# ---------------------------------------------------------------------------


def test_two_identical_malformed_occurrences_not_merged_and_retry_idempotent(
    migrated_conn: Connection,
) -> None:
    """Two distinct malformed NULL-identity occurrences persist separately and deduplicate on retry."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-m26-occ-unit")
    run = create_import_run(
        migrated_conn,
        boundary_version_id=bv_id,
        source_name="malformed_occ.osm",
        source_checksum_sha256=VALID_SHA256,
    )
    begin_staging_run(
        migrated_conn,
        import_run_id=run.id,
        expected_source_fingerprint=run.source_fingerprint,
        expected_source_checksum_sha256=run.source_checksum_sha256,
    )

    shared_details = {
        "raw_osm_type": "node",
        "raw_osm_id": -1,
        "matched_categories": ["cafe"],
        "matched_tags": {"amenity": "cafe"},
    }
    occ_1 = NormalizationIssue(
        severity="ERROR",
        stage="VALIDATE",
        issue_code="INVALID_OSM_IDENTITY",
        message="OSM ID must be between 1 and 9223372036854775807, got -1.",
        osm_type=None,
        osm_id=None,
        details=shared_details,
        source_occurrence_id="node_stream_index:2",
    )
    occ_2 = NormalizationIssue(
        severity="ERROR",
        stage="VALIDATE",
        issue_code="INVALID_OSM_IDENTITY",
        message="OSM ID must be between 1 and 9223372036854775807, got -1.",
        osm_type=None,
        osm_id=None,
        details=shared_details,
        source_occurrence_id="node_stream_index:5",
    )

    fp_1 = compute_issue_fingerprint(import_run_id=run.id, issue=occ_1)
    fp_2 = compute_issue_fingerprint(import_run_id=run.id, issue=occ_2)
    assert fp_1 != fp_2
    assert fp_1 == compute_issue_fingerprint(import_run_id=run.id, issue=occ_1)

    # First pass: both genuinely distinct occurrences must be recorded (not merged).
    res_1 = stage_candidate_batch(
        migrated_conn,
        import_run_id=run.id,
        candidates=[],
        issues=[occ_1, occ_2],
    )
    assert res_1.issues_recorded == 2
    assert res_1.duplicate_issues_skipped == 0

    # Retry pass: replaying the exact same two occurrences must NOT duplicate either issue.
    res_2 = stage_candidate_batch(
        migrated_conn,
        import_run_id=run.id,
        candidates=[],
        issues=[occ_1, occ_2],
    )
    assert res_2.issues_recorded == 0
    assert res_2.duplicate_issues_skipped == 2

    # Transactional consistency: rolling back a new occurrence leaves zero orphan fingerprints.
    occ_3 = NormalizationIssue(
        severity="ERROR",
        stage="VALIDATE",
        issue_code="INVALID_OSM_IDENTITY",
        message="OSM ID must be between 1 and 9223372036854775807, got -1.",
        osm_type=None,
        osm_id=None,
        details=shared_details,
        source_occurrence_id="node_stream_index:9",
    )
    with pytest.raises(RuntimeError, match="rollback check"):
        with migrated_conn.begin_nested():
            stage_candidate_batch(
                migrated_conn,
                import_run_id=run.id,
                candidates=[],
                issues=[occ_3],
            )
            raise RuntimeError("rollback check")

    issue_rows = migrated_conn.execute(
        text(
            """
            SELECT i.id, i.osm_type, i.osm_id, i.details, f.import_issue_id
            FROM import_issues i
            JOIN staging_issue_fingerprints f
              ON f.import_run_id = i.import_run_id
             AND f.import_issue_id = i.id
            WHERE i.import_run_id = :run_id
            ORDER BY i.id
            """
        ),
        {"run_id": run.id},
    ).fetchall()
    assert len(issue_rows) == 2
    assert [r.details["source_occurrence_id"] for r in issue_rows] == [
        "node_stream_index:2",
        "node_stream_index:5",
    ]
    assert all(r.osm_type is None and r.osm_id is None for r in issue_rows)

    fp_count = migrated_conn.execute(
        text(
            "SELECT count(*) FROM staging_issue_fingerprints "
            "WHERE import_run_id = :run_id"
        ),
        {"run_id": run.id},
    ).scalar_one()
    unlinked_fp_count = migrated_conn.execute(
        text(
            "SELECT count(*) FROM staging_issue_fingerprints "
            "WHERE import_run_id = :run_id AND import_issue_id IS NULL"
        ),
        {"run_id": run.id},
    ).scalar_one()
    assert fp_count == 2
    assert unlinked_fp_count == 0


def test_e2e_pyosmium_two_identical_malformed_negative_id_nodes_and_retry(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """E2E: Two identical <node id='-1'> occurrences persist as 2 issues and stay 2 on retry."""
    malformed_xml = work_dir / "two_identical_malformed.osm"
    malformed_xml.write_text(
        """<?xml version='1.0' encoding='UTF-8'?>
<osm version="0.6" generator="geosearch-m26-test">
  <node id="1001" version="1" lat="10.7769" lon="106.7009">
    <tag k="amenity" v="cafe"/>
    <tag k="name" v="Valid Cafe"/>
  </node>
  <node id="-1" version="1" lat="10.7770" lon="106.7010">
    <tag k="amenity" v="cafe"/>
    <tag k="name" v="Malformed Cafe"/>
  </node>
  <node id="-1" version="1" lat="10.7780" lon="106.7020">
    <tag k="amenity" v="cafe"/>
    <tag k="name" v="Malformed Cafe"/>
  </node>
</osm>
""",
        encoding="utf-8",
    )

    with disposable_database("m26_occ_e2e") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m26-occ-e2e")

            # Pass 1: Extract without preparing.
            pass_1 = ingest_osm_nodes_to_staging(
                engine,
                source_path=malformed_xml,
                boundary_version_id=bv_id,
                batch_size=10,
                prepare_on_completion=False,
            )
            assert pass_1.staged_candidates == 1
            assert pass_1.extraction_stats.invalid_candidates == 2
            assert pass_1.issues_recorded == 2
            assert pass_1.duplicate_issues_skipped == 0

            # Pass 2: Retry exact same fixture on the same run -> 0 new issues, 2 skipped.
            pass_2 = ingest_osm_nodes_to_staging(
                engine,
                source_path=malformed_xml,
                boundary_version_id=bv_id,
                import_run_id=pass_1.import_run.id,
                batch_size=10,
                prepare_on_completion=True,
            )
            assert pass_2.import_run.status == "PREPARED"
            assert pass_2.staged_candidates == 0
            assert pass_2.duplicate_identical == 1
            assert pass_2.issues_recorded == 0
            assert pass_2.duplicate_issues_skipped == 2
            assert pass_2.readiness_report is not None
            assert pass_2.readiness_report.issue_count == 2
            assert pass_2.readiness_report.invalid_candidates_emitted == 2

            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT i.issue_code, i.osm_type, i.osm_id, i.details, f.issue_fingerprint
                        FROM import_issues i
                        JOIN staging_issue_fingerprints f
                          ON f.import_run_id = i.import_run_id
                         AND f.import_issue_id = i.id
                        WHERE i.import_run_id = :run_id
                        ORDER BY i.id
                        """
                    ),
                    {"run_id": pass_1.import_run.id},
                ).fetchall()
                assert len(rows) == 2
                assert [r.issue_code for r in rows] == [
                    "INVALID_OSM_IDENTITY",
                    "INVALID_OSM_IDENTITY",
                ]
                assert all(r.osm_type is None and r.osm_id is None for r in rows)
                assert [r.details["source_occurrence_id"] for r in rows] == [
                    "node_stream_index:2",
                    "node_stream_index:3",
                ]
                assert rows[0].issue_fingerprint != rows[1].issue_fingerprint

                # Confirm 1:1 transactional consistency between tables.
                total_issues = conn.execute(
                    text("SELECT count(*) FROM import_issues WHERE import_run_id = :run_id"),
                    {"run_id": pass_1.import_run.id},
                ).scalar_one()
                total_fps = conn.execute(
                    text(
                        "SELECT count(*) FROM staging_issue_fingerprints "
                        "WHERE import_run_id = :run_id AND import_issue_id IS NOT NULL"
                    ),
                    {"run_id": pass_1.import_run.id},
                ).scalar_one()
                assert total_issues == 2
                assert total_fps == 2
        finally:
            engine.dispose()

