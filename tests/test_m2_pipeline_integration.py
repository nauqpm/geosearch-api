"""M2.5 end-to-end Node ingestion pipeline integration tests on PostgreSQL/PostGIS.

NOTE: All boundary polygons and OSM fixture files used here are deterministic synthetic
fixtures for verifying M2 pipeline mechanics. Independently verified post-2025 Ho Chi Minh
City boundary polygons and reference points (former HCMC, former Binh Duong, former
Ba Ria-Vung Tau, Con Dao) remain a pending acceptance requirement outside synthetic tests.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import osmium
import pytest
from sqlalchemy import Connection, text

from geosearch.db import create_test_engine
from geosearch.ingestion import (
    EXTRACTION_SCOPE_NODE_ONLY,
    OsmSourceError,
    StagingReadinessError,
    compute_file_sha256,
    ingest_osm_nodes_to_staging,
    verify_and_prepare_run,
)
from tests.helpers import insert_boundary_version, upgrade_test_db_to_head

FIXTURE_OSM_XML = Path(__file__).resolve().parent / "fixtures" / "sample_synthetic_nodes.osm"


def _write_pbf(path: Path, nodes: list[osmium.osm.mutable.Node]) -> Path:
    writer = osmium.SimpleWriter(str(path))
    try:
        for node in nodes:
            writer.add_node(node)
    finally:
        writer.close()
    return path


def test_e2e_single_valid_supported_node_staged(
    migrated_conn: Connection,
    work_dir: Path,
) -> None:
    """Scenario 1 & 9: Single valid supported Node from PBF stages with exact PostGIS lon/lat."""
    pbf = _write_pbf(
        work_dir / "single_node.osm.pbf",
        [
            osmium.osm.mutable.Node(
                id=42001,
                version=2,
                changeset=900,
                timestamp="2026-01-20T05:00:00Z",
                location=(106.7009, 10.7769),
                tags={"amenity": "cafe", "name": "Cà Phê Một Mình", "outdoor_seating": "yes"},
            )
        ],
    )
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-single")
    report = ingest_osm_nodes_to_staging(
        migrated_conn,
        source_path=pbf,
        boundary_version_id=bv_id,
        batch_size=10,
    )

    assert report.import_run.status == "PREPARED"
    assert report.staged_candidates == 1
    assert report.issues_recorded == 0
    assert report.readiness_report is not None
    assert report.readiness_report.staged_candidate_count == 1
    assert report.readiness_report.extraction_scope == EXTRACTION_SCOPE_NODE_ONLY

    row = migrated_conn.execute(
        text(
            """
            SELECT osm_type, osm_id, name, name_search, category,
                   ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat,
                   raw_tags, source_provenance
            FROM staging_place_candidates
            WHERE import_run_id = :run_id
            """
        ),
        {"run_id": report.import_run.id},
    ).one()
    assert row.osm_type == "node"
    assert row.osm_id == 42001
    assert row.name == "Cà Phê Một Mình"
    assert row.name_search == "ca phe mot minh"
    assert row.category == "cafe"
    assert math.isclose(row.lon, 106.7009, abs_tol=1e-7)
    assert math.isclose(row.lat, 10.7769, abs_tol=1e-7)
    assert row.raw_tags["outdoor_seating"] == "yes"
    assert "source_fingerprint" not in row.raw_tags
    assert row.source_provenance["source_fingerprint"] == report.import_run.source_fingerprint

    # Live places table must remain completely untouched.
    assert migrated_conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0


def test_e2e_all_five_categories_issues_normalization_and_reproducible_fingerprints(
    migrated_conn: Connection,
) -> None:
    """Scenarios 2-10, 14, 16-18: Full XML fixture across two isolated runs with distinct batch sizes."""
    bv_id = insert_boundary_version(migrated_conn, version_code="bv-full-xml")
    expected_checksum = compute_file_sha256(FIXTURE_OSM_XML)

    # Run 1 with batch_size=2; Run 2 with batch_size=100.
    report_1 = ingest_osm_nodes_to_staging(
        migrated_conn,
        source_path=FIXTURE_OSM_XML,
        boundary_version_id=bv_id,
        batch_size=2,
    )
    report_2 = ingest_osm_nodes_to_staging(
        migrated_conn,
        source_path=FIXTURE_OSM_XML,
        boundary_version_id=bv_id,
        batch_size=100,
    )

    assert report_1.import_run.id != report_2.import_run.id
    assert report_1.import_run.source_checksum_sha256 == expected_checksum
    assert report_2.import_run.source_checksum_sha256 == expected_checksum
    # Operational batch size must not alter semantic fingerprint.
    assert report_1.import_run.source_fingerprint == report_2.import_run.source_fingerprint
    assert report_1.import_run.status == "PREPARED"
    assert report_2.import_run.status == "PREPARED"

    # Stats reconcile exactly against fixture contents and DB counts.
    assert report_1.extraction_stats.total_nodes_seen == 13
    assert report_1.extraction_stats.unsupported_nodes_seen == 4
    assert report_1.extraction_stats.supported_nodes_seen == 9
    assert report_1.extraction_stats.valid_candidates == 6
    assert report_1.extraction_stats.invalid_candidates == 3
    assert report_1.staged_candidates == 6
    assert report_1.issues_recorded == 3
    assert report_1.batches_committed > 1

    staged_rows = migrated_conn.execute(
        text(
            """
            SELECT osm_id, category, name, name_search, address,
                   ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat,
                   raw_tags
            FROM staging_place_candidates
            WHERE import_run_id = :run_id
            ORDER BY osm_id
            """
        ),
        {"run_id": report_1.import_run.id},
    ).fetchall()
    assert len(staged_rows) == 6
    assert [r.category for r in staged_rows] == [
        "cafe",
        "restaurant",
        "atm",
        "atm",
        "pharmacy",
        "supermarket",
    ]

    # Verify Vietnamese normalization, unnamed ATM, named ATM, raw tags, and lon/lat ordering.
    by_id = {r.osm_id: r for r in staged_rows}
    assert by_id[1001].name == "Cà Phê Sữa Đá"
    assert by_id[1001].name_search == "ca phe sua da"
    assert by_id[1001].address == "12 Lê Lợi, Quận 1"
    assert by_id[1001].raw_tags["cuisine"] == "coffee_shop"
    assert math.isclose(by_id[1001].lon, 106.7009, abs_tol=1e-7)
    assert math.isclose(by_id[1001].lat, 10.7769, abs_tol=1e-7)

    assert by_id[1002].name == "Quán   Phở Đêm"
    assert by_id[1002].name_search == "quan pho dem"

    assert by_id[1003].name is None
    assert by_id[1003].name_search is None
    assert by_id[1003].raw_tags == {"amenity": "atm", "operator": "Vietcombank"}

    assert by_id[1004].name == "ATM Đakao"
    assert by_id[1004].name_search == "atm dakao"

    assert by_id[1005].name_search == "nha thuoc dac loi"
    assert by_id[1005].address == "45 Điện Biên Phủ, Quận 3"

    assert by_id[1006].name_search == "sieu thi dong khoi"

    # Verify traceable import_issues for invalid & ambiguous nodes (and NOT unsupported nodes).
    issues = migrated_conn.execute(
        text(
            """
            SELECT osm_id, stage, severity, issue_code
            FROM import_issues
            WHERE import_run_id = :run_id
            ORDER BY osm_id
            """
        ),
        {"run_id": report_1.import_run.id},
    ).fetchall()
    assert [(i.osm_id, i.stage, i.severity, i.issue_code) for i in issues] == [
        (3001, "VALIDATE", "ERROR", "MISSING_REQUIRED_NAME"),
        (3002, "VALIDATE", "ERROR", "BLANK_NAME"),
        (3003, "CLASSIFY", "WARNING", "AMBIGUOUS_CATEGORY"),
    ]

    # Run isolation: Run 2 holds its own 6 rows independently of Run 1.
    run_2_count = migrated_conn.execute(
        text("SELECT count(*) FROM staging_place_candidates WHERE import_run_id = :run_id"),
        {"run_id": report_2.import_run.id},
    ).scalar_one()
    assert run_2_count == 6
    assert migrated_conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0


def test_e2e_retry_identical_input_and_conflicting_duplicate_identity(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Scenarios 11 & 12: Retry does not duplicate staged rows; conflicting identity blocks readiness."""
    with disposable_database("m2_e2e_retry") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-e2e-retry")

            # Pass 1: Extract without preparing (simulating interrupted/pre-verify run).
            first_pass = ingest_osm_nodes_to_staging(
                engine,
                source_path=FIXTURE_OSM_XML,
                boundary_version_id=bv_id,
                batch_size=3,
                prepare_on_completion=False,
            )
            assert first_pass.import_run.status == "PROCESSING"
            assert first_pass.staged_candidates == 6

            # Pass 2: Retry exact same fixture on the same run -> 0 new rows, 6 identical duplicates,
            # 0 duplicate import_issues, and non-inflated checkpoint counters.
            retry_pass = ingest_osm_nodes_to_staging(
                engine,
                source_path=FIXTURE_OSM_XML,
                boundary_version_id=bv_id,
                import_run_id=first_pass.import_run.id,
                batch_size=3,
                prepare_on_completion=True,
            )
            assert retry_pass.import_run.status == "PREPARED"
            assert retry_pass.staged_candidates == 0
            assert retry_pass.duplicate_identical == 6
            assert retry_pass.conflicts == 0
            assert retry_pass.issues_recorded == 0
            assert retry_pass.duplicate_issues_skipped == 3
            assert retry_pass.batches_committed == first_pass.batches_committed

            with engine.connect() as conn:
                staged_count = conn.execute(
                    text(
                        "SELECT count(*) FROM staging_place_candidates "
                        "WHERE import_run_id = :run_id"
                    ),
                    {"run_id": first_pass.import_run.id},
                ).scalar_one()
                assert staged_count == 6

                issue_count = conn.execute(
                    text("SELECT count(*) FROM import_issues WHERE import_run_id = :run_id"),
                    {"run_id": first_pass.import_run.id},
                ).scalar_one()
                assert issue_count == 3

                cp = conn.execute(
                    text(
                        """
                        SELECT current_pass_number, resume_count, batches_committed,
                               staged_candidate_count, issue_count, pass_inserted_count,
                               duplicate_identical_count, source_verified_post_parse
                        FROM staging_run_checkpoints
                        WHERE import_run_id = :run_id
                        """
                    ),
                    {"run_id": first_pass.import_run.id},
                ).one()
                assert cp.current_pass_number == 2
                assert cp.resume_count == 1
                assert cp.batches_committed == first_pass.batches_committed
                assert cp.staged_candidate_count == 6
                assert cp.issue_count == 3
                assert cp.pass_inserted_count == 0
                assert cp.duplicate_identical_count == 6
                assert cp.source_verified_post_parse is True

            # Conflicting duplicate OSM identity inside the same OSM fixture.
            conflict_pbf = _write_pbf(
                work_dir / "conflict_nodes.osm.pbf",
                [
                    osmium.osm.mutable.Node(
                        id=77001,
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "First Version Cafe"},
                    ),
                    osmium.osm.mutable.Node(
                        id=77001,
                        location=(106.7009, 10.7769),
                        tags={"amenity": "cafe", "name": "Conflicting Second Cafe"},
                    ),
                ],
            )
            with pytest.raises(StagingReadinessError, match="conflict"):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=conflict_pbf,
                    boundary_version_id=bv_id,
                    batch_size=10,
                    prepare_on_completion=True,
                )

            with engine.connect() as conn:
                conflict_run_id = conn.execute(
                    text(
                        "SELECT id FROM import_runs WHERE source_name = 'conflict_nodes.osm.pbf'"
                    )
                ).scalar_one()
                status = conn.execute(
                    text("SELECT status FROM import_runs WHERE id = :id"),
                    {"id": conflict_run_id},
                ).scalar_one()
                assert status == "PROCESSING"

                # First version preserved untouched, never silently overwritten.
                stored_name = conn.execute(
                    text(
                        "SELECT name FROM staging_place_candidates "
                        "WHERE import_run_id = :id AND osm_id = 77001"
                    ),
                    {"id": conflict_run_id},
                ).scalar_one()
                assert stored_name == "First Version Cafe"

                conflict_issues = conn.execute(
                    text(
                        "SELECT issue_code FROM import_issues WHERE import_run_id = :id"
                    ),
                    {"id": conflict_run_id},
                ).scalars().all()
                assert conflict_issues == ["STAGING_CONFLICT"]
        finally:
            engine.dispose()


def test_e2e_failed_batch_leaves_no_partial_batch_writes_and_fails_readiness(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """Scenarios 13 & 15: Mid-run batch failure rolls back that batch and blocks PREPARED."""
    with disposable_database("m2_e2e_batch_fail") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-batch-fail")

            def _fail_on_second_batch(batch_index: int, _conn: Connection) -> None:
                if batch_index == 2:
                    raise RuntimeError("Injected failure on staging batch #2")

            with pytest.raises(RuntimeError, match="Injected failure on staging batch #2"):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=FIXTURE_OSM_XML,
                    boundary_version_id=bv_id,
                    batch_size=2,
                    on_before_batch_commit=_fail_on_second_batch,
                )

            with engine.connect() as conn:
                run_id = conn.execute(text("SELECT id FROM import_runs")).scalar_one()
                run_status = conn.execute(
                    text("SELECT status FROM import_runs WHERE id = :id"),
                    {"id": run_id},
                ).scalar_one()
                assert run_status == "PROCESSING"

                # Exactly Batch 1 (osm_ids 1001, 1002) committed; Batch 2 rolled back cleanly.
                staged_ids = conn.execute(
                    text(
                        "SELECT osm_id FROM staging_place_candidates "
                        "WHERE import_run_id = :id ORDER BY osm_id"
                    ),
                    {"id": run_id},
                ).scalars().all()
                assert staged_ids == [1001, 1002]

                cp = conn.execute(
                    text(
                        "SELECT staging_state, batches_committed, staged_candidate_count "
                        "FROM staging_run_checkpoints WHERE import_run_id = :id"
                    ),
                    {"id": run_id},
                ).one()
                assert cp.staging_state == "IN_PROGRESS"
                assert cp.batches_committed == 1
                assert cp.staged_candidate_count == 2

                # Partial staging MUST NOT pass readiness checks.
                with pytest.raises(StagingReadinessError, match="EXTRACTED"):
                    verify_and_prepare_run(conn, import_run_id=run_id)

                # Live places table untouched.
                assert conn.execute(text("SELECT count(*) FROM places")).scalar_one() == 0
        finally:
            engine.dispose()


def test_e2e_corrupt_source_file_marks_run_failed_with_source_integrity_issue(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Corrupt OSM stream marks the run FAILED with a traceable SOURCE_INTEGRITY_ERROR issue."""
    corrupt_pbf = work_dir / "corrupt_stream.osm.pbf"
    corrupt_pbf.write_bytes(b"corrupt-non-pbf-payload")

    with disposable_database("m2_e2e_corrupt") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-corrupt")

            with pytest.raises(OsmSourceError):
                ingest_osm_nodes_to_staging(
                    engine,
                    source_path=corrupt_pbf,
                    boundary_version_id=bv_id,
                )

            with engine.connect() as conn:
                run_id, status = conn.execute(
                    text("SELECT id, status FROM import_runs")
                ).one()
                assert status == "FAILED"

                cp_state = conn.execute(
                    text(
                        "SELECT staging_state FROM staging_run_checkpoints "
                        "WHERE import_run_id = :id"
                    ),
                    {"id": run_id},
                ).scalar_one()
                assert cp_state == "FAILED"

                issue_codes = conn.execute(
                    text("SELECT issue_code FROM import_issues WHERE import_run_id = :id"),
                    {"id": run_id},
                ).scalars().all()
                assert issue_codes == ["SOURCE_INTEGRITY_ERROR"]

                with pytest.raises(StagingReadinessError):
                    verify_and_prepare_run(conn, import_run_id=run_id)
        finally:
            engine.dispose()
