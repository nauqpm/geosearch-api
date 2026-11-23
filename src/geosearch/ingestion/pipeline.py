"""End-to-end M2 Node ingestion pipeline orchestration (OSM snapshot -> Staging -> Readiness)."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from sqlalchemy import Connection, Engine, select

from geosearch.db.models import BoundaryVersion
from geosearch.ingestion.fingerprint import (
    compute_config_digest,
    compute_source_fingerprint,
)
from geosearch.ingestion.models import (
    DEFAULT_STAGING_BATCH_SIZE,
    EXTRACTION_SCOPE_NODE_ONLY,
    NORMALIZER_VERSION,
    PIPELINE_VERSION,
    CanonicalPlaceCandidate,
    ExtractionStats,
    NormalizationIssue,
    NormalizationOutcomeKind,
    PipelineStage,
)
from geosearch.ingestion.osm_extractor import (
    OsmSourceError,
    VerifiedSourceSnapshot,
    iter_osm_node_outcomes,
    open_verified_source_snapshot,
    verify_source_snapshot_integrity,
)
from geosearch.ingestion.staging import (
    ImportRunRecord,
    ImportRunStateError,
    ReadinessReport,
    begin_staging_run,
    create_import_run,
    load_import_run,
    mark_extraction_completed,
    mark_staging_failed,
    stage_candidate_batch,
    verify_and_prepare_run,
)


@dataclass(frozen=True, slots=True)
class IngestionRunReport:
    """Complete deterministic report returned by M2 Node ingestion orchestration."""

    import_run: ImportRunRecord
    extraction_stats: ExtractionStats
    batches_committed: int
    staged_candidates: int
    outside_boundary: int
    duplicate_identical: int
    conflicts: int
    issues_recorded: int
    duplicate_issues_skipped: int
    readiness_report: ReadinessReport | None


@contextmanager
def _bounded_unit_of_work(engine_or_conn: Engine | Connection) -> Iterator[Connection]:
    """Run one bounded transaction on an Engine or a nested SAVEPOINT on a Connection."""
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield conn
    else:
        with engine_or_conn.begin_nested():
            yield engine_or_conn


def ingest_osm_nodes_to_staging(
    engine_or_conn: Engine | Connection,
    *,
    source_path: str | Path,
    boundary_version_id: int,
    import_run_id: int | None = None,
    source_name: str | None = None,
    source_snapshot_id: str | None = None,
    expected_source_checksum_sha256: str | None = None,
    batch_size: int = DEFAULT_STAGING_BATCH_SIZE,
    apply_boundary_filter: bool = True,
    prepare_on_completion: bool = True,
    verify_original_source_unchanged: bool = True,
    pipeline_version: str = PIPELINE_VERSION,
    normalizer_version: str = NORMALIZER_VERSION,
    on_before_parse: Callable[[VerifiedSourceSnapshot], None] | None = None,
    on_before_batch_commit: Callable[[int, Connection], None] | None = None,
) -> IngestionRunReport:
    """Stream an OSM file (PBF/XML) through an isolated verified snapshot into M2 staging.

    Integrity & safety guarantees:
    - Streams `source_path` into an isolated read-only temporary snapshot while computing
      SHA-256 over the exact bytes that Pyosmium parses.
    - Re-verifies snapshot and original source integrity before and after parsing.
    - Rejects mismatched `expected_source_checksum_sha256`, truncated/corrupt streams, or
      modified sources with a traceable `SOURCE_INTEGRITY_ERROR` and `FAILED` run status.
    - Persists candidates and issues idempotently in bounded transactions (`batch_size`).
    - Never marks an empty, conflicted, incomplete, or uncovered dataset `PREPARED`.
    - Never touches the live `places` table.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}.")

    resolved_path = Path(source_path)
    resolved_source_name = source_name if source_name is not None else resolved_path.name

    with open_verified_source_snapshot(resolved_path) as snapshot:
        source_checksum = snapshot.checksum_sha256

        # If caller supplied an approved source checksum that differs from the actual file
        # (e.g. truncated or swapped input), record a traceable FAILED run / issue and fail closed.
        if (
            expected_source_checksum_sha256 is not None
            and source_checksum != expected_source_checksum_sha256
        ):
            mismatch_msg = (
                f"OSM source checksum mismatch for '{resolved_source_name}': "
                f"expected '{expected_source_checksum_sha256}', got '{source_checksum}'."
            )
            mismatch_details = {
                "reason": "approved_checksum_mismatch",
                "expected_checksum_sha256": expected_source_checksum_sha256,
                "actual_checksum_sha256": source_checksum,
                "source_path": str(resolved_path),
            }
            with _bounded_unit_of_work(engine_or_conn) as conn:
                if import_run_id is None:
                    failed_run = create_import_run(
                        conn,
                        boundary_version_id=boundary_version_id,
                        source_name=resolved_source_name,
                        source_checksum_sha256=expected_source_checksum_sha256,
                        source_snapshot_id=source_snapshot_id,
                        pipeline_version=pipeline_version,
                        normalizer_version=normalizer_version,
                        apply_boundary_filter=apply_boundary_filter,
                    )
                    begin_staging_run(
                        conn,
                        import_run_id=failed_run.id,
                        expected_source_fingerprint=failed_run.source_fingerprint,
                        expected_source_checksum_sha256=expected_source_checksum_sha256,
                        extraction_scope=EXTRACTION_SCOPE_NODE_ONLY,
                    )
                    target_run_id = failed_run.id
                else:
                    target_run_id = import_run_id
                mark_staging_failed(
                    conn,
                    import_run_id=target_run_id,
                    error_message=mismatch_msg,
                    issue_code="SOURCE_INTEGRITY_ERROR",
                    stage=PipelineStage.EXTRACT.value,
                    details=mismatch_details,
                )
            raise OsmSourceError(
                mismatch_msg,
                source_path=str(resolved_path),
                details=mismatch_details,
            )

        # Transaction 1: Create or validate/resume ImportRun and initialize checkpoint.
        with _bounded_unit_of_work(engine_or_conn) as conn:
            if import_run_id is None:
                run_record = create_import_run(
                    conn,
                    boundary_version_id=boundary_version_id,
                    source_name=resolved_source_name,
                    source_checksum_sha256=source_checksum,
                    source_snapshot_id=source_snapshot_id,
                    pipeline_version=pipeline_version,
                    normalizer_version=normalizer_version,
                    apply_boundary_filter=apply_boundary_filter,
                )
                active_run = begin_staging_run(
                    conn,
                    import_run_id=run_record.id,
                    expected_source_fingerprint=run_record.source_fingerprint,
                    expected_source_checksum_sha256=source_checksum,
                    extraction_scope=EXTRACTION_SCOPE_NODE_ONLY,
                )
            else:
                existing = load_import_run(conn, import_run_id=import_run_id)
                if boundary_version_id != existing.boundary_version_id:
                    raise ImportRunStateError(
                        f"Mismatched boundary_version_id while resuming ImportRun id={import_run_id}: "
                        f"run uses {existing.boundary_version_id}, caller passed {boundary_version_id}."
                    )
                bv_checksum = conn.execute(
                    select(BoundaryVersion.checksum_sha256).where(
                        BoundaryVersion.id == existing.boundary_version_id
                    )
                ).scalar_one()
                expected_config = compute_config_digest(
                    extraction_scope=EXTRACTION_SCOPE_NODE_ONLY,
                    apply_boundary_filter=apply_boundary_filter,
                )
                snapshot_id = (
                    source_snapshot_id if source_snapshot_id is not None else existing.source_name
                )
                expected_fp = compute_source_fingerprint(
                    source_checksum_sha256=source_checksum,
                    source_snapshot_id=snapshot_id,
                    boundary_version_code=existing.boundary_version_code,
                    boundary_checksum_sha256=str(bv_checksum),
                    config_digest=expected_config,
                    pipeline_version=pipeline_version,
                    normalizer_version=normalizer_version,
                )
                active_run = begin_staging_run(
                    conn,
                    import_run_id=import_run_id,
                    expected_source_fingerprint=expected_fp,
                    expected_source_checksum_sha256=source_checksum,
                    extraction_scope=EXTRACTION_SCOPE_NODE_ONLY,
                )

        stats = ExtractionStats()
        pending_candidates: list[CanonicalPlaceCandidate] = []
        pending_issues: list[NormalizationIssue] = []

        total_batches = 0
        total_inserted = 0
        total_outside = 0
        total_dup_identical = 0
        total_conflicts = 0
        total_issues = 0
        total_dup_issues_skipped = 0

        def _flush_batch() -> None:
            nonlocal total_batches, total_inserted, total_outside
            nonlocal total_dup_identical, total_conflicts, total_issues, total_dup_issues_skipped

            if not pending_candidates and not pending_issues:
                return

            batch_index = total_batches + 1
            with _bounded_unit_of_work(engine_or_conn) as conn:
                result = stage_candidate_batch(
                    conn,
                    import_run_id=active_run.id,
                    candidates=pending_candidates,
                    issues=pending_issues,
                    apply_boundary_filter=apply_boundary_filter,
                    boundary_version_id=active_run.boundary_version_id,
                )
                if on_before_batch_commit is not None:
                    on_before_batch_commit(batch_index, conn)

            total_batches += 1
            total_inserted += result.inserted
            total_outside += result.outside_boundary
            total_dup_identical += result.duplicate_identical
            total_conflicts += result.conflicts
            total_issues += result.issues_recorded
            total_dup_issues_skipped += result.duplicate_issues_skipped
            pending_candidates.clear()
            pending_issues.clear()

        provenance = {
            "source_name": active_run.source_name,
            "source_checksum_sha256": active_run.source_checksum_sha256,
            "source_fingerprint": active_run.source_fingerprint,
            "pipeline_version": active_run.pipeline_version,
            "normalizer_version": active_run.normalizer_version,
            "extraction_scope": EXTRACTION_SCOPE_NODE_ONLY,
        }

        try:
            if on_before_parse is not None:
                on_before_parse(snapshot)

            verify_source_snapshot_integrity(
                snapshot,
                expected_checksum_sha256=active_run.source_checksum_sha256,
                verify_original_unchanged=verify_original_source_unchanged,
            )

            for outcome in iter_osm_node_outcomes(
                snapshot.snapshot_path,
                stats=stats,
                source_provenance=provenance,
            ):
                if outcome.kind == NormalizationOutcomeKind.VALID and outcome.candidate is not None:
                    pending_candidates.append(outcome.candidate)
                elif outcome.issue is not None:
                    pending_issues.append(outcome.issue)

                if len(pending_candidates) + len(pending_issues) >= batch_size:
                    _flush_batch()

            _flush_batch()

            verify_source_snapshot_integrity(
                snapshot,
                expected_checksum_sha256=active_run.source_checksum_sha256,
                verify_original_unchanged=verify_original_source_unchanged,
            )

            with _bounded_unit_of_work(engine_or_conn) as conn:
                mark_extraction_completed(
                    conn,
                    import_run_id=active_run.id,
                    stats=stats,
                    source_verified_post_parse=True,
                )
        except OsmSourceError as exc:
            with _bounded_unit_of_work(engine_or_conn) as conn:
                mark_staging_failed(
                    conn,
                    import_run_id=active_run.id,
                    error_message=str(exc),
                    issue_code="SOURCE_INTEGRITY_ERROR",
                    stage=PipelineStage.EXTRACT.value,
                    details={"source_path": exc.source_path, **exc.details},
                )
            raise

    readiness_report: ReadinessReport | None = None
    if prepare_on_completion:
        with _bounded_unit_of_work(engine_or_conn) as conn:
            readiness_report = verify_and_prepare_run(
                conn,
                import_run_id=active_run.id,
            )

    with _bounded_unit_of_work(engine_or_conn) as conn:
        final_run = load_import_run(conn, import_run_id=active_run.id)

    return IngestionRunReport(
        import_run=final_run,
        extraction_stats=stats,
        batches_committed=total_batches,
        staged_candidates=total_inserted,
        outside_boundary=total_outside,
        duplicate_identical=total_dup_identical,
        conflicts=total_conflicts,
        issues_recorded=total_issues,
        duplicate_issues_skipped=total_dup_issues_skipped,
        readiness_report=readiness_report,
    )
