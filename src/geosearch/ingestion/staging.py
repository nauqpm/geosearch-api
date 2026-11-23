"""Run-scoped technical staging persistence, import issues, and readiness verification (M2.4/M2.6).

This module writes ONLY to import_runs, import_issues, staging_place_candidates,
staging_issue_fingerprints, and staging_run_checkpoints. It never modifies the live
canonical `places` table or performs M3 publication / active-slot switching.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Connection, select, text, tuple_

from geosearch.db.models import BoundaryVersion, ImportRun
from geosearch.db.staging import StagingPlaceCandidate, StagingRunCheckpoint
from geosearch.db.types import validate_wgs84_coordinates
from geosearch.ingestion.fingerprint import (
    compute_candidate_content_digest,
    compute_config_digest,
    compute_issue_fingerprint,
    compute_source_fingerprint,
)
from geosearch.ingestion.models import (
    EXTRACTION_SCOPE_NODE_ONLY,
    NORMALIZER_VERSION,
    PIPELINE_VERSION,
    CanonicalPlaceCandidate,
    ExtractionStats,
    IssueSeverity,
    NormalizationIssue,
    PipelineStage,
)

_NON_STAGEABLE_RUN_STATUSES: frozenset[str] = frozenset(
    {"PREPARED", "PUBLISHED", "SUPERSEDED", "FAILED", "ABORTED"}
)


class StagingError(RuntimeError):
    """Base error for M2 technical staging operations."""


class ImportRunStateError(StagingError):
    """Raised when an ImportRun is in an invalid state or has mismatched provenance."""


class StagingReadinessError(StagingError):
    """Raised when an ImportRun fails M2 staging readiness checks for PREPARED transition."""

    def __init__(self, message: str, *, reasons: Sequence[str]) -> None:
        super().__init__(message)
        self.reasons: tuple[str, ...] = tuple(reasons)


@dataclass(frozen=True, slots=True)
class ImportRunRecord:
    """Typed summary of a newly created or loaded ImportRun."""

    id: int
    boundary_version_id: int
    boundary_version_code: str
    boundary_approval_status: str
    source_name: str
    source_checksum_sha256: str
    source_fingerprint: str
    pipeline_version: str
    normalizer_version: str
    config_digest: str
    status: str


@dataclass(frozen=True, slots=True)
class BatchStageResult:
    """Deterministic outcome summary for a single bounded staging batch."""

    inserted: int
    outside_boundary: int
    duplicate_identical: int
    conflicts: int
    issues_recorded: int
    duplicate_issues_skipped: int = 0


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    """Structured M2 staging readiness verification evidence."""

    import_run_id: int
    extraction_scope: str
    boundary_version_id: int
    boundary_version_code: str
    boundary_approval_status: str
    boundary_filter_verified: bool
    uncovered_staged_count: int
    stale_staged_count: int
    source_verified_post_parse: bool
    source_fingerprint: str
    source_checksum_sha256: str
    current_pass_number: int
    resume_count: int
    staged_candidate_count: int
    pass_inserted_count: int
    outside_boundary_count: int
    duplicate_identical_count: int
    pass_conflict_count: int
    conflict_count: int
    issue_count: int
    total_nodes_seen: int
    supported_nodes_seen: int
    unsupported_nodes_seen: int
    valid_candidates_emitted: int
    invalid_candidates_emitted: int
    batches_committed: int

    def to_dict(self) -> dict[str, Any]:
        """Serialize readiness evidence as a JSONB-compatible dictionary."""
        return {
            "import_run_id": self.import_run_id,
            "extraction_scope": self.extraction_scope,
            "coverage_restriction": "node_only_incomplete_osm_coverage",
            "boundary_version_id": self.boundary_version_id,
            "boundary_version_code": self.boundary_version_code,
            "boundary_approval_status": self.boundary_approval_status,
            "boundary_filter_verified": self.boundary_filter_verified,
            "uncovered_staged_count": self.uncovered_staged_count,
            "stale_staged_count": self.stale_staged_count,
            "source_verified_post_parse": self.source_verified_post_parse,
            "source_fingerprint": self.source_fingerprint,
            "source_checksum_sha256": self.source_checksum_sha256,
            "current_pass_number": self.current_pass_number,
            "resume_count": self.resume_count,
            "staged_candidate_count": self.staged_candidate_count,
            "pass_inserted_count": self.pass_inserted_count,
            "outside_boundary_count": self.outside_boundary_count,
            "duplicate_identical_count": self.duplicate_identical_count,
            "pass_conflict_count": self.pass_conflict_count,
            "conflict_count": self.conflict_count,
            "issue_count": self.issue_count,
            "total_nodes_seen": self.total_nodes_seen,
            "supported_nodes_seen": self.supported_nodes_seen,
            "unsupported_nodes_seen": self.unsupported_nodes_seen,
            "valid_candidates_emitted": self.valid_candidates_emitted,
            "invalid_candidates_emitted": self.invalid_candidates_emitted,
            "batches_committed": self.batches_committed,
            "note": (
                "M2 staging readiness verified for node_only extraction and PostGIS ST_Covers "
                "membership against the run's bound boundary_version. Node-only staging does not "
                "represent full Way/Relation OSM coverage; boundary approval and live publication "
                "gates belong to M3."
            ),
        }


def _lock_import_run_row(conn: Connection, *, import_run_id: int) -> Any:
    """Acquire a row-level FOR UPDATE lock on import_runs to serialize per-run transitions."""
    row = conn.execute(
        text(
            """
            SELECT
                r.id,
                r.boundary_version_id,
                r.source_name,
                r.source_checksum_sha256,
                r.source_fingerprint,
                r.pipeline_version,
                r.normalizer_version,
                r.config_digest,
                r.status,
                r.active_slot
            FROM import_runs r
            WHERE r.id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()
    if row is None:
        raise StagingError(f"ImportRun id={import_run_id} does not exist.")
    return row


def _persist_unique_issues(
    conn: Connection,
    *,
    import_run_id: int,
    issues: Sequence[NormalizationIssue],
) -> tuple[list[tuple[int, NormalizationIssue]], int]:
    """Persist run-scoped issues idempotently using staging_issue_fingerprints.

    Returns (newly_inserted_pairs, duplicate_issues_skipped). Existing persisted issues are
    never overwritten or duplicated, even when osm_type and osm_id are NULL.
    """
    if not issues:
        return [], 0

    inserted: list[tuple[int, NormalizationIssue]] = []
    skipped = 0
    seen_in_call: set[str] = set()

    for issue in issues:
        issue_fp = compute_issue_fingerprint(import_run_id=import_run_id, issue=issue)
        if issue_fp in seen_in_call:
            skipped += 1
            continue
        seen_in_call.add(issue_fp)

        claimed = conn.execute(
            text(
                """
                INSERT INTO staging_issue_fingerprints (
                    import_run_id,
                    issue_fingerprint
                )
                VALUES (:run_id, :issue_fp)
                ON CONFLICT (import_run_id, issue_fingerprint) DO NOTHING
                RETURNING issue_fingerprint
                """
            ),
            {"run_id": import_run_id, "issue_fp": issue_fp},
        ).scalar_one_or_none()

        if claimed is None:
            skipped += 1
            continue

        persisted_details: dict[str, Any] | None
        if issue.source_occurrence_id is not None:
            persisted_details = {
                **(issue.details or {}),
                "source_occurrence_id": issue.source_occurrence_id,
            }
        else:
            persisted_details = issue.details

        details_json = (
            json.dumps(persisted_details, sort_keys=True, ensure_ascii=False)
            if persisted_details is not None
            else None
        )
        issue_id = int(
            conn.execute(
                text(
                    """
                    INSERT INTO import_issues (
                        import_run_id,
                        severity,
                        stage,
                        issue_code,
                        message,
                        osm_type,
                        osm_id,
                        details
                    )
                    VALUES (
                        :import_run_id,
                        :severity,
                        :stage,
                        :issue_code,
                        :message,
                        :osm_type,
                        :osm_id,
                        CAST(:details_json AS jsonb)
                    )
                    RETURNING id
                    """
                ),
                {
                    "import_run_id": import_run_id,
                    "severity": issue.severity,
                    "stage": issue.stage,
                    "issue_code": issue.issue_code,
                    "message": issue.message,
                    "osm_type": issue.osm_type,
                    "osm_id": issue.osm_id,
                    "details_json": details_json,
                },
            ).scalar_one()
        )
        conn.execute(
            text(
                """
                UPDATE staging_issue_fingerprints
                SET import_issue_id = :issue_id
                WHERE import_run_id = :run_id
                  AND issue_fingerprint = :issue_fp
                """
            ),
            {"issue_id": issue_id, "run_id": import_run_id, "issue_fp": issue_fp},
        )
        inserted.append((issue_id, issue))

    return inserted, skipped


def create_import_run(
    conn: Connection,
    *,
    boundary_version_id: int,
    source_name: str,
    source_checksum_sha256: str,
    source_snapshot_id: str | None = None,
    pipeline_version: str = PIPELINE_VERSION,
    normalizer_version: str = NORMALIZER_VERSION,
    apply_boundary_filter: bool = True,
) -> ImportRunRecord:
    """Create a new PENDING import_runs row with a reproducible source fingerprint."""
    boundary_row = conn.execute(
        select(
            BoundaryVersion.id,
            BoundaryVersion.version_code,
            BoundaryVersion.checksum_sha256,
            BoundaryVersion.approval_status,
        ).where(BoundaryVersion.id == boundary_version_id)
    ).one_or_none()
    if boundary_row is None:
        raise StagingError(f"BoundaryVersion id={boundary_version_id} does not exist.")

    snapshot_id = source_snapshot_id if source_snapshot_id is not None else source_name
    config_digest = compute_config_digest(
        extraction_scope=EXTRACTION_SCOPE_NODE_ONLY,
        apply_boundary_filter=apply_boundary_filter,
    )
    source_fingerprint = compute_source_fingerprint(
        source_checksum_sha256=source_checksum_sha256,
        source_snapshot_id=snapshot_id,
        boundary_version_code=boundary_row.version_code,
        boundary_checksum_sha256=boundary_row.checksum_sha256,
        config_digest=config_digest,
        pipeline_version=pipeline_version,
        normalizer_version=normalizer_version,
    )

    run_id = conn.execute(
        text(
            """
            INSERT INTO import_runs (
                boundary_version_id,
                run_type,
                source_name,
                source_checksum_sha256,
                source_fingerprint,
                pipeline_version,
                normalizer_version,
                config_digest,
                status
            )
            VALUES (
                :boundary_version_id,
                'IMPORT',
                :source_name,
                :source_checksum_sha256,
                :source_fingerprint,
                :pipeline_version,
                :normalizer_version,
                :config_digest,
                'PENDING'
            )
            RETURNING id
            """
        ),
        {
            "boundary_version_id": boundary_version_id,
            "source_name": source_name,
            "source_checksum_sha256": source_checksum_sha256,
            "source_fingerprint": source_fingerprint,
            "pipeline_version": pipeline_version,
            "normalizer_version": normalizer_version,
            "config_digest": config_digest,
        },
    ).scalar_one()

    return ImportRunRecord(
        id=int(run_id),
        boundary_version_id=int(boundary_row.id),
        boundary_version_code=str(boundary_row.version_code),
        boundary_approval_status=str(boundary_row.approval_status),
        source_name=source_name,
        source_checksum_sha256=source_checksum_sha256,
        source_fingerprint=source_fingerprint,
        pipeline_version=pipeline_version,
        normalizer_version=normalizer_version,
        config_digest=config_digest,
        status="PENDING",
    )


def load_import_run(conn: Connection, *, import_run_id: int) -> ImportRunRecord:
    """Load an existing ImportRun joined with its BoundaryVersion provenance."""
    row = conn.execute(
        select(
            ImportRun.id,
            ImportRun.boundary_version_id,
            BoundaryVersion.version_code,
            BoundaryVersion.approval_status,
            ImportRun.source_name,
            ImportRun.source_checksum_sha256,
            ImportRun.source_fingerprint,
            ImportRun.pipeline_version,
            ImportRun.normalizer_version,
            ImportRun.config_digest,
            ImportRun.status,
        )
        .join(BoundaryVersion, BoundaryVersion.id == ImportRun.boundary_version_id)
        .where(ImportRun.id == import_run_id)
    ).one_or_none()
    if row is None:
        raise StagingError(f"ImportRun id={import_run_id} does not exist.")
    return ImportRunRecord(
        id=int(row.id),
        boundary_version_id=int(row.boundary_version_id),
        boundary_version_code=str(row.version_code),
        boundary_approval_status=str(row.approval_status),
        source_name=str(row.source_name),
        source_checksum_sha256=str(row.source_checksum_sha256),
        source_fingerprint=str(row.source_fingerprint),
        pipeline_version=str(row.pipeline_version),
        normalizer_version=str(row.normalizer_version),
        config_digest=str(row.config_digest),
        status=str(row.status),
    )


def begin_staging_run(
    conn: Connection,
    *,
    import_run_id: int,
    expected_source_fingerprint: str,
    expected_source_checksum_sha256: str,
    extraction_scope: str = EXTRACTION_SCOPE_NODE_ONLY,
) -> ImportRunRecord:
    """Start or safely resume staging for an ImportRun under a FOR UPDATE row lock.

    - PENDING -> transitions to PROCESSING and initializes staging_run_checkpoints (pass 1).
    - PROCESSING -> allowed ONLY if source_fingerprint and source_checksum_sha256 match
      both import_runs and staging_run_checkpoints; increments current_pass_number and
      resume_count while resetting current-pass counters and preserving cumulative unique evidence.
    - PREPARED / PUBLISHED / SUPERSEDED / FAILED / ABORTED -> rejected fail-closed.
    """
    locked_run = _lock_import_run_row(conn, import_run_id=import_run_id)

    if locked_run.status in _NON_STAGEABLE_RUN_STATUSES:
        raise ImportRunStateError(
            f"Refusing to stage into ImportRun id={import_run_id} "
            f"with status '{locked_run.status}'."
        )

    if locked_run.source_fingerprint != expected_source_fingerprint:
        raise ImportRunStateError(
            f"Source fingerprint mismatch for ImportRun id={import_run_id}: "
            f"run has '{locked_run.source_fingerprint}', incoming is "
            f"'{expected_source_fingerprint}'."
        )

    if locked_run.source_checksum_sha256 != expected_source_checksum_sha256:
        raise ImportRunStateError(
            f"Source checksum mismatch for ImportRun id={import_run_id}: "
            f"run has '{locked_run.source_checksum_sha256}', incoming is "
            f"'{expected_source_checksum_sha256}'."
        )

    if locked_run.status == "PENDING":
        conn.execute(
            text("UPDATE import_runs SET status = 'PROCESSING' WHERE id = :run_id"),
            {"run_id": import_run_id},
        )
        conn.execute(
            text(
                """
                INSERT INTO staging_run_checkpoints (
                    import_run_id,
                    extraction_scope,
                    staging_state,
                    source_fingerprint,
                    source_checksum_sha256
                )
                VALUES (
                    :run_id,
                    :extraction_scope,
                    'IN_PROGRESS',
                    :source_fingerprint,
                    :source_checksum_sha256
                )
                """
            ),
            {
                "run_id": import_run_id,
                "extraction_scope": extraction_scope,
                "source_fingerprint": expected_source_fingerprint,
                "source_checksum_sha256": expected_source_checksum_sha256,
            },
        )
        return load_import_run(conn, import_run_id=import_run_id)

    # Guarded resume on PROCESSING run: lock checkpoint row as well.
    checkpoint = conn.execute(
        text(
            """
            SELECT
                staging_state,
                source_fingerprint,
                source_checksum_sha256,
                extraction_scope
            FROM staging_run_checkpoints
            WHERE import_run_id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()
    if checkpoint is None:
        raise ImportRunStateError(
            f"PROCESSING ImportRun id={import_run_id} is missing a staging checkpoint."
        )
    if checkpoint.staging_state in ("VERIFIED", "FAILED"):
        raise ImportRunStateError(
            f"Refusing to resume ImportRun id={import_run_id} with checkpoint state "
            f"'{checkpoint.staging_state}'."
        )
    if (
        checkpoint.source_fingerprint != expected_source_fingerprint
        or checkpoint.source_checksum_sha256 != expected_source_checksum_sha256
        or checkpoint.extraction_scope != extraction_scope
    ):
        raise ImportRunStateError(
            f"Checkpoint provenance mismatch while resuming ImportRun id={import_run_id}."
        )

    actual_staged = int(
        conn.execute(
            text(
                "SELECT count(*) FROM staging_place_candidates WHERE import_run_id = :run_id"
            ),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    actual_issues = int(
        conn.execute(
            text("SELECT count(*) FROM import_issues WHERE import_run_id = :run_id"),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    conn.execute(
        text(
            """
            UPDATE staging_run_checkpoints
            SET staging_state = 'IN_PROGRESS',
                source_verified_post_parse = false,
                current_pass_number = current_pass_number + 1,
                resume_count = resume_count + 1,
                batches_committed = 0,
                total_nodes_seen = 0,
                supported_nodes_seen = 0,
                unsupported_nodes_seen = 0,
                valid_candidates_emitted = 0,
                invalid_candidates_emitted = 0,
                pass_inserted_count = 0,
                outside_boundary_count = 0,
                duplicate_identical_count = 0,
                pass_conflict_count = 0,
                staged_candidate_count = :actual_staged,
                issue_count = :actual_issues,
                extraction_completed_at = NULL,
                verified_at = NULL,
                readiness_evidence = NULL,
                updated_at = now()
            WHERE import_run_id = :run_id
            """
        ),
        {
            "run_id": import_run_id,
            "actual_staged": actual_staged,
            "actual_issues": actual_issues,
        },
    )
    return load_import_run(conn, import_run_id=import_run_id)


def record_import_issue(
    conn: Connection,
    *,
    import_run_id: int,
    issue: NormalizationIssue,
    sync_checkpoint: bool = True,
) -> int:
    """Persist a single NormalizationIssue idempotently into import_issues.

    If an identical issue fingerprint already exists for this run, returns the existing
    import_issues.id without inserting a duplicate row or inflating checkpoint.issue_count.
    """
    inserted, _ = _persist_unique_issues(
        conn,
        import_run_id=import_run_id,
        issues=[issue],
    )
    if inserted:
        issue_id = inserted[0][0]
        if sync_checkpoint:
            conn.execute(
                text(
                    """
                    UPDATE staging_run_checkpoints
                    SET issue_count = issue_count + 1,
                        updated_at = now()
                    WHERE import_run_id = :run_id
                    """
                ),
                {"run_id": import_run_id},
            )
        return issue_id

    issue_fp = compute_issue_fingerprint(import_run_id=import_run_id, issue=issue)
    existing_id = conn.execute(
        text(
            """
            SELECT import_issue_id
            FROM staging_issue_fingerprints
            WHERE import_run_id = :run_id
              AND issue_fingerprint = :issue_fp
            """
        ),
        {"run_id": import_run_id, "issue_fp": issue_fp},
    ).scalar_one()
    return int(existing_id)


def filter_candidates_by_boundary(
    conn: Connection,
    *,
    boundary_version_id: int,
    candidates: Sequence[CanonicalPlaceCandidate],
) -> tuple[list[CanonicalPlaceCandidate], list[CanonicalPlaceCandidate]]:
    """Filter candidates against boundary_versions.geom using real PostGIS ST_Covers."""
    if not candidates:
        return [], []

    idxs: list[int] = []
    lons: list[float] = []
    lats: list[float] = []
    for idx, cand in enumerate(candidates):
        lon, lat = validate_wgs84_coordinates(cand.longitude, cand.latitude)
        idxs.append(idx)
        lons.append(lon)
        lats.append(lat)

    rows = conn.execute(
        text(
            """
            WITH pts AS (
                SELECT idx, lon, lat
                FROM unnest(
                    CAST(:idxs AS integer[]),
                    CAST(:lons AS double precision[]),
                    CAST(:lats AS double precision[])
                ) AS t(idx, lon, lat)
            )
            SELECT
                p.idx,
                ST_Covers(b.geom, ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326)) AS covered
            FROM pts p
            CROSS JOIN boundary_versions b
            WHERE b.id = :boundary_version_id
            ORDER BY p.idx
            """
        ),
        {
            "idxs": idxs,
            "lons": lons,
            "lats": lats,
            "boundary_version_id": boundary_version_id,
        },
    ).fetchall()

    if len(rows) != len(candidates):
        raise StagingError(
            f"BoundaryVersion id={boundary_version_id} not found during ST_Covers filtering."
        )

    covered: list[CanonicalPlaceCandidate] = []
    outside: list[CanonicalPlaceCandidate] = []
    for cand, row in zip(candidates, rows, strict=True):
        if bool(row.covered):
            covered.append(cand)
        else:
            outside.append(cand)
    return covered, outside


def stage_candidate_batch(
    conn: Connection,
    *,
    import_run_id: int,
    candidates: Sequence[CanonicalPlaceCandidate],
    issues: Sequence[NormalizationIssue] = (),
    apply_boundary_filter: bool = True,
    boundary_version_id: int | None = None,
) -> BatchStageResult:
    """Persist a bounded batch of canonical candidates and issues under a row lock.

    Guarantees:
    - Serializes against concurrent transitions on import_run_id via SELECT ... FOR UPDATE.
    - Rejects any caller-supplied boundary_version_id that differs from import_runs.boundary_version_id.
    - Identical (osm_type, osm_id) with equal content_digest -> idempotent no-op updating only
      the technical pass marker `last_seen_pass` without mutating semantic evidence.
    - Conflicting (osm_type, osm_id) with differing content_digest -> preserves existing staged
      row untouched, records a unique ERROR STAGING_CONFLICT issue, and increments conflict counters.
    - Issues are persisted idempotently via staging_issue_fingerprints so retries never duplicate
      import_issues rows or inflate checkpoint.issue_count.
    """
    locked_run = _lock_import_run_row(conn, import_run_id=import_run_id)
    checkpoint = conn.execute(
        text(
            """
            SELECT staging_state, current_pass_number
            FROM staging_run_checkpoints
            WHERE import_run_id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()
    if checkpoint is None:
        raise ImportRunStateError(
            f"ImportRun id={import_run_id} is not initialized for staging."
        )
    if locked_run.status != "PROCESSING" or checkpoint.staging_state != "IN_PROGRESS":
        raise ImportRunStateError(
            f"Cannot stage batch into ImportRun id={import_run_id} "
            f"(run status={locked_run.status!r}, staging_state={checkpoint.staging_state!r})."
        )

    run_boundary_id = int(locked_run.boundary_version_id)
    if boundary_version_id is not None and int(boundary_version_id) != run_boundary_id:
        raise ImportRunStateError(
            f"Mismatched boundary_version_id for ImportRun id={import_run_id}: "
            f"run is bound to boundary_version_id={run_boundary_id}, "
            f"caller supplied boundary_version_id={boundary_version_id}."
        )

    current_pass = int(checkpoint.current_pass_number)
    batch_issues: list[NormalizationIssue] = list(issues)
    duplicate_identical = 0
    pass_conflicts = 0
    outside_boundary = 0
    inserted = 0

    # Step 1: Resolve duplicates within the incoming batch deterministically.
    unique_batch: list[tuple[CanonicalPlaceCandidate, str]] = []
    seen_in_batch: dict[tuple[str, int], str] = {}
    for cand in candidates:
        key = (cand.osm_type, cand.osm_id)
        digest = compute_candidate_content_digest(cand)
        existing_batch_digest = seen_in_batch.get(key)
        if existing_batch_digest is None:
            seen_in_batch[key] = digest
            unique_batch.append((cand, digest))
        elif existing_batch_digest == digest:
            duplicate_identical += 1
        else:
            pass_conflicts += 1
            batch_issues.append(
                NormalizationIssue(
                    severity=IssueSeverity.ERROR.value,
                    stage=PipelineStage.STAGE.value,
                    issue_code="STAGING_CONFLICT",
                    message=(
                        f"Conflicting duplicate OSM identity ({cand.osm_type}, {cand.osm_id}) "
                        "within staging batch; existing canonical candidate preserved."
                    ),
                    osm_type=cand.osm_type,
                    osm_id=cand.osm_id,
                    details={
                        "existing_content_digest": existing_batch_digest,
                        "incoming_content_digest": digest,
                        "scope": "within_batch",
                    },
                    source_occurrence_id=(
                        str(cand.source_provenance["source_occurrence_id"])
                        if cand.source_provenance.get("source_occurrence_id") is not None
                        else None
                    ),
                )
            )

    # Step 2: Optional real PostGIS ST_Covers boundary filter tied to the run's boundary.
    if apply_boundary_filter and unique_batch:
        covered_candidates, outside_candidates = filter_candidates_by_boundary(
            conn,
            boundary_version_id=run_boundary_id,
            candidates=[item[0] for item in unique_batch],
        )
        outside_boundary = len(outside_candidates)
        covered_identities = {(c.osm_type, c.osm_id) for c in covered_candidates}
        unique_batch = [
            (cand, digest)
            for cand, digest in unique_batch
            if (cand.osm_type, cand.osm_id) in covered_identities
        ]

    # Step 3: Compare against already-staged rows for this run (run-scoped idempotency/conflict).
    to_insert: list[tuple[CanonicalPlaceCandidate, str]] = []
    matched_existing_keys: list[tuple[str, int]] = []
    if unique_batch:
        keys = [(cand.osm_type, cand.osm_id) for cand, _ in unique_batch]
        existing_rows = conn.execute(
            select(
                StagingPlaceCandidate.osm_type,
                StagingPlaceCandidate.osm_id,
                StagingPlaceCandidate.content_digest,
            ).where(
                StagingPlaceCandidate.import_run_id == import_run_id,
                tuple_(StagingPlaceCandidate.osm_type, StagingPlaceCandidate.osm_id).in_(keys),
            )
        ).fetchall()
        existing_by_key = {
            (str(r.osm_type), int(r.osm_id)): str(r.content_digest) for r in existing_rows
        }

        for cand, digest in unique_batch:
            key = (cand.osm_type, cand.osm_id)
            stored_digest = existing_by_key.get(key)
            if stored_digest is None:
                to_insert.append((cand, digest))
            elif stored_digest == digest:
                duplicate_identical += 1
                matched_existing_keys.append(key)
            else:
                pass_conflicts += 1
                batch_issues.append(
                    NormalizationIssue(
                        severity=IssueSeverity.ERROR.value,
                        stage=PipelineStage.STAGE.value,
                        issue_code="STAGING_CONFLICT",
                        message=(
                            f"Conflicting OSM identity ({cand.osm_type}, {cand.osm_id}) against "
                            "already-staged record in this run; existing record preserved."
                        ),
                        osm_type=cand.osm_type,
                        osm_id=cand.osm_id,
                        details={
                            "existing_content_digest": stored_digest,
                            "incoming_content_digest": digest,
                            "scope": "existing_run_staging",
                        },
                        source_occurrence_id=(
                            str(cand.source_provenance["source_occurrence_id"])
                            if cand.source_provenance.get("source_occurrence_id") is not None
                            else None
                        ),
                    )
                )

    # Mark identically replayed staged rows as verified in the current pass without mutating data.
    if matched_existing_keys:
        conn.execute(
            text(
                """
                UPDATE staging_place_candidates
                SET last_seen_pass = :current_pass
                WHERE import_run_id = :run_id
                  AND osm_type = :osm_type
                  AND osm_id = :osm_id
                """
            ),
            [
                {
                    "current_pass": current_pass,
                    "run_id": import_run_id,
                    "osm_type": osm_type,
                    "osm_id": osm_id,
                }
                for osm_type, osm_id in matched_existing_keys
            ],
        )

    # Step 4: Persist new canonical candidates using validated WGS84 ST_MakePoint(lon, lat).
    if to_insert:
        insert_params: list[dict[str, Any]] = []
        for cand, digest in to_insert:
            lon, lat = validate_wgs84_coordinates(cand.longitude, cand.latitude)
            insert_params.append(
                {
                    "import_run_id": import_run_id,
                    "osm_type": cand.osm_type,
                    "osm_id": cand.osm_id,
                    "name": cand.name,
                    "name_search": cand.name_search,
                    "category": cand.category,
                    "address": cand.address,
                    "lon": lon,
                    "lat": lat,
                    "raw_tags_json": json.dumps(cand.raw_tags, sort_keys=True, ensure_ascii=False),
                    "osm_version": cand.osm_version,
                    "osm_changeset": cand.osm_changeset,
                    "osm_timestamp": cand.osm_timestamp,
                    "content_digest": digest,
                    "source_provenance_json": json.dumps(
                        cand.source_provenance, sort_keys=True, ensure_ascii=False
                    ),
                    "last_seen_pass": current_pass,
                }
            )
        conn.execute(
            text(
                """
                INSERT INTO staging_place_candidates (
                    import_run_id,
                    osm_type,
                    osm_id,
                    name,
                    name_search,
                    category,
                    address,
                    location,
                    raw_tags,
                    osm_version,
                    osm_changeset,
                    osm_timestamp,
                    content_digest,
                    source_provenance,
                    last_seen_pass
                )
                VALUES (
                    :import_run_id,
                    :osm_type,
                    :osm_id,
                    :name,
                    :name_search,
                    :category,
                    :address,
                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                    CAST(:raw_tags_json AS jsonb),
                    :osm_version,
                    :osm_changeset,
                    :osm_timestamp,
                    :content_digest,
                    CAST(:source_provenance_json AS jsonb),
                    :last_seen_pass
                )
                """
            ),
            insert_params,
        )
        inserted = len(insert_params)

    # Step 5: Persist unique issues idempotently (no duplicate import_issues on retry).
    inserted_issues, duplicate_issues_skipped = _persist_unique_issues(
        conn,
        import_run_id=import_run_id,
        issues=batch_issues,
    )
    new_unique_issues = len(inserted_issues)
    new_unique_conflicts = sum(
        1 for _, issue in inserted_issues if issue.issue_code == "STAGING_CONFLICT"
    )

    last_osm_id = max((c.osm_id for c in candidates), default=None)
    conn.execute(
        text(
            """
            UPDATE staging_run_checkpoints
            SET batches_committed = batches_committed + 1,
                pass_inserted_count = pass_inserted_count + :inserted,
                staged_candidate_count = staged_candidate_count + :inserted,
                outside_boundary_count = outside_boundary_count + :outside_boundary,
                duplicate_identical_count = duplicate_identical_count + :duplicate_identical,
                pass_conflict_count = pass_conflict_count + :pass_conflicts,
                conflict_count = conflict_count + :new_unique_conflicts,
                issue_count = issue_count + :new_unique_issues,
                last_osm_id = COALESCE(:last_osm_id, last_osm_id),
                updated_at = now()
            WHERE import_run_id = :run_id
            """
        ),
        {
            "run_id": import_run_id,
            "inserted": inserted,
            "outside_boundary": outside_boundary,
            "duplicate_identical": duplicate_identical,
            "pass_conflicts": pass_conflicts,
            "new_unique_conflicts": new_unique_conflicts,
            "new_unique_issues": new_unique_issues,
            "last_osm_id": last_osm_id,
        },
    )

    return BatchStageResult(
        inserted=inserted,
        outside_boundary=outside_boundary,
        duplicate_identical=duplicate_identical,
        conflicts=pass_conflicts,
        issues_recorded=new_unique_issues,
        duplicate_issues_skipped=duplicate_issues_skipped,
    )


def mark_extraction_completed(
    conn: Connection,
    *,
    import_run_id: int,
    stats: ExtractionStats,
    source_verified_post_parse: bool = True,
) -> None:
    """Record extraction EOF statistics under a row lock without marking the run PREPARED."""
    locked_run = _lock_import_run_row(conn, import_run_id=import_run_id)
    if locked_run.status != "PROCESSING":
        raise ImportRunStateError(
            f"Cannot mark extraction completed for ImportRun id={import_run_id}: "
            f"run status is '{locked_run.status}'."
        )

    cp = conn.execute(
        text(
            """
            SELECT
                staging_state,
                pass_inserted_count,
                outside_boundary_count,
                duplicate_identical_count,
                pass_conflict_count
            FROM staging_run_checkpoints
            WHERE import_run_id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()
    if cp is None or cp.staging_state != "IN_PROGRESS":
        raise ImportRunStateError(
            f"Cannot mark extraction completed for ImportRun id={import_run_id}: "
            "checkpoint missing or not IN_PROGRESS."
        )

    if stats.total_nodes_seen != stats.supported_nodes_seen + stats.unsupported_nodes_seen:
        raise ImportRunStateError(
            f"Inconsistent ExtractionStats for ImportRun id={import_run_id}: "
            f"total_nodes_seen ({stats.total_nodes_seen}) != "
            f"supported ({stats.supported_nodes_seen}) + unsupported ({stats.unsupported_nodes_seen})."
        )
    if stats.supported_nodes_seen != stats.valid_candidates + stats.invalid_candidates:
        raise ImportRunStateError(
            f"Inconsistent ExtractionStats for ImportRun id={import_run_id}: "
            f"supported_nodes_seen ({stats.supported_nodes_seen}) != "
            f"valid ({stats.valid_candidates}) + invalid ({stats.invalid_candidates})."
        )

    pass_accounted = (
        int(cp.pass_inserted_count)
        + int(cp.outside_boundary_count)
        + int(cp.duplicate_identical_count)
        + int(cp.pass_conflict_count)
    )
    if pass_accounted != stats.valid_candidates:
        raise ImportRunStateError(
            f"Current-pass accounting mismatch for ImportRun id={import_run_id}: "
            f"pass_inserted({cp.pass_inserted_count}) + outside({cp.outside_boundary_count}) + "
            f"duplicate_identical({cp.duplicate_identical_count}) + "
            f"pass_conflict({cp.pass_conflict_count}) = {pass_accounted}, "
            f"but valid_candidates_emitted = {stats.valid_candidates}."
        )

    conn.execute(
        text(
            """
            UPDATE staging_run_checkpoints
            SET staging_state = 'EXTRACTED',
                source_verified_post_parse = :source_verified_post_parse,
                total_nodes_seen = :total_nodes_seen,
                supported_nodes_seen = :supported_nodes_seen,
                unsupported_nodes_seen = :unsupported_nodes_seen,
                valid_candidates_emitted = :valid_candidates_emitted,
                invalid_candidates_emitted = :invalid_candidates_emitted,
                extraction_completed_at = now(),
                updated_at = now()
            WHERE import_run_id = :run_id
              AND staging_state = 'IN_PROGRESS'
            """
        ),
        {
            "run_id": import_run_id,
            "source_verified_post_parse": bool(source_verified_post_parse),
            "total_nodes_seen": stats.total_nodes_seen,
            "supported_nodes_seen": stats.supported_nodes_seen,
            "unsupported_nodes_seen": stats.unsupported_nodes_seen,
            "valid_candidates_emitted": stats.valid_candidates,
            "invalid_candidates_emitted": stats.invalid_candidates,
        },
    )


def mark_staging_failed(
    conn: Connection,
    *,
    import_run_id: int,
    error_message: str,
    issue_code: str = "SOURCE_INTEGRITY_ERROR",
    stage: str = PipelineStage.EXTRACT.value,
    details: dict[str, Any] | None = None,
) -> None:
    """Record a unique run-level failure issue and transition run/checkpoint into FAILED state."""
    _lock_import_run_row(conn, import_run_id=import_run_id)
    record_import_issue(
        conn,
        import_run_id=import_run_id,
        issue=NormalizationIssue(
            severity=IssueSeverity.ERROR.value,
            stage=stage,
            issue_code=issue_code,
            message=error_message,
            osm_type=None,
            osm_id=None,
            details=details,
        ),
        sync_checkpoint=True,
    )
    conn.execute(
        text(
            """
            UPDATE staging_run_checkpoints
            SET staging_state = 'FAILED',
                source_verified_post_parse = false,
                updated_at = now()
            WHERE import_run_id = :run_id
            """
        ),
        {"run_id": import_run_id},
    )
    conn.execute(
        text(
            """
            UPDATE import_runs
            SET status = 'FAILED',
                completed_at = now()
            WHERE id = :run_id
              AND status IN ('PENDING', 'PROCESSING')
            """
        ),
        {"run_id": import_run_id},
    )


def verify_and_prepare_run(
    conn: Connection,
    *,
    import_run_id: int,
    on_before_prepare_commit: Callable[[Connection], None] | None = None,
) -> ReadinessReport:
    """Verify strict M2 staging readiness under FOR UPDATE row locks and transition to PREPARED.

    Fail-closed invariants enforced:
    - Run must be PROCESSING with active_slot IS NULL.
    - Checkpoint must exist in EXTRACTED state with extraction_completed_at and
      source_verified_post_parse = TRUE.
    - Run and checkpoint source_fingerprint & source_checksum_sha256 must match.
    - extraction_scope must be explicitly 'node_only'.
    - Zero unresolved staging conflicts (cumulative & pass) and zero blocking ERROR issues.
    - Exact DB row reconciliation against staging_place_candidates and import_issues.
    - Exact current-pass accounting:
      pass_inserted + outside_boundary + duplicate_identical + pass_conflict == valid_emitted.
    - Zero stale/orphaned rows in staging_place_candidates (all rows verified in current pass).
    - Non-empty staged dataset (actual_staged_count > 0; fail-closed, no bypass flag).
    - Valid bound boundary_versions MultiPolygon (SRID 4326) with ZERO uncovered staged rows
      under PostGIS ST_Covers(boundary_versions.geom, staging_place_candidates.location::geometry).
    """
    locked_run = conn.execute(
        text(
            """
            SELECT
                r.id AS run_id,
                r.status AS run_status,
                r.active_slot,
                r.source_fingerprint AS run_fingerprint,
                r.source_checksum_sha256 AS run_checksum,
                r.boundary_version_id
            FROM import_runs r
            WHERE r.id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()

    if locked_run is None:
        raise StagingReadinessError(
            f"ImportRun id={import_run_id} does not exist.",
            reasons=[f"ImportRun id={import_run_id} not found."],
        )

    boundary_row = conn.execute(
        text(
            """
            SELECT
                b.id,
                b.version_code,
                b.approval_status,
                ST_IsValid(b.geom) AS geom_valid,
                NOT ST_IsEmpty(b.geom) AS geom_non_empty,
                ST_SRID(b.geom) AS geom_srid
            FROM boundary_versions b
            WHERE b.id = :bv_id
            """
        ),
        {"bv_id": locked_run.boundary_version_id},
    ).one_or_none()

    cp = conn.execute(
        text(
            """
            SELECT
                extraction_scope,
                staging_state,
                source_fingerprint AS cp_fingerprint,
                source_checksum_sha256 AS cp_checksum,
                source_verified_post_parse,
                current_pass_number,
                resume_count,
                batches_committed,
                total_nodes_seen,
                supported_nodes_seen,
                unsupported_nodes_seen,
                valid_candidates_emitted,
                invalid_candidates_emitted,
                pass_inserted_count,
                outside_boundary_count,
                duplicate_identical_count,
                pass_conflict_count,
                staged_candidate_count,
                conflict_count,
                issue_count,
                extraction_completed_at
            FROM staging_run_checkpoints
            WHERE import_run_id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()

    reasons: list[str] = []
    if locked_run.run_status != "PROCESSING":
        reasons.append(
            f"ImportRun status must be 'PROCESSING', found '{locked_run.run_status}'."
        )
    if locked_run.active_slot is not None:
        reasons.append("ImportRun already holds active_slot and cannot be prepared.")

    if boundary_row is None:
        reasons.append(
            f"Referenced boundary_version_id={locked_run.boundary_version_id} is missing."
        )
    else:
        if not boundary_row.geom_valid or not boundary_row.geom_non_empty:
            reasons.append("Referenced boundary geometry is invalid or empty.")
        if int(boundary_row.geom_srid) != 4326:
            reasons.append(
                f"Referenced boundary SRID must be 4326, found {boundary_row.geom_srid}."
            )

    if cp is None:
        reasons.append("Missing staging_run_checkpoints record for this ImportRun.")
    else:
        if cp.staging_state != "EXTRACTED":
            reasons.append(
                f"Staging checkpoint must be 'EXTRACTED' before verification, "
                f"found '{cp.staging_state}'."
            )
        if cp.extraction_completed_at is None:
            reasons.append("Checkpoint is missing extraction_completed_at timestamp.")
        if not bool(cp.source_verified_post_parse):
            reasons.append(
                "Source checksum was not verified after parsing (source_verified_post_parse=false)."
            )
        if str(cp.extraction_scope) != EXTRACTION_SCOPE_NODE_ONLY:
            reasons.append(
                f"Unsupported extraction_scope '{cp.extraction_scope}'; expected 'node_only'."
            )
        if cp.cp_fingerprint != locked_run.run_fingerprint:
            reasons.append("Checkpoint source_fingerprint does not match import_runs.")
        if cp.cp_checksum != locked_run.run_checksum:
            reasons.append("Checkpoint source_checksum_sha256 does not match import_runs.")
        if int(cp.conflict_count) > 0 or int(cp.pass_conflict_count) > 0:
            reasons.append(
                "Unresolved staging identity conflicts detected "
                f"(conflict_count={cp.conflict_count}, pass_conflict_count={cp.pass_conflict_count})."
            )

    actual_staged_count = int(
        conn.execute(
            text(
                "SELECT count(*) FROM staging_place_candidates WHERE import_run_id = :run_id"
            ),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    actual_issue_count = int(
        conn.execute(
            text("SELECT count(*) FROM import_issues WHERE import_run_id = :run_id"),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    blocking_issue_count = int(
        conn.execute(
            text(
                """
                SELECT count(*)
                FROM import_issues
                WHERE import_run_id = :run_id
                  AND severity = 'ERROR'
                  AND issue_code IN ('STAGING_CONFLICT', 'SOURCE_INTEGRITY_ERROR')
                """
            ),
            {"run_id": import_run_id},
        ).scalar_one()
    )

    if blocking_issue_count > 0:
        reasons.append(
            f"Run has {blocking_issue_count} blocking ERROR issue(s) "
            "(STAGING_CONFLICT or SOURCE_INTEGRITY_ERROR)."
        )

    if actual_staged_count == 0:
        reasons.append(
            "Staged dataset is empty (0 canonical candidates); refusing PREPARED transition."
        )

    uncovered_staged_count = 0
    if boundary_row is not None and actual_staged_count > 0:
        uncovered_staged_count = int(
            conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM staging_place_candidates s
                    JOIN boundary_versions b ON b.id = :bv_id
                    WHERE s.import_run_id = :run_id
                      AND NOT ST_Covers(b.geom, s.location::geometry)
                    """
                ),
                {"run_id": import_run_id, "bv_id": locked_run.boundary_version_id},
            ).scalar_one()
        )
        if uncovered_staged_count > 0:
            reasons.append(
                f"Geographic membership violation: {uncovered_staged_count} staged candidate(s) "
                f"are not covered by boundary_version_id={locked_run.boundary_version_id} "
                "under PostGIS ST_Covers."
            )

    stale_staged_count = 0
    if cp is not None:
        if actual_staged_count != int(cp.staged_candidate_count):
            reasons.append(
                f"Staged row count mismatch: actual={actual_staged_count}, "
                f"checkpoint={cp.staged_candidate_count}."
            )
        if actual_issue_count != int(cp.issue_count):
            reasons.append(
                f"Import issue count mismatch: actual={actual_issue_count}, "
                f"checkpoint={cp.issue_count}."
            )
        if int(cp.total_nodes_seen) != int(cp.supported_nodes_seen) + int(
            cp.unsupported_nodes_seen
        ):
            reasons.append("Node total counter does not equal supported + unsupported.")
        if int(cp.supported_nodes_seen) != int(cp.valid_candidates_emitted) + int(
            cp.invalid_candidates_emitted
        ):
            reasons.append("Supported node counter does not equal valid + invalid.")

        pass_accounted = (
            int(cp.pass_inserted_count)
            + int(cp.outside_boundary_count)
            + int(cp.duplicate_identical_count)
            + int(cp.pass_conflict_count)
        )
        if pass_accounted != int(cp.valid_candidates_emitted):
            reasons.append(
                f"Current-pass candidate accounting mismatch: accounted={pass_accounted}, "
                f"valid_candidates_emitted={cp.valid_candidates_emitted}."
            )

        stale_staged_count = int(
            conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM staging_place_candidates
                    WHERE import_run_id = :run_id
                      AND last_seen_pass <> :current_pass
                    """
                ),
                {
                    "run_id": import_run_id,
                    "current_pass": int(cp.current_pass_number),
                },
            ).scalar_one()
        )
        if stale_staged_count > 0:
            reasons.append(
                f"Detected {stale_staged_count} stale/unverified staging row(s) not matched "
                f"during final pass #{cp.current_pass_number}."
            )

    if reasons:
        raise StagingReadinessError(
            f"ImportRun id={import_run_id} failed M2 staging readiness verification: "
            + "; ".join(reasons),
            reasons=reasons,
        )

    assert cp is not None and boundary_row is not None

    if on_before_prepare_commit is not None:
        on_before_prepare_commit(conn)

    report = ReadinessReport(
        import_run_id=import_run_id,
        extraction_scope=str(cp.extraction_scope),
        boundary_version_id=int(locked_run.boundary_version_id),
        boundary_version_code=str(boundary_row.version_code),
        boundary_approval_status=str(boundary_row.approval_status),
        boundary_filter_verified=True,
        uncovered_staged_count=uncovered_staged_count,
        stale_staged_count=stale_staged_count,
        source_verified_post_parse=bool(cp.source_verified_post_parse),
        source_fingerprint=str(locked_run.run_fingerprint),
        source_checksum_sha256=str(locked_run.run_checksum),
        current_pass_number=int(cp.current_pass_number),
        resume_count=int(cp.resume_count),
        staged_candidate_count=actual_staged_count,
        pass_inserted_count=int(cp.pass_inserted_count),
        outside_boundary_count=int(cp.outside_boundary_count),
        duplicate_identical_count=int(cp.duplicate_identical_count),
        pass_conflict_count=int(cp.pass_conflict_count),
        conflict_count=int(cp.conflict_count),
        issue_count=actual_issue_count,
        total_nodes_seen=int(cp.total_nodes_seen),
        supported_nodes_seen=int(cp.supported_nodes_seen),
        unsupported_nodes_seen=int(cp.unsupported_nodes_seen),
        valid_candidates_emitted=int(cp.valid_candidates_emitted),
        invalid_candidates_emitted=int(cp.invalid_candidates_emitted),
        batches_committed=int(cp.batches_committed),
    )

    conn.execute(
        text(
            """
            UPDATE staging_run_checkpoints
            SET staging_state = 'VERIFIED',
                verified_at = GREATEST(now(), extraction_completed_at),
                readiness_evidence = CAST(:evidence_json AS jsonb),
                updated_at = now()
            WHERE import_run_id = :run_id
            """
        ),
        {
            "run_id": import_run_id,
            "evidence_json": json.dumps(report.to_dict(), sort_keys=True),
        },
    )
    conn.execute(
        text(
            """
            UPDATE import_runs
            SET status = 'PREPARED',
                completed_at = now()
            WHERE id = :run_id
            """
        ),
        {"run_id": import_run_id},
    )
    return report
