"""Typed domain models, enums, fault hooks, and exceptions for M3 transactional publication."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Final

from sqlalchemy import Connection

# Deterministic 64-bit signed PostgreSQL transaction advisory lock key for publication.
# Derived from UTF-8 SHA-256 prefix of "geosearch.publication.active_slot.v1" masked to signed int64.
PUBLICATION_ADVISORY_LOCK_KEY: Final[int] = 0x47535055425F5631  # "GSPUB_V1" (5139543997253654065)
RECOVERY_ARTIFACT_FORMAT_VERSION: Final[str] = "geosearch.recovery_artifact.v1"
RECOVERY_PIPELINE_VERSION: Final[str] = "geosearch-recovery-0.1.0"
NODE_ONLY_COVERAGE_RESTRICTION: Final[str] = "node_only_incomplete_osm_coverage"


class PublicationMode(str, Enum):
    """Explicit authorization mode separating controlled Node-only tests from authoritative publish.

    - AUTHORITATIVE: Default mode. Requires complete OSM coverage (not node_only) and
      externally verified authoritative boundary evidence.
    - CONTROLLED_TEST_NODE_ONLY: Explicit opt-in for isolated engineering verification using
      synthetic APPROVED boundaries and Node-only datasets. Never permitted to overwrite an
      active authoritative/full-coverage dataset or deactivate Way/Relation rows.
    """

    AUTHORITATIVE = "AUTHORITATIVE"
    CONTROLLED_TEST_NODE_ONLY = "CONTROLLED_TEST_NODE_ONLY"


class EligibilityStatus(str, Enum):
    """Typed fail-closed publication eligibility classification."""

    ELIGIBLE = "ELIGIBLE"
    ALREADY_PUBLISHED = "ALREADY_PUBLISHED"
    NOT_PREPARED = "NOT_PREPARED"
    INVALID_STAGING = "INVALID_STAGING"
    BOUNDARY_NOT_APPROVED = "BOUNDARY_NOT_APPROVED"
    COVERAGE_INCOMPATIBLE = "COVERAGE_INCOMPATIBLE"
    PUBLICATION_CONFLICT = "PUBLICATION_CONFLICT"
    SOURCE_PROVENANCE_MISMATCH = "SOURCE_PROVENANCE_MISMATCH"
    MISSING_RECOVERY_ARTIFACT = "MISSING_RECOVERY_ARTIFACT"
    OTHER_BLOCKING_FAILURE = "OTHER_BLOCKING_FAILURE"


class EligibilityTier(str, Enum):
    """Explicit distinction between staging readiness, test publication, and authoritative publish."""

    NOT_ELIGIBLE = "NOT_ELIGIBLE"
    STAGING_READY = "STAGING_READY"
    ELIGIBLE_FOR_CONTROLLED_TEST_PUBLICATION = "ELIGIBLE_FOR_CONTROLLED_TEST_PUBLICATION"
    ELIGIBLE_FOR_AUTHORITATIVE_PUBLICATION = "ELIGIBLE_FOR_AUTHORITATIVE_PUBLICATION"


class DuplicateDetectionState(str, Enum):
    """Explicit state distinguishing unexecuted duplicate detection from verified empty results."""

    NOT_EXECUTED = "NOT_EXECUTED"
    VERIFIED_EMPTY = "VERIFIED_EMPTY"
    CANDIDATES_PROVIDED = "CANDIDATES_PROVIDED"


class PublicationOutcomeStatus(str, Enum):
    """Final durable outcome of a publication call."""

    PUBLISHED = "PUBLISHED"
    ALREADY_PUBLISHED = "ALREADY_PUBLISHED"


class PublicationError(RuntimeError):
    """Base error for M3 transactional publication operations."""


class PublicationEligibilityError(PublicationError):
    """Raised when a run fails fail-closed preflight or lock-guarded eligibility checks."""

    def __init__(
        self,
        message: str,
        *,
        status: EligibilityStatus,
        reasons: tuple[str, ...],
        eligibility_result: PublicationEligibilityResult | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.reasons = reasons
        self.eligibility_result = eligibility_result


class PublicationConflictError(PublicationEligibilityError):
    """Raised on conflicting publication operation UUIDs, ownership, or stale active baselines."""


class RecoveryArtifactError(PublicationError):
    """Raised when a required pre-publication or historical recovery artifact is invalid."""


class RecoveryApprovalError(PublicationError):
    """Raised when mandatory manual recovery approval is missing, malformed, or mismatched."""


class PublicationInvariantError(PublicationError):
    """Raised when pre-COMMIT live verification fails inside the publication transaction."""


class LostCommitAcknowledgementError(PublicationError):
    """Raised when COMMIT succeeded durably in PostgreSQL but client acknowledgement was lost."""

    def __init__(
        self,
        message: str,
        *,
        import_run_id: int,
        publication_operation_id: uuid.UUID,
    ) -> None:
        super().__init__(message)
        self.import_run_id = import_run_id
        self.publication_operation_id = publication_operation_id


@dataclass(frozen=True, slots=True)
class StagedDuplicateCandidate:
    """Validated run-scoped suspected duplicate pair identified by OSM identity before UPSERT."""

    osm_type_a: str
    osm_id_a: int
    osm_type_b: str
    osm_id_b: int
    detector_version: str
    evidence: dict[str, Any]
    confidence_score: Decimal | None = None
    distance_meters: float | None = None


@dataclass(frozen=True, slots=True)
class DuplicateCandidateInput:
    """Explicit duplicate candidate resolution payload distinguishing unexecuted vs verified empty."""

    state: DuplicateDetectionState = DuplicateDetectionState.NOT_EXECUTED
    detector_version: str | None = None
    candidates: tuple[StagedDuplicateCandidate, ...] = ()


@dataclass(frozen=True, slots=True)
class PublicationOperationEvidence:
    """Durable database evidence observed for a publication operation UUID or import run."""

    import_run_id: int
    status: str
    active_slot: int | None
    boundary_version_id: int
    publication_operation_id: uuid.UUID | None
    published_at: datetime | None
    superseded_at: datetime | None
    publication_evidence: dict[str, Any] | None

    @property
    def is_committed_publication(self) -> bool:
        """True only when the run durably reached PUBLISHED or SUPERSEDED with published_at."""
        return (
            self.status in ("PUBLISHED", "SUPERSEDED")
            and self.publication_operation_id is not None
            and self.published_at is not None
        )


@dataclass(frozen=True, slots=True)
class PublicationEligibilityResult:
    """Typed result produced by M3.1 publication preflight and lock-guarded revalidation."""

    status: EligibilityStatus
    tier: EligibilityTier
    eligible: bool
    import_run_id: int
    publication_mode: PublicationMode
    publication_operation_id: uuid.UUID | None
    current_active_run_id: int | None
    expected_active_run_id: int | None
    boundary_version_id: int | None
    boundary_approval_status: str | None
    extraction_scope: str | None
    coverage_restriction: str | None
    staged_candidate_count: int
    is_first_publication: bool
    reasons: tuple[str, ...] = ()
    existing_operation_evidence: PublicationOperationEvidence | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize eligibility diagnostics for structured logging or audit."""
        return {
            "status": self.status.value,
            "tier": self.tier.value,
            "eligible": self.eligible,
            "import_run_id": self.import_run_id,
            "publication_mode": self.publication_mode.value,
            "publication_operation_id": (
                str(self.publication_operation_id)
                if self.publication_operation_id is not None
                else None
            ),
            "current_active_run_id": self.current_active_run_id,
            "expected_active_run_id": self.expected_active_run_id,
            "boundary_version_id": self.boundary_version_id,
            "boundary_approval_status": self.boundary_approval_status,
            "extraction_scope": self.extraction_scope,
            "coverage_restriction": self.coverage_restriction,
            "staged_candidate_count": self.staged_candidate_count,
            "is_first_publication": self.is_first_publication,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class RecoveryArtifactRecord:
    """Metadata describing a finalized, verified immutable recovery artifact on disk."""

    artifact_dir: Path
    manifest_path: Path
    manifest_checksum_path: Path
    places_snapshot_path: Path
    duplicate_candidates_snapshot_path: Path
    artifact_format: str
    source_import_run_id: int
    source_publication_operation_id: uuid.UUID
    source_boundary_version_id: int
    source_boundary_checksum_sha256: str
    active_place_count: int
    duplicate_candidate_count: int
    active_osm_identities_sha256: str
    content_sha256: str
    manifest_sha256: str
    manifest: dict[str, Any]

    def to_evidence_dict(self) -> dict[str, Any]:
        """Return compact durable evidence summary suitable for import_runs.publication_evidence."""
        return {
            "artifact_format": self.artifact_format,
            "artifact_dir": str(self.artifact_dir),
            "manifest_path": str(self.manifest_path),
            "source_import_run_id": self.source_import_run_id,
            "source_publication_operation_id": str(self.source_publication_operation_id),
            "source_boundary_version_id": self.source_boundary_version_id,
            "source_boundary_checksum_sha256": self.source_boundary_checksum_sha256,
            "active_place_count": self.active_place_count,
            "duplicate_candidate_count": self.duplicate_candidate_count,
            "active_osm_identities_sha256": self.active_osm_identities_sha256,
            "content_sha256": self.content_sha256,
            "manifest_sha256": self.manifest_sha256,
        }


@dataclass(frozen=True, slots=True)
class PublicationFaultHooks:
    """Deterministic checkpoint hooks for M3.5 fault-injection and concurrency testing.

    Every hook receives the active SQLAlchemy Connection (except post-commit acknowledgement
    and pre-lock hooks where no publication transaction is open yet). Hooks may synchronize
    threads or raise exceptions to prove atomicity and rollback safety.
    """

    on_during_registration: Callable[[Connection], None] | None = None
    on_before_lock: Callable[[], None] | None = None
    on_after_lock: Callable[[Connection], None] | None = None
    on_after_revalidation: Callable[[Connection], None] | None = None
    on_before_upsert: Callable[[Connection], None] | None = None
    on_during_upsert: Callable[[Connection, int], None] | None = None
    on_after_upsert: Callable[[Connection], None] | None = None
    on_during_reconciliation: Callable[[Connection], None] | None = None
    on_after_deactivation: Callable[[Connection], None] | None = None
    on_during_duplicate_resolution: Callable[[Connection], None] | None = None
    on_during_pre_commit_verification: Callable[[Connection], None] | None = None
    on_during_active_slot_switch: Callable[[Connection], None] | None = None
    on_before_commit: Callable[[Connection], None] | None = None
    on_after_commit_before_ack: Callable[[int, uuid.UUID], None] | None = None


@dataclass(frozen=True, slots=True)
class PublicationResult:
    """Structured durable result returned by publish_prepared_run."""

    outcome: PublicationOutcomeStatus
    import_run_id: int
    publication_operation_id: uuid.UUID
    boundary_version_id: int
    publication_mode: PublicationMode
    previous_active_run_id: int | None
    published_at: datetime
    active_place_count: int
    inserted_place_count: int
    updated_place_count: int
    reactivated_place_count: int
    deactivated_place_count: int
    duplicate_candidate_count: int
    duplicate_detection_state: DuplicateDetectionState
    recovery_artifact: RecoveryArtifactRecord | None
    publication_evidence: dict[str, Any] = field(default_factory=dict)
    already_committed: bool = False


@dataclass(frozen=True, slots=True)
class RecoveryApproval:
    """Traceable manual operator approval tied to a specific artifact, operation, and active run.

    Trust assumption:
    - This internal Python contract records durable approval provenance (`approved_by`,
      `approved_at`, `approval_reason`) and binds it cryptographically to the verified source
      artifact (`source_import_run_id`, `source_artifact_manifest_sha256`,
      `source_artifact_content_sha256`), the recovery `publication_operation_id`, and
      `expected_active_run_id`.
    - Human reviewer authentication and authorization (verifying the identity behind
      `approved_by`) are external to this library entry point.
    """

    approved_by: str
    approved_at: datetime
    approval_reason: str
    publication_operation_id: uuid.UUID
    source_import_run_id: int
    source_artifact_manifest_sha256: str
    source_artifact_content_sha256: str
    expected_active_run_id: int | None

    def to_evidence_dict(self) -> dict[str, Any]:
        """Serialize approval metadata canonically for durable checkpoint & publication audit."""
        from datetime import timezone

        return {
            "approved_by": self.approved_by.strip(),
            "approved_at": self.approved_at.astimezone(timezone.utc).isoformat(),
            "approval_reason": self.approval_reason.strip(),
            "publication_operation_id": str(self.publication_operation_id),
            "source_import_run_id": int(self.source_import_run_id),
            "source_artifact_manifest_sha256": self.source_artifact_manifest_sha256,
            "source_artifact_content_sha256": self.source_artifact_content_sha256,
            "expected_active_run_id": (
                int(self.expected_active_run_id)
                if self.expected_active_run_id is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class RecoveryArtifactPlaceRecord:
    """Validated canonical place record parsed from a verified `places_snapshot.jsonl`."""

    place_id: int
    osm_type: str
    osm_id: int
    name: str | None
    name_search: str | None
    category: str
    address: str | None
    longitude: float
    latitude: float
    location_srid: int
    location_ewkb_hex: str
    raw_tags: dict[str, Any]
    last_seen_import_id: int
    is_active: bool
    osm_version: int | None
    osm_changeset: int | None
    osm_timestamp: datetime | None
    created_at: datetime | None
    updated_at: datetime | None


@dataclass(frozen=True, slots=True)
class VerifiedHistoricalRecoveryArtifact:
    """Historical source artifact verified against disk checksums, JSONL content, and DB provenance."""

    record: RecoveryArtifactRecord
    places: tuple[RecoveryArtifactPlaceRecord, ...]
    source_extraction_scope: str
    source_coverage_restriction: str | None
    source_publication_mode: str | None
    source_run_status_in_db: str
    source_boundary_version_code: str
    manifest_sha256: str
    places_file_sha256: str
    duplicates_file_sha256: str


@dataclass(frozen=True, slots=True)
class RecoveryFaultHooks:
    """Deterministic checkpoint hooks for M5 recovery preparation, TOCTOU, and publication tests."""

    on_after_source_verification: Callable[[Path], None] | None = None
    on_before_prepare_commit: Callable[[Connection], None] | None = None
    on_after_prepare_before_publish: Callable[[int, uuid.UUID], None] | None = None
    publication_hooks: PublicationFaultHooks | None = None


@dataclass(frozen=True, slots=True)
class PreparedRecoveryRun:
    """Summary of an atomically staged and PREPARED recovery ImportRun."""

    import_run_id: int
    publication_operation_id: uuid.UUID
    source_artifact: RecoveryArtifactRecord
    approval: RecoveryApproval
    expected_active_run_id: int | None
    boundary_version_id: int
    staged_place_count: int
    recovery_evidence: dict[str, Any]
    reused_existing_run: bool = False
    already_committed: bool = False


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    """Structured durable outcome returned by execute_controlled_recovery."""

    publication_result: PublicationResult
    source_artifact: RecoveryArtifactRecord
    protection_artifact: RecoveryArtifactRecord | None
    approval: RecoveryApproval
    reused_prepared_run: bool = False

    @property
    def outcome(self) -> PublicationOutcomeStatus:
        """Delegate final publication outcome (PUBLISHED or ALREADY_PUBLISHED)."""
        return self.publication_result.outcome

    @property
    def recovery_import_run_id(self) -> int:
        """Return the newly created (or reused) RECOVERY import_runs.id."""
        return self.publication_result.import_run_id

    @property
    def publication_operation_id(self) -> uuid.UUID:
        """Return the recovery publication operation UUID."""
        return self.publication_result.publication_operation_id

    @property
    def already_committed(self) -> bool:
        """True when retry resolved an already-committed (PUBLISHED or SUPERSEDED) recovery."""
        return self.publication_result.already_committed
