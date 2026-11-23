"""SQLAlchemy 2.x technical staging models for M2 OSM ingestion.

Architecture note:
- GeoSearch maintains exactly five primary business tables in geosearch.db.models:
  places, boundary_versions, import_runs, import_issues, duplicate_candidates.
- The approved architecture baseline explicitly allows technical staging structures.
- These run-scoped technical staging tables accumulate validated candidates,
  idempotent issue fingerprints, and checkpoint/readiness metadata before M3
  publication without touching the five primary tables or live `places`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from geosearch.db.base import Base
from geosearch.db.types import PostGISGeography

STAGING_RUN_STATES: tuple[str, ...] = (
    "IN_PROGRESS",
    "EXTRACTED",
    "VERIFIED",
    "FAILED",
)
STAGING_EXTRACTION_SCOPES: tuple[str, ...] = ("node_only",)


class StagingPlaceCandidate(Base):
    """Run-scoped technical staging table for validated canonical OSM place candidates."""

    __tablename__ = "staging_place_candidates"
    __table_args__ = (
        UniqueConstraint(
            "import_run_id",
            "osm_type",
            "osm_id",
            name="uq_staging_place_candidates_run_osm_identity",
        ),
        CheckConstraint("osm_type IN ('node', 'way', 'relation')", name="osm_type_domain"),
        CheckConstraint("osm_id > 0", name="osm_id_positive"),
        CheckConstraint(
            "category IN ('cafe', 'restaurant', 'atm', 'pharmacy', 'supermarket')",
            name="category_domain",
        ),
        CheckConstraint(
            "category = 'atm' OR name IS NOT NULL",
            name="name_required_non_atm",
        ),
        CheckConstraint(r"name IS NULL OR name ~ '\S'", name="name_non_blank"),
        CheckConstraint(
            "(name IS NULL AND name_search IS NULL) "
            r"OR (name IS NOT NULL AND name_search IS NOT NULL AND name_search ~ '\S')",
            name="name_search_consistency",
        ),
        CheckConstraint(r"address IS NULL OR address ~ '\S'", name="address_non_blank"),
        CheckConstraint("jsonb_typeof(raw_tags) = 'object'", name="raw_tags_json_object"),
        CheckConstraint("NOT ST_IsEmpty(location::geometry)", name="location_non_empty"),
        CheckConstraint(
            "ST_X(location::geometry) <> 'NaN'::double precision "
            "AND ST_Y(location::geometry) <> 'NaN'::double precision "
            "AND ST_X(location::geometry) BETWEEN -180.0 AND 180.0 "
            "AND ST_Y(location::geometry) BETWEEN -90.0 AND 90.0",
            name="location_finite_wgs84",
        ),
        CheckConstraint(
            "osm_version IS NULL OR osm_version > 0",
            name="osm_version_positive",
        ),
        CheckConstraint(
            "osm_changeset IS NULL OR osm_changeset > 0",
            name="osm_changeset_positive",
        ),
        CheckConstraint(
            "content_digest ~ '^[0-9a-f]{64}$'",
            name="content_digest_format",
        ),
        CheckConstraint(
            "jsonb_typeof(source_provenance) = 'object'",
            name="source_provenance_json_object",
        ),
        CheckConstraint("last_seen_pass >= 1", name="last_seen_pass_positive"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    import_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("import_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    osm_type: Mapped[str] = mapped_column(Text, nullable=False)
    osm_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    name_search: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    location: Mapped[Any] = mapped_column(PostGISGeography("Point", 4326), nullable=False)
    raw_tags: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    osm_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    osm_changeset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    osm_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    content_digest: Mapped[str] = mapped_column(String(64), nullable=False)
    source_provenance: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    last_seen_pass: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1")
    )
    staged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class StagingIssueFingerprint(Base):
    """Run-scoped technical registry enforcing idempotent import_issues persistence.

    Using a non-null SHA-256 fingerprint keyed by (import_run_id, issue_fingerprint)
    prevents duplicate issue rows on retry even when osm_type/osm_id are NULL, without
    altering the canonical M1 `import_issues` table schema.
    """

    __tablename__ = "staging_issue_fingerprints"
    __table_args__ = (
        UniqueConstraint(
            "import_issue_id",
            name="uq_staging_issue_fingerprints_import_issue_id",
        ),
        CheckConstraint(
            "issue_fingerprint ~ '^[0-9a-f]{64}$'",
            name="fingerprint_format",
        ),
    )

    import_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("import_runs.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    issue_fingerprint: Mapped[str] = mapped_column(String(64), primary_key=True)
    import_issue_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey("import_issues.id", ondelete="RESTRICT"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class StagingRunCheckpoint(Base):
    """Run-scoped technical staging progress, separated pass vs cumulative counters, and readiness."""

    __tablename__ = "staging_run_checkpoints"
    __table_args__ = (
        CheckConstraint(
            "extraction_scope IN ('node_only')",
            name="extraction_scope_domain",
        ),
        CheckConstraint(
            "staging_state IN ('IN_PROGRESS', 'EXTRACTED', 'VERIFIED', 'FAILED')",
            name="staging_state_domain",
        ),
        CheckConstraint(
            r"source_fingerprint ~ '\S'",
            name="source_fingerprint_non_blank",
        ),
        CheckConstraint(
            "source_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="source_checksum_sha256_format",
        ),
        CheckConstraint("current_pass_number >= 1", name="current_pass_positive"),
        CheckConstraint("resume_count >= 0", name="resume_count_non_negative"),
        CheckConstraint("batches_committed >= 0", name="batches_committed_non_negative"),
        CheckConstraint("total_nodes_seen >= 0", name="total_nodes_seen_non_negative"),
        CheckConstraint("supported_nodes_seen >= 0", name="supported_nodes_non_negative"),
        CheckConstraint(
            "unsupported_nodes_seen >= 0", name="unsupported_nodes_non_negative"
        ),
        CheckConstraint(
            "valid_candidates_emitted >= 0", name="valid_emitted_non_negative"
        ),
        CheckConstraint(
            "invalid_candidates_emitted >= 0", name="invalid_emitted_non_negative"
        ),
        CheckConstraint(
            "pass_inserted_count >= 0", name="pass_inserted_non_negative"
        ),
        CheckConstraint(
            "pass_conflict_count >= 0", name="pass_conflict_non_negative"
        ),
        CheckConstraint(
            "staged_candidate_count >= 0", name="staged_count_non_negative"
        ),
        CheckConstraint(
            "outside_boundary_count >= 0", name="outside_count_non_negative"
        ),
        CheckConstraint(
            "duplicate_identical_count >= 0", name="dup_identical_non_negative"
        ),
        CheckConstraint("conflict_count >= 0", name="conflict_count_non_negative"),
        CheckConstraint("issue_count >= 0", name="issue_count_non_negative"),
        CheckConstraint("last_osm_id IS NULL OR last_osm_id > 0", name="last_osm_id_positive"),
        CheckConstraint(
            "readiness_evidence IS NULL OR jsonb_typeof(readiness_evidence) = 'object'",
            name="readiness_evidence_json_object",
        ),
        CheckConstraint(
            "total_nodes_seen = supported_nodes_seen + unsupported_nodes_seen "
            "AND supported_nodes_seen = valid_candidates_emitted + invalid_candidates_emitted",
            name="node_counts_consistent",
        ),
        CheckConstraint(
            "staging_state NOT IN ('EXTRACTED', 'VERIFIED') "
            "OR (pass_inserted_count + outside_boundary_count + duplicate_identical_count "
            "+ pass_conflict_count = valid_candidates_emitted)",
            name="extracted_pass_accounting",
        ),
        CheckConstraint(
            "("
            "(staging_state = 'IN_PROGRESS' "
            "AND extraction_completed_at IS NULL AND verified_at IS NULL)"
            " OR "
            "(staging_state = 'EXTRACTED' "
            "AND extraction_completed_at IS NOT NULL AND verified_at IS NULL)"
            " OR "
            "(staging_state = 'VERIFIED' "
            "AND extraction_completed_at IS NOT NULL AND verified_at IS NOT NULL "
            "AND verified_at >= extraction_completed_at "
            "AND source_verified_post_parse = true "
            "AND conflict_count = 0 AND pass_conflict_count = 0 "
            "AND staged_candidate_count > 0 "
            "AND readiness_evidence IS NOT NULL)"
            " OR "
            "(staging_state = 'FAILED' AND verified_at IS NULL)"
            ")",
            name="lifecycle_consistency",
        ),
    )

    import_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("import_runs.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    extraction_scope: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'node_only'")
    )
    staging_state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'IN_PROGRESS'")
    )
    source_fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    source_checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_verified_post_parse: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    current_pass_number: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1")
    )
    resume_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    # Current-pass committed batches (reset on resume; not inflated across retries).
    batches_committed: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    # Current-pass extraction counters.
    total_nodes_seen: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    supported_nodes_seen: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    unsupported_nodes_seen: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    valid_candidates_emitted: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    invalid_candidates_emitted: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    pass_inserted_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    outside_boundary_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    duplicate_identical_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    pass_conflict_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    # Cumulative unique persisted evidence counters for the ImportRun.
    staged_candidate_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    conflict_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    issue_count: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    last_osm_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    extraction_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    readiness_evidence: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
