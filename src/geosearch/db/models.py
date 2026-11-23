"""SQLAlchemy 2.x models for the five approved canonical GeoSearch tables."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    Identity,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from geosearch.db.base import Base
from geosearch.db.types import PostGISGeography, PostGISGeometry

OSM_TYPES: tuple[str, ...] = ("node", "way", "relation")
PLACE_CATEGORIES: tuple[str, ...] = (
    "cafe",
    "restaurant",
    "atm",
    "pharmacy",
    "supermarket",
)
BOUNDARY_APPROVAL_STATUSES: tuple[str, ...] = (
    "DRAFT",
    "VALIDATED",
    "APPROVED",
)
IMPORT_RUN_TYPES: tuple[str, ...] = ("IMPORT", "RECOVERY")
IMPORT_RUN_STATUSES: tuple[str, ...] = (
    "PENDING",
    "PROCESSING",
    "PREPARED",
    "PUBLISHED",
    "SUPERSEDED",
    "FAILED",
    "ABORTED",
)
IMPORT_ISSUE_SEVERITIES: tuple[str, ...] = ("INFO", "WARNING", "ERROR")


class BoundaryVersion(Base):
    """Versioned geographic boundary polygons used to scope imports and publications.

    Lifecycle: DRAFT -> VALIDATED -> APPROVED.

    M1 schema invariants enforce structural PostGIS validity (ST_IsValid, non-empty,
    SRID 4326) and row-level consistency of validation/approval provenance metadata.
    Note: single-row CHECK constraints do not enforce historical state-transition
    ordering, immutable UPDATE protection on geometry/provenance, or cross-table
    approval verification at publish time; those guards are deferred to M2/M3.
    """

    __tablename__ = "boundary_versions"
    __table_args__ = (
        UniqueConstraint("version_code", name="uq_boundary_versions_version_code"),
        CheckConstraint(r"version_code ~ '\S'", name="version_code_non_blank"),
        CheckConstraint(r"source_name ~ '\S'", name="source_name_non_blank"),
        CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="checksum_sha256_format",
        ),
        CheckConstraint(
            "approval_status IN ('DRAFT', 'VALIDATED', 'APPROVED')",
            name="approval_status_domain",
        ),
        CheckConstraint(
            "validation_evidence IS NULL OR jsonb_typeof(validation_evidence) = 'object'",
            name="validation_evidence_json_object",
        ),
        CheckConstraint(
            "("
            "(approval_status = 'DRAFT' "
            "AND validated_by IS NULL AND validated_at IS NULL "
            "AND validation_evidence IS NULL "
            "AND approved_by IS NULL AND approved_at IS NULL)"
            " OR "
            "(approval_status = 'VALIDATED' "
            r"AND validated_by IS NOT NULL AND validated_by ~ '\S' "
            "AND validated_at IS NOT NULL AND validation_evidence IS NOT NULL "
            "AND approved_by IS NULL AND approved_at IS NULL)"
            " OR "
            "(approval_status = 'APPROVED' "
            r"AND validated_by IS NOT NULL AND validated_by ~ '\S' "
            "AND validated_at IS NOT NULL AND validation_evidence IS NOT NULL "
            r"AND approved_by IS NOT NULL AND approved_by ~ '\S' "
            "AND approved_at IS NOT NULL AND approved_at >= validated_at)"
            ")",
            name="lifecycle_metadata_consistency",
        ),
        CheckConstraint("ST_IsValid(geom)", name="geom_is_valid"),
        CheckConstraint("NOT ST_IsEmpty(geom)", name="geom_non_empty"),
        CheckConstraint("ST_SRID(geom) = 4326", name="geom_srid_4326"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    version_code: Mapped[str] = mapped_column(Text, nullable=False)
    geom: Mapped[Any] = mapped_column(PostGISGeometry("MultiPolygon", 4326), nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_retrieved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'DRAFT'")
    )
    validated_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    validated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    validation_evidence: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )
    approved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    approval_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    import_runs: Mapped[list[ImportRun]] = relationship(back_populates="boundary_version")


class ImportRun(Base):
    """Independent import or recovery execution with guarded single-active-slot state."""

    __tablename__ = "import_runs"
    __table_args__ = (
        UniqueConstraint("active_slot", name="uq_import_runs_active_slot"),
        UniqueConstraint(
            "publication_operation_id",
            name="uq_import_runs_publication_operation_id",
        ),
        CheckConstraint("run_type IN ('IMPORT', 'RECOVERY')", name="run_type_domain"),
        CheckConstraint(r"source_name ~ '\S'", name="source_name_non_blank"),
        CheckConstraint(
            "source_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name="source_checksum_sha256_format",
        ),
        CheckConstraint(
            r"source_fingerprint ~ '\S'",
            name="source_fingerprint_non_blank",
        ),
        CheckConstraint(
            r"pipeline_version ~ '\S'",
            name="pipeline_version_non_blank",
        ),
        CheckConstraint(
            r"normalizer_version ~ '\S'",
            name="normalizer_version_non_blank",
        ),
        CheckConstraint(r"config_digest ~ '\S'", name="config_digest_non_blank"),
        CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'PREPARED', "
            "'PUBLISHED', 'SUPERSEDED', 'FAILED', 'ABORTED')",
            name="status_domain",
        ),
        CheckConstraint("active_slot IS NULL OR active_slot = 1", name="active_slot_value"),
        CheckConstraint(
            "(active_slot IS NOT NULL AND status = 'PUBLISHED') "
            "OR (active_slot IS NULL AND status <> 'PUBLISHED')",
            name="active_slot_status",
        ),
        CheckConstraint(
            "("
            "(status = 'PUBLISHED' AND publication_operation_id IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NULL)"
            " OR "
            "(status = 'SUPERSEDED' AND publication_operation_id IS NOT NULL "
            "AND published_at IS NOT NULL AND superseded_at IS NOT NULL "
            "AND superseded_at >= published_at)"
            " OR "
            "(status NOT IN ('PUBLISHED', 'SUPERSEDED') "
            "AND published_at IS NULL AND superseded_at IS NULL)"
            ")",
            name="publication_lifecycle_evidence",
        ),
        CheckConstraint(
            "publication_evidence IS NULL OR jsonb_typeof(publication_evidence) = 'object'",
            name="publication_evidence_json_object",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    boundary_version_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("boundary_versions.id", ondelete="RESTRICT"),
        nullable=False,
    )
    run_type: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'IMPORT'")
    )
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    source_checksum_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    source_fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(Text, nullable=False)
    normalizer_version: Mapped[str] = mapped_column(Text, nullable=False)
    config_digest: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'PENDING'")
    )
    publication_operation_id: Mapped[uuid.UUID | None] = mapped_column(
        PGUUID(as_uuid=True), nullable=True
    )
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    superseded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    publication_evidence: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )
    active_slot: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    boundary_version: Mapped[BoundaryVersion] = relationship(back_populates="import_runs")
    places: Mapped[list[Place]] = relationship(back_populates="last_seen_import")
    import_issues: Mapped[list[ImportIssue]] = relationship(back_populates="import_run")
    duplicate_candidates: Mapped[list[DuplicateCandidate]] = relationship(
        back_populates="import_run"
    )


class Place(Base):
    """Canonical place record keyed by stable internal ID and unique OSM identity."""

    __tablename__ = "places"
    __table_args__ = (
        UniqueConstraint("osm_type", "osm_id", name="uq_places_osm_identity"),
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
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    osm_type: Mapped[str] = mapped_column(Text, nullable=False)
    osm_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    name_search: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(Text, nullable=False)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)
    location: Mapped[Any] = mapped_column(PostGISGeography("Point", 4326), nullable=False)
    raw_tags: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    last_seen_import_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("import_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    osm_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    osm_changeset: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    osm_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    last_seen_import: Mapped[ImportRun] = relationship(back_populates="places")


class ImportIssue(Base):
    """Diagnostic and quarantine issue records linked to an import run."""

    __tablename__ = "import_issues"
    __table_args__ = (
        CheckConstraint("severity IN ('INFO', 'WARNING', 'ERROR')", name="severity_domain"),
        CheckConstraint(r"stage ~ '\S'", name="stage_non_blank"),
        CheckConstraint(r"issue_code ~ '\S'", name="issue_code_non_blank"),
        CheckConstraint(r"message ~ '\S'", name="message_non_blank"),
        CheckConstraint(
            "osm_type IS NULL OR osm_type IN ('node', 'way', 'relation')",
            name="osm_type_domain",
        ),
        CheckConstraint("osm_id IS NULL OR osm_id > 0", name="osm_id_positive"),
        CheckConstraint(
            "(osm_type IS NULL AND osm_id IS NULL) "
            "OR (osm_type IS NOT NULL AND osm_id IS NOT NULL)",
            name="osm_identity_pair",
        ),
        CheckConstraint(
            "details IS NULL OR jsonb_typeof(details) = 'object'",
            name="details_json_object",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    import_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("import_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    severity: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(Text, nullable=False)
    issue_code: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    osm_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    osm_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    import_run: Mapped[ImportRun] = relationship(back_populates="import_issues")


class DuplicateCandidate(Base):
    """Suspected real-world duplicate POI pair recorded per import run without auto-merge."""

    __tablename__ = "duplicate_candidates"
    __table_args__ = (
        UniqueConstraint(
            "import_run_id",
            "place_a_id",
            "place_b_id",
            name="uq_duplicate_candidates_run_pair",
        ),
        CheckConstraint("place_a_id < place_b_id", name="canonical_pair_order"),
        CheckConstraint(
            r"detector_version ~ '\S'",
            name="detector_version_non_blank",
        ),
        CheckConstraint(
            "confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1)",
            name="confidence_score_range",
        ),
        CheckConstraint(
            "distance_meters IS NULL OR distance_meters >= 0",
            name="distance_meters_non_negative",
        ),
        CheckConstraint("jsonb_typeof(evidence) = 'object'", name="evidence_json_object"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    import_run_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("import_runs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    place_a_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("places.id", ondelete="RESTRICT"),
        nullable=False,
    )
    place_b_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("places.id", ondelete="RESTRICT"),
        nullable=False,
    )
    detector_version: Mapped[str] = mapped_column(Text, nullable=False)
    confidence_score: Mapped[Decimal | None] = mapped_column(Numeric(5, 4), nullable=True)
    distance_meters: Mapped[float | None] = mapped_column(Double, nullable=True)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    import_run: Mapped[ImportRun] = relationship(back_populates="duplicate_candidates")
    place_a: Mapped[Place] = relationship(foreign_keys=[place_a_id])
    place_b: Mapped[Place] = relationship(foreign_keys=[place_b_id])
