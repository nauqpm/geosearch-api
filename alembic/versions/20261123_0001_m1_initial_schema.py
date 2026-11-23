"""Create initial M1 canonical five-table PostgreSQL/PostGIS schema.

Revision ID: 0001_m1_initial_schema
Revises: None
Create Date: 2026-11-23 10:40:00+07:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from geosearch.db.types import PostGISGeography, PostGISGeometry

revision: str = "0001_m1_initial_schema"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CANONICAL_TABLES_DROP_ORDER: tuple[str, ...] = (
    "duplicate_candidates",
    "import_issues",
    "places",
    "import_runs",
    "boundary_versions",
)


def upgrade() -> None:
    """Enable PostGIS and create the five approved canonical tables and constraints."""
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")

    op.create_table(
        "boundary_versions",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("version_code", sa.Text(), nullable=False),
        sa.Column("geom", PostGISGeometry("MultiPolygon", 4326), nullable=False),
        sa.Column("source_name", sa.Text(), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("source_retrieved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "approval_status",
            sa.Text(),
            server_default=sa.text("'DRAFT'"),
            nullable=False,
        ),
        sa.Column("validated_by", sa.Text(), nullable=True),
        sa.Column("validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "validation_evidence",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("approval_notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_boundary_versions")),
        sa.UniqueConstraint("version_code", name=op.f("uq_boundary_versions_version_code")),
        sa.CheckConstraint(
            r"version_code ~ '\S'",
            name=op.f("ck_boundary_versions_version_code_non_blank"),
        ),
        sa.CheckConstraint(
            r"source_name ~ '\S'",
            name=op.f("ck_boundary_versions_source_name_non_blank"),
        ),
        sa.CheckConstraint(
            "checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_boundary_versions_checksum_sha256_format"),
        ),
        sa.CheckConstraint(
            "approval_status IN ('DRAFT', 'VALIDATED', 'APPROVED')",
            name=op.f("ck_boundary_versions_approval_status_domain"),
        ),
        sa.CheckConstraint(
            "validation_evidence IS NULL OR jsonb_typeof(validation_evidence) = 'object'",
            name=op.f("ck_boundary_versions_validation_evidence_json_object"),
        ),
        sa.CheckConstraint(
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
            name=op.f("ck_boundary_versions_lifecycle_metadata_consistency"),
        ),
        sa.CheckConstraint("ST_IsValid(geom)", name=op.f("ck_boundary_versions_geom_is_valid")),
        sa.CheckConstraint(
            "NOT ST_IsEmpty(geom)",
            name=op.f("ck_boundary_versions_geom_non_empty"),
        ),
        sa.CheckConstraint(
            "ST_SRID(geom) = 4326",
            name=op.f("ck_boundary_versions_geom_srid_4326"),
        ),
    )

    op.create_table(
        "import_runs",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("boundary_version_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "run_type",
            sa.Text(),
            server_default=sa.text("'IMPORT'"),
            nullable=False,
        ),
        sa.Column("source_name", sa.Text(), nullable=False),
        sa.Column("source_checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_fingerprint", sa.Text(), nullable=False),
        sa.Column("pipeline_version", sa.Text(), nullable=False),
        sa.Column("normalizer_version", sa.Text(), nullable=False),
        sa.Column("config_digest", sa.Text(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column(
            "publication_operation_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("superseded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "publication_evidence",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("active_slot", sa.SmallInteger(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_import_runs")),
        sa.ForeignKeyConstraint(
            ["boundary_version_id"],
            ["boundary_versions.id"],
            name=op.f("fk_import_runs_boundary_version_id_boundary_versions"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("active_slot", name=op.f("uq_import_runs_active_slot")),
        sa.UniqueConstraint(
            "publication_operation_id",
            name=op.f("uq_import_runs_publication_operation_id"),
        ),
        sa.CheckConstraint(
            "run_type IN ('IMPORT', 'RECOVERY')",
            name=op.f("ck_import_runs_run_type_domain"),
        ),
        sa.CheckConstraint(
            r"source_name ~ '\S'",
            name=op.f("ck_import_runs_source_name_non_blank"),
        ),
        sa.CheckConstraint(
            "source_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_import_runs_source_checksum_sha256_format"),
        ),
        sa.CheckConstraint(
            r"source_fingerprint ~ '\S'",
            name=op.f("ck_import_runs_source_fingerprint_non_blank"),
        ),
        sa.CheckConstraint(
            r"pipeline_version ~ '\S'",
            name=op.f("ck_import_runs_pipeline_version_non_blank"),
        ),
        sa.CheckConstraint(
            r"normalizer_version ~ '\S'",
            name=op.f("ck_import_runs_normalizer_version_non_blank"),
        ),
        sa.CheckConstraint(
            r"config_digest ~ '\S'",
            name=op.f("ck_import_runs_config_digest_non_blank"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'PREPARED', "
            "'PUBLISHED', 'SUPERSEDED', 'FAILED', 'ABORTED')",
            name=op.f("ck_import_runs_status_domain"),
        ),
        sa.CheckConstraint(
            "active_slot IS NULL OR active_slot = 1",
            name=op.f("ck_import_runs_active_slot_value"),
        ),
        sa.CheckConstraint(
            "(active_slot IS NOT NULL AND status = 'PUBLISHED') "
            "OR (active_slot IS NULL AND status <> 'PUBLISHED')",
            name=op.f("ck_import_runs_active_slot_status"),
        ),
        sa.CheckConstraint(
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
            name=op.f("ck_import_runs_publication_lifecycle_evidence"),
        ),
        sa.CheckConstraint(
            "publication_evidence IS NULL OR jsonb_typeof(publication_evidence) = 'object'",
            name=op.f("ck_import_runs_publication_evidence_json_object"),
        ),
    )

    op.create_table(
        "places",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("osm_type", sa.Text(), nullable=False),
        sa.Column("osm_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("name_search", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("location", PostGISGeography("Point", 4326), nullable=False),
        sa.Column("raw_tags", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("last_seen_import_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "is_active",
            sa.Boolean(),
            server_default=sa.text("true"),
            nullable=False,
        ),
        sa.Column("osm_version", sa.Integer(), nullable=True),
        sa.Column("osm_changeset", sa.BigInteger(), nullable=True),
        sa.Column("osm_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_places")),
        sa.ForeignKeyConstraint(
            ["last_seen_import_id"],
            ["import_runs.id"],
            name=op.f("fk_places_last_seen_import_id_import_runs"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("osm_type", "osm_id", name=op.f("uq_places_osm_identity")),
        sa.CheckConstraint(
            "osm_type IN ('node', 'way', 'relation')",
            name=op.f("ck_places_osm_type_domain"),
        ),
        sa.CheckConstraint("osm_id > 0", name=op.f("ck_places_osm_id_positive")),
        sa.CheckConstraint(
            "category IN ('cafe', 'restaurant', 'atm', 'pharmacy', 'supermarket')",
            name=op.f("ck_places_category_domain"),
        ),
        sa.CheckConstraint(
            "category = 'atm' OR name IS NOT NULL",
            name=op.f("ck_places_name_required_non_atm"),
        ),
        sa.CheckConstraint(
            r"name IS NULL OR name ~ '\S'",
            name=op.f("ck_places_name_non_blank"),
        ),
        sa.CheckConstraint(
            "(name IS NULL AND name_search IS NULL) "
            r"OR (name IS NOT NULL AND name_search IS NOT NULL AND name_search ~ '\S')",
            name=op.f("ck_places_name_search_consistency"),
        ),
        sa.CheckConstraint(
            r"address IS NULL OR address ~ '\S'",
            name=op.f("ck_places_address_non_blank"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(raw_tags) = 'object'",
            name=op.f("ck_places_raw_tags_json_object"),
        ),
        sa.CheckConstraint(
            "NOT ST_IsEmpty(location::geometry)",
            name=op.f("ck_places_location_non_empty"),
        ),
        sa.CheckConstraint(
            "ST_X(location::geometry) <> 'NaN'::double precision "
            "AND ST_Y(location::geometry) <> 'NaN'::double precision "
            "AND ST_X(location::geometry) BETWEEN -180.0 AND 180.0 "
            "AND ST_Y(location::geometry) BETWEEN -90.0 AND 90.0",
            name=op.f("ck_places_location_finite_wgs84"),
        ),
        sa.CheckConstraint(
            "osm_version IS NULL OR osm_version > 0",
            name=op.f("ck_places_osm_version_positive"),
        ),
        sa.CheckConstraint(
            "osm_changeset IS NULL OR osm_changeset > 0",
            name=op.f("ck_places_osm_changeset_positive"),
        ),
    )

    op.create_table(
        "import_issues",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("import_run_id", sa.BigInteger(), nullable=False),
        sa.Column("severity", sa.Text(), nullable=False),
        sa.Column("stage", sa.Text(), nullable=False),
        sa.Column("issue_code", sa.Text(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("osm_type", sa.Text(), nullable=True),
        sa.Column("osm_id", sa.BigInteger(), nullable=True),
        sa.Column("details", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_import_issues")),
        sa.ForeignKeyConstraint(
            ["import_run_id"],
            ["import_runs.id"],
            name=op.f("fk_import_issues_import_run_id_import_runs"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "severity IN ('INFO', 'WARNING', 'ERROR')",
            name=op.f("ck_import_issues_severity_domain"),
        ),
        sa.CheckConstraint(r"stage ~ '\S'", name=op.f("ck_import_issues_stage_non_blank")),
        sa.CheckConstraint(
            r"issue_code ~ '\S'",
            name=op.f("ck_import_issues_issue_code_non_blank"),
        ),
        sa.CheckConstraint(
            r"message ~ '\S'",
            name=op.f("ck_import_issues_message_non_blank"),
        ),
        sa.CheckConstraint(
            "osm_type IS NULL OR osm_type IN ('node', 'way', 'relation')",
            name=op.f("ck_import_issues_osm_type_domain"),
        ),
        sa.CheckConstraint(
            "osm_id IS NULL OR osm_id > 0",
            name=op.f("ck_import_issues_osm_id_positive"),
        ),
        sa.CheckConstraint(
            "(osm_type IS NULL AND osm_id IS NULL) "
            "OR (osm_type IS NOT NULL AND osm_id IS NOT NULL)",
            name=op.f("ck_import_issues_osm_identity_pair"),
        ),
        sa.CheckConstraint(
            "details IS NULL OR jsonb_typeof(details) = 'object'",
            name=op.f("ck_import_issues_details_json_object"),
        ),
    )

    op.create_table(
        "duplicate_candidates",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("import_run_id", sa.BigInteger(), nullable=False),
        sa.Column("place_a_id", sa.BigInteger(), nullable=False),
        sa.Column("place_b_id", sa.BigInteger(), nullable=False),
        sa.Column("detector_version", sa.Text(), nullable=False),
        sa.Column("confidence_score", sa.Numeric(precision=5, scale=4), nullable=True),
        sa.Column("distance_meters", sa.Double(), nullable=True),
        sa.Column("evidence", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_duplicate_candidates")),
        sa.ForeignKeyConstraint(
            ["import_run_id"],
            ["import_runs.id"],
            name=op.f("fk_duplicate_candidates_import_run_id_import_runs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["place_a_id"],
            ["places.id"],
            name=op.f("fk_duplicate_candidates_place_a_id_places"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["place_b_id"],
            ["places.id"],
            name=op.f("fk_duplicate_candidates_place_b_id_places"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "import_run_id",
            "place_a_id",
            "place_b_id",
            name=op.f("uq_duplicate_candidates_run_pair"),
        ),
        sa.CheckConstraint(
            "place_a_id < place_b_id",
            name=op.f("ck_duplicate_candidates_canonical_pair_order"),
        ),
        sa.CheckConstraint(
            r"detector_version ~ '\S'",
            name=op.f("ck_duplicate_candidates_detector_version_non_blank"),
        ),
        sa.CheckConstraint(
            "confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1)",
            name=op.f("ck_duplicate_candidates_confidence_score_range"),
        ),
        sa.CheckConstraint(
            "distance_meters IS NULL OR distance_meters >= 0",
            name=op.f("ck_duplicate_candidates_distance_meters_non_negative"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(evidence) = 'object'",
            name=op.f("ck_duplicate_candidates_evidence_json_object"),
        ),
    )


def downgrade() -> None:
    """Conditionally drop canonical tables ONLY when all five tables are empty.

    Dropping populated canonical tables would destroy audit and POI history.
    For non-empty databases, this downgrade fails closed; use a roll-forward
    migration or controlled recovery procedure instead.
    """
    bind = op.get_bind()
    non_empty_tables: list[str] = []
    for table_name in CANONICAL_TABLES_DROP_ORDER:
        has_row = bind.execute(
            sa.select(sa.exists(sa.select(sa.literal(1)).select_from(sa.table(table_name))))
        ).scalar_one()
        if has_row:
            non_empty_tables.append(table_name)

    if non_empty_tables:
        raise RuntimeError(
            "Refusing destructive downgrade of 0001_m1_initial_schema: "
            f"canonical tables contain data ({', '.join(sorted(non_empty_tables))}). "
            "Use a roll-forward migration or controlled recovery procedure instead."
        )

    for table_name in CANONICAL_TABLES_DROP_ORDER:
        op.drop_table(table_name)
