"""Add M2 run-scoped technical staging tables for deterministic OSM ingestion.

Revision ID: 0002_m2_staging_tables
Revises: 0001_m1_initial_schema
Create Date: 2026-11-23 12:00:00+07:00
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from geosearch.db.types import PostGISGeography

revision: str = "0002_m2_staging_tables"
down_revision: str | Sequence[str] | None = "0001_m1_initial_schema"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STAGING_TABLES_DROP_ORDER: tuple[str, ...] = (
    "staging_issue_fingerprints",
    "staging_place_candidates",
    "staging_run_checkpoints",
)


def upgrade() -> None:
    """Create run-scoped technical staging tables without altering the five primary tables."""
    op.create_table(
        "staging_place_candidates",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("import_run_id", sa.BigInteger(), nullable=False),
        sa.Column("osm_type", sa.Text(), nullable=False),
        sa.Column("osm_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("name_search", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), nullable=False),
        sa.Column("address", sa.Text(), nullable=True),
        sa.Column("location", PostGISGeography("Point", 4326), nullable=False),
        sa.Column("raw_tags", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("osm_version", sa.Integer(), nullable=True),
        sa.Column("osm_changeset", sa.BigInteger(), nullable=True),
        sa.Column("osm_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("content_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "source_provenance",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "last_seen_pass",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column(
            "staged_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_staging_place_candidates")),
        sa.ForeignKeyConstraint(
            ["import_run_id"],
            ["import_runs.id"],
            name=op.f("fk_staging_place_candidates_import_run_id_import_runs"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "import_run_id",
            "osm_type",
            "osm_id",
            name=op.f("uq_staging_place_candidates_run_osm_identity"),
        ),
        sa.CheckConstraint(
            "osm_type IN ('node', 'way', 'relation')",
            name=op.f("ck_staging_place_candidates_osm_type_domain"),
        ),
        sa.CheckConstraint(
            "osm_id > 0",
            name=op.f("ck_staging_place_candidates_osm_id_positive"),
        ),
        sa.CheckConstraint(
            "category IN ('cafe', 'restaurant', 'atm', 'pharmacy', 'supermarket')",
            name=op.f("ck_staging_place_candidates_category_domain"),
        ),
        sa.CheckConstraint(
            "category = 'atm' OR name IS NOT NULL",
            name=op.f("ck_staging_place_candidates_name_required_non_atm"),
        ),
        sa.CheckConstraint(
            r"name IS NULL OR name ~ '\S'",
            name=op.f("ck_staging_place_candidates_name_non_blank"),
        ),
        sa.CheckConstraint(
            "(name IS NULL AND name_search IS NULL) "
            r"OR (name IS NOT NULL AND name_search IS NOT NULL AND name_search ~ '\S')",
            name=op.f("ck_staging_place_candidates_name_search_consistency"),
        ),
        sa.CheckConstraint(
            r"address IS NULL OR address ~ '\S'",
            name=op.f("ck_staging_place_candidates_address_non_blank"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(raw_tags) = 'object'",
            name=op.f("ck_staging_place_candidates_raw_tags_json_object"),
        ),
        sa.CheckConstraint(
            "NOT ST_IsEmpty(location::geometry)",
            name=op.f("ck_staging_place_candidates_location_non_empty"),
        ),
        sa.CheckConstraint(
            "ST_X(location::geometry) <> 'NaN'::double precision "
            "AND ST_Y(location::geometry) <> 'NaN'::double precision "
            "AND ST_X(location::geometry) BETWEEN -180.0 AND 180.0 "
            "AND ST_Y(location::geometry) BETWEEN -90.0 AND 90.0",
            name=op.f("ck_staging_place_candidates_location_finite_wgs84"),
        ),
        sa.CheckConstraint(
            "osm_version IS NULL OR osm_version > 0",
            name=op.f("ck_staging_place_candidates_osm_version_positive"),
        ),
        sa.CheckConstraint(
            "osm_changeset IS NULL OR osm_changeset > 0",
            name=op.f("ck_staging_place_candidates_osm_changeset_positive"),
        ),
        sa.CheckConstraint(
            "content_digest ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_staging_place_candidates_content_digest_format"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(source_provenance) = 'object'",
            name=op.f("ck_staging_place_candidates_source_provenance_json_object"),
        ),
        sa.CheckConstraint(
            "last_seen_pass >= 1",
            name=op.f("ck_staging_place_candidates_last_seen_pass_positive"),
        ),
    )

    op.create_table(
        "staging_issue_fingerprints",
        sa.Column("import_run_id", sa.BigInteger(), nullable=False),
        sa.Column("issue_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("import_issue_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint(
            "import_run_id",
            "issue_fingerprint",
            name=op.f("pk_staging_issue_fingerprints"),
        ),
        sa.ForeignKeyConstraint(
            ["import_run_id"],
            ["import_runs.id"],
            name=op.f("fk_staging_issue_fingerprints_import_run_id_import_runs"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["import_issue_id"],
            ["import_issues.id"],
            name=op.f("fk_staging_issue_fingerprints_import_issue_id_import_issues"),
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "import_issue_id",
            name=op.f("uq_staging_issue_fingerprints_import_issue_id"),
        ),
        sa.CheckConstraint(
            "issue_fingerprint ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_staging_issue_fingerprints_fingerprint_format"),
        ),
    )

    op.create_table(
        "staging_run_checkpoints",
        sa.Column("import_run_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "extraction_scope",
            sa.Text(),
            server_default=sa.text("'node_only'"),
            nullable=False,
        ),
        sa.Column(
            "staging_state",
            sa.Text(),
            server_default=sa.text("'IN_PROGRESS'"),
            nullable=False,
        ),
        sa.Column("source_fingerprint", sa.Text(), nullable=False),
        sa.Column("source_checksum_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "source_verified_post_parse",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column(
            "current_pass_number",
            sa.Integer(),
            server_default=sa.text("1"),
            nullable=False,
        ),
        sa.Column(
            "resume_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "batches_committed",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "total_nodes_seen",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "supported_nodes_seen",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "unsupported_nodes_seen",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "valid_candidates_emitted",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "invalid_candidates_emitted",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "pass_inserted_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "outside_boundary_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "duplicate_identical_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "pass_conflict_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "staged_candidate_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "conflict_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "issue_count",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("last_osm_id", sa.BigInteger(), nullable=True),
        sa.Column("extraction_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "readiness_evidence",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("import_run_id", name=op.f("pk_staging_run_checkpoints")),
        sa.ForeignKeyConstraint(
            ["import_run_id"],
            ["import_runs.id"],
            name=op.f("fk_staging_run_checkpoints_import_run_id_import_runs"),
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "extraction_scope IN ('node_only')",
            name=op.f("ck_staging_run_checkpoints_extraction_scope_domain"),
        ),
        sa.CheckConstraint(
            "staging_state IN ('IN_PROGRESS', 'EXTRACTED', 'VERIFIED', 'FAILED')",
            name=op.f("ck_staging_run_checkpoints_staging_state_domain"),
        ),
        sa.CheckConstraint(
            r"source_fingerprint ~ '\S'",
            name=op.f("ck_staging_run_checkpoints_source_fingerprint_non_blank"),
        ),
        sa.CheckConstraint(
            "source_checksum_sha256 ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_staging_run_checkpoints_source_checksum_sha256_format"),
        ),
        sa.CheckConstraint(
            "current_pass_number >= 1",
            name=op.f("ck_staging_run_checkpoints_current_pass_positive"),
        ),
        sa.CheckConstraint(
            "resume_count >= 0",
            name=op.f("ck_staging_run_checkpoints_resume_count_non_negative"),
        ),
        sa.CheckConstraint(
            "batches_committed >= 0",
            name=op.f("ck_staging_run_checkpoints_batches_committed_non_negative"),
        ),
        sa.CheckConstraint(
            "total_nodes_seen >= 0",
            name=op.f("ck_staging_run_checkpoints_total_nodes_seen_non_negative"),
        ),
        sa.CheckConstraint(
            "supported_nodes_seen >= 0",
            name=op.f("ck_staging_run_checkpoints_supported_nodes_non_negative"),
        ),
        sa.CheckConstraint(
            "unsupported_nodes_seen >= 0",
            name=op.f("ck_staging_run_checkpoints_unsupported_nodes_non_negative"),
        ),
        sa.CheckConstraint(
            "valid_candidates_emitted >= 0",
            name=op.f("ck_staging_run_checkpoints_valid_emitted_non_negative"),
        ),
        sa.CheckConstraint(
            "invalid_candidates_emitted >= 0",
            name=op.f("ck_staging_run_checkpoints_invalid_emitted_non_negative"),
        ),
        sa.CheckConstraint(
            "pass_inserted_count >= 0",
            name=op.f("ck_staging_run_checkpoints_pass_inserted_non_negative"),
        ),
        sa.CheckConstraint(
            "pass_conflict_count >= 0",
            name=op.f("ck_staging_run_checkpoints_pass_conflict_non_negative"),
        ),
        sa.CheckConstraint(
            "staged_candidate_count >= 0",
            name=op.f("ck_staging_run_checkpoints_staged_count_non_negative"),
        ),
        sa.CheckConstraint(
            "outside_boundary_count >= 0",
            name=op.f("ck_staging_run_checkpoints_outside_count_non_negative"),
        ),
        sa.CheckConstraint(
            "duplicate_identical_count >= 0",
            name=op.f("ck_staging_run_checkpoints_dup_identical_non_negative"),
        ),
        sa.CheckConstraint(
            "conflict_count >= 0",
            name=op.f("ck_staging_run_checkpoints_conflict_count_non_negative"),
        ),
        sa.CheckConstraint(
            "issue_count >= 0",
            name=op.f("ck_staging_run_checkpoints_issue_count_non_negative"),
        ),
        sa.CheckConstraint(
            "last_osm_id IS NULL OR last_osm_id > 0",
            name=op.f("ck_staging_run_checkpoints_last_osm_id_positive"),
        ),
        sa.CheckConstraint(
            "readiness_evidence IS NULL OR jsonb_typeof(readiness_evidence) = 'object'",
            name=op.f("ck_staging_run_checkpoints_readiness_evidence_json_object"),
        ),
        sa.CheckConstraint(
            "total_nodes_seen = supported_nodes_seen + unsupported_nodes_seen "
            "AND supported_nodes_seen = valid_candidates_emitted + invalid_candidates_emitted",
            name=op.f("ck_staging_run_checkpoints_node_counts_consistent"),
        ),
        sa.CheckConstraint(
            "staging_state NOT IN ('EXTRACTED', 'VERIFIED') "
            "OR (pass_inserted_count + outside_boundary_count + duplicate_identical_count "
            "+ pass_conflict_count = valid_candidates_emitted)",
            name=op.f("ck_staging_run_checkpoints_extracted_pass_accounting"),
        ),
        sa.CheckConstraint(
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
            name=op.f("ck_staging_run_checkpoints_lifecycle_consistency"),
        ),
    )


def downgrade() -> None:
    """Conditionally drop M2 staging tables ONLY when all staging tables are empty."""
    bind = op.get_bind()
    non_empty_tables: list[str] = []
    for table_name in STAGING_TABLES_DROP_ORDER:
        has_row = bind.execute(
            sa.select(sa.exists(sa.select(sa.literal(1)).select_from(sa.table(table_name))))
        ).scalar_one()
        if has_row:
            non_empty_tables.append(table_name)

    if non_empty_tables:
        raise RuntimeError(
            "Refusing destructive downgrade of 0002_m2_staging_tables: "
            f"staging tables contain data ({', '.join(sorted(non_empty_tables))}). "
            "Archive or clean staging runs explicitly before rolling back."
        )

    for table_name in STAGING_TABLES_DROP_ORDER:
        op.drop_table(table_name)
