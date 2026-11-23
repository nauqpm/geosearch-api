"""Add M3 PostgreSQL publication, boundary, identity, and audit immutability triggers.

Revision ID: 0003_m3_publication_guards
Revises: 0002_m2_staging_tables
Create Date: 2026-11-23 15:20:00+07:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003_m3_publication_guards"
down_revision: str | Sequence[str] | None = "0002_m2_staging_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Install database-level immutability and transition guard triggers for M3."""
    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_boundary_versions_update()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
                OR NEW.version_code IS DISTINCT FROM OLD.version_code
                OR ST_AsEWKB(NEW.geom) IS DISTINCT FROM ST_AsEWKB(OLD.geom)
                OR NEW.source_name IS DISTINCT FROM OLD.source_name
                OR NEW.source_url IS DISTINCT FROM OLD.source_url
                OR NEW.source_retrieved_at IS DISTINCT FROM OLD.source_retrieved_at
                OR NEW.checksum_sha256 IS DISTINCT FROM OLD.checksum_sha256
                OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION
                    'boundary_versions geometry and provenance fields are immutable'
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_boundary_versions_immutable_provenance';
            END IF;

            IF OLD.approval_status = 'APPROVED' AND NEW.approval_status <> 'APPROVED' THEN
                RAISE EXCEPTION
                    'APPROVED boundary_versions cannot transition backward to %',
                    NEW.approval_status
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_boundary_versions_monotonic_lifecycle';
            END IF;

            IF OLD.approval_status = 'VALIDATED' AND NEW.approval_status = 'DRAFT' THEN
                RAISE EXCEPTION
                    'VALIDATED boundary_versions cannot transition backward to DRAFT'
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_boundary_versions_monotonic_lifecycle';
            END IF;

            IF OLD.approval_status IN ('VALIDATED', 'APPROVED') THEN
                IF NEW.validated_by IS DISTINCT FROM OLD.validated_by
                    OR NEW.validated_at IS DISTINCT FROM OLD.validated_at
                    OR NEW.validation_evidence IS DISTINCT FROM OLD.validation_evidence THEN
                    RAISE EXCEPTION
                        'boundary_versions validation provenance is immutable once recorded'
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_boundary_versions_immutable_validation';
                END IF;
            END IF;

            IF OLD.approval_status = 'APPROVED' THEN
                IF NEW.approved_by IS DISTINCT FROM OLD.approved_by
                    OR NEW.approved_at IS DISTINCT FROM OLD.approved_at
                    OR NEW.approval_notes IS DISTINCT FROM OLD.approval_notes THEN
                    RAISE EXCEPTION
                        'boundary_versions approval metadata is immutable once APPROVED'
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_boundary_versions_immutable_approval';
                END IF;
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_boundary_versions_guard
        BEFORE UPDATE ON boundary_versions
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_boundary_versions_update();
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_import_runs_write()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            bv_status TEXT;
        BEGIN
            IF NEW.status = 'PUBLISHED' THEN
                SELECT approval_status
                INTO bv_status
                FROM boundary_versions
                WHERE id = NEW.boundary_version_id;

                IF bv_status IS DISTINCT FROM 'APPROVED' THEN
                    RAISE EXCEPTION
                        'PUBLISHED import_runs row requires an APPROVED boundary_version (got %)',
                        COALESCE(bv_status, 'MISSING')
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_runs_published_requires_approved_boundary';
                END IF;
            END IF;

            IF TG_OP = 'UPDATE' THEN
                IF NEW.id IS DISTINCT FROM OLD.id
                    OR NEW.boundary_version_id IS DISTINCT FROM OLD.boundary_version_id
                    OR NEW.run_type IS DISTINCT FROM OLD.run_type
                    OR NEW.source_name IS DISTINCT FROM OLD.source_name
                    OR NEW.source_checksum_sha256 IS DISTINCT FROM OLD.source_checksum_sha256
                    OR NEW.source_fingerprint IS DISTINCT FROM OLD.source_fingerprint
                    OR NEW.pipeline_version IS DISTINCT FROM OLD.pipeline_version
                    OR NEW.normalizer_version IS DISTINCT FROM OLD.normalizer_version
                    OR NEW.config_digest IS DISTINCT FROM OLD.config_digest
                    OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                    RAISE EXCEPTION
                        'import_runs immutable provenance fields cannot be modified'
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_runs_immutable_provenance';
                END IF;

                IF OLD.publication_operation_id IS NOT NULL
                    AND NEW.publication_operation_id IS DISTINCT FROM OLD.publication_operation_id THEN
                    RAISE EXCEPTION
                        'import_runs.publication_operation_id is immutable once registered'
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_runs_immutable_publication_operation_id';
                END IF;

                IF OLD.published_at IS NOT NULL
                    AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
                    RAISE EXCEPTION
                        'import_runs.published_at is immutable once published'
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_runs_immutable_published_at';
                END IF;

                IF OLD.superseded_at IS NOT NULL
                    AND NEW.superseded_at IS DISTINCT FROM OLD.superseded_at THEN
                    RAISE EXCEPTION
                        'import_runs.superseded_at is immutable once superseded'
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_runs_immutable_superseded_at';
                END IF;

                IF OLD.status = 'PUBLISHED'
                    AND NEW.status NOT IN ('PUBLISHED', 'SUPERSEDED') THEN
                    RAISE EXCEPTION
                        'PUBLISHED import_runs cannot transition to %',
                        NEW.status
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_runs_published_transition';
                END IF;

                IF OLD.status = 'SUPERSEDED' THEN
                    IF NEW.status <> 'SUPERSEDED'
                        OR NEW.active_slot IS DISTINCT FROM OLD.active_slot
                        OR NEW.publication_evidence IS DISTINCT FROM OLD.publication_evidence THEN
                        RAISE EXCEPTION
                            'SUPERSEDED import_runs historical evidence and status are immutable'
                            USING ERRCODE = '23514',
                                  CONSTRAINT = 'trg_import_runs_immutable_superseded';
                    END IF;
                END IF;
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_import_runs_guard
        BEFORE INSERT OR UPDATE ON import_runs
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_import_runs_write();
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_places_identity_update()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.id IS DISTINCT FROM OLD.id
                OR NEW.osm_type IS DISTINCT FROM OLD.osm_type
                OR NEW.osm_id IS DISTINCT FROM OLD.osm_id
                OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION
                    'places stable id, OSM identity (osm_type, osm_id), and created_at are immutable'
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_places_immutable_identity';
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_places_identity_guard
        BEFORE UPDATE ON places
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_places_identity_update();
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_duplicate_candidates_update()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'duplicate_candidates historical evidence is immutable'
                USING ERRCODE = '23514',
                      CONSTRAINT = 'trg_duplicate_candidates_immutable';
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_duplicate_candidates_immutable
        BEFORE UPDATE ON duplicate_candidates
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_duplicate_candidates_update();
        """
    )


def downgrade() -> None:
    """Drop M3 guard triggers and trigger functions without affecting stored data."""
    op.execute("DROP TRIGGER IF EXISTS trg_duplicate_candidates_immutable ON duplicate_candidates")
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_duplicate_candidates_update()")

    op.execute("DROP TRIGGER IF EXISTS trg_places_identity_guard ON places")
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_places_identity_update()")

    op.execute("DROP TRIGGER IF EXISTS trg_import_runs_guard ON import_runs")
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_import_runs_write()")

    op.execute("DROP TRIGGER IF EXISTS trg_boundary_versions_guard ON boundary_versions")
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_boundary_versions_update()")
