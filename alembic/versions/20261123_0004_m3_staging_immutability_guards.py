"""Add M3.7 prepared staging immutability triggers and strengthen import_runs evidence guards.

Revision ID: 0004_m3_staging_immutability
Revises: 0003_m3_publication_guards
Create Date: 2026-11-23 16:45:00+07:00
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004_m3_staging_immutability"
down_revision: str | Sequence[str] | None = "0003_m3_publication_guards"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Install race-safe prepared-staging immutability triggers and harden import_runs guards."""
    # 1. Race-safe trigger function protecting staging tables once parent ImportRun is PREPARED+.
    # Acquires FOR SHARE on parent import_runs row so concurrent writes serialize cleanly against
    # verify_and_prepare_run's FOR UPDATE lock and UPDATE import_runs SET status = 'PREPARED'.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_prepared_staging_write()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            target_run_id BIGINT;
            parent_status TEXT;
        BEGIN
            IF TG_OP = 'UPDATE' AND OLD.import_run_id IS DISTINCT FROM NEW.import_run_id THEN
                RAISE EXCEPTION
                    'Staging table % cannot change import_run_id from % to %',
                    TG_TABLE_NAME, OLD.import_run_id, NEW.import_run_id
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_prepared_staging_immutable';
            END IF;

            IF TG_OP = 'DELETE' THEN
                target_run_id := OLD.import_run_id;
            ELSE
                target_run_id := NEW.import_run_id;
            END IF;

            SELECT status
            INTO parent_status
            FROM import_runs
            WHERE id = target_run_id
            FOR SHARE;

            IF parent_status IN ('PREPARED', 'PUBLISHED', 'SUPERSEDED') THEN
                RAISE EXCEPTION
                    'Staging table % is immutable for ImportRun id=% in status % (% rejected)',
                    TG_TABLE_NAME, target_run_id, parent_status, TG_OP
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_prepared_staging_immutable';
            END IF;

            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )

    op.execute(
        """
        CREATE TRIGGER trg_staging_place_candidates_immutability
        BEFORE INSERT OR UPDATE OR DELETE ON staging_place_candidates
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_prepared_staging_write();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_staging_run_checkpoints_immutability
        BEFORE INSERT OR UPDATE OR DELETE ON staging_run_checkpoints
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_prepared_staging_write();
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_staging_issue_fingerprints_immutability
        BEFORE INSERT OR UPDATE OR DELETE ON staging_issue_fingerprints
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_prepared_staging_write();
        """
    )

    # 2. Protect run-scoped import_issues evidence after preparation/publication.
    # BEFORE UPDATE/DELETE unconditionally blocks mutation once PREPARED/PUBLISHED/SUPERSEDED.
    # AFTER INSERT fires after M1 CHECK/FK constraints and blocks inserts for PUBLISHED/SUPERSEDED
    # runs and for any staged PREPARED run (having a staging_run_checkpoints row).
    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_import_issues_mutate()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            parent_status TEXT;
        BEGIN
            IF TG_OP = 'UPDATE' AND OLD.import_run_id IS DISTINCT FROM NEW.import_run_id THEN
                RAISE EXCEPTION
                    'import_issues cannot change import_run_id from % to %',
                    OLD.import_run_id, NEW.import_run_id
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_import_issues_immutable';
            END IF;

            SELECT status
            INTO parent_status
            FROM import_runs
            WHERE id = OLD.import_run_id
            FOR SHARE;

            IF parent_status IN ('PREPARED', 'PUBLISHED', 'SUPERSEDED') THEN
                RAISE EXCEPTION
                    'import_issues is immutable for ImportRun id=% in status % (% rejected)',
                    OLD.import_run_id, parent_status, TG_OP
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_import_issues_immutable';
            END IF;

            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_import_issues_mutate_guard
        BEFORE UPDATE OR DELETE ON import_issues
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_import_issues_mutate();
        """
    )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION geosearch_guard_import_issues_insert()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            parent_status TEXT;
            has_staging_checkpoint BOOLEAN;
        BEGIN
            SELECT status
            INTO parent_status
            FROM import_runs
            WHERE id = NEW.import_run_id
            FOR SHARE;

            IF parent_status IN ('PUBLISHED', 'SUPERSEDED') THEN
                RAISE EXCEPTION
                    'Cannot insert import_issues for ImportRun id=% in status %',
                    NEW.import_run_id, parent_status
                    USING ERRCODE = '23514',
                          CONSTRAINT = 'trg_import_issues_immutable';
            END IF;

            IF parent_status = 'PREPARED' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM staging_run_checkpoints
                    WHERE import_run_id = NEW.import_run_id
                )
                INTO has_staging_checkpoint;

                IF has_staging_checkpoint THEN
                    RAISE EXCEPTION
                        'Cannot insert import_issues for staged PREPARED ImportRun id=%',
                        NEW.import_run_id
                        USING ERRCODE = '23514',
                              CONSTRAINT = 'trg_import_issues_immutable';
                END IF;
            END IF;

            RETURN NEW;
        END;
        $$;
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_import_issues_insert_guard
        AFTER INSERT ON import_issues
        FOR EACH ROW
        EXECUTE FUNCTION geosearch_guard_import_issues_insert();
        """
    )

    # 3. Strengthen geosearch_guard_import_runs_write to freeze started_at and protect
    # original publication_evidence keys while PUBLISHED and during PUBLISHED -> SUPERSEDED.
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
                    OR NEW.started_at IS DISTINCT FROM OLD.started_at
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

                IF OLD.status = 'PUBLISHED' AND NEW.status = 'PUBLISHED' THEN
                    IF NEW.publication_evidence IS DISTINCT FROM OLD.publication_evidence
                        OR NEW.active_slot IS DISTINCT FROM OLD.active_slot
                        OR NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
                        RAISE EXCEPTION
                            'PUBLISHED import_runs evidence and metadata cannot be mutated in place'
                            USING ERRCODE = '23514',
                                  CONSTRAINT = 'trg_import_runs_immutable_published_evidence';
                    END IF;
                END IF;

                IF OLD.status = 'PUBLISHED' AND NEW.status = 'SUPERSEDED' THEN
                    IF OLD.publication_evidence IS NOT NULL THEN
                        IF NEW.publication_evidence IS NULL OR EXISTS (
                            SELECT 1
                            FROM jsonb_each(OLD.publication_evidence) AS o(k, v)
                            WHERE NOT (NEW.publication_evidence ? o.k)
                               OR (NEW.publication_evidence -> o.k) IS DISTINCT FROM o.v
                        ) THEN
                            RAISE EXCEPTION
                                'PUBLISHED -> SUPERSEDED must preserve all original publication_evidence keys/values'
                                USING ERRCODE = '23514',
                                      CONSTRAINT = 'trg_import_runs_immutable_published_evidence';
                        END IF;
                    END IF;
                END IF;

                IF OLD.status = 'SUPERSEDED' THEN
                    IF NEW.status <> 'SUPERSEDED'
                        OR NEW.active_slot IS DISTINCT FROM OLD.active_slot
                        OR NEW.publication_evidence IS DISTINCT FROM OLD.publication_evidence
                        OR NEW.completed_at IS DISTINCT FROM OLD.completed_at THEN
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


def downgrade() -> None:
    """Drop M3.7 staging immutability triggers and restore 0003 import_runs guard function."""
    op.execute("DROP TRIGGER IF EXISTS trg_import_issues_insert_guard ON import_issues")
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_import_issues_insert()")

    op.execute("DROP TRIGGER IF EXISTS trg_import_issues_mutate_guard ON import_issues")
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_import_issues_mutate()")

    op.execute(
        "DROP TRIGGER IF EXISTS trg_staging_issue_fingerprints_immutability "
        "ON staging_issue_fingerprints"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_staging_run_checkpoints_immutability "
        "ON staging_run_checkpoints"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS trg_staging_place_candidates_immutability "
        "ON staging_place_candidates"
    )
    op.execute("DROP FUNCTION IF EXISTS geosearch_guard_prepared_staging_write()")

    # Restore exact 0003_m3_publication_guards implementation of geosearch_guard_import_runs_write.
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
