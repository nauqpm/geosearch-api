"""Executable G1-G6 migration gate verification on isolated disposable PostgreSQL/PostGIS databases."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.script import ScriptDirectory
from psycopg import sql as psycopg_sql
from sqlalchemy import Connection, text
from sqlalchemy.exc import IntegrityError, ProgrammingError

from geosearch.acceptance.evaluator import get_git_state
from geosearch.db import (
    Base,
    UnsafeDatabaseConfigError,
    build_disposable_test_database_name,
    create_test_engine,
    get_test_database_url,
    is_recognized_test_database_name,
    validate_test_database_url,
    verify_connected_test_database,
)
from geosearch.db.types import (
    alembic_compare_type as compare_type,
    alembic_render_item,
    get_default_schema,
    get_extension_managed_tables,
    get_visible_extension_managed_tables,
    make_alembic_include_object,
)

EXPECTED_HEAD_REVISION = "0004_m3_staging_immutability"

EXPECTED_CANONICAL_TABLES = [
    "boundary_versions",
    "duplicate_candidates",
    "import_issues",
    "import_runs",
    "places",
]

EXPECTED_STAGING_TABLES = [
    "staging_issue_fingerprints",
    "staging_place_candidates",
    "staging_run_checkpoints",
]

EXPECTED_ALL_TABLES = sorted(EXPECTED_CANONICAL_TABLES + EXPECTED_STAGING_TABLES)

EXPECTED_SPATIAL_COLUMNS = [
    ("boundary_versions", "geom", "geometry(MultiPolygon,4326)"),
    ("places", "location", "geography(Point,4326)"),
    ("staging_place_candidates", "location", "geography(Point,4326)"),
]

EXPECTED_TRIGGERS_BY_TABLE: dict[str, set[str]] = {
    "boundary_versions": {"trg_boundary_versions_guard"},
    "import_runs": {"trg_import_runs_guard"},
    "places": {"trg_places_identity_guard"},
    "duplicate_candidates": {"trg_duplicate_candidates_immutable"},
    "import_issues": {"trg_import_issues_mutate_guard", "trg_import_issues_insert_guard"},
    "staging_place_candidates": {"trg_staging_place_candidates_immutability"},
    "staging_run_checkpoints": {"trg_staging_run_checkpoints_immutability"},
    "staging_issue_fingerprints": {"trg_staging_issue_fingerprints_immutability"},
}

EXPECTED_CONSTRAINTS_BY_TABLE: dict[str, set[str]] = {
    "boundary_versions": {
        "pk_boundary_versions",
        "uq_boundary_versions_version_code",
        "ck_boundary_versions_version_code_non_blank",
        "ck_boundary_versions_source_name_non_blank",
        "ck_boundary_versions_checksum_sha256_format",
        "ck_boundary_versions_approval_status_domain",
        "ck_boundary_versions_validation_evidence_json_object",
        "ck_boundary_versions_lifecycle_metadata_consistency",
        "ck_boundary_versions_geom_is_valid",
        "ck_boundary_versions_geom_non_empty",
        "ck_boundary_versions_geom_srid_4326",
    },
    "import_runs": {
        "pk_import_runs",
        "fk_import_runs_boundary_version_id_boundary_versions",
        "uq_import_runs_active_slot",
        "uq_import_runs_publication_operation_id",
        "ck_import_runs_run_type_domain",
        "ck_import_runs_source_name_non_blank",
        "ck_import_runs_source_checksum_sha256_format",
        "ck_import_runs_source_fingerprint_non_blank",
        "ck_import_runs_pipeline_version_non_blank",
        "ck_import_runs_normalizer_version_non_blank",
        "ck_import_runs_config_digest_non_blank",
        "ck_import_runs_status_domain",
        "ck_import_runs_active_slot_value",
        "ck_import_runs_active_slot_status",
        "ck_import_runs_publication_lifecycle_evidence",
        "ck_import_runs_publication_evidence_json_object",
    },
    "places": {
        "pk_places",
        "fk_places_last_seen_import_id_import_runs",
        "uq_places_osm_identity",
        "ck_places_osm_type_domain",
        "ck_places_osm_id_positive",
        "ck_places_category_domain",
        "ck_places_name_required_non_atm",
        "ck_places_name_non_blank",
        "ck_places_name_search_consistency",
        "ck_places_address_non_blank",
        "ck_places_raw_tags_json_object",
        "ck_places_location_non_empty",
        "ck_places_location_finite_wgs84",
        "ck_places_osm_version_positive",
        "ck_places_osm_changeset_positive",
    },
    "import_issues": {
        "pk_import_issues",
        "fk_import_issues_import_run_id_import_runs",
        "ck_import_issues_severity_domain",
        "ck_import_issues_stage_non_blank",
        "ck_import_issues_issue_code_non_blank",
        "ck_import_issues_message_non_blank",
        "ck_import_issues_osm_type_domain",
        "ck_import_issues_osm_id_positive",
        "ck_import_issues_osm_identity_pair",
        "ck_import_issues_details_json_object",
    },
    "duplicate_candidates": {
        "pk_duplicate_candidates",
        "fk_duplicate_candidates_import_run_id_import_runs",
        "fk_duplicate_candidates_place_a_id_places",
        "fk_duplicate_candidates_place_b_id_places",
        "uq_duplicate_candidates_run_pair",
        "ck_duplicate_candidates_canonical_pair_order",
        "ck_duplicate_candidates_detector_version_non_blank",
        "ck_duplicate_candidates_confidence_score_range",
        "ck_duplicate_candidates_distance_meters_non_negative",
        "ck_duplicate_candidates_evidence_json_object",
    },
    "staging_place_candidates": {
        "pk_staging_place_candidates",
        "fk_staging_place_candidates_import_run_id_import_runs",
        "uq_staging_place_candidates_run_osm_identity",
        "ck_staging_place_candidates_osm_type_domain",
        "ck_staging_place_candidates_osm_id_positive",
        "ck_staging_place_candidates_category_domain",
        "ck_staging_place_candidates_name_required_non_atm",
        "ck_staging_place_candidates_name_non_blank",
        "ck_staging_place_candidates_name_search_consistency",
        "ck_staging_place_candidates_address_non_blank",
        "ck_staging_place_candidates_raw_tags_json_object",
        "ck_staging_place_candidates_location_non_empty",
        "ck_staging_place_candidates_location_finite_wgs84",
        "ck_staging_place_candidates_osm_version_positive",
        "ck_staging_place_candidates_osm_changeset_positive",
        "ck_staging_place_candidates_content_digest_format",
        "ck_staging_place_candidates_source_provenance_json_object",
        "ck_staging_place_candidates_last_seen_pass_positive",
    },
    "staging_issue_fingerprints": {
        "pk_staging_issue_fingerprints",
        "fk_staging_issue_fingerprints_import_run_id_import_runs",
        "fk_staging_issue_fingerprints_import_issue_id_import_issues",
        "uq_staging_issue_fingerprints_import_issue_id",
        "ck_staging_issue_fingerprints_fingerprint_format",
    },
    "staging_run_checkpoints": {
        "pk_staging_run_checkpoints",
        "fk_staging_run_checkpoints_import_run_id_import_runs",
        "ck_staging_run_checkpoints_extraction_scope_domain",
        "ck_staging_run_checkpoints_staging_state_domain",
        "ck_staging_run_checkpoints_source_fingerprint_non_blank",
        "ck_staging_run_checkpoints_source_checksum_sha256_format",
        "ck_staging_run_checkpoints_current_pass_positive",
        "ck_staging_run_checkpoints_resume_count_non_negative",
        "ck_staging_run_checkpoints_batches_committed_non_negative",
        "ck_staging_run_checkpoints_total_nodes_seen_non_negative",
        "ck_staging_run_checkpoints_supported_nodes_non_negative",
        "ck_staging_run_checkpoints_unsupported_nodes_non_negative",
        "ck_staging_run_checkpoints_valid_emitted_non_negative",
        "ck_staging_run_checkpoints_invalid_emitted_non_negative",
        "ck_staging_run_checkpoints_pass_inserted_non_negative",
        "ck_staging_run_checkpoints_pass_conflict_non_negative",
        "ck_staging_run_checkpoints_staged_count_non_negative",
        "ck_staging_run_checkpoints_outside_count_non_negative",
        "ck_staging_run_checkpoints_dup_identical_non_negative",
        "ck_staging_run_checkpoints_conflict_count_non_negative",
        "ck_staging_run_checkpoints_issue_count_non_negative",
        "ck_staging_run_checkpoints_last_osm_id_positive",
        "ck_staging_run_checkpoints_readiness_evidence_json_object",
        "ck_staging_run_checkpoints_node_counts_consistent",
        "ck_staging_run_checkpoints_extracted_pass_accounting",
        "ck_staging_run_checkpoints_lifecycle_consistency",
    },
}

_VALID_MULTIPOLYGON_WKT = (
    "MULTIPOLYGON(((106.6 10.7, 106.8 10.7, 106.8 10.9, 106.6 10.9, 106.6 10.7)))"
)
_VALID_SHA256 = "a" * 64


def _load_optional_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(override=False)


def _execute_db_ddl(connection: Connection, statement: psycopg_sql.Composed) -> None:
    raw_conn = connection.connection.driver_connection
    if raw_conn is None:
        raise UnsafeDatabaseConfigError(
            "Unable to access underlying psycopg connection for safe DDL execution."
        )
    with raw_conn.cursor() as cursor:
        cursor.execute(statement)


@contextmanager
def managed_disposable_database(base_test_url: str, prefix: str) -> Generator[str, None, None]:
    """Create and drop an isolated disposable test database using fail-closed guards."""
    parsed_base = validate_test_database_url(base_test_url)
    db_name = build_disposable_test_database_name(prefix)
    disposable_url = parsed_base.set(database=db_name).render_as_string(hide_password=False)
    validate_test_database_url(disposable_url)

    admin_engine = create_test_engine(base_test_url, pool_size=1, max_overflow=0)
    created = False
    try:
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            verify_connected_test_database(conn, expected_database=parsed_base.database)
            _execute_db_ddl(
                conn,
                psycopg_sql.SQL("CREATE DATABASE {}").format(psycopg_sql.Identifier(db_name)),
            )
            created = True
        yield disposable_url
    finally:
        if created:
            with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                verify_connected_test_database(conn, expected_database=parsed_base.database)
                if not is_recognized_test_database_name(db_name):
                    raise UnsafeDatabaseConfigError(
                        f"Refusing to drop unrecognized database '{db_name}'."
                    )
                conn.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :db_name AND pid <> pg_backend_pid()"
                    ),
                    {"db_name": db_name},
                )
                _execute_db_ddl(
                    conn,
                    psycopg_sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        psycopg_sql.Identifier(db_name)
                    ),
                )
        admin_engine.dispose()


def _make_alembic_config(repo_root: Path, database_url: str) -> Config:
    validate_test_database_url(database_url)
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def get_alembic_repo_heads(repo_root: Path) -> list[str]:
    """Return Alembic head revision(s) declared in the repository migration scripts."""
    cfg = Config(str(repo_root / "alembic.ini"))
    cfg.set_main_option("script_location", str(repo_root / "alembic"))
    script = ScriptDirectory.from_config(cfg)
    return list(script.get_heads())


def _compute_schema_diff(db_url: str) -> list[str]:
    engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
    try:
        with engine.connect() as conn:
            default_schema = get_default_schema(conn)
            ignored_tables = get_extension_managed_tables(conn)
            visible_ignored_tables = get_visible_extension_managed_tables(conn)
            mc = MigrationContext.configure(
                conn,
                opts={
                    "include_object": make_alembic_include_object(
                        ignored_tables,
                        visible_ignored_tables=visible_ignored_tables,
                        default_schema=default_schema,
                    ),
                    "compare_type": compare_type,
                    "compare_server_default": True,
                    "render_item": alembic_render_item,
                },
            )
            return [str(diff) for diff in compare_metadata(mc, Base.metadata)]
    finally:
        engine.dispose()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def run_migration_gates(
    *,
    repo_root: Path,
    output_dir: Path,
    test_database_url: str | None = None,
) -> dict[str, Any]:
    """Execute G1-G6 migration gates on isolated disposable DBs and emit structured artifacts."""
    _load_optional_dotenv()
    resolved_url = test_database_url or get_test_database_url()
    validate_test_database_url(resolved_url)

    output_dir.mkdir(parents=True, exist_ok=True)
    executed_at = datetime.now(timezone.utc).isoformat()
    git_state = get_git_state(repo_root)
    git_sha = git_state["git_commit_sha"]
    working_tree_dirty = git_state["working_tree_dirty"]
    working_tree_fp = git_state["working_tree_fingerprint"]
    repo_heads = get_alembic_repo_heads(repo_root)
    repo_head = repo_heads[0] if len(repo_heads) == 1 else ",".join(repo_heads)

    gates: dict[str, dict[str, Any]] = {}
    schema_report: dict[str, Any] = {
        "schema_report_version": "1.0.0",
        "git_commit_sha": git_sha,
        "working_tree_dirty": working_tree_dirty,
        "working_tree_fingerprint": working_tree_fp,
        "executed_at_utc": executed_at,
        "alembic_repo_heads": repo_heads,
        "applied_revision": None,
        "status": "FAIL",
    }
    db_versions: dict[str, Any] = {
        "python_version": platform.python_version(),
        "postgresql_version": None,
        "postgresql_server_version": None,
        "postgis_full_version": None,
        "postgis_lib_version": None,
        "alembic_repo_head": repo_head,
        "alembic_applied_revision": None,
    }

    # --- G1 & G2: Clean Upgrade + Catalog Contract ---
    try:
        with managed_disposable_database(resolved_url, "gate_g1_g2") as db_url:
            cfg = _make_alembic_config(repo_root, db_url)
            command.upgrade(cfg, "head")
            # Idempotent re-upgrade check.
            command.upgrade(cfg, "head")

            engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
            try:
                with engine.connect() as conn:
                    applied_rev = conn.execute(
                        text("SELECT version_num FROM alembic_version")
                    ).scalar_one()
                    pg_version = conn.execute(text("SELECT version()")).scalar_one()
                    pg_server_version = conn.execute(text("SHOW server_version")).scalar_one()
                    postgis_lib = conn.execute(text("SELECT PostGIS_Lib_Version()")).scalar_one()
                    postgis_full = conn.execute(text("SELECT PostGIS_Full_Version()")).scalar_one()

                    db_versions["postgresql_version"] = str(pg_version)
                    db_versions["postgresql_server_version"] = str(pg_server_version)
                    db_versions["postgis_lib_version"] = str(postgis_lib)
                    db_versions["postgis_full_version"] = str(postgis_full)
                    db_versions["alembic_applied_revision"] = str(applied_rev)

                    g1_ok = (
                        len(repo_heads) == 1
                        and applied_rev == repo_head
                        and applied_rev == EXPECTED_HEAD_REVISION
                    )
                    gates["G1"] = {
                        "gate_id": "G1",
                        "name": "G1 — Clean Upgrade",
                        "status": "PASS" if g1_ok else "FAIL",
                        "details": {
                            "repo_heads": repo_heads,
                            "applied_revision": applied_rev,
                            "expected_head": EXPECTED_HEAD_REVISION,
                            "idempotent_reupgrade_verified": True,
                        },
                    }

                    # G2 Catalog Verification
                    tables = list(
                        conn.execute(
                            text(
                                """
                                SELECT tablename FROM pg_tables
                                WHERE schemaname = 'public'
                                  AND tablename NOT IN ('spatial_ref_sys', 'alembic_version')
                                ORDER BY tablename
                                """
                            )
                        ).scalars().all()
                    )
                    spatial_types = [
                        (str(r[0]), str(r[1]), str(r[2]))
                        for r in conn.execute(
                            text(
                                """
                                SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod)
                                FROM pg_attribute a
                                JOIN pg_class c ON c.oid = a.attrelid
                                JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE n.nspname = 'public'
                                  AND c.relname IN ('boundary_versions', 'places', 'staging_place_candidates')
                                  AND a.attname IN ('geom', 'location')
                                  AND NOT a.attisdropped
                                ORDER BY c.relname, a.attname
                                """
                            )
                        ).fetchall()
                    ]
                    constraint_rows = conn.execute(
                        text(
                            """
                            SELECT c.relname, con.conname, con.contype, con.convalidated, con.confdeltype
                            FROM pg_constraint con
                            JOIN pg_class c ON c.oid = con.conrelid
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = 'public'
                              AND c.relname = ANY(:tables)
                            ORDER BY c.relname, con.conname
                            """
                        ),
                        {"tables": EXPECTED_ALL_TABLES},
                    ).fetchall()

                    actual_constraints: dict[str, set[str]] = {t: set() for t in EXPECTED_ALL_TABLES}
                    unvalidated_constraints: list[str] = []
                    non_restrict_fks: list[str] = []
                    for relname, conname, contype, convalidated, confdeltype in constraint_rows:
                        actual_constraints[str(relname)].add(str(conname))
                        if convalidated is not True:
                            unvalidated_constraints.append(f"{relname}.{conname}")
                        if contype == "f" and confdeltype != "r":
                            non_restrict_fks.append(f"{relname}.{conname}({confdeltype})")

                    trigger_rows = conn.execute(
                        text(
                            """
                            SELECT c.relname, t.tgname
                            FROM pg_trigger t
                            JOIN pg_class c ON c.oid = t.tgrelid
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE n.nspname = 'public'
                              AND NOT t.tgisinternal
                              AND c.relname = ANY(:tables)
                            ORDER BY c.relname, t.tgname
                            """
                        ),
                        {"tables": EXPECTED_ALL_TABLES},
                    ).fetchall()
                    actual_triggers: dict[str, set[str]] = {}
                    for relname, tgname in trigger_rows:
                        actual_triggers.setdefault(str(relname), set()).add(str(tgname))

                    tables_match = tables == EXPECTED_ALL_TABLES
                    spatial_match = spatial_types == EXPECTED_SPATIAL_COLUMNS
                    constraints_match = actual_constraints == EXPECTED_CONSTRAINTS_BY_TABLE
                    triggers_match = actual_triggers == EXPECTED_TRIGGERS_BY_TABLE
                    g2_ok = (
                        tables_match
                        and spatial_match
                        and constraints_match
                        and triggers_match
                        and not unvalidated_constraints
                        and not non_restrict_fks
                    )

                    schema_report.update(
                        {
                            "applied_revision": applied_rev,
                            "status": "PASS" if g2_ok else "FAIL",
                            "tables_actual": tables,
                            "tables_expected": EXPECTED_ALL_TABLES,
                            "tables_match": tables_match,
                            "spatial_columns_actual": [list(x) for x in spatial_types],
                            "spatial_columns_expected": [list(x) for x in EXPECTED_SPATIAL_COLUMNS],
                            "spatial_columns_match": spatial_match,
                            "constraint_counts_by_table": {
                                t: len(v) for t, v in sorted(actual_constraints.items())
                            },
                            "total_constraints_verified": sum(
                                len(v) for v in actual_constraints.values()
                            ),
                            "constraints_match": constraints_match,
                            "unvalidated_constraints": unvalidated_constraints,
                            "non_restrict_foreign_keys": non_restrict_fks,
                            "triggers_by_table": {
                                t: sorted(v) for t, v in sorted(actual_triggers.items())
                            },
                            "triggers_match": triggers_match,
                        }
                    )
                    gates["G2"] = {
                        "gate_id": "G2",
                        "name": "G2 — Schema Contract Verification",
                        "status": "PASS" if g2_ok else "FAIL",
                        "details": {
                            "tables_match": tables_match,
                            "spatial_columns_match": spatial_match,
                            "constraints_match": constraints_match,
                            "triggers_match": triggers_match,
                            "total_constraints_verified": schema_report["total_constraints_verified"],
                            "unvalidated_constraints": unvalidated_constraints,
                            "non_restrict_foreign_keys": non_restrict_fks,
                        },
                    }
            finally:
                engine.dispose()
    except Exception as exc:
        gates.setdefault(
            "G1",
            {
                "gate_id": "G1",
                "name": "G1 — Clean Upgrade",
                "status": "FAIL",
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        gates.setdefault(
            "G2",
            {
                "gate_id": "G2",
                "name": "G2 — Schema Contract Verification",
                "status": "FAIL",
                "error": f"{type(exc).__name__}: {exc}",
            },
        )

    # --- G3: Schema Drift Detection ---
    try:
        with managed_disposable_database(resolved_url, "gate_g3") as db_url:
            cfg = _make_alembic_config(repo_root, db_url)
            command.upgrade(cfg, "head")
            clean_diff = _compute_schema_diff(db_url)

            engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
            try:
                with engine.begin() as conn:
                    conn.execute(text("ALTER TABLE places ADD COLUMN m6_drift_probe TEXT"))
            finally:
                engine.dispose()

            injected_diff = _compute_schema_diff(db_url)
            drift_detected = any("m6_drift_probe" in entry for entry in injected_diff)
            g3_ok = len(clean_diff) == 0 and drift_detected
            gates["G3"] = {
                "gate_id": "G3",
                "name": "G3 — Schema Drift Detection",
                "status": "PASS" if g3_ok else "FAIL",
                "details": {
                    "clean_upgrade_diff_count": len(clean_diff),
                    "injected_drift_detected": drift_detected,
                    "injected_diff_count": len(injected_diff),
                },
            }
    except Exception as exc:
        gates["G3"] = {
            "gate_id": "G3",
            "name": "G3 — Schema Drift Detection",
            "status": "FAIL",
            "error": f"{type(exc).__name__}: {exc}",
        }

    # --- G4: Existing-data Compatibility ---
    try:
        with managed_disposable_database(resolved_url, "gate_g4") as db_url:
            cfg = _make_alembic_config(repo_root, db_url)
            command.upgrade(cfg, "0001_m1_initial_schema")

            engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
            try:
                op_v1 = uuid.uuid4()
                with engine.begin() as conn:
                    bv_approved = conn.execute(
                        text(
                            """
                            INSERT INTO boundary_versions (
                                version_code, geom, source_name, checksum_sha256,
                                approval_status, validated_by, validated_at, validation_evidence,
                                approved_by, approved_at
                            )
                            VALUES (
                                'g4-approved', ST_GeomFromText(:wkt, 4326), 'synthetic', :sha,
                                'APPROVED', 'validator@test', '2026-01-01T00:00:00Z',
                                '{"method":"synthetic"}'::jsonb, 'reviewer@test', '2026-01-01T01:00:00Z'
                            )
                            RETURNING id
                            """
                        ),
                        {"wkt": _VALID_MULTIPOLYGON_WKT, "sha": _VALID_SHA256},
                    ).scalar_one()
                    bv_draft = conn.execute(
                        text(
                            """
                            INSERT INTO boundary_versions (
                                version_code, geom, source_name, checksum_sha256, approval_status
                            )
                            VALUES ('g4-draft', ST_GeomFromText(:wkt, 4326), 'synthetic', :sha, 'DRAFT')
                            RETURNING id
                            """
                        ),
                        {"wkt": _VALID_MULTIPOLYGON_WKT, "sha": _VALID_SHA256},
                    ).scalar_one()
                    run_id = conn.execute(
                        text(
                            """
                            INSERT INTO import_runs (
                                boundary_version_id, run_type, source_name, source_checksum_sha256,
                                source_fingerprint, pipeline_version, normalizer_version, config_digest,
                                status, publication_operation_id, published_at, publication_evidence, active_slot
                            )
                            VALUES (
                                :bv_id, 'IMPORT', 'hist.osm', :sha, 'fp-g4', '0.1.0', '0.1.0', 'cfg-g4',
                                'PUBLISHED', :op_id, '2026-01-02T00:00:00Z', '{"scope":"synthetic"}'::jsonb, 1
                            )
                            RETURNING id
                            """
                        ),
                        {"bv_id": bv_approved, "sha": _VALID_SHA256, "op_id": op_v1},
                    ).scalar_one()
                    conn.execute(
                        text(
                            """
                            INSERT INTO places (
                                osm_type, osm_id, name, name_search, category, address,
                                location, raw_tags, last_seen_import_id, is_active
                            )
                            VALUES (
                                'node', 90001, 'Cafe G4', 'cafe g4', 'cafe', '1 Le Loi',
                                ST_SetSRID(ST_MakePoint(106.701, 10.777), 4326)::geography,
                                '{"amenity":"cafe","name":"Cafe G4"}'::jsonb, :run_id, true
                            )
                            """
                        ),
                        {"run_id": run_id},
                    )

                command.upgrade(cfg, "head")

                with engine.connect() as conn:
                    rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                    place_count = conn.execute(text("SELECT count(*) FROM places")).scalar_one()
                    bv_count = conn.execute(
                        text("SELECT count(*) FROM boundary_versions")
                    ).scalar_one()

                # Verify stricter constraint over incompatible DRAFT row fails without modifying row.
                incompatible_blocked = False
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text(
                                "ALTER TABLE boundary_versions "
                                "ADD CONSTRAINT ck_g4_probe_approved CHECK (approval_status = 'APPROVED')"
                            )
                        )
                except IntegrityError:
                    incompatible_blocked = True

                with engine.connect() as conn:
                    draft_status = conn.execute(
                        text("SELECT approval_status FROM boundary_versions WHERE id = :id"),
                        {"id": bv_draft},
                    ).scalar_one()

                g4_ok = (
                    rev == EXPECTED_HEAD_REVISION
                    and place_count == 1
                    and bv_count == 2
                    and incompatible_blocked
                    and draft_status == "DRAFT"
                )
                gates["G4"] = {
                    "gate_id": "G4",
                    "name": "G4 — Existing-data Compatibility",
                    "status": "PASS" if g4_ok else "FAIL",
                    "details": {
                        "upgraded_from": "0001_m1_initial_schema",
                        "upgraded_to": rev,
                        "historical_places_preserved": int(place_count),
                        "historical_boundaries_preserved": int(bv_count),
                        "incompatible_stricter_constraint_blocked": incompatible_blocked,
                        "incompatible_row_unmodified": draft_status == "DRAFT",
                    },
                }
            finally:
                engine.dispose()
    except Exception as exc:
        gates["G4"] = {
            "gate_id": "G4",
            "name": "G4 — Existing-data Compatibility",
            "status": "FAIL",
            "error": f"{type(exc).__name__}: {exc}",
        }

    # --- G5: Migration Failure Safety ---
    try:
        with managed_disposable_database(resolved_url, "gate_g5") as db_url:
            cfg = _make_alembic_config(repo_root, db_url)
            command.upgrade(cfg, "0001_m1_initial_schema")

            engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("CREATE TABLE public.staging_run_checkpoints (id BIGINT PRIMARY KEY)")
                    )

                upgrade_failed_accurately = False
                try:
                    command.upgrade(cfg, "head")
                except ProgrammingError:
                    upgrade_failed_accurately = True

                with engine.connect() as conn:
                    rev_after_fail = conn.execute(
                        text("SELECT version_num FROM alembic_version")
                    ).scalar_one()
                    partial_table = conn.execute(
                        text("SELECT to_regclass('public.staging_place_candidates')")
                    ).scalar_one()

                g5_ok = (
                    upgrade_failed_accurately
                    and rev_after_fail == "0001_m1_initial_schema"
                    and partial_table is None
                )
                gates["G5"] = {
                    "gate_id": "G5",
                    "name": "G5 — Migration Failure Safety",
                    "status": "PASS" if g5_ok else "FAIL",
                    "details": {
                        "alembic_failure_reported": upgrade_failed_accurately,
                        "revision_after_failure": rev_after_fail,
                        "partial_table_rolled_back": partial_table is None,
                    },
                }
            finally:
                engine.dispose()
    except Exception as exc:
        gates["G5"] = {
            "gate_id": "G5",
            "name": "G5 — Migration Failure Safety",
            "status": "FAIL",
            "error": f"{type(exc).__name__}: {exc}",
        }

    # --- G6: Conditional Downgrade / Re-upgrade ---
    try:
        with managed_disposable_database(resolved_url, "gate_g6") as db_url:
            cfg = _make_alembic_config(repo_root, db_url)
            command.upgrade(cfg, "head")
            command.downgrade(cfg, "base")
            command.upgrade(cfg, "head")
            empty_reupgrade_diff = _compute_schema_diff(db_url)

            engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
            try:
                with engine.begin() as conn:
                    bv_id = conn.execute(
                        text(
                            """
                            INSERT INTO boundary_versions (
                                version_code, geom, source_name, checksum_sha256, approval_status
                            )
                            VALUES ('g6-guard', ST_GeomFromText(:wkt, 4326), 'synthetic', :sha, 'DRAFT')
                            RETURNING id
                            """
                        ),
                        {"wkt": _VALID_MULTIPOLYGON_WKT, "sha": _VALID_SHA256},
                    ).scalar_one()
                    run_id = conn.execute(
                        text(
                            """
                            INSERT INTO import_runs (
                                boundary_version_id, source_name, source_checksum_sha256,
                                source_fingerprint, pipeline_version, normalizer_version, config_digest
                            )
                            VALUES (:bv_id, 'g6.osm', :sha, 'fp-g6', '0.2.0', '0.2.0', 'cfg-g6')
                            RETURNING id
                            """
                        ),
                        {"bv_id": bv_id, "sha": _VALID_SHA256},
                    ).scalar_one()
                    conn.execute(
                        text(
                            """
                            INSERT INTO staging_run_checkpoints (
                                import_run_id, source_fingerprint, source_checksum_sha256
                            )
                            VALUES (:run_id, 'fp-g6', :sha)
                            """
                        ),
                        {"run_id": run_id, "sha": _VALID_SHA256},
                    )

                # Non-destructive trigger downgrade 0004 -> 0002 succeeds on populated data.
                command.downgrade(cfg, "0002_m2_staging_tables")
                populated_staging_blocked = False
                try:
                    command.downgrade(cfg, "0001_m1_initial_schema")
                except RuntimeError:
                    populated_staging_blocked = True

                command.upgrade(cfg, "head")
                reupgrade_after_guard_diff = _compute_schema_diff(db_url)

                with engine.begin() as conn:
                    conn.execute(
                        text("DELETE FROM staging_run_checkpoints WHERE import_run_id = :run_id"),
                        {"run_id": run_id},
                    )

                populated_canonical_blocked = False
                try:
                    command.downgrade(cfg, "base")
                except RuntimeError:
                    populated_canonical_blocked = True

                with engine.connect() as conn:
                    rev_after_canonical_block = conn.execute(
                        text("SELECT version_num FROM alembic_version")
                    ).scalar_one()

                command.upgrade(cfg, "head")
                g6_ok = (
                    len(empty_reupgrade_diff) == 0
                    and len(reupgrade_after_guard_diff) == 0
                    and populated_staging_blocked
                    and populated_canonical_blocked
                    and rev_after_canonical_block == "0001_m1_initial_schema"
                )
                gates["G6"] = {
                    "gate_id": "G6",
                    "name": "G6 — Conditional Downgrade/Re-upgrade",
                    "status": "PASS" if g6_ok else "FAIL",
                    "applicability_rule": "G6_POPULATED_DESTRUCTIVE_DOWNGRADE",
                    "non_reversible_recovery_plan": (
                        "Populated 0001_m1_initial_schema -> base and 0002_m2_staging_tables -> 0001_m1_initial_schema "
                        "downgrades fail closed to protect historical POI, audit, and staging data. "
                        "Use a roll-forward Alembic migration or M5 controlled recovery from an immutable verified artifact."
                    ),
                    "details": {
                        "empty_downgrade_reupgrade_zero_drift": len(empty_reupgrade_diff) == 0,
                        "populated_trigger_downgrade_0004_to_0002_succeeded": True,
                        "populated_0002_to_0001_failed_closed": populated_staging_blocked,
                        "populated_0001_to_base_failed_closed": populated_canonical_blocked,
                        "revision_preserved_after_canonical_block": rev_after_canonical_block,
                        "reupgrade_to_head_zero_drift": len(reupgrade_after_guard_diff) == 0,
                    },
                }
            finally:
                engine.dispose()
    except Exception as exc:
        gates["G6"] = {
            "gate_id": "G6",
            "name": "G6 — Conditional Downgrade/Re-upgrade",
            "status": "FAIL",
            "error": f"{type(exc).__name__}: {exc}",
        }

    schema_path = output_dir / "schema_verification.json"
    schema_bytes = (json.dumps(schema_report, indent=2, sort_keys=False) + "\n").encode("utf-8")
    schema_path.write_bytes(schema_bytes)

    all_passed = all(gates.get(gid, {}).get("status") == "PASS" for gid in ("G1", "G2", "G3", "G4", "G5", "G6"))
    gates_payload: dict[str, Any] = {
        "migration_gates_schema_version": "1.0.0",
        "git_commit_sha": git_sha,
        "working_tree_dirty": working_tree_dirty,
        "working_tree_fingerprint": working_tree_fp,
        "executed_at_utc": executed_at,
        "environment": db_versions,
        "schema_verification_file": schema_path.name,
        "schema_verification_sha256": _sha256_bytes(schema_bytes),
        "all_gates_passed": all_passed,
        "gates": gates,
    }

    gates_path = output_dir / "migration_gates.json"
    gates_path.write_text(json.dumps(gates_payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return gates_payload
