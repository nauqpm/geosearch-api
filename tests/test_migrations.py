"""M1.4/M1.5 migration lifecycle and Alembic/PostGIS verification tests (G1-G6)."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from contextlib import AbstractContextManager

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.autogenerate import compare_metadata, render
from alembic.autogenerate.api import AutogenContext
from alembic.migration import MigrationContext
from alembic.operations import ops
from sqlalchemy import inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError, ProgrammingError

from geosearch.db import Base, PostGISGeography, PostGISGeometry, create_test_engine
from geosearch.db.types import (
    alembic_compare_type as compare_type,
    alembic_render_item,
    get_default_schema,
    get_extension_managed_tables,
    get_visible_extension_managed_tables,
    make_alembic_include_object,
)
from tests.helpers import (
    REPO_ROOT,
    assert_pg_error,
    insert_boundary_version,
    make_alembic_config,
)

HEAD_REVISION = "0004_m3_staging_immutability"

EXPECTED_M3_TRIGGERS_BY_TABLE: dict[str, set[str]] = {
    "boundary_versions": {"trg_boundary_versions_guard"},
    "import_runs": {"trg_import_runs_guard"},
    "places": {"trg_places_identity_guard"},
    "duplicate_candidates": {"trg_duplicate_candidates_immutable"},
    "import_issues": {"trg_import_issues_mutate_guard", "trg_import_issues_insert_guard"},
    "staging_place_candidates": {"trg_staging_place_candidates_immutability"},
    "staging_run_checkpoints": {"trg_staging_run_checkpoints_immutability"},
    "staging_issue_fingerprints": {"trg_staging_issue_fingerprints_immutability"},
}

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


def _compute_schema_diff(db_url: str) -> list[object]:
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
            return compare_metadata(mc, Base.metadata)
    finally:
        engine.dispose()


def test_g1_g2_clean_upgrade_idempotency_and_catalog_contract(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """G1 & G2: Clean upgrade succeeds, re-upgrade is idempotent, catalog matches M1 contract."""
    with disposable_database("g1_g2") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")
        # Re-running upgrade at head must be a safe no-op.
        command.upgrade(cfg, "head")

        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.connect() as conn:
                rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                assert rev == HEAD_REVISION

                tables = conn.execute(
                    text(
                        """
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                          AND tablename NOT IN ('spatial_ref_sys', 'alembic_version')
                        ORDER BY tablename
                        """
                    )
                ).scalars().all()
                assert tables == EXPECTED_ALL_TABLES

                spatial_types = conn.execute(
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
                assert spatial_types == [
                    ("boundary_versions", "geom", "geometry(MultiPolygon,4326)"),
                    ("places", "location", "geography(Point,4326)"),
                    ("staging_place_candidates", "location", "geography(Point,4326)"),
                ]

                rows = conn.execute(
                    text(
                        """
                        SELECT c.relname, con.conname, con.contype, con.convalidated, con.confdeltype
                        FROM pg_constraint con
                        JOIN pg_class c ON c.oid = con.conrelid
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public'
                          AND c.relname = ANY(:tables)
                        """
                    ),
                    {"tables": EXPECTED_ALL_TABLES},
                ).fetchall()

                actual_by_table: dict[str, set[str]] = {t: set() for t in EXPECTED_ALL_TABLES}
                for relname, conname, contype, convalidated, confdeltype in rows:
                    assert convalidated is True, f"Constraint {conname} on {relname} is not validated"
                    if contype == "f":
                        assert confdeltype == "r", (
                            f"Foreign key {conname} on {relname} must use ON DELETE RESTRICT"
                        )
                    actual_by_table[relname].add(conname)

                assert actual_by_table == EXPECTED_CONSTRAINTS_BY_TABLE

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
                        """
                    ),
                    {"tables": EXPECTED_ALL_TABLES},
                ).fetchall()
                actual_triggers: dict[str, set[str]] = {}
                for relname, tgname in trigger_rows:
                    actual_triggers.setdefault(str(relname), set()).add(str(tgname))
                assert actual_triggers == EXPECTED_M3_TRIGGERS_BY_TABLE
        finally:
            engine.dispose()


def test_g3_schema_drift_detection(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """G3: Upgraded schema has zero drift; out-of-band DDL drift is detected."""
    with disposable_database("g3_drift") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")

        # Zero drift immediately after clean upgrade.
        assert _compute_schema_diff(db_url) == []

        # Introduce deliberate out-of-band schema drift on the disposable database.
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                conn.execute(text("ALTER TABLE places ADD COLUMN unmigrated_drift_col TEXT"))
        finally:
            engine.dispose()

        drift_ops = _compute_schema_diff(db_url)
        assert len(drift_ops) > 0
        assert any("unmigrated_drift_col" in str(op) for op in drift_ops)


def test_g4_existing_invalid_data_blocks_stricter_constraint_without_silent_repair(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """G4: Adding a stricter constraint over incompatible existing data fails cleanly without modifying rows."""
    with disposable_database("g4_compat") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")

        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(
                    conn, version_code="legacy-v1", approval_status="DRAFT"
                )

            # Simulate a future migration trying to add a constraint violated by existing row.
            with pytest.raises(IntegrityError) as exc_info:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            ALTER TABLE boundary_versions
                            ADD CONSTRAINT ck_boundary_versions_must_be_approved
                            CHECK (approval_status = 'APPROVED')
                            """
                        )
                    )
            assert_pg_error(
                exc_info,
                sqlstate="23514",
                constraint_name="ck_boundary_versions_must_be_approved",
            )

            # Existing row remains intact and unmodified.
            with engine.connect() as conn:
                status = conn.execute(
                    text("SELECT approval_status FROM boundary_versions WHERE id = :id"),
                    {"id": bv_id},
                ).scalar_one()
                assert status == "DRAFT"
        finally:
            engine.dispose()


def test_g5_failed_ddl_transaction_rolls_back_atomically(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """G5: Transactional DDL rolls back completely on failure without advancing alembic_version."""
    with disposable_database("g5_fail") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")

        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with pytest.raises(ProgrammingError):
                with engine.begin() as conn:
                    conn.execute(text("CREATE TABLE partial_migration_probe (id BIGINT PRIMARY KEY)"))
                    conn.execute(
                        text("UPDATE alembic_version SET version_num = '9999_broken_rev'")
                    )
                    # Deliberate DDL failure inside the same transaction.
                    conn.execute(text("ALTER TABLE non_existent_table ADD COLUMN broken TEXT"))

            with engine.connect() as conn:
                rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                assert rev == HEAD_REVISION
                probe_exists = conn.execute(
                    text("SELECT to_regclass('public.partial_migration_probe')")
                ).scalar_one()
                assert probe_exists is None
        finally:
            engine.dispose()


def test_g6_conditional_downgrade_empty_reupgrade_and_populated_guard(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """G6: Downgrade succeeds on an empty DB (and re-upgrades cleanly), but fails closed when data exists."""
    with disposable_database("g6_downgrade") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")

        # Empty DB: conditional downgrade to base succeeds and removes canonical tables.
        command.downgrade(cfg, "base")
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.connect() as conn:
                remaining = conn.execute(
                    text(
                        """
                        SELECT tablename FROM pg_tables
                        WHERE schemaname = 'public'
                          AND tablename = ANY(:tables)
                        """
                    ),
                    {"tables": EXPECTED_ALL_TABLES},
                ).scalars().all()
                assert remaining == []
        finally:
            engine.dispose()

        # Re-upgrade to head restores exact schema with zero drift.
        command.upgrade(cfg, "head")
        assert _compute_schema_diff(db_url) == []

        # Populated staging table: downgrade 0002 -> 0001 must fail closed and stay at HEAD_REVISION.
        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="populated-guard-v1")
                run_id = conn.execute(
                    text(
                        """
                        INSERT INTO import_runs (
                            boundary_version_id, source_name, source_checksum_sha256,
                            source_fingerprint, pipeline_version, normalizer_version, config_digest
                        )
                        VALUES (
                            :bv_id, 'fixture.osm', repeat('a', 64),
                            'fp-guard', '0.2.0', '0.2.0', 'cfg-guard'
                        )
                        RETURNING id
                        """
                    ),
                    {"bv_id": bv_id},
                ).scalar_one()
                conn.execute(
                    text(
                        """
                        INSERT INTO staging_run_checkpoints (
                            import_run_id, source_fingerprint, source_checksum_sha256
                        )
                        VALUES (:run_id, 'fp-guard', repeat('a', 64))
                        """
                    ),
                    {"run_id": run_id},
                )

            # Non-destructive trigger-only downgrade 0003 -> 0002 succeeds on populated data,
            # while populated staging table still blocks 0002 -> 0001 fail-closed!
            command.downgrade(cfg, "0002_m2_staging_tables")
            with engine.connect() as conn:
                rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                assert rev == "0002_m2_staging_tables"

            with pytest.raises(RuntimeError, match="Refusing destructive downgrade"):
                command.downgrade(cfg, "0001_m1_initial_schema")

            with engine.connect() as conn:
                rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                assert rev == "0002_m2_staging_tables"

            # Re-upgrade back to 0003 restores triggers with zero drift.
            command.upgrade(cfg, "head")
            assert _compute_schema_diff(db_url) == []

            # Empty staging tables + populated canonical table: 0003->0001 succeeds, 0001->base refuses.
            with engine.begin() as conn:
                conn.execute(
                    text("DELETE FROM staging_run_checkpoints WHERE import_run_id = :run_id"),
                    {"run_id": run_id},
                )

            with pytest.raises(RuntimeError, match="Refusing destructive downgrade"):
                command.downgrade(cfg, "base")

            with engine.connect() as conn:
                rev = conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                assert rev == "0001_m1_initial_schema"
                persisted_code = conn.execute(
                    text("SELECT version_code FROM boundary_versions WHERE id = :id"),
                    {"id": bv_id},
                ).scalar_one()
                assert persisted_code == "populated-guard-v1"
        finally:
            engine.dispose()


def test_alembic_autogenerate_renders_custom_postgis_types_with_imports() -> None:
    """Autogenerated Alembic operations render PostGIS types with executable import statements."""
    upgrade_ops = ops.UpgradeOps(
        [
            ops.AddColumnOp(
                "places",
                sa.Column("location", PostGISGeography("Point", 4326), nullable=False),
            ),
            ops.AddColumnOp(
                "boundary_versions",
                sa.Column("geom", PostGISGeometry("MultiPolygon", 4326), nullable=False),
            ),
        ]
    )
    mc = MigrationContext.configure(
        dialect=postgresql.dialect(),
        opts={
            "alembic_module_prefix": "op.",
            "sqlalchemy_module_prefix": "sa.",
            "user_module_prefix": None,
            "render_item": alembic_render_item,
        },
    )
    ac = AutogenContext(mc)
    rendered_body = render._render_cmd_body(upgrade_ops, ac)

    assert "from geosearch.db.types import PostGISGeography" in ac.imports
    assert "from geosearch.db.types import PostGISGeometry" in ac.imports
    assert "PostGISGeography('Point', 4326)" in rendered_body
    assert "PostGISGeometry('MultiPolygon', 4326)" in rendered_body

    # Execute the generated migration snippet in an isolated namespace to verify zero NameError.
    recorded: list[tuple[str, sa.Column[object]]] = []

    class _RecorderOp:
        @staticmethod
        def add_column(table_name: str, column: sa.Column[object]) -> None:
            recorded.append((table_name, column))

    module_source = "\n".join(
        [
            "import sqlalchemy as sa",
            *sorted(ac.imports),
            "def upgrade():",
            *[f"    {line}" for line in rendered_body.splitlines()],
        ]
    )
    namespace: dict[str, object] = {"op": _RecorderOp}
    exec(compile(module_source, "<autogenerated_migration>", "exec"), namespace)
    upgrade_fn = namespace["upgrade"]
    assert callable(upgrade_fn)
    upgrade_fn()

    assert len(recorded) == 2
    assert isinstance(recorded[0][1].type, PostGISGeography)
    assert recorded[0][1].type.spatial_signature == ("GEOGRAPHY", "POINT", 4326)
    assert isinstance(recorded[1][1].type, PostGISGeometry)
    assert recorded[1][1].type.spatial_signature == ("GEOMETRY", "MULTIPOLYGON", 4326)


def test_postgis_reflection_distinguishes_srid_0_from_4326(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """SQLAlchemy reflection distinguishes unconstrained SRID 0 from explicit SRID 4326 and 3857."""
    with disposable_database("srid_reflect") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")

        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        CREATE TABLE public._srid_reflection_probe (
                            id BIGINT PRIMARY KEY,
                            g_bare geometry,
                            g_point_unconstrained geometry(Point),
                            g_point_zero geometry(Point, 0),
                            g_point_4326 geometry(Point, 4326),
                            g_mp_3857 geometry(MultiPolygon, 3857),
                            gg_bare geography,
                            gg_point_4326 geography(Point, 4326)
                        )
                        """
                    )
                )

            with engine.connect() as conn:
                cols = {
                    col["name"]: col["type"]
                    for col in inspect(conn).get_columns("_srid_reflection_probe", schema="public")
                }

            assert isinstance(cols["g_bare"], PostGISGeometry)
            assert cols["g_bare"].spatial_signature == ("GEOMETRY", "GEOMETRY", 0)

            assert isinstance(cols["g_point_unconstrained"], PostGISGeometry)
            assert cols["g_point_unconstrained"].spatial_signature == ("GEOMETRY", "POINT", 0)

            assert isinstance(cols["g_point_zero"], PostGISGeometry)
            assert cols["g_point_zero"].spatial_signature == ("GEOMETRY", "POINT", 0)

            assert isinstance(cols["g_point_4326"], PostGISGeometry)
            assert cols["g_point_4326"].spatial_signature == ("GEOMETRY", "POINT", 4326)

            assert isinstance(cols["g_mp_3857"], PostGISGeometry)
            assert cols["g_mp_3857"].spatial_signature == ("GEOMETRY", "MULTIPOLYGON", 3857)

            assert isinstance(cols["gg_bare"], PostGISGeography)
            assert cols["gg_bare"].spatial_signature == ("GEOGRAPHY", "GEOMETRY", 0)

            assert isinstance(cols["gg_point_4326"], PostGISGeography)
            assert cols["gg_point_4326"].spatial_signature == ("GEOGRAPHY", "POINT", 4326)
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    ("alter_sql", "expected_srid"),
    [
        ("geometry(MultiPolygon, 0)", 0),
        ("geometry(MultiPolygon, 3857)", 3857),
    ],
)
def test_incorrect_spatial_srid_detected_as_schema_drift(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    alter_sql: str,
    expected_srid: int,
) -> None:
    """Changing a spatial column to unconstrained SRID 0 or wrong SRID 3857 is detected as drift."""
    with disposable_database("srid_drift") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")
        assert _compute_schema_diff(db_url) == []

        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "ALTER TABLE boundary_versions "
                        "DROP CONSTRAINT ck_boundary_versions_geom_srid_4326"
                    )
                )
                conn.execute(
                    text(
                        f"ALTER TABLE boundary_versions "
                        f"ALTER COLUMN geom TYPE {alter_sql}"
                    )
                )
        finally:
            engine.dispose()

        drift_ops = _compute_schema_diff(db_url)
        assert len(drift_ops) > 0
        assert any(
            "modify_type" in str(diff)
            and "boundary_versions" in str(diff)
            and "geom" in str(diff)
            and f"PostGISGeometry('MultiPolygon', {expected_srid})" in str(diff)
            for diff in drift_ops
        ), f"Expected SRID {expected_srid} type drift in {drift_ops!r}"


def test_extension_table_name_collision_does_not_exclude_user_tables(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """User tables in public sharing names with tiger/topology extension tables are NOT ignored."""
    with disposable_database("ext_collision") as db_url:
        cfg = make_alembic_config(db_url)
        command.upgrade(cfg, "head")

        engine = create_test_engine(db_url, pool_size=1, max_overflow=0)
        try:
            # Mirror PostGIS container setup where tiger schema is on search_path with 'place'/'edges'.
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS tiger"))
                conn.execute(text("CREATE TABLE tiger.place (id BIGINT PRIMARY KEY)"))
                conn.execute(text("CREATE TABLE tiger.edges (id BIGINT PRIMARY KEY)"))
                conn.execute(
                    text(
                        """
                        DO $$
                        BEGIN
                            EXECUTE format(
                                'ALTER DATABASE %I SET search_path = "$user", public, tiger',
                                current_database()
                            );
                        END
                        $$;
                        """
                    )
                )

            # Extension/helper schema tables on search_path must remain cleanly excluded.
            assert _compute_schema_diff(db_url) == []

            # Now create user tables in public whose names collide with tiger.place and tiger.edges.
            with engine.begin() as conn:
                conn.execute(text("CREATE TABLE public.place (id BIGINT PRIMARY KEY)"))
                conn.execute(text("CREATE TABLE public.edges (id BIGINT PRIMARY KEY)"))

            with engine.connect() as conn:
                ext_tables = get_extension_managed_tables(conn)
                visible_ext = get_visible_extension_managed_tables(conn)
                assert ("public", "spatial_ref_sys") in ext_tables
                assert ("tiger", "place") in ext_tables
                assert ("tiger", "edges") in ext_tables
                assert ("public", "place") not in ext_tables
                assert ("public", "edges") not in ext_tables
                assert "spatial_ref_sys" in visible_ext
                assert "place" not in visible_ext
                assert "edges" not in visible_ext
        finally:
            engine.dispose()

        drift_ops = _compute_schema_diff(db_url)
        drift_summary = [str(op) for op in drift_ops]
        assert any("remove_table" in entry and "'place'" in entry for entry in drift_summary)
        assert any("remove_table" in entry and "'edges'" in entry for entry in drift_summary)
        assert not any("spatial_ref_sys" in entry for entry in drift_summary)


def test_alembic_workflow_succeeds_without_python_dotenv(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """Core installation without python-dotenv supports Alembic upgrade, current, and check."""
    with disposable_database("core_alembic") as db_url:
        script = """
import sys
from alembic import command
from alembic.config import Config

# Simulate a core-only installation where optional dev dependency python-dotenv is absent.
sys.modules["dotenv"] = None

cfg = Config("alembic.ini")
command.upgrade(cfg, "head")
command.current(cfg)
command.check(cfg)
"""
        env = os.environ.copy()
        env["DATABASE_URL"] = db_url
        env.pop("TEST_DATABASE_URL", None)

        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, (
            f"Alembic core-only workflow failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
        assert HEAD_REVISION in result.stdout
