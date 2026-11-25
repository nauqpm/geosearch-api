"""Canonical S1-S5 test suite classification for GeoSearch API acceptance verification."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class SuiteDefinition:
    """Metadata and reproducible pytest selector for one approved acceptance suite."""

    suite_id: str
    marker: str
    name: str
    coverage: str
    requires_database: bool
    junit_filename: str

    @property
    def pytest_command(self) -> str:
        return f"pytest -m {self.marker} -v"


SUITE_DEFINITIONS: Final[tuple[SuiteDefinition, ...]] = (
    SuiteDefinition(
        suite_id="S1",
        marker="s1",
        name="S1 — Unit",
        coverage="Pure Python rules, normalization, validation, classification, guards, and acceptance evaluation",
        requires_database=False,
        junit_filename="s1_unit.xml",
    ),
    SuiteDefinition(
        suite_id="S2",
        marker="s2",
        name="S2 — Database Integration",
        coverage="Real PostgreSQL/PostGIS constraints, staging integration, spatial queries, and API query endpoints",
        requires_database=True,
        junit_filename="s2_db_integration.xml",
    ),
    SuiteDefinition(
        suite_id="S3",
        marker="s3",
        name="S3 — Migration",
        coverage="Alembic G1-G6 migration lifecycle, catalog contract, drift, and PostGIS reflection checks",
        requires_database=True,
        junit_filename="s3_migration.xml",
    ),
    SuiteDefinition(
        suite_id="S4",
        marker="s4",
        name="S4 — Workflow",
        coverage="Publication, controlled recovery, durable evidence, history, and duplicate-candidate workflows",
        requires_database=True,
        junit_filename="s4_workflow.xml",
    ),
    SuiteDefinition(
        suite_id="S5",
        marker="s5",
        name="S5 — Race & Fault Injection",
        coverage="Independent connections, deterministic checkpoints, rollback, concurrency, and reader snapshot isolation",
        requires_database=True,
        junit_filename="s5_race_fault.xml",
    ),
)

SUITE_BY_ID: Final[dict[str, SuiteDefinition]] = {s.suite_id: s for s in SUITE_DEFINITIONS}
SUITE_BY_MARKER: Final[dict[str, SuiteDefinition]] = {s.marker: s for s in SUITE_DEFINITIONS}

# Default suite assigned to every test in a file unless overridden in NODE_SUITE_OVERRIDES.
# Every test file in tests/ MUST appear here so newly added test files cannot go unclassified.
FILE_DEFAULT_SUITE: Final[dict[str, str]] = {
    "tests/test_db_smoke.py": "S1",
    "tests/test_m2_classification.py": "S1",
    "tests/test_m2_osm_extractor.py": "S1",
    "tests/test_m2_validation.py": "S1",
    "tests/test_m6_acceptance.py": "S1",
    "tests/test_schema_column_validation.py": "S2",
    "tests/test_schema_uniqueness_and_runs.py": "S2",
    "tests/test_spatial_integrity.py": "S2",
    "tests/test_m2_staging.py": "S2",
    "tests/test_m2_pipeline_integration.py": "S2",
    "tests/test_m2_remediation.py": "S2",
    "tests/test_m4_health.py": "S2",
    "tests/test_m4_places_detail.py": "S2",
    "tests/test_m4_places_search.py": "S2",
    "tests/test_m4_nearby.py": "S2",
    "tests/test_m4_api_consistency.py": "S5",
    "tests/test_migrations.py": "S3",
    "tests/test_m3_eligibility_and_guards.py": "S4",
    "tests/test_m3_recovery_artifact.py": "S4",
    "tests/test_m3_publication_e2e.py": "S4",
    "tests/test_m37_remediation.py": "S4",
    "tests/test_m5_recovery_verification_and_approval.py": "S4",
    "tests/test_m5_recovery_e2e_and_history.py": "S4",
    "tests/test_m3_fault_injection_and_concurrency.py": "S5",
    "tests/test_m5_faults_concurrency_and_duplicates.py": "S5",
}

# Function-level overrides keyed by "<relative_file>::<function_name>" (without parametrize suffix).
NODE_SUITE_OVERRIDES: Final[dict[str, str]] = {
    # Live PostgreSQL smoke checks in test_db_smoke.py belong to S2 (pure URL guard checks stay S1).
    "tests/test_db_smoke.py::test_select_one": "S2",
    "tests/test_db_smoke.py::test_database_identity": "S2",
    "tests/test_db_smoke.py::test_postgis_extension_available": "S2",
    "tests/test_db_smoke.py::test_postgis_spatial_function_execution": "S2",
    # Pure Python coordinate validator in test_schema_column_validation.py belongs to S1.
    "tests/test_schema_column_validation.py::test_python_layer_rejects_invalid_wgs84_coordinates": "S1",
    # Pure Python fingerprint/digest unit check in test_m2_staging.py belongs to S1.
    "tests/test_m2_staging.py::test_deterministic_source_fingerprint_and_content_digest": "S1",
    # Fault/rollback and deterministic concurrency checks in M2 belong to S5.
    "tests/test_m2_staging.py::test_failed_staging_transaction_rolls_back_cleanly_on_real_commit": "S5",
    "tests/test_m2_pipeline_integration.py::test_e2e_failed_batch_leaves_no_partial_batch_writes_and_fails_readiness": "S5",
    "tests/test_m2_pipeline_integration.py::test_e2e_corrupt_source_file_marks_run_failed_with_source_integrity_issue": "S5",
    "tests/test_m2_remediation.py::test_blocker2_source_modified_between_checksum_and_parse_is_rejected": "S5",
    "tests/test_m2_remediation.py::test_blocker2_source_replaced_with_other_valid_file_and_clean_truncation_rejected": "S5",
    "tests/test_m2_remediation.py::test_blocker2_midstream_truncated_xml_fails_safely_and_blocks_partial_batches": "S5",
    "tests/test_m2_remediation.py::test_concurrent_staging_writer_blocked_during_verify_and_prepare_run": "S5",
    # Concurrent staging races, lost commit ACK, and reader rollback isolation in M3.7 belong to S5.
    "tests/test_m37_remediation.py::test_blocker2_concurrent_direct_staging_write_racing_with_prepared_transition": "S5",
    "tests/test_m37_remediation.py::test_blocker2_concurrent_staging_mutation_during_open_publication_and_in_tx_digest_guard": "S5",
    "tests/test_m37_remediation.py::test_section5_lost_commit_ack_followed_by_v3_supersession_returns_already_published": "S5",
    "tests/test_m37_remediation.py::test_section5_reader_isolation_during_failed_rolled_back_publication": "S5",
    # Pure Python config/lifespan/LIKE/validation tests in M4 belong to S1; DB-only consistency to S2.
    "tests/test_m4_health.py::test_m41_load_api_settings_rejects_missing_database_url_without_test_fallback": "S1",
    "tests/test_m4_health.py::test_m41_load_api_settings_rejects_invalid_database_url_and_invalid_tuning": "S1",
    "tests/test_m4_health.py::test_m41_load_api_settings_preserves_explicit_env_over_dotenv": "S1",
    "tests/test_m4_health.py::test_m41_app_lifespan_disposes_owned_engine_only": "S1",
    "tests/test_m4_places_search.py::test_m42_escape_like_literal_unit": "S1",
    "tests/test_m4_api_consistency.py::test_m44_test_database_identity_guards_remain_fail_closed": "S1",
    "tests/test_m4_api_consistency.py::test_m47_ac1_validation_rejects_invalid_requests_before_database_access": "S1",
    "tests/test_m4_api_consistency.py::test_m44_postgresql_isolation_levels_readonly_transactions_and_write_methods_blocked": "S2",
    "tests/test_m4_api_consistency.py::test_m47_ac3_no_active_publication_returns_empty_and_hides_orphan_active_place": "S2",
    # Duplicate-candidate workflow & history preservation tests in M5 belong to S4.
    "tests/test_m5_faults_concurrency_and_duplicates.py::test_m5_ac6_duplicate_evidence_preserved_across_supersession_recovery_retry_and_deactivation": "S4",
    "tests/test_m5_faults_concurrency_and_duplicates.py::test_m5_ac6_default_recovery_records_not_executed_and_preserves_v1_v2_duplicate_history_only": "S4",
}


def base_node_id(node_id: str) -> str:
    """Strip any pytest parametrize bracket suffix while normalizing path separators."""
    normalized = node_id.replace("\\", "/")
    bracket_idx = normalized.find("[")
    if bracket_idx != -1:
        return normalized[:bracket_idx]
    return normalized


def classify_test_node(node_id: str) -> str:
    """Classify a pytest nodeid into exactly one suite ID (S1..S5), failing closed if unknown."""
    normalized = base_node_id(node_id)
    if normalized in NODE_SUITE_OVERRIDES:
        suite_id = NODE_SUITE_OVERRIDES[normalized]
    else:
        file_part = normalized.split("::", 1)[0]
        if file_part not in FILE_DEFAULT_SUITE:
            raise ValueError(
                f"Unclassified test file '{file_part}' for node '{node_id}'. "
                "Register it in geosearch.acceptance.suites.FILE_DEFAULT_SUITE."
            )
        suite_id = FILE_DEFAULT_SUITE[file_part]

    if suite_id not in SUITE_BY_ID:
        raise ValueError(f"Invalid suite ID '{suite_id}' configured for '{node_id}'.")
    return suite_id
