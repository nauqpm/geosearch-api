"""M6 Acceptance Registry, S1-S5 suite classification, G1-G6 gates, and evidence evaluation."""

from geosearch.acceptance.evaluator import (
    DEFAULT_REGISTRY_RELPATH,
    RegistryValidationResult,
    compute_file_sha256,
    compute_working_tree_fingerprint,
    discover_repository_test_nodes,
    evaluate_evidence_bundle,
    load_registry,
    validate_registry,
)
from geosearch.acceptance.migration_gates import run_migration_gates
from geosearch.acceptance.reporter import render_acceptance_report, write_acceptance_report
from geosearch.acceptance.suites import (
    SUITE_BY_ID,
    SUITE_BY_MARKER,
    SUITE_DEFINITIONS,
    SuiteDefinition,
    classify_test_node,
)

__all__ = [
    "DEFAULT_REGISTRY_RELPATH",
    "RegistryValidationResult",
    "SUITE_BY_ID",
    "SUITE_BY_MARKER",
    "SUITE_DEFINITIONS",
    "SuiteDefinition",
    "classify_test_node",
    "compute_file_sha256",
    "compute_working_tree_fingerprint",
    "discover_repository_test_nodes",
    "evaluate_evidence_bundle",
    "load_registry",
    "render_acceptance_report",
    "run_migration_gates",
    "validate_registry",
    "write_acceptance_report",
]
