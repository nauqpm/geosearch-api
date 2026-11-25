"""M6 Acceptance Registry, S1-S5 suite mapping, and policy evaluation unit tests (S1)."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from geosearch.acceptance.evaluator import (
    DEFAULT_REGISTRY_RELPATH,
    REQUIRED_CI_JOBS,
    REQUIRED_COMBINED_POLICY_MEMBERSHIPS,
    REQUIRED_POLICY_SCOPES,
    compute_file_sha256,
    compute_working_tree_fingerprint,
    discover_repository_test_nodes,
    evaluate_evidence_bundle,
    load_registry,
    validate_registry,
)
from geosearch.acceptance.reporter import render_acceptance_report
from geosearch.acceptance.suites import SUITE_DEFINITIONS, base_node_id, classify_test_node

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_synthetic_passing_evidence(
    *,
    evidence_dir: Path,
    registry: dict[str, Any],
    commit_sha: str,
    working_tree_fingerprint: str | None = None,
    node_outcome_overrides: dict[str, str] | None = None,
    gate_status_overrides: dict[str, str] | None = None,
    suite_sha_overrides: dict[str, str] | None = None,
    suite_tree_fp_overrides: dict[str, str | None] | None = None,
    omit_suites: set[str] | None = None,
    tamper_junit_checksum_suites: set[str] | None = None,
    malform_xml_suites: set[str] | None = None,
    gates_sha_override: str | None = None,
    gates_tree_fp_override: str | None = None,
    schema_tree_fp_override: str | None = None,
    schema_status_override: Any = "PASS",
    omit_schema_status: bool = False,
) -> None:
    """Create a complete synthetic S1-S5 + G1-G6 evidence directory for evaluation testing."""
    overrides = node_outcome_overrides or {}
    gate_overrides = gate_status_overrides or {}
    sha_overrides = suite_sha_overrides or {}
    tree_fp_overrides = suite_tree_fp_overrides or {}
    omitted = omit_suites or set()
    tampered = tamper_junit_checksum_suites or set()
    malformed = malform_xml_suites or set()
    default_tree_fp = working_tree_fingerprint or f"clean:{commit_sha}"

    junit_dir = evidence_dir / "junit"
    junit_dir.mkdir(parents=True, exist_ok=True)

    # Collect all referenced nodes grouped by their canonical suite.
    nodes_by_suite: dict[str, list[str]] = {s.suite_id: [] for s in SUITE_DEFINITIONS}
    seen_per_suite: dict[str, set[str]] = {s.suite_id: set() for s in SUITE_DEFINITIONS}
    for case in registry["cases"]:
        if not case.get("blocking", False):
            continue
        for raw_node in case.get("test_nodes", []):
            sid = classify_test_node(raw_node)
            if raw_node not in seen_per_suite[sid]:
                seen_per_suite[sid].add(raw_node)
                nodes_by_suite[sid].append(raw_node)

    for sdef in SUITE_DEFINITIONS:
        sid = sdef.suite_id
        if sid in omitted:
            continue

        xml_path = junit_dir / sdef.junit_filename
        meta_path = junit_dir / f"{sid.lower()}_meta.json"

        if sid in malformed:
            xml_path.write_text("<testsuite><broken", encoding="utf-8")
        else:
            tc_lines: list[str] = []
            for raw_node in nodes_by_suite[sid]:
                file_part, func_part = raw_node.split("::", 1)
                classname = file_part.removesuffix(".py").replace("/", ".")
                outcome = overrides.get(raw_node) or overrides.get(base_node_id(raw_node)) or "PASS"
                if outcome == "PASS":
                    tc_lines.append(
                        f'  <testcase classname="{classname}" name="{func_part}" time="0.01"/>'
                    )
                elif outcome == "FAIL":
                    tc_lines.append(
                        f'  <testcase classname="{classname}" name="{func_part}" time="0.01">'
                        '<failure message="injected failure">boom</failure></testcase>'
                    )
                elif outcome == "SKIPPED":
                    tc_lines.append(
                        f'  <testcase classname="{classname}" name="{func_part}" time="0.01">'
                        '<skipped type="pytest.skip" message="skipped in test"/>'
                        "</testcase>"
                    )
                elif outcome == "XFAIL":
                    tc_lines.append(
                        f'  <testcase classname="{classname}" name="{func_part}" time="0.01">'
                        '<skipped type="pytest.xfail" message="expected failure"/>'
                        "</testcase>"
                    )
                else:
                    raise ValueError(f"Unsupported test outcome override: {outcome}")

            xml_content = (
                '<?xml version="1.0" encoding="utf-8"?>\n'
                f'<testsuite name="{sid}" tests="{len(tc_lines)}" time="0.1">\n'
                + "\n".join(tc_lines)
                + "\n</testsuite>\n"
            )
            xml_path.write_text(xml_content, encoding="utf-8")

        xml_sha = compute_file_sha256(xml_path)
        if sid in tampered:
            xml_sha = "0" * 64

        meta: dict[str, Any] = {
            "suite_id": sid,
            "marker": sdef.marker,
            "command": sdef.pytest_command,
            "git_commit_sha": sha_overrides.get(sid, commit_sha),
            "working_tree_dirty": not default_tree_fp.startswith("clean:"),
            "exit_code": 0,
            "junit_file": sdef.junit_filename,
            "junit_sha256": xml_sha,
        }
        if sid in tree_fp_overrides:
            if tree_fp_overrides[sid] is not None:
                meta["working_tree_fingerprint"] = tree_fp_overrides[sid]
        else:
            meta["working_tree_fingerprint"] = default_tree_fp
        meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    # Write schema_verification.json and migration_gates.json.
    gates_sha = gates_sha_override or commit_sha
    gates_tree_fp = gates_tree_fp_override or default_tree_fp
    schema_tree_fp = schema_tree_fp_override or gates_tree_fp
    schema_payload: dict[str, Any] = {
        "schema_report_version": "1.0.0",
        "git_commit_sha": gates_sha,
        "working_tree_dirty": not schema_tree_fp.startswith("clean:"),
        "working_tree_fingerprint": schema_tree_fp,
        "applied_revision": "0004_m3_staging_immutability",
        "tables_actual": [
            "boundary_versions",
            "duplicate_candidates",
            "import_issues",
            "import_runs",
            "places",
            "staging_issue_fingerprints",
            "staging_place_candidates",
            "staging_run_checkpoints",
        ],
        "total_constraints_verified": 111,
        "triggers_match": True,
    }
    if not omit_schema_status:
        schema_payload["status"] = schema_status_override
    schema_bytes = (json.dumps(schema_payload, indent=2) + "\n").encode("utf-8")
    (evidence_dir / "schema_verification.json").write_bytes(schema_bytes)

    gates_map = {
        gid: {
            "gate_id": gid,
            "name": f"{gid} gate",
            "status": gate_overrides.get(gid, "PASS"),
        }
        for gid in ("G1", "G2", "G3", "G4", "G5", "G6")
    }
    gates_payload = {
        "migration_gates_schema_version": "1.0.0",
        "git_commit_sha": gates_sha,
        "working_tree_dirty": not gates_tree_fp.startswith("clean:"),
        "working_tree_fingerprint": gates_tree_fp,
        "environment": {
            "python_version": "3.12.10",
            "postgresql_server_version": "16.4",
            "postgis_lib_version": "3.4.3",
            "alembic_repo_head": "0004_m3_staging_immutability",
            "alembic_applied_revision": "0004_m3_staging_immutability",
        },
        "schema_verification_file": "schema_verification.json",
        "schema_verification_sha256": hashlib.sha256(schema_bytes).hexdigest(),
        "all_gates_passed": all(g["status"] == "PASS" for g in gates_map.values()),
        "gates": gates_map,
    }
    (evidence_dir / "migration_gates.json").write_text(
        json.dumps(gates_payload, indent=2) + "\n", encoding="utf-8"
    )


def test_registry_accounting_and_suite_completeness() -> None:
    """Acceptance Registry preserves 18+26+28+15=87 memberships and maps every blocking case to real tests."""
    registry_path = REPO_ROOT / DEFAULT_REGISTRY_RELPATH
    registry = load_registry(registry_path)
    result = validate_registry(registry, repo_root=REPO_ROOT)

    assert result.valid is True, f"Registry validation errors: {result.errors}"
    assert result.scope_counts == REQUIRED_POLICY_SCOPES
    assert result.combined_policy_memberships == REQUIRED_COMBINED_POLICY_MEMBERSHIPS
    assert result.blocking_cases >= 87
    assert result.deferred_cases >= 1

    # Every discovered test function in tests/ maps to exactly one suite S1..S5.
    discovered = discover_repository_test_nodes(REPO_ROOT)
    assert len(discovered) > 0
    valid_suites = {s.suite_id for s in SUITE_DEFINITIONS}
    for node_id, suite_id in discovered.items():
        assert suite_id in valid_suites, f"Node {node_id} mapped to unknown suite {suite_id}"

    # Verify every suite has at least one assigned test function.
    counts_by_suite = {s.suite_id: 0 for s in SUITE_DEFINITIONS}
    for suite_id in discovered.values():
        counts_by_suite[suite_id] += 1
    for sid, count in counts_by_suite.items():
        assert count > 0, f"Suite {sid} has zero tests assigned"


def test_evaluator_passes_only_on_complete_fresh_evidence_and_leaves_signoff_pending(
    work_dir: Path,
) -> None:
    """Valid PASS evidence from the reviewed SHA produces TECHNICALLY_VERIFIED while keeping sign-off pending."""
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    reviewed_sha = "1111111111111111111111111111111111111111"
    ev_dir = work_dir / "pass_case"
    _write_synthetic_passing_evidence(
        evidence_dir=ev_dir,
        registry=registry,
        commit_sha=reviewed_sha,
    )

    ci_jobs = {job: {"result": "success"} for job in REQUIRED_CI_JOBS}
    bundle = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=ev_dir,
        expected_commit_sha=reviewed_sha,
        ci_mode=True,
        ci_jobs=ci_jobs,
    )
    assert bundle["technical_verdict"] == "TECHNICALLY_VERIFIED"
    assert bundle["qualifies_for_technically_verified"] is True
    assert bundle["formal_sign_off_status"] == "PENDING_EXPLICIT_SIGN_OFF"
    assert bundle["blocking_summary"]["open_blocking_count"] == 0
    assert bundle["overall_blockers"] == []

    report_md = render_acceptance_report(bundle)
    assert "TECHNICALLY_VERIFIED" in report_md
    assert "PENDING_EXPLICIT_SIGN_OFF" in report_md
    assert reviewed_sha in report_md
    assert "ACC-DEF-01" in report_md
    assert "G6_POPULATED_DESTRUCTIVE_DOWNGRADE" in report_md


def test_evaluator_rejects_missing_failed_skipped_xfailed_and_stale_evidence(
    work_dir: Path,
) -> None:
    """Evaluator fails closed on missing, failed, skipped, xfailed, malformed, stale, or failed-CI evidence."""
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    reviewed_sha = "2222222222222222222222222222222222222222"
    other_sha = "9999999999999999999999999999999999999999"

    # 1. Missing required suite evidence -> UNVERIFIED (not TECHNICALLY_VERIFIED).
    d_missing = work_dir / "missing_suite"
    _write_synthetic_passing_evidence(
        evidence_dir=d_missing, registry=registry, commit_sha=reviewed_sha, omit_suites={"S5"}
    )
    b_missing = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_missing, expected_commit_sha=reviewed_sha
    )
    assert b_missing["technical_verdict"] == "UNVERIFIED"
    assert b_missing["suites"]["S5"]["status"] == "MISSING"
    assert b_missing["blocking_summary"]["open_blocking_count"] > 0

    # 2. Failed blocking test -> VERIFICATION_FAILED.
    d_fail = work_dir / "failed_blocking"
    _write_synthetic_passing_evidence(
        evidence_dir=d_fail,
        registry=registry,
        commit_sha=reviewed_sha,
        node_outcome_overrides={
            "tests/test_schema_uniqueness_and_runs.py::test_duplicate_osm_identity_rejected": "FAIL"
        },
    )
    b_fail = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_fail, expected_commit_sha=reviewed_sha
    )
    assert b_fail["technical_verdict"] == "VERIFICATION_FAILED"
    uq01 = next(c for c in b_fail["cases"] if c["case_id"] == "ACC-UQ-01")
    assert uq01["actual_result"] == "FAIL"
    assert uq01["satisfied"] is False

    # 3. Skipped blocking test -> VERIFICATION_FAILED (SKIPPED never satisfies blocking).
    d_skip = work_dir / "skipped_blocking"
    _write_synthetic_passing_evidence(
        evidence_dir=d_skip,
        registry=registry,
        commit_sha=reviewed_sha,
        node_outcome_overrides={
            "tests/test_schema_uniqueness_and_runs.py::test_duplicate_osm_identity_rejected": "SKIPPED"
        },
    )
    b_skip = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_skip, expected_commit_sha=reviewed_sha
    )
    assert b_skip["technical_verdict"] == "VERIFICATION_FAILED"
    uq01_skip = next(c for c in b_skip["cases"] if c["case_id"] == "ACC-UQ-01")
    assert uq01_skip["actual_result"] == "SKIPPED"
    assert uq01_skip["satisfied"] is False

    # 4. XFailed blocking test -> VERIFICATION_FAILED (XFAIL never satisfies blocking).
    d_xfail = work_dir / "xfailed_blocking"
    _write_synthetic_passing_evidence(
        evidence_dir=d_xfail,
        registry=registry,
        commit_sha=reviewed_sha,
        node_outcome_overrides={
            "tests/test_schema_uniqueness_and_runs.py::test_duplicate_osm_identity_rejected": "XFAIL"
        },
    )
    b_xfail = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_xfail, expected_commit_sha=reviewed_sha
    )
    assert b_xfail["technical_verdict"] == "VERIFICATION_FAILED"
    uq01_xf = next(c for c in b_xfail["cases"] if c["case_id"] == "ACC-UQ-01")
    assert uq01_xf["actual_result"] == "XFAIL"
    assert uq01_xf["satisfied"] is False

    # 5. Stale suite evidence from a different git commit SHA -> UNVERIFIED.
    d_stale = work_dir / "stale_sha"
    _write_synthetic_passing_evidence(
        evidence_dir=d_stale,
        registry=registry,
        commit_sha=reviewed_sha,
        suite_sha_overrides={"S4": other_sha},
    )
    b_stale = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_stale, expected_commit_sha=reviewed_sha
    )
    assert b_stale["technical_verdict"] == "UNVERIFIED"
    assert b_stale["suites"]["S4"]["status"] == "STALE"
    assert any(
        oc["actual_result"] == "STALE" for oc in b_stale["blocking_summary"]["open_blocking_cases"]
    )

    # 6. Stale migration gates from a different SHA -> UNVERIFIED.
    d_stale_gate = work_dir / "stale_gate"
    _write_synthetic_passing_evidence(
        evidence_dir=d_stale_gate,
        registry=registry,
        commit_sha=reviewed_sha,
        gates_sha_override=other_sha,
    )
    b_stale_gate = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_stale_gate, expected_commit_sha=reviewed_sha
    )
    assert b_stale_gate["technical_verdict"] == "UNVERIFIED"
    assert b_stale_gate["migration_gates"]["gates"]["G1"]["status"] == "STALE"

    # 7. Malformed JUnit XML or tampered JUnit checksum -> fails closed.
    d_malformed = work_dir / "malformed_xml"
    _write_synthetic_passing_evidence(
        evidence_dir=d_malformed,
        registry=registry,
        commit_sha=reviewed_sha,
        malform_xml_suites={"S1"},
    )
    b_malformed = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_malformed, expected_commit_sha=reviewed_sha
    )
    assert b_malformed["technical_verdict"] != "TECHNICALLY_VERIFIED"
    assert b_malformed["suites"]["S1"]["status"] == "MALFORMED"

    d_tampered = work_dir / "tampered_sha"
    _write_synthetic_passing_evidence(
        evidence_dir=d_tampered,
        registry=registry,
        commit_sha=reviewed_sha,
        tamper_junit_checksum_suites={"S2"},
    )
    b_tampered = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=d_tampered, expected_commit_sha=reviewed_sha
    )
    assert b_tampered["technical_verdict"] != "TECHNICALLY_VERIFIED"
    assert b_tampered["suites"]["S2"]["status"] == "FAIL"

    # 8. CI mode rejects missing or failed upstream CI jobs even if artifacts exist.
    d_ci_bad = work_dir / "ci_missing_job"
    _write_synthetic_passing_evidence(
        evidence_dir=d_ci_bad, registry=registry, commit_sha=reviewed_sha
    )
    bad_ci_jobs = {
        "s1-unit": {"result": "success"},
        "s2-db-integration": {"result": "failure"},
        "s3-migration-gates": {"result": "success"},
        "s4-workflow": {"result": "success"},
        # s5-race-fault intentionally omitted!
    }
    b_ci_bad = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=d_ci_bad,
        expected_commit_sha=reviewed_sha,
        ci_mode=True,
        ci_jobs=bad_ci_jobs,
    )
    assert b_ci_bad["technical_verdict"] == "VERIFICATION_FAILED"
    assert any("s2-db-integration" in err for err in b_ci_bad["ci_jobs"]["errors"])
    assert any("s5-race-fault" in err for err in b_ci_bad["ci_jobs"]["errors"])


def test_evaluator_preserves_rerun_failure_history_and_redacts_secrets(work_dir: Path) -> None:
    """Rerunning after a failure preserves the original failed attempt; secret DSNs are rejected."""
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    reviewed_sha = "3333333333333333333333333333333333333333"
    ev_dir = work_dir / "rerun_history"

    # Attempt 1: Failing gate G5.
    _write_synthetic_passing_evidence(
        evidence_dir=ev_dir,
        registry=registry,
        commit_sha=reviewed_sha,
        gate_status_overrides={"G5": "FAIL"},
    )
    bundle_1 = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=ev_dir, expected_commit_sha=reviewed_sha
    )
    assert bundle_1["technical_verdict"] == "VERIFICATION_FAILED"
    assert len(bundle_1["attempt_history"]) == 1
    assert bundle_1["attempt_history"][0]["attempt_number"] == 1
    assert bundle_1["attempt_history"][0]["technical_verdict"] == "VERIFICATION_FAILED"
    assert bundle_1["attempt_history"][0]["gate_statuses"]["G5"] == "FAIL"

    # Attempt 2: Fix G5 and re-evaluate in the same evidence directory.
    _write_synthetic_passing_evidence(
        evidence_dir=ev_dir,
        registry=registry,
        commit_sha=reviewed_sha,
    )
    bundle_2 = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=ev_dir, expected_commit_sha=reviewed_sha
    )
    assert bundle_2["technical_verdict"] == "TECHNICALLY_VERIFIED"
    assert len(bundle_2["attempt_history"]) == 2
    # Original failed attempt MUST be preserved untouched!
    assert bundle_2["attempt_history"][0]["technical_verdict"] == "VERIFICATION_FAILED"
    assert bundle_2["attempt_history"][0]["gate_statuses"]["G5"] == "FAIL"
    assert bundle_2["attempt_history"][1]["technical_verdict"] == "TECHNICALLY_VERIFIED"

    # Secret leak guard: embedding a database password/DSN in gate metadata fails closed.
    d_secret = work_dir / "secret_guard"
    _write_synthetic_passing_evidence(
        evidence_dir=d_secret, registry=registry, commit_sha=reviewed_sha
    )
    gates_path = d_secret / "migration_gates.json"
    gates_data = json.loads(gates_path.read_text(encoding="utf-8"))
    gates_data["gates"]["G1"]["leaked"] = "postgresql+psycopg://user:secret_pw@127.0.0.1:5432/db"
    gates_path.write_text(json.dumps(gates_data), encoding="utf-8")

    with pytest.raises(RuntimeError, match="potential secrets"):
        evaluate_evidence_bundle(
            repo_root=REPO_ROOT, evidence_dir=d_secret, expected_commit_sha=reviewed_sha
        )


def test_evaluator_rejects_mixed_working_trees_same_commit_sha(work_dir: Path) -> None:
    """Evidence from two different working trees sharing the exact same Git commit SHA is rejected."""
    # 1. Verify compute_working_tree_fingerprint distinguishes clean, dirty A, dirty B, and untracked
    #    states on the exact same Git commit SHA.
    git_repo = work_dir / "mini_repo"
    git_repo.mkdir()
    subprocess.run(["git", "init"], cwd=str(git_repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "config", "user.email", "test@example.com"],
        cwd=str(git_repo),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "GeoSearch Test"],
        cwd=str(git_repo),
        check=True,
        capture_output=True,
    )
    tracked = git_repo / "app.py"
    tracked.write_text("x = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "app.py"], cwd=str(git_repo), check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "initial"],
        cwd=str(git_repo),
        check=True,
        capture_output=True,
    )
    sha = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=str(git_repo), text=True
    ).strip()

    dirty_clean, _, fp_clean = compute_working_tree_fingerprint(git_repo, sha)
    assert dirty_clean is False
    assert fp_clean == f"clean:{sha}"

    tracked.write_text("x = 2\n", encoding="utf-8")
    dirty_a, _, fp_dirty_a = compute_working_tree_fingerprint(git_repo, sha)
    assert dirty_a is True
    assert fp_dirty_a.startswith(f"dirty:{sha}:")
    assert fp_dirty_a != fp_clean

    tracked.write_text("x = 3\n", encoding="utf-8")
    dirty_b, _, fp_dirty_b = compute_working_tree_fingerprint(git_repo, sha)
    assert dirty_b is True
    assert fp_dirty_b.startswith(f"dirty:{sha}:")
    assert fp_dirty_b != fp_dirty_a

    # 2. Evaluator rejects a suite produced from dirty tree A when evaluating dirty tree B (same SHA).
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    d_mixed_suite = work_dir / "mixed_suite_trees"
    _write_synthetic_passing_evidence(
        evidence_dir=d_mixed_suite,
        registry=registry,
        commit_sha=sha,
        working_tree_fingerprint=fp_dirty_b,
        suite_tree_fp_overrides={"S2": fp_dirty_a},
    )
    b_mixed_suite = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=d_mixed_suite,
        expected_commit_sha=sha,
        expected_working_tree_fingerprint=fp_dirty_b,
    )
    assert b_mixed_suite["technical_verdict"] == "UNVERIFIED"
    assert b_mixed_suite["suites"]["S2"]["status"] == "STALE"
    assert any("different working tree" in err for err in b_mixed_suite["suites"]["S2"]["errors"])
    uq01 = next(c for c in b_mixed_suite["cases"] if c["case_id"] == "ACC-UQ-01")
    assert uq01["actual_result"] == "STALE"
    assert uq01["satisfied"] is False

    # 3. Evaluator rejects clean evidence evaluated against a dirty working tree on the same SHA.
    d_clean_vs_dirty = work_dir / "clean_vs_dirty"
    _write_synthetic_passing_evidence(
        evidence_dir=d_clean_vs_dirty,
        registry=registry,
        commit_sha=sha,
        working_tree_fingerprint=fp_clean,
    )
    b_clean_vs_dirty = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=d_clean_vs_dirty,
        expected_commit_sha=sha,
        expected_working_tree_fingerprint=fp_dirty_a,
    )
    assert b_clean_vs_dirty["technical_verdict"] == "UNVERIFIED"
    assert all(b_clean_vs_dirty["suites"][s.suite_id]["status"] == "STALE" for s in SUITE_DEFINITIONS)
    assert b_clean_vs_dirty["migration_gates"]["gates"]["G1"]["status"] == "STALE"
    assert b_clean_vs_dirty["schema_verification"]["status"] == "STALE"

    # 4. Evaluator rejects migration gates / schema verification from tree A against tree B (same SHA).
    d_mixed_gates = work_dir / "mixed_gates_tree"
    _write_synthetic_passing_evidence(
        evidence_dir=d_mixed_gates,
        registry=registry,
        commit_sha=sha,
        working_tree_fingerprint=fp_dirty_b,
        gates_tree_fp_override=fp_dirty_a,
    )
    b_mixed_gates = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=d_mixed_gates,
        expected_commit_sha=sha,
        expected_working_tree_fingerprint=fp_dirty_b,
    )
    assert b_mixed_gates["technical_verdict"] == "UNVERIFIED"
    assert b_mixed_gates["migration_gates"]["all_gates_passed"] is False
    assert b_mixed_gates["migration_gates"]["gates"]["G1"]["status"] == "STALE"
    assert b_mixed_gates["schema_verification"]["status"] == "STALE"

    # 5. Evaluator rejects missing working_tree_fingerprint in suite metadata.
    d_missing_fp = work_dir / "missing_tree_fp"
    _write_synthetic_passing_evidence(
        evidence_dir=d_missing_fp,
        registry=registry,
        commit_sha=sha,
        working_tree_fingerprint=fp_dirty_a,
        suite_tree_fp_overrides={"S1": None},
    )
    b_missing_fp = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=d_missing_fp,
        expected_commit_sha=sha,
        expected_working_tree_fingerprint=fp_dirty_a,
    )
    assert b_missing_fp["technical_verdict"] == "UNVERIFIED"
    assert b_missing_fp["suites"]["S1"]["status"] == "MALFORMED"


@pytest.mark.parametrize(
    ("label", "mode", "bad_value", "expected_suite_status"),
    [
        ("missing_key", "delete", None, "MALFORMED"),
        ("null_value", "set", None, "MALFORMED"),
        ("bool_false_trap", "set", False, "MALFORMED"),
        ("string_zero", "set", "0", "MALFORMED"),
        ("nonzero_int", "set", 1, "FAIL"),
    ],
)
def test_evaluator_rejects_missing_or_invalid_suite_exit_code(
    work_dir: Path,
    label: str,
    mode: str,
    bad_value: Any,
    expected_suite_status: str,
) -> None:
    """Suite metadata missing exit_code or containing null/bool/string/non-zero exit_code fails closed."""
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    reviewed_sha = "4444444444444444444444444444444444444444"
    ev_dir = work_dir / f"exit_code_{label}"
    _write_synthetic_passing_evidence(
        evidence_dir=ev_dir,
        registry=registry,
        commit_sha=reviewed_sha,
    )

    meta_path = ev_dir / "junit" / "s1_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if mode == "delete":
        meta.pop("exit_code", None)
    else:
        meta["exit_code"] = bad_value
    meta_path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    bundle = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=ev_dir,
        expected_commit_sha=reviewed_sha,
    )
    assert bundle["technical_verdict"] != "TECHNICALLY_VERIFIED"
    assert bundle["qualifies_for_technically_verified"] is False
    assert bundle["suites"]["S1"]["status"] == expected_suite_status
    assert any("exit_code" in err or "non-zero code" in err for err in bundle["suites"]["S1"]["errors"])
    # Blocking S1 cases must NOT be satisfied when suite exit_code is missing or invalid.
    cv03 = next(c for c in bundle["cases"] if c["case_id"] == "ACC-CV-03")
    assert cv03["satisfied"] is False


def test_evaluator_rejects_schema_report_missing_checksum_or_fail_status(work_dir: Path) -> None:
    """Missing/invalid schema_verification_sha256 or FAIL/non-PASS schema status fails closed."""
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    reviewed_sha = "5555555555555555555555555555555555555555"

    # 1. Missing schema_verification_sha256 key, empty string, null, and malformed checksum.
    for label, mutation in (
        ("missing_key", "delete"),
        ("empty_str", ""),
        ("null_val", None),
        ("malformed_hex", "not-a-valid-sha256"),
    ):
        ev_dir = work_dir / f"schema_sha_{label}"
        _write_synthetic_passing_evidence(
            evidence_dir=ev_dir, registry=registry, commit_sha=reviewed_sha
        )
        gates_path = ev_dir / "migration_gates.json"
        gates_data = json.loads(gates_path.read_text(encoding="utf-8"))
        if mutation == "delete":
            gates_data.pop("schema_verification_sha256", None)
        else:
            gates_data["schema_verification_sha256"] = mutation
        gates_path.write_text(json.dumps(gates_data, indent=2) + "\n", encoding="utf-8")

        bundle = evaluate_evidence_bundle(
            repo_root=REPO_ROOT, evidence_dir=ev_dir, expected_commit_sha=reviewed_sha
        )
        assert bundle["technical_verdict"] == "UNVERIFIED", f"Failed for {label}"
        assert bundle["migration_gates"]["all_gates_passed"] is False
        assert bundle["schema_verification"]["status"] == "UNVERIFIED"
        assert any("schema_verification_sha256" in err for err in bundle["migration_gates"]["errors"])
        mg04 = next(c for c in bundle["cases"] if c["case_id"] == "ACC-MG-04")
        xt03 = next(c for c in bundle["cases"] if c["case_id"] == "ACC-XT-03")
        assert mg04["satisfied"] is False
        assert xt03["satisfied"] is False

    # 2. schema_verification.json reports status=FAIL (even with valid SHA-256 and G2=PASS).
    ev_fail = work_dir / "schema_status_fail"
    _write_synthetic_passing_evidence(
        evidence_dir=ev_fail,
        registry=registry,
        commit_sha=reviewed_sha,
        schema_status_override="FAIL",
    )
    b_fail = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=ev_fail, expected_commit_sha=reviewed_sha
    )
    assert b_fail["technical_verdict"] == "VERIFICATION_FAILED"
    assert b_fail["migration_gates"]["all_gates_passed"] is False
    assert b_fail["schema_verification"]["status"] == "FAIL"
    assert b_fail["schema_verification"]["raw_status"] == "FAIL"
    mg04_fail = next(c for c in b_fail["cases"] if c["case_id"] == "ACC-MG-04")
    xt03_fail = next(c for c in b_fail["cases"] if c["case_id"] == "ACC-XT-03")
    assert mg04_fail["actual_result"] == "FAIL"
    assert mg04_fail["satisfied"] is False
    assert xt03_fail["actual_result"] == "FAIL"
    assert xt03_fail["satisfied"] is False

    # 3. schema_verification.json omits status field -> fails closed as UNVERIFIED.
    ev_no_status = work_dir / "schema_status_missing"
    _write_synthetic_passing_evidence(
        evidence_dir=ev_no_status,
        registry=registry,
        commit_sha=reviewed_sha,
        omit_schema_status=True,
    )
    b_no_status = evaluate_evidence_bundle(
        repo_root=REPO_ROOT, evidence_dir=ev_no_status, expected_commit_sha=reviewed_sha
    )
    assert b_no_status["technical_verdict"] == "UNVERIFIED"
    assert b_no_status["migration_gates"]["all_gates_passed"] is False
    assert b_no_status["schema_verification"]["status"] == "UNVERIFIED"


def test_evaluator_rejects_unknown_working_tree_fingerprint(work_dir: Path) -> None:
    """Regression: unknown:<sha> fingerprints (e.g. git status failure) fail closed even when both sides match."""
    sha = "6666666666666666666666666666666666666666"
    unknown_fp = f"unknown:{sha}"

    # 1. Confirm git status failure in a non-git directory produces unknown:<sha>.
    non_git_dir = work_dir / "not_a_git_repo"
    non_git_dir.mkdir()
    is_dirty, dirty_entries, computed_fp = compute_working_tree_fingerprint(non_git_dir, sha)
    assert is_dirty is False
    assert dirty_entries == []
    assert computed_fp == unknown_fp

    # 2. Even if ALL suites, migration gates, schema report, AND the reviewed fingerprint match
    #    at unknown:<sha>, evaluation MUST fail closed as UNVERIFIED.
    registry = load_registry(REPO_ROOT / DEFAULT_REGISTRY_RELPATH)
    ev_both_unknown = work_dir / "both_unknown"
    _write_synthetic_passing_evidence(
        evidence_dir=ev_both_unknown,
        registry=registry,
        commit_sha=sha,
        working_tree_fingerprint=unknown_fp,
    )
    bundle_both = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=ev_both_unknown,
        expected_commit_sha=sha,
        expected_working_tree_fingerprint=unknown_fp,
    )
    assert bundle_both["technical_verdict"] == "UNVERIFIED"
    assert bundle_both["qualifies_for_technically_verified"] is False
    assert any("unknown" in b.lower() for b in bundle_both["overall_blockers"])
    assert all(bundle_both["suites"][s.suite_id]["status"] == "MALFORMED" for s in SUITE_DEFINITIONS)
    assert bundle_both["migration_gates"]["all_gates_passed"] is False
    assert bundle_both["migration_gates"]["gates"]["G1"]["status"] == "UNVERIFIED"
    assert bundle_both["schema_verification"]["status"] == "UNVERIFIED"
    assert bundle_both["blocking_summary"]["passed_blocking"] == 0
    assert bundle_both["blocking_summary"]["open_blocking_count"] == bundle_both["blocking_summary"]["total_blocking"]

    # 3. Single suite with unknown:<sha> against a valid reviewed clean:<sha> is also rejected.
    ev_single_unknown = work_dir / "single_unknown"
    _write_synthetic_passing_evidence(
        evidence_dir=ev_single_unknown,
        registry=registry,
        commit_sha=sha,
        working_tree_fingerprint=f"clean:{sha}",
        suite_tree_fp_overrides={"S3": unknown_fp},
    )
    bundle_single = evaluate_evidence_bundle(
        repo_root=REPO_ROOT,
        evidence_dir=ev_single_unknown,
        expected_commit_sha=sha,
        expected_working_tree_fingerprint=f"clean:{sha}",
    )
    assert bundle_single["technical_verdict"] == "UNVERIFIED"
    assert bundle_single["suites"]["S3"]["status"] == "MALFORMED"
    assert any("unknown" in err.lower() for err in bundle_single["suites"]["S3"]["errors"])


