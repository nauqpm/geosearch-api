"""Unified CLI for local and CI GeoSearch API M6 acceptance verification."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from geosearch.acceptance.evaluator import (
    DEFAULT_REGISTRY_RELPATH,
    compute_file_sha256,
    evaluate_evidence_bundle,
    get_git_state,
    load_registry,
    validate_registry,
)
from geosearch.acceptance.migration_gates import run_migration_gates
from geosearch.acceptance.reporter import write_acceptance_report
from geosearch.acceptance.suites import SUITE_BY_ID, SUITE_DEFINITIONS


def _resolve_repo_root(raw_root: str | None) -> Path:
    if raw_root:
        return Path(raw_root).resolve()
    return Path.cwd().resolve()


def _run_suite_command(
    *,
    repo_root: Path,
    evidence_dir: Path,
    suite_id: str,
) -> int:
    """Run one S1-S5 suite with pytest, writing JUnit XML and commit-bound metadata."""
    suite_def = SUITE_BY_ID[suite_id]
    junit_dir = evidence_dir / "junit"
    junit_dir.mkdir(parents=True, exist_ok=True)
    junit_path = junit_dir / suite_def.junit_filename
    meta_path = junit_dir / f"{suite_id.lower()}_meta.json"

    git_state = get_git_state(repo_root)
    started_at = datetime.now(timezone.utc).isoformat()
    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "-m",
        suite_def.marker,
        "-v",
        f"--junitxml={junit_path}",
    ]
    print(f"\n==> Running {suite_def.name}: {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, cwd=str(repo_root), check=False)
    completed_at = datetime.now(timezone.utc).isoformat()

    meta: dict[str, Any] = {
        "suite_id": suite_id,
        "marker": suite_def.marker,
        "command": suite_def.pytest_command,
        "git_commit_sha": git_state["git_commit_sha"],
        "working_tree_dirty": git_state["working_tree_dirty"],
        "working_tree_fingerprint": git_state["working_tree_fingerprint"],
        "started_at_utc": started_at,
        "completed_at_utc": completed_at,
        "exit_code": proc.returncode,
        "junit_file": suite_def.junit_filename,
        "junit_sha256": compute_file_sha256(junit_path) if junit_path.is_file() else None,
        "ci_metadata": git_state["ci_metadata"],
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return proc.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m geosearch.acceptance",
        description="GeoSearch API M6 Acceptance Registry, Migration Gates, and Evidence Evaluator.",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Repository root directory (defaults to current working directory).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # validate-registry
    p_val = sub.add_parser("validate-registry", help="Validate Acceptance Registry and S1-S5 coverage.")
    p_val.add_argument("--registry", default=None, help="Path to acceptance_registry.json.")

    # run-suite
    p_suite = sub.add_parser("run-suite", help="Execute a single suite (S1..S5) and emit JUnit + metadata.")
    p_suite.add_argument("--suite", required=True, choices=[s.suite_id for s in SUITE_DEFINITIONS])
    p_suite.add_argument("--output-dir", default="artifacts/acceptance")

    # run-migration-gates
    p_gates = sub.add_parser("run-migration-gates", help="Execute G1-G6 gates on isolated disposable DBs.")
    p_gates.add_argument("--output-dir", default="artifacts/acceptance")

    # evaluate
    p_eval = sub.add_parser("evaluate", help="Aggregate evidence and evaluate acceptance policy verdict.")
    p_eval.add_argument("--output-dir", default="artifacts/acceptance")
    p_eval.add_argument("--registry", default=None)
    p_eval.add_argument("--expected-sha", default=None)
    p_eval.add_argument("--ci-mode", action="store_true")
    p_eval.add_argument("--ci-jobs-json", default=None, help="JSON string or file path for CI needs payload.")

    # run-local
    p_local = sub.add_parser(
        "run-local",
        help="Run full local M6 verification (registry check, S1-S5, G1-G6, aggregation, and report).",
    )
    p_local.add_argument("--output-dir", default="artifacts/acceptance")
    p_local.add_argument("--registry", default=None)

    args = parser.parse_args(argv)
    repo_root = _resolve_repo_root(args.repo_root)

    if args.command == "validate-registry":
        reg_path = Path(args.registry) if args.registry else (repo_root / DEFAULT_REGISTRY_RELPATH)
        result = validate_registry(load_registry(reg_path), repo_root=repo_root)
        print(
            json.dumps(
                {
                    "valid": result.valid,
                    "scope_counts": result.scope_counts,
                    "combined_policy_memberships": result.combined_policy_memberships,
                    "total_cases": result.total_cases,
                    "blocking_cases": result.blocking_cases,
                    "deferred_cases": result.deferred_cases,
                    "discovered_test_functions": result.discovered_test_functions,
                    "errors": list(result.errors),
                },
                indent=2,
            )
        )
        return 0 if result.valid else 1

    if args.command == "run-suite":
        out_dir = (repo_root / args.output_dir).resolve()
        return _run_suite_command(repo_root=repo_root, evidence_dir=out_dir, suite_id=args.suite)

    if args.command == "run-migration-gates":
        out_dir = (repo_root / args.output_dir).resolve()
        gates_payload = run_migration_gates(repo_root=repo_root, output_dir=out_dir)
        print(
            json.dumps(
                {
                    "all_gates_passed": gates_payload["all_gates_passed"],
                    "environment": gates_payload["environment"],
                    "gates": {k: v["status"] for k, v in gates_payload["gates"].items()},
                },
                indent=2,
            )
        )
        return 0 if gates_payload["all_gates_passed"] else 1

    if args.command == "evaluate":
        out_dir = (repo_root / args.output_dir).resolve()
        reg_path = Path(args.registry).resolve() if args.registry else None
        ci_jobs: dict[str, Any] | None = None
        if args.ci_jobs_json:
            candidate = Path(args.ci_jobs_json)
            raw_jobs = candidate.read_text(encoding="utf-8") if candidate.is_file() else args.ci_jobs_json
            ci_jobs = json.loads(raw_jobs)

        bundle = evaluate_evidence_bundle(
            repo_root=repo_root,
            evidence_dir=out_dir,
            registry_path=reg_path,
            expected_commit_sha=args.expected_sha,
            ci_mode=args.ci_mode,
            ci_jobs=ci_jobs,
        )
        report_path = write_acceptance_report(bundle, out_dir / "acceptance_report.md")
        print(f"Technical verdict: {bundle['technical_verdict']}")
        print(f"Evidence bundle:   {out_dir / 'evidence_bundle.json'}")
        print(f"Acceptance report: {report_path}")
        if bundle["overall_blockers"]:
            print("Blockers:")
            for b in bundle["overall_blockers"]:
                print(f"  - {b}")
        return 0 if bundle["technical_verdict"] == "TECHNICALLY_VERIFIED" else 1

    if args.command == "run-local":
        out_dir = (repo_root / args.output_dir).resolve()
        reg_path = Path(args.registry).resolve() if args.registry else (repo_root / DEFAULT_REGISTRY_RELPATH)

        print("==> Validating Acceptance Registry & S1-S5 classification", flush=True)
        reg_res = validate_registry(load_registry(reg_path), repo_root=repo_root)
        if not reg_res.valid:
            for err in reg_res.errors:
                print(f"REGISTRY ERROR: {err}", file=sys.stderr)

        # Run every suite without short-circuiting so failed suites still emit evidence.
        for suite_def in SUITE_DEFINITIONS:
            _run_suite_command(
                repo_root=repo_root,
                evidence_dir=out_dir,
                suite_id=suite_def.suite_id,
            )

        print("\n==> Executing G1-G6 Migration Gates on isolated disposable databases", flush=True)
        run_migration_gates(repo_root=repo_root, output_dir=out_dir)

        print("\n==> Evaluating evidence bundle under docs/ACCEPTANCE_POLICY.md", flush=True)
        bundle = evaluate_evidence_bundle(
            repo_root=repo_root,
            evidence_dir=out_dir,
            registry_path=reg_path,
        )
        report_path = write_acceptance_report(bundle, out_dir / "acceptance_report.md")
        print(f"\nTechnical verdict: {bundle['technical_verdict']}")
        print(f"Evidence bundle:   {out_dir / 'evidence_bundle.json'}")
        print(f"Acceptance report: {report_path}")
        if bundle["overall_blockers"]:
            print("Blockers:")
            for b in bundle["overall_blockers"]:
                print(f"  - {b}")
        return 0 if bundle["technical_verdict"] == "TECHNICALLY_VERIFIED" else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
