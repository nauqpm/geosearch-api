"""Human-readable Markdown technical acceptance report renderer."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def render_acceptance_report(bundle: dict[str, Any]) -> str:
    """Render a concise, auditable Markdown acceptance report from an evaluated evidence bundle."""
    rev = bundle["revision"]
    gov = bundle["governance"]
    env = bundle["environment"]
    totals = bundle["test_totals"]
    blocking = bundle["blocking_summary"]
    gates = bundle["migration_gates"]["gates"]
    suites = bundle["suites"]
    scope_acc = gov["scope_accounting"]

    lines: list[str] = [
        "# GeoSearch API — Technical Acceptance Report (M6)",
        "",
        f"- **Technical Verdict**: `{bundle['technical_verdict']}`",
        f"- **Formal Sign-Off Status**: `{bundle['formal_sign_off_status']}` *(requires explicit project-owner/reviewer sign-off for `IMPLEMENTATION_ACCEPTED`)*",
        f"- **Declared Scope**: `{bundle['declared_scope']}`",
        f"- **Evaluated At (UTC)**: `{bundle['evaluated_at_utc']}`",
        f"- **CI Execution Status**: `{rev['ci_execution_status']}`",
        "",
        "## 1. Revision, Governance & Environment",
        "",
        "| Attribute | Value |",
        "|---|---|",
        f"| Reviewed Git Commit SHA | `{rev['reviewed_commit_sha']}` |",
        f"| Working Tree Dirty | `{rev['working_tree_dirty']}` (entries: `{rev['dirty_file_count']}`) |",
        f"| Working Tree Fingerprint | `{rev.get('reviewed_working_tree_fingerprint')}` |",
        f"| Acceptance Registry Version | `{gov['registry_version']}` |",
        f"| Acceptance Registry SHA-256 | `{gov['registry_sha256']}` |",
        f"| Python Version | `{env.get('python_version')}` |",
        f"| PostgreSQL Server Version | `{env.get('postgresql_server_version')}` |",
        f"| PostGIS Library Version | `{env.get('postgis_lib_version')}` |",
        f"| Alembic Repo Head | `{env.get('alembic_repo_head')}` |",
        f"| Alembic Applied Revision | `{env.get('alembic_applied_revision')}` |",
        "",
        "### Approved Governance Document Checksums",
        "",
        "| Document | Status | SHA-256 |",
        "|---|---|---|",
    ]
    for doc_path, info in gov["approved_spec_checksums"].items():
        lines.append(f"| `{doc_path}` | `{info['status']}` | `{info['sha256']}` |")

    lines.extend(
        [
            "",
            "### Tracked Fixtures",
            "",
            "| Fixture ID | Path | Scope | Authoritative | SHA-256 |",
            "|---|---|---|---|---|",
        ]
    )
    for fix in bundle.get("fixtures", []):
        lines.append(
            f"| `{fix['fixture_id']}` | `{fix['path']}` | `{fix['scope']}` | "
            f"`{fix['authoritative']}` | `{fix['sha256']}` |"
        )

    lines.extend(
        [
            "",
            "## 2. Suite Results (S1–S5)",
            "",
            "| Suite | Command | Passed | Failed | Errors | Skipped | XFail | Status |",
            "|---|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for sid in ("S1", "S2", "S3", "S4", "S5"):
        s = suites[sid]
        c = s["counts"]
        lines.append(
            f"| {s['name']} | `{s['command']}` | {c['passed']} | {c['failed']} | "
            f"{c['errors']} | {c['skipped']} | {c['xfailed']} | `{s['status']}` |"
        )
    lines.append(
        f"| **Total** | — | **{totals['passed']}** | **{totals['failed']}** | "
        f"**{totals['errors']}** | **{totals['skipped']}** | **{totals['xfailed']}** | "
        f"`{bundle['technical_verdict']}` |"
    )

    lines.extend(
        [
            "",
            "## 3. Migration Gates (G1–G6) & Schema Contract",
            "",
            "| Gate | Check | Status |",
            "|---|---|---|",
        ]
    )
    for gid in ("G1", "G2", "G3", "G4", "G5", "G6"):
        g = gates[gid]
        lines.append(f"| `{gid}` | {g.get('name', gid)} | `{g.get('status', 'MISSING')}` |")

    schema = bundle["schema_verification"]
    lines.extend(
        [
            "",
            f"- Schema contract status: `{schema.get('status')}` "
            f"(applied revision: `{schema.get('applied_revision')}`, "
            f"validated constraints: `{schema.get('total_constraints_verified')}`, "
            f"triggers matched: `{schema.get('triggers_match')}`).",
            "- Non-reversible migration applicability rule (`G6_POPULATED_DESTRUCTIVE_DOWNGRADE`): "
            "Populated `0001 -> base` and `0002 -> 0001` downgrades intentionally fail closed; "
            "approved recovery uses a roll-forward migration or M5 controlled recovery from an immutable verified artifact.",
            "",
            "## 4. Acceptance Registry Scope Accounting & Blocking Status",
            "",
            "| Approved Policy Scope | Registered Memberships | Policy Target |",
            "|---|---:|---:|",
            f"| Fault Injection & Recovery | {scope_acc['policy_scope_counts']['fault_injection_and_recovery']} | 18 |",
            f"| Unique Constraints & Idempotency | {scope_acc['policy_scope_counts']['unique_constraints_and_idempotency']} | 26 |",
            f"| Cross-table Integrity & Recovery | {scope_acc['policy_scope_counts']['cross_table_integrity_and_recovery']} | 28 |",
            f"| Migration & Schema | {scope_acc['policy_scope_counts']['migration_and_schema']} | 15 |",
            f"| **Combined Policy Memberships** | **{scope_acc['combined_policy_memberships']}** | **87** |",
            "",
            f"- Blocking cases for `{bundle['declared_scope']}`: **{blocking['passed_blocking']} / {blocking['total_blocking']} satisfied** "
            f"(open: **{blocking['open_blocking_count']}**).",
            f"- Fault-injection evidence: **{bundle['specialized_evidence']['fault_injection']['passed_cases']} / "
            f"{bundle['specialized_evidence']['fault_injection']['total_cases']}** cases satisfied "
            "(13 publication checkpoints + 14 recovery pre-COMMIT checkpoints).",
            f"- Recovery verification evidence: **{bundle['specialized_evidence']['recovery_verification']['passed_cases']} / "
            f"{bundle['specialized_evidence']['recovery_verification']['total_cases']}** cases satisfied.",
            "",
        ]
    )

    if bundle["overall_blockers"]:
        lines.extend(["### Open Blockers", ""])
        for blocker in bundle["overall_blockers"]:
            lines.append(f"- {blocker}")
        lines.append("")

    if blocking["open_blocking_cases"]:
        lines.extend(
            [
                "### Unsatisfied Blocking Cases",
                "",
                "| Case ID | Suite | Actual Result |",
                "|---|---|---|",
            ]
        )
        for oc in blocking["open_blocking_cases"]:
            lines.append(f"| `{oc['case_id']}` | `{oc['suite']}` | `{oc['actual_result']}` |")
        lines.append("")

    lines.extend(
        [
            "## 5. Deferred, Unverified External Dependencies & Out-of-Scope Items",
            "",
            "| Case ID | Status | Authoritative Blocking | Source | Rationale |",
            "|---|---|---|---|---|",
        ]
    )
    for item in bundle.get("deferred_and_out_of_scope", []):
        lines.append(
            f"| `{item['case_id']}` | `{item['actual_result']}` | "
            f"`{item['blocking_for_authoritative_publication']}` | "
            f"`{item['requirement_ref']}` | {item['deferral_rationale']} |"
        )

    lines.extend(
        [
            "",
            "## 6. Traceability Notes & Attempt History",
            "",
        ]
    )
    for note in gov.get("source_traceability_notes", []):
        lines.append(f"- {note}")

    lines.extend(
        [
            "",
            "| Attempt | Evaluated At (UTC) | Commit SHA | Dirty | Verdict | Passed | Failed | Open Blocking |",
            "|---:|---|---|---|---|---:|---:|---:|",
        ]
    )
    for att in bundle.get("attempt_history", []):
        c = att["test_totals"]
        lines.append(
            f"| {att['attempt_number']} | `{att['evaluated_at_utc']}` | "
            f"`{att['git_commit_sha'][:12]}` | `{att['working_tree_dirty']}` | "
            f"`{att['technical_verdict']}` | {c['passed']} | {c['failed'] + c['errors']} | "
            f"{att['blocking_open_count']} |"
        )

    lines.append("")
    return "\n".join(lines)


def write_acceptance_report(bundle: dict[str, Any], output_path: Path) -> Path:
    """Render and write acceptance_report.md to disk."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_acceptance_report(bundle), encoding="utf-8")
    return output_path
