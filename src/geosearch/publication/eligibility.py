"""Strict fail-closed M3.1 publication preflight and lock-guarded revalidation.

Distinguishes three explicit readiness/authorization tiers:
1. STAGING_READY:
   M2 `PREPARED` + `staging_run_checkpoints.staging_state = 'VERIFIED'`.
   Proves extraction, normalization, source checksum verification, and ST_Covers
   succeeded for the run's extraction_scope. NOT sufficient for authoritative publication.
2. ELIGIBLE_FOR_CONTROLLED_TEST_PUBLICATION:
   Explicitly requested `PublicationMode.CONTROLLED_TEST_NODE_ONLY` with an APPROVED
   boundary (synthetic boundaries allowed only in isolated test databases) and a
   compatible active baseline (either first publication or an active controlled
   node_only test publication; NEVER over an active authoritative/full-coverage dataset).
3. ELIGIBLE_FOR_AUTHORITATIVE_PUBLICATION:
   Default `PublicationMode.AUTHORITATIVE`. Requires full OSM coverage (rejects
   node_only / `node_only_incomplete_osm_coverage`) AND externally verified,
   non-synthetic authoritative boundary evidence in `boundary_versions.validation_evidence`.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

from sqlalchemy import Connection, text
from sqlalchemy.engine import URL

from geosearch.db.engine import (
    UnsafeDatabaseConfigError,
    validate_test_database_url,
    verify_connected_test_database,
)
from geosearch.ingestion.fingerprint import compute_candidate_content_digest
from geosearch.ingestion.models import EXTRACTION_SCOPE_NODE_ONLY, CanonicalPlaceCandidate
from geosearch.publication.models import (
    NODE_ONLY_COVERAGE_RESTRICTION,
    EligibilityStatus,
    EligibilityTier,
    PublicationEligibilityResult,
    PublicationMode,
    PublicationOperationEvidence,
)
from geosearch.publication.operations import (
    inspect_publication_operation,
    inspect_run_publication_state,
    parse_publication_operation_id,
)

SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
UNSET_BASELINE: Final[object] = object()


def verify_controlled_test_database_isolation(conn: Connection) -> str:
    """Fail closed unless `conn` is verified to target an isolated local GeoSearch test database.

    Enforces:
    - Live PostgreSQL `SELECT current_database()` matches the approved `geosearch_test` /
      `geosearch_test_*` naming contract and differs from configured `DATABASE_URL`.
    - Underlying SQLAlchemy Engine URL passes `validate_test_database_url` (driver
      `postgresql+psycopg`, loopback/`postgres_test` host, approved test DB name, and
      canonical `(host, port)` server isolation from configured `DATABASE_URL`).
    """
    current_db = verify_connected_test_database(conn)

    engine = getattr(conn, "engine", None)
    engine_url = getattr(engine, "url", None)
    if not isinstance(engine_url, URL):
        raise UnsafeDatabaseConfigError(
            "Unable to verify underlying SQLAlchemy Engine URL for controlled-test isolation."
        )
    rendered_url = engine_url.render_as_string(hide_password=False)
    parsed_url = validate_test_database_url(rendered_url)
    if parsed_url.database != current_db:
        raise UnsafeDatabaseConfigError(
            f"Connected PostgreSQL database '{current_db}' does not match Engine URL database "
            f"'{parsed_url.database}'."
        )
    return current_db


def compute_staging_dataset_digest(
    conn: Connection,
    *,
    import_run_id: int,
    lock_rows: bool = False,
) -> str:
    """Compute a deterministic canonical SHA-256 digest of all staged candidates for a run.

    When `lock_rows=True`, acquires `FOR SHARE` row locks in deterministic `(osm_type, osm_id)`
    order so concurrent mutations cannot alter staging rows during the publication transaction.
    """
    lock_clause = "FOR SHARE" if lock_rows else ""
    rows = conn.execute(
        text(
            f"""
            SELECT
                id,
                osm_type,
                osm_id,
                name,
                name_search,
                category,
                address,
                encode(ST_AsEWKB(location::geometry), 'hex') AS location_ewkb_hex,
                raw_tags,
                osm_version,
                osm_changeset,
                osm_timestamp,
                content_digest,
                last_seen_pass
            FROM staging_place_candidates
            WHERE import_run_id = :run_id
            ORDER BY osm_type, osm_id
            {lock_clause}
            """
        ),
        {"run_id": import_run_id},
    ).fetchall()

    hasher = hashlib.sha256()
    for row in rows:
        ts_iso = (
            row.osm_timestamp.astimezone(timezone.utc).isoformat()
            if isinstance(row.osm_timestamp, datetime)
            else None
        )
        payload = {
            "id": int(row.id),
            "osm_type": str(row.osm_type),
            "osm_id": int(row.osm_id),
            "name": row.name,
            "name_search": row.name_search,
            "category": str(row.category),
            "address": row.address,
            "location_ewkb_hex": str(row.location_ewkb_hex),
            "raw_tags": dict(row.raw_tags),
            "osm_version": int(row.osm_version) if row.osm_version is not None else None,
            "osm_changeset": int(row.osm_changeset) if row.osm_changeset is not None else None,
            "osm_timestamp": ts_iso,
            "content_digest": str(row.content_digest),
            "last_seen_pass": int(row.last_seen_pass),
        }
        line = (
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
        ).encode("utf-8")
        hasher.update(line)
    return hasher.hexdigest()


def _is_authoritative_boundary_evidence(validation_evidence: Any) -> tuple[bool, list[str]]:
    """Verify boundary validation_evidence proves independent non-synthetic external verification."""
    reasons: list[str] = []
    if not isinstance(validation_evidence, dict):
        return False, ["Boundary validation_evidence must be a JSON object."]

    if validation_evidence.get("synthetic") is True:
        reasons.append("Boundary validation_evidence is explicitly marked synthetic.")
    method = str(validation_evidence.get("method", "")).strip().lower()
    if "synthetic" in method:
        reasons.append(
            f"Boundary validation method {method!r} is synthetic and not eligible "
            "for authoritative publication."
        )
    if validation_evidence.get("verification_class") != "authoritative_external":
        reasons.append(
            "Authoritative publication requires boundary validation_evidence."
            "verification_class == 'authoritative_external'."
        )
    if validation_evidence.get("authoritative_verification") is not True:
        reasons.append(
            "Authoritative publication requires boundary validation_evidence."
            "authoritative_verification == true."
        )
    ext_source = validation_evidence.get("external_reference_source")
    if not isinstance(ext_source, str) or not ext_source.strip():
        reasons.append(
            "Authoritative publication requires a non-blank "
            "validation_evidence.external_reference_source."
        )
    return (len(reasons) == 0), reasons


def _verify_staged_rows_integrity(
    conn: Connection,
    *,
    import_run_id: int,
    expected_fingerprint: str,
    expected_checksum: str,
    extraction_scope: str,
    verify_content_digests: bool,
) -> tuple[list[str], list[str]]:
    """Inspect staged candidate rows for provenance consistency, scope purity, and digest integrity.

    Returns (provenance_errors, staging_errors).
    """
    provenance_errors: list[str] = []
    staging_errors: list[str] = []

    if extraction_scope == EXTRACTION_SCOPE_NODE_ONLY:
        non_node_count = int(
            conn.execute(
                text(
                    """
                    SELECT count(*)
                    FROM staging_place_candidates
                    WHERE import_run_id = :run_id
                      AND osm_type <> 'node'
                    """
                ),
                {"run_id": import_run_id},
            ).scalar_one()
        )
        if non_node_count > 0:
            staging_errors.append(
                f"Found {non_node_count} non-node staged row(s) inside a 'node_only' run."
            )

    rows = conn.execute(
        text(
            """
            SELECT
                id,
                osm_type,
                osm_id,
                name,
                name_search,
                category,
                address,
                ST_X(location::geometry) AS lon,
                ST_Y(location::geometry) AS lat,
                raw_tags,
                osm_version,
                osm_changeset,
                osm_timestamp,
                content_digest,
                source_provenance
            FROM staging_place_candidates
            WHERE import_run_id = :run_id
            ORDER BY osm_type, osm_id
            """
        ),
        {"run_id": import_run_id},
    ).fetchall()

    for row in rows:
        prov = row.source_provenance
        if not isinstance(prov, dict):
            staging_errors.append(
                f"Staged row id={row.id} ({row.osm_type}, {row.osm_id}) has non-object source_provenance."
            )
            continue

        row_fp = prov.get("source_fingerprint")
        if row_fp is not None and str(row_fp) != expected_fingerprint:
            provenance_errors.append(
                f"Staged row ({row.osm_type}, {row.osm_id}) source_fingerprint mismatch."
            )
            break

        row_sha = prov.get("source_checksum_sha256")
        if row_sha is not None and str(row_sha) != expected_checksum:
            provenance_errors.append(
                f"Staged row ({row.osm_type}, {row.osm_id}) source_checksum_sha256 mismatch."
            )
            break

        row_scope = prov.get("extraction_scope")
        if row_scope is not None and str(row_scope) != extraction_scope:
            staging_errors.append(
                f"Staged row ({row.osm_type}, {row.osm_id}) extraction_scope "
                f"{row_scope!r} != checkpoint scope {extraction_scope!r}."
            )
            break

        if verify_content_digests:
            cand = CanonicalPlaceCandidate(
                osm_type=str(row.osm_type),
                osm_id=int(row.osm_id),
                name=row.name,
                name_search=row.name_search,
                category=str(row.category),
                address=row.address,
                longitude=float(row.lon),
                latitude=float(row.lat),
                raw_tags=dict(row.raw_tags),
                osm_version=int(row.osm_version) if row.osm_version is not None else None,
                osm_changeset=int(row.osm_changeset) if row.osm_changeset is not None else None,
                osm_timestamp=(
                    row.osm_timestamp if isinstance(row.osm_timestamp, datetime) else None
                ),
                source_provenance=dict(prov),
            )
            recomputed = compute_candidate_content_digest(cand)
            if recomputed != str(row.content_digest):
                staging_errors.append(
                    f"Staged row ({row.osm_type}, {row.osm_id}) content_digest verification failed "
                    f"(stored={row.content_digest}, recomputed={recomputed})."
                )
                break

    return provenance_errors, staging_errors


def validate_publication_eligibility(
    conn: Connection,
    *,
    import_run_id: int,
    publication_operation_id: uuid.UUID | str,
    publication_mode: PublicationMode = PublicationMode.AUTHORITATIVE,
    expected_active_run_id: int | None | object = UNSET_BASELINE,
    require_recovery_artifact_for_replacement: bool = False,
    recovery_artifact_dir: Path | None = None,
    verify_content_digests: bool = True,
) -> PublicationEligibilityResult:
    """Execute strict fail-closed M3.1 publication preflight validation without modifying live data.

    Can be invoked both before acquiring the advisory lock (preflight) and again inside
    the guarded publication transaction after acquiring `pg_advisory_xact_lock` (revalidation).
    """
    # 1. Validate operation UUID format first (fail closed).
    try:
        op_uuid = parse_publication_operation_id(publication_operation_id)
    except Exception as exc:
        return PublicationEligibilityResult(
            status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
            tier=EligibilityTier.NOT_ELIGIBLE,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=None,
            current_active_run_id=None,
            expected_active_run_id=(
                None
                if expected_active_run_id is UNSET_BASELINE
                else (
                    int(expected_active_run_id)
                    if expected_active_run_id is not None
                    else None
                )
            ),
            boundary_version_id=None,
            boundary_approval_status=None,
            extraction_scope=None,
            coverage_restriction=None,
            staged_candidate_count=0,
            is_first_publication=True,
            reasons=(str(exc),),
        )

    resolved_expected_active: int | None = (
        None
        if expected_active_run_id is UNSET_BASELINE
        else (int(expected_active_run_id) if expected_active_run_id is not None else None)
    )

    # 2. Query current active publication (active_slot = 1).
    active_run = conn.execute(
        text(
            """
            SELECT
                id,
                boundary_version_id,
                status,
                publication_operation_id,
                published_at,
                publication_evidence,
                source_fingerprint
            FROM import_runs
            WHERE active_slot = 1
            """
        )
    ).one_or_none()
    current_active_run_id: int | None = int(active_run.id) if active_run is not None else None
    is_first_publication = current_active_run_id is None

    # 3. Inspect existing operation ownership & target run state before anything else.
    op_owner = inspect_publication_operation(conn, publication_operation_id=op_uuid)
    run_evidence = inspect_run_publication_state(conn, import_run_id=import_run_id)

    if op_owner is not None and op_owner.import_run_id != import_run_id:
        return PublicationEligibilityResult(
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            tier=EligibilityTier.NOT_ELIGIBLE,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=op_uuid,
            current_active_run_id=current_active_run_id,
            expected_active_run_id=resolved_expected_active,
            boundary_version_id=run_evidence.boundary_version_id if run_evidence else None,
            boundary_approval_status=None,
            extraction_scope=None,
            coverage_restriction=None,
            staged_candidate_count=0,
            is_first_publication=is_first_publication,
            reasons=(
                f"publication_operation_id={op_uuid} is already owned by "
                f"ImportRun id={op_owner.import_run_id} (status={op_owner.status!r}).",
            ),
            existing_operation_evidence=op_owner,
        )

    if run_evidence is None:
        return PublicationEligibilityResult(
            status=EligibilityStatus.NOT_PREPARED,
            tier=EligibilityTier.NOT_ELIGIBLE,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=op_uuid,
            current_active_run_id=current_active_run_id,
            expected_active_run_id=resolved_expected_active,
            boundary_version_id=None,
            boundary_approval_status=None,
            extraction_scope=None,
            coverage_restriction=None,
            staged_candidate_count=0,
            is_first_publication=is_first_publication,
            reasons=(f"ImportRun id={import_run_id} does not exist.",),
        )

    # Same operation UUID and already durably committed -> explicit ALREADY_PUBLISHED.
    if (
        run_evidence.publication_operation_id == op_uuid
        and run_evidence.is_committed_publication
    ):
        return PublicationEligibilityResult(
            status=EligibilityStatus.ALREADY_PUBLISHED,
            tier=EligibilityTier.NOT_ELIGIBLE,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=op_uuid,
            current_active_run_id=current_active_run_id,
            expected_active_run_id=resolved_expected_active,
            boundary_version_id=run_evidence.boundary_version_id,
            boundary_approval_status=None,
            extraction_scope=None,
            coverage_restriction=None,
            staged_candidate_count=0,
            is_first_publication=is_first_publication,
            reasons=(
                f"ImportRun id={import_run_id} was already committed under "
                f"publication_operation_id={op_uuid} (status={run_evidence.status!r}).",
            ),
            existing_operation_evidence=run_evidence,
        )

    # Different operation UUID already owns this run -> PUBLICATION_CONFLICT.
    if (
        run_evidence.publication_operation_id is not None
        and run_evidence.publication_operation_id != op_uuid
    ):
        return PublicationEligibilityResult(
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            tier=EligibilityTier.NOT_ELIGIBLE,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=op_uuid,
            current_active_run_id=current_active_run_id,
            expected_active_run_id=resolved_expected_active,
            boundary_version_id=run_evidence.boundary_version_id,
            boundary_approval_status=None,
            extraction_scope=None,
            coverage_restriction=None,
            staged_candidate_count=0,
            is_first_publication=is_first_publication,
            reasons=(
                f"ImportRun id={import_run_id} is already owned by "
                f"publication_operation_id={run_evidence.publication_operation_id} "
                f"(status={run_evidence.status!r}); refusing operation {op_uuid}.",
            ),
            existing_operation_evidence=run_evidence,
        )

    # Already PUBLISHED or SUPERSEDED (without matching UUID).
    if run_evidence.status in ("PUBLISHED", "SUPERSEDED"):
        return PublicationEligibilityResult(
            status=EligibilityStatus.ALREADY_PUBLISHED,
            tier=EligibilityTier.NOT_ELIGIBLE,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=op_uuid,
            current_active_run_id=current_active_run_id,
            expected_active_run_id=resolved_expected_active,
            boundary_version_id=run_evidence.boundary_version_id,
            boundary_approval_status=None,
            extraction_scope=None,
            coverage_restriction=None,
            staged_candidate_count=0,
            is_first_publication=is_first_publication,
            reasons=(
                f"ImportRun id={import_run_id} is already in status {run_evidence.status!r}.",
            ),
            existing_operation_evidence=run_evidence,
        )

    # 4. Load full ImportRun provenance + BoundaryVersion + StagingRunCheckpoint.
    run_row = conn.execute(
        text(
            """
            SELECT
                id,
                boundary_version_id,
                run_type,
                source_name,
                source_checksum_sha256,
                source_fingerprint,
                pipeline_version,
                normalizer_version,
                config_digest,
                status,
                active_slot,
                publication_operation_id
            FROM import_runs
            WHERE id = :run_id
            """
        ),
        {"run_id": import_run_id},
    ).one()

    boundary_row = conn.execute(
        text(
            """
            SELECT
                id,
                version_code,
                source_name,
                checksum_sha256,
                approval_status,
                validated_by,
                validated_at,
                validation_evidence,
                approved_by,
                approved_at,
                ST_IsValid(geom) AS geom_valid,
                NOT ST_IsEmpty(geom) AS geom_non_empty,
                ST_SRID(geom) AS geom_srid
            FROM boundary_versions
            WHERE id = :bv_id
            """
        ),
        {"bv_id": int(run_row.boundary_version_id)},
    ).one_or_none()

    cp = conn.execute(
        text(
            """
            SELECT
                extraction_scope,
                staging_state,
                source_fingerprint,
                source_checksum_sha256,
                source_verified_post_parse,
                current_pass_number,
                resume_count,
                batches_committed,
                total_nodes_seen,
                supported_nodes_seen,
                unsupported_nodes_seen,
                valid_candidates_emitted,
                invalid_candidates_emitted,
                pass_inserted_count,
                outside_boundary_count,
                duplicate_identical_count,
                pass_conflict_count,
                staged_candidate_count,
                conflict_count,
                issue_count,
                extraction_completed_at,
                verified_at,
                readiness_evidence
            FROM staging_run_checkpoints
            WHERE import_run_id = :run_id
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()

    bv_id: int | None = int(boundary_row.id) if boundary_row is not None else None
    bv_status: str | None = str(boundary_row.approval_status) if boundary_row is not None else None
    extraction_scope: str | None = str(cp.extraction_scope) if cp is not None else None
    readiness_evidence = cp.readiness_evidence if (cp is not None and isinstance(cp.readiness_evidence, dict)) else None
    coverage_restriction: str | None = (
        str(readiness_evidence["coverage_restriction"])
        if readiness_evidence is not None and readiness_evidence.get("coverage_restriction") is not None
        else None
    )

    actual_staged_count = int(
        conn.execute(
            text("SELECT count(*) FROM staging_place_candidates WHERE import_run_id = :run_id"),
            {"run_id": import_run_id},
        ).scalar_one()
    )

    def _reject(
        status: EligibilityStatus,
        reasons: list[str],
        *,
        tier: EligibilityTier = EligibilityTier.NOT_ELIGIBLE,
    ) -> PublicationEligibilityResult:
        return PublicationEligibilityResult(
            status=status,
            tier=tier,
            eligible=False,
            import_run_id=import_run_id,
            publication_mode=publication_mode,
            publication_operation_id=op_uuid,
            current_active_run_id=current_active_run_id,
            expected_active_run_id=resolved_expected_active,
            boundary_version_id=bv_id,
            boundary_approval_status=bv_status,
            extraction_scope=extraction_scope,
            coverage_restriction=coverage_restriction,
            staged_candidate_count=actual_staged_count,
            is_first_publication=is_first_publication,
            reasons=tuple(reasons),
            existing_operation_evidence=run_evidence,
        )

    # 5. Check ImportRun status == PREPARED and required provenance fields.
    if run_row.status != "PREPARED":
        return _reject(
            EligibilityStatus.NOT_PREPARED,
            [f"ImportRun id={import_run_id} status must be 'PREPARED', found {run_row.status!r}."],
        )

    if run_row.active_slot is not None:
        return _reject(
            EligibilityStatus.PUBLICATION_CONFLICT,
            [f"ImportRun id={import_run_id} unexpectedly holds active_slot={run_row.active_slot}."],
        )

    # Check expected active baseline when caller explicitly supplied one.
    if (
        expected_active_run_id is not UNSET_BASELINE
        and current_active_run_id != resolved_expected_active
    ):
        return _reject(
            EligibilityStatus.PUBLICATION_CONFLICT,
            [
                f"Active publication baseline mismatch: expected active run "
                f"{resolved_expected_active!r}, but current active run is {current_active_run_id!r}."
            ],
        )

    # Validate immutable run provenance metadata.
    run_prov_errors: list[str] = []
    if not str(run_row.source_name).strip():
        run_prov_errors.append("ImportRun.source_name is blank.")
    if SHA256_RE.fullmatch(str(run_row.source_checksum_sha256)) is None:
        run_prov_errors.append("ImportRun.source_checksum_sha256 is not a valid 64-hex SHA-256.")
    if not str(run_row.source_fingerprint).strip():
        run_prov_errors.append("ImportRun.source_fingerprint is blank.")
    if not str(run_row.pipeline_version).strip():
        run_prov_errors.append("ImportRun.pipeline_version is blank.")
    if not str(run_row.normalizer_version).strip():
        run_prov_errors.append("ImportRun.normalizer_version is blank.")
    if not str(run_row.config_digest).strip():
        run_prov_errors.append("ImportRun.config_digest is blank.")
    if run_prov_errors:
        return _reject(EligibilityStatus.SOURCE_PROVENANCE_MISMATCH, run_prov_errors)

    # 6. Validate StagingRunCheckpoint and provenance parity.
    if cp is None:
        return _reject(
            EligibilityStatus.INVALID_STAGING,
            [f"Missing staging_run_checkpoints record for ImportRun id={import_run_id}."],
        )

    provenance_errors: list[str] = []
    if str(cp.source_fingerprint) != str(run_row.source_fingerprint):
        provenance_errors.append(
            "staging_run_checkpoints.source_fingerprint does not match import_runs.source_fingerprint."
        )
    if str(cp.source_checksum_sha256) != str(run_row.source_checksum_sha256):
        provenance_errors.append(
            "staging_run_checkpoints.source_checksum_sha256 does not match "
            "import_runs.source_checksum_sha256."
        )
    if readiness_evidence is not None:
        ev_fp = readiness_evidence.get("source_fingerprint")
        if ev_fp is not None and str(ev_fp) != str(run_row.source_fingerprint):
            provenance_errors.append(
                "readiness_evidence.source_fingerprint does not match import_runs.source_fingerprint."
            )
        ev_sha = readiness_evidence.get("source_checksum_sha256")
        if ev_sha is not None and str(ev_sha) != str(run_row.source_checksum_sha256):
            provenance_errors.append(
                "readiness_evidence.source_checksum_sha256 does not match "
                "import_runs.source_checksum_sha256."
            )
        ev_bv = readiness_evidence.get("boundary_version_id")
        if ev_bv is not None and int(ev_bv) != int(run_row.boundary_version_id):
            provenance_errors.append(
                "readiness_evidence.boundary_version_id does not match "
                "import_runs.boundary_version_id."
            )
    if provenance_errors:
        return _reject(EligibilityStatus.SOURCE_PROVENANCE_MISMATCH, provenance_errors)

    staging_errors: list[str] = []
    if str(cp.staging_state) != "VERIFIED":
        staging_errors.append(
            f"Staging checkpoint state must be 'VERIFIED', found {cp.staging_state!r}."
        )
    if not bool(cp.source_verified_post_parse):
        staging_errors.append("Post-parse source integrity verification did not succeed.")
    if cp.extraction_completed_at is None or cp.verified_at is None:
        staging_errors.append("Staging checkpoint is missing extraction_completed_at or verified_at.")
    elif cp.verified_at < cp.extraction_completed_at:
        staging_errors.append("Staging checkpoint verified_at precedes extraction_completed_at.")
    if readiness_evidence is None:
        staging_errors.append("Staging checkpoint is missing readiness_evidence JSON object.")
    if int(cp.conflict_count) > 0 or int(cp.pass_conflict_count) > 0:
        staging_errors.append(
            f"Unresolved staging conflicts remain (conflict_count={cp.conflict_count}, "
            f"pass_conflict_count={cp.pass_conflict_count})."
        )
    if actual_staged_count == 0:
        staging_errors.append("Staging candidate dataset is empty (0 rows).")
    if actual_staged_count != int(cp.staged_candidate_count):
        staging_errors.append(
            f"Staged candidate count mismatch: actual={actual_staged_count}, "
            f"checkpoint={cp.staged_candidate_count}."
        )

    actual_issue_count = int(
        conn.execute(
            text("SELECT count(*) FROM import_issues WHERE import_run_id = :run_id"),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    if actual_issue_count != int(cp.issue_count):
        staging_errors.append(
            f"Import issue count mismatch: actual={actual_issue_count}, checkpoint={cp.issue_count}."
        )

    blocking_issues = int(
        conn.execute(
            text(
                """
                SELECT count(*)
                FROM import_issues
                WHERE import_run_id = :run_id
                  AND severity = 'ERROR'
                  AND issue_code IN ('STAGING_CONFLICT', 'SOURCE_INTEGRITY_ERROR')
                """
            ),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    if blocking_issues > 0:
        staging_errors.append(
            f"Found {blocking_issues} blocking ERROR import issue(s) for ImportRun id={import_run_id}."
        )

    if int(cp.total_nodes_seen) != int(cp.supported_nodes_seen) + int(cp.unsupported_nodes_seen):
        staging_errors.append("Checkpoint node totals do not reconcile (total != supported + unsupported).")
    if int(cp.supported_nodes_seen) != int(cp.valid_candidates_emitted) + int(
        cp.invalid_candidates_emitted
    ):
        staging_errors.append(
            "Checkpoint supported node totals do not reconcile (supported != valid + invalid)."
        )
    pass_accounted = (
        int(cp.pass_inserted_count)
        + int(cp.outside_boundary_count)
        + int(cp.duplicate_identical_count)
        + int(cp.pass_conflict_count)
    )
    if pass_accounted != int(cp.valid_candidates_emitted):
        staging_errors.append(
            f"Checkpoint pass accounting mismatch: accounted={pass_accounted}, "
            f"valid_candidates_emitted={cp.valid_candidates_emitted}."
        )

    stale_rows = int(
        conn.execute(
            text(
                """
                SELECT count(*)
                FROM staging_place_candidates
                WHERE import_run_id = :run_id
                  AND last_seen_pass <> :current_pass
                """
            ),
            {"run_id": import_run_id, "current_pass": int(cp.current_pass_number)},
        ).scalar_one()
    )
    if stale_rows > 0:
        staging_errors.append(
            f"Found {stale_rows} stale staged row(s) not verified in pass #{cp.current_pass_number}."
        )

    if staging_errors:
        return _reject(EligibilityStatus.INVALID_STAGING, staging_errors)

    row_prov_errors, row_staging_errors = _verify_staged_rows_integrity(
        conn,
        import_run_id=import_run_id,
        expected_fingerprint=str(run_row.source_fingerprint),
        expected_checksum=str(run_row.source_checksum_sha256),
        extraction_scope=str(cp.extraction_scope),
        verify_content_digests=verify_content_digests,
    )
    if row_prov_errors:
        return _reject(EligibilityStatus.SOURCE_PROVENANCE_MISMATCH, row_prov_errors)
    if row_staging_errors:
        return _reject(EligibilityStatus.INVALID_STAGING, row_staging_errors)

    # 7. Geographic contract checks (validity, SRID 4326, ST_Covers, approval status).
    if boundary_row is None:
        return _reject(
            EligibilityStatus.BOUNDARY_NOT_APPROVED,
            [f"Referenced boundary_version_id={run_row.boundary_version_id} does not exist."],
        )

    geo_staging_errors: list[str] = []
    if not bool(boundary_row.geom_valid) or not bool(boundary_row.geom_non_empty):
        geo_staging_errors.append("Boundary geometry is invalid or empty.")
    if int(boundary_row.geom_srid) != 4326:
        geo_staging_errors.append(f"Boundary SRID must be 4326, found {boundary_row.geom_srid}.")

    uncovered_count = int(
        conn.execute(
            text(
                """
                SELECT count(*)
                FROM staging_place_candidates s
                JOIN boundary_versions b ON b.id = :bv_id
                WHERE s.import_run_id = :run_id
                  AND NOT ST_Covers(b.geom, s.location::geometry)
                """
            ),
            {"run_id": import_run_id, "bv_id": int(boundary_row.id)},
        ).scalar_one()
    )
    if uncovered_count > 0:
        geo_staging_errors.append(
            f"{uncovered_count} staged candidate(s) fail PostGIS ST_Covers against "
            f"boundary_version_id={boundary_row.id}."
        )
    if geo_staging_errors:
        return _reject(EligibilityStatus.INVALID_STAGING, geo_staging_errors)

    # At this point, M2 STAGING_READY has been independently re-verified!
    # Next enforce boundary approval and dataset coverage authorization.
    boundary_approval_errors: list[str] = []
    if str(boundary_row.approval_status) != "APPROVED":
        boundary_approval_errors.append(
            f"BoundaryVersion id={boundary_row.id} ({boundary_row.version_code}) has "
            f"approval_status={boundary_row.approval_status!r}; 'APPROVED' is required for publication."
        )
    else:
        if (
            not boundary_row.validated_by
            or boundary_row.validated_at is None
            or not isinstance(boundary_row.validation_evidence, dict)
            or not boundary_row.approved_by
            or boundary_row.approved_at is None
            or boundary_row.approved_at < boundary_row.validated_at
        ):
            boundary_approval_errors.append(
                f"BoundaryVersion id={boundary_row.id} has incomplete validation/approval metadata."
            )

    if boundary_approval_errors:
        return _reject(
            EligibilityStatus.BOUNDARY_NOT_APPROVED,
            boundary_approval_errors,
            tier=EligibilityTier.STAGING_READY,
        )

    # 8. Dataset coverage & mode compatibility checks.
    coverage_errors: list[str] = []
    if publication_mode == PublicationMode.AUTHORITATIVE:
        if (
            extraction_scope == EXTRACTION_SCOPE_NODE_ONLY
            or coverage_restriction == NODE_ONLY_COVERAGE_RESTRICTION
        ):
            coverage_errors.append(
                "Refusing AUTHORITATIVE publication for incomplete 'node_only' dataset "
                f"(extraction_scope={extraction_scope!r}, "
                f"coverage_restriction={coverage_restriction!r}). "
                "Use explicit PublicationMode.CONTROLLED_TEST_NODE_ONLY only in isolated "
                "engineering verification databases."
            )
        if coverage_errors:
            return _reject(
                EligibilityStatus.COVERAGE_INCOMPATIBLE,
                coverage_errors,
                tier=EligibilityTier.STAGING_READY,
            )

        auth_ok, auth_reasons = _is_authoritative_boundary_evidence(
            boundary_row.validation_evidence
        )
        if not auth_ok:
            return _reject(
                EligibilityStatus.BOUNDARY_NOT_APPROVED,
                auth_reasons,
                tier=EligibilityTier.STAGING_READY,
            )

    elif publication_mode == PublicationMode.CONTROLLED_TEST_NODE_ONLY:
        # Enforce actual live PostgreSQL test-database identity & dev/prod isolation fail-closed.
        try:
            verify_controlled_test_database_isolation(conn)
        except UnsafeDatabaseConfigError as exc:
            return _reject(
                EligibilityStatus.COVERAGE_INCOMPATIBLE,
                [
                    "Controlled-test Node-only publication is forbidden against an unauthorized "
                    f"database/environment: {exc}"
                ],
                tier=EligibilityTier.STAGING_READY,
            )

        if extraction_scope != EXTRACTION_SCOPE_NODE_ONLY:
            coverage_errors.append(
                f"CONTROLLED_TEST_NODE_ONLY mode requires extraction_scope='node_only', "
                f"found {extraction_scope!r}."
            )
        if active_run is not None:
            active_ev = (
                active_run.publication_evidence
                if isinstance(active_run.publication_evidence, dict)
                else {}
            )
            active_mode = active_ev.get("publication_mode")
            active_scope = active_ev.get("extraction_scope")
            if (
                active_mode != PublicationMode.CONTROLLED_TEST_NODE_ONLY.value
                or active_scope != EXTRACTION_SCOPE_NODE_ONLY
            ):
                coverage_errors.append(
                    f"Refusing to publish 'node_only' dataset over active ImportRun "
                    f"id={active_run.id} with incompatible coverage contract "
                    f"(active publication_mode={active_mode!r}, "
                    f"active extraction_scope={active_scope!r})."
                )
            active_non_node_count = int(
                conn.execute(
                    text(
                        "SELECT count(*) FROM places "
                        "WHERE is_active = true AND osm_type <> 'node'"
                    )
                ).scalar_one()
            )
            if active_non_node_count > 0:
                coverage_errors.append(
                    f"Refusing to publish 'node_only' dataset over incompatible mixed-coverage "
                    f"active dataset containing {active_non_node_count} active non-node "
                    "(way/relation) place(s)."
                )
        if coverage_errors:
            return _reject(
                EligibilityStatus.COVERAGE_INCOMPATIBLE,
                coverage_errors,
                tier=EligibilityTier.STAGING_READY,
            )
    else:
        return _reject(
            EligibilityStatus.OTHER_BLOCKING_FAILURE,
            [f"Unsupported publication_mode: {publication_mode!r}."],
            tier=EligibilityTier.STAGING_READY,
        )

    # 9. Verify no untracked orphaned active places of ANY OSM type exist on first publication.
    if is_first_publication:
        orphan_active_count = int(
            conn.execute(text("SELECT count(*) FROM places WHERE is_active = true")).scalar_one()
        )
        if orphan_active_count > 0:
            return _reject(
                EligibilityStatus.OTHER_BLOCKING_FAILURE,
                [
                    f"Inconsistent live state: found {orphan_active_count} active place(s) "
                    "(across all OSM types) while no active PUBLISHED import_run "
                    "(active_slot = 1) exists."
                ],
                tier=EligibilityTier.STAGING_READY,
            )

    # 10. Check recovery artifact presence requirement for replacement publications.
    if (
        require_recovery_artifact_for_replacement
        and not is_first_publication
        and recovery_artifact_dir is None
    ):
        return _reject(
            EligibilityStatus.MISSING_RECOVERY_ARTIFACT,
            [
                f"Replacement publication over active ImportRun id={current_active_run_id} "
                "requires a verified pre-publication immutable recovery artifact."
            ],
            tier=EligibilityTier.STAGING_READY,
        )

    tier = (
        EligibilityTier.ELIGIBLE_FOR_AUTHORITATIVE_PUBLICATION
        if publication_mode == PublicationMode.AUTHORITATIVE
        else EligibilityTier.ELIGIBLE_FOR_CONTROLLED_TEST_PUBLICATION
    )

    return PublicationEligibilityResult(
        status=EligibilityStatus.ELIGIBLE,
        tier=tier,
        eligible=True,
        import_run_id=import_run_id,
        publication_mode=publication_mode,
        publication_operation_id=op_uuid,
        current_active_run_id=current_active_run_id,
        expected_active_run_id=resolved_expected_active,
        boundary_version_id=bv_id,
        boundary_approval_status=bv_status,
        extraction_scope=extraction_scope,
        coverage_restriction=coverage_restriction,
        staged_candidate_count=actual_staged_count,
        is_first_publication=is_first_publication,
        reasons=(),
        existing_operation_evidence=run_evidence,
    )
