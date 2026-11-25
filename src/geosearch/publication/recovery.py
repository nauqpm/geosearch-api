"""M5 controlled recovery from immutable publication artifacts.

Implements:
- Historical source artifact verification (manifest, format version, checksums, JSONL canonical
  content, name_search parity, PostGIS EWKB & ST_Covers parity, historical import_runs &
  APPROVED boundary_versions provenance, and existing stable places.id verification).
- TOCTOU-safe isolated snapshot copying so staging consumes the exact verified bytes without
  mutating the source artifact, while detecting any on-disk tampering between verification
  and staging.
- Mandatory traceable manual approval (`RecoveryApproval`) bound cryptographically to the
  source artifact, recovery `publication_operation_id`, and `expected_active_run_id`.
- Atomic recovery run preparation (`prepare_recovery_run`) creating one `RECOVERY`
  `import_runs` row bound to `publication_operation_id` with full staging and `0004`
  immutability triggers, plus idempotent retry lookup that rejects UUID repurposing.
- Guarded end-to-end recovery execution (`execute_controlled_recovery`) reusing M3
  `publish_prepared_run` while keeping the V1 source artifact and V2 protection artifact
  explicitly distinct.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final, Iterator

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError

from geosearch.db.engine import UnsafeDatabaseConfigError
from geosearch.ingestion.fingerprint import compute_candidate_content_digest
from geosearch.ingestion.models import EXTRACTION_SCOPE_NODE_ONLY, CanonicalPlaceCandidate
from geosearch.ingestion.validation import normalize_name_search
from geosearch.publication.eligibility import (
    _is_authoritative_boundary_evidence,
    verify_controlled_test_database_isolation,
)
from geosearch.publication.engine import publish_prepared_run
from geosearch.publication.models import (
    NODE_ONLY_COVERAGE_RESTRICTION,
    RECOVERY_ARTIFACT_FORMAT_VERSION,
    RECOVERY_PIPELINE_VERSION,
    DuplicateCandidateInput,
    DuplicateDetectionState,
    EligibilityStatus,
    PreparedRecoveryRun,
    PublicationConflictError,
    PublicationEligibilityError,
    PublicationMode,
    RecoveryApproval,
    RecoveryApprovalError,
    RecoveryArtifactError,
    RecoveryArtifactPlaceRecord,
    RecoveryArtifactRecord,
    RecoveryFaultHooks,
    RecoveryResult,
    VerifiedHistoricalRecoveryArtifact,
)
from geosearch.publication.operations import (
    inspect_publication_operation,
    parse_publication_operation_id,
)
from geosearch.publication.recovery_artifact import (
    DUPLICATES_SNAPSHOT_FILENAME,
    MANIFEST_CHECKSUM_FILENAME,
    MANIFEST_FILENAME,
    PLACES_SNAPSHOT_FILENAME,
    verify_recovery_artifact,
)

_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_ARTIFACT_FILES: Final[tuple[str, ...]] = (
    MANIFEST_FILENAME,
    MANIFEST_CHECKSUM_FILENAME,
    PLACES_SNAPSHOT_FILENAME,
    DUPLICATES_SNAPSHOT_FILENAME,
)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _parse_iso_datetime(value: Any, *, field_name: str, line_no: int) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RecoveryArtifactError(
            f"Line {line_no}: invalid {field_name} timestamp {value!r}."
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise RecoveryArtifactError(
            f"Line {line_no}: unparseable {field_name} timestamp {value!r}: {exc}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecoveryArtifactError(
            f"Line {line_no}: {field_name} timestamp must include timezone offset."
        )
    return parsed.astimezone(timezone.utc)


def _compute_recovery_config_digest(
    *,
    extraction_scope: str,
    artifact_format: str,
) -> str:
    payload = {
        "schema": "geosearch.recovery.config.v1",
        "extraction_scope": extraction_scope,
        "artifact_format": artifact_format,
        "pipeline_version": RECOVERY_PIPELINE_VERSION,
    }
    return _sha256_bytes(_canonical_json_bytes(payload))


def _compute_recovery_source_fingerprint(
    *,
    source_artifact: RecoveryArtifactRecord,
    config_digest: str,
    normalizer_version: str,
) -> str:
    payload = {
        "schema": "geosearch.recovery.fingerprint.v1",
        "artifact_format": source_artifact.artifact_format,
        "source_import_run_id": source_artifact.source_import_run_id,
        "source_publication_operation_id": str(source_artifact.source_publication_operation_id),
        "source_boundary_version_id": source_artifact.source_boundary_version_id,
        "source_boundary_checksum_sha256": source_artifact.source_boundary_checksum_sha256,
        "manifest_sha256": source_artifact.manifest_sha256,
        "content_sha256": source_artifact.content_sha256,
        "active_osm_identities_sha256": source_artifact.active_osm_identities_sha256,
        "pipeline_version": RECOVERY_PIPELINE_VERSION,
        "normalizer_version": normalizer_version,
        "config_digest": config_digest,
    }
    return _sha256_bytes(_canonical_json_bytes(payload))


def build_recovery_approval_from_artifact(
    source_artifact: RecoveryArtifactRecord,
    *,
    publication_operation_id: uuid.UUID | str,
    expected_active_run_id: int | None,
    approved_by: str,
    approval_reason: str,
    approved_at: datetime | None = None,
) -> RecoveryApproval:
    """Construct a typed `RecoveryApproval` bound to a verified source artifact and operation."""
    op_uuid = parse_publication_operation_id(publication_operation_id)
    ts = approved_at if approved_at is not None else datetime.now(timezone.utc)
    return RecoveryApproval(
        approved_by=approved_by,
        approved_at=ts,
        approval_reason=approval_reason,
        publication_operation_id=op_uuid,
        source_import_run_id=source_artifact.source_import_run_id,
        source_artifact_manifest_sha256=source_artifact.manifest_sha256,
        source_artifact_content_sha256=source_artifact.content_sha256,
        expected_active_run_id=expected_active_run_id,
    )


def validate_recovery_approval(
    approval: RecoveryApproval | None,
    *,
    expected_publication_operation_id: uuid.UUID,
    source_artifact: RecoveryArtifactRecord | None = None,
) -> RecoveryApproval:
    """Fail-closed validation of manual recovery approval structure and cryptographic bindings."""
    if approval is None:
        raise RecoveryApprovalError(
            "Controlled recovery requires explicit traceable manual RecoveryApproval."
        )
    if not isinstance(approval, RecoveryApproval):
        raise RecoveryApprovalError(
            f"approval must be a RecoveryApproval instance, got {type(approval).__name__}."
        )
    if not isinstance(approval.approved_by, str) or not approval.approved_by.strip():
        raise RecoveryApprovalError("RecoveryApproval.approved_by must be a non-blank string.")
    if not isinstance(approval.approval_reason, str) or not approval.approval_reason.strip():
        raise RecoveryApprovalError("RecoveryApproval.approval_reason must be a non-blank string.")
    if (
        not isinstance(approval.approved_at, datetime)
        or approval.approved_at.tzinfo is None
        or approval.approved_at.utcoffset() is None
    ):
        raise RecoveryApprovalError(
            "RecoveryApproval.approved_at must be a timezone-aware datetime."
        )
    try:
        approval_op = parse_publication_operation_id(approval.publication_operation_id)
    except Exception as exc:
        raise RecoveryApprovalError(
            f"RecoveryApproval.publication_operation_id is invalid: {exc}"
        ) from exc
    if approval_op != expected_publication_operation_id:
        raise RecoveryApprovalError(
            f"RecoveryApproval.publication_operation_id ({approval_op}) does not match "
            f"requested recovery operation ({expected_publication_operation_id})."
        )
    if (
        isinstance(approval.source_import_run_id, bool)
        or not isinstance(approval.source_import_run_id, int)
        or approval.source_import_run_id <= 0
    ):
        raise RecoveryApprovalError(
            f"RecoveryApproval.source_import_run_id must be a positive int, got "
            f"{approval.source_import_run_id!r}."
        )
    if (
        not isinstance(approval.source_artifact_manifest_sha256, str)
        or _SHA256_RE.fullmatch(approval.source_artifact_manifest_sha256) is None
    ):
        raise RecoveryApprovalError(
            "RecoveryApproval.source_artifact_manifest_sha256 must be a valid 64-hex SHA-256."
        )
    if (
        not isinstance(approval.source_artifact_content_sha256, str)
        or _SHA256_RE.fullmatch(approval.source_artifact_content_sha256) is None
    ):
        raise RecoveryApprovalError(
            "RecoveryApproval.source_artifact_content_sha256 must be a valid 64-hex SHA-256."
        )
    if approval.expected_active_run_id is not None and (
        isinstance(approval.expected_active_run_id, bool)
        or not isinstance(approval.expected_active_run_id, int)
        or approval.expected_active_run_id <= 0
    ):
        raise RecoveryApprovalError(
            f"RecoveryApproval.expected_active_run_id must be a positive int or None, got "
            f"{approval.expected_active_run_id!r}."
        )

    if source_artifact is not None:
        if approval.source_import_run_id != source_artifact.source_import_run_id:
            raise RecoveryApprovalError(
                f"RecoveryApproval.source_import_run_id ({approval.source_import_run_id}) does "
                f"not match verified source artifact run id={source_artifact.source_import_run_id}."
            )
        if approval.source_artifact_manifest_sha256 != source_artifact.manifest_sha256:
            raise RecoveryApprovalError(
                "RecoveryApproval.source_artifact_manifest_sha256 does not match verified "
                "source artifact manifest_sha256."
            )
        if approval.source_artifact_content_sha256 != source_artifact.content_sha256:
            raise RecoveryApprovalError(
                "RecoveryApproval.source_artifact_content_sha256 does not match verified "
                "source artifact content_sha256."
            )

    return approval


@contextmanager
def _isolated_artifact_copy(source_dir: Path) -> Iterator[Path]:
    """Copy source artifact files read-only into an isolated temp directory without modifying source."""
    if not source_dir.exists() or not source_dir.is_dir():
        raise RecoveryArtifactError(
            f"Recovery source artifact directory does not exist: '{source_dir}'."
        )
    for fname in _REQUIRED_ARTIFACT_FILES:
        src_file = source_dir / fname
        if not src_file.is_file():
            raise RecoveryArtifactError(
                f"Recovery artifact is missing required file '{fname}' in '{source_dir}'."
            )

    with tempfile.TemporaryDirectory(prefix="geosearch_recovery_src_") as tmp_name:
        isolated = Path(tmp_name)
        for fname in _REQUIRED_ARTIFACT_FILES:
            shutil.copyfile(source_dir / fname, isolated / fname)
        yield isolated


def _verify_source_dir_unchanged(
    source_dir: Path,
    *,
    expected_manifest_sha256: str,
    expected_places_sha256: str,
    expected_duplicates_sha256: str,
) -> None:
    """Confirm original source artifact files on disk still match verified snapshot hashes."""
    if not source_dir.exists() or not source_dir.is_dir():
        raise RecoveryArtifactError(
            f"Source recovery artifact directory '{source_dir}' disappeared between "
            "verification and staging."
        )
    for fname in _REQUIRED_ARTIFACT_FILES:
        if not (source_dir / fname).is_file():
            raise RecoveryArtifactError(
                f"Source recovery artifact file '{fname}' disappeared from '{source_dir}' "
                "between verification and staging."
            )

    checksum_raw = (source_dir / MANIFEST_CHECKSUM_FILENAME).read_text(encoding="utf-8").strip()
    if not checksum_raw or checksum_raw.split()[0] != expected_manifest_sha256:
        raise RecoveryArtifactError(
            "Source recovery artifact manifest.sha256 changed on disk between verification "
            "and staging."
        )
    if _sha256_file(source_dir / MANIFEST_FILENAME) != expected_manifest_sha256:
        raise RecoveryArtifactError(
            "Source recovery artifact manifest.json changed on disk between verification "
            "and staging."
        )
    if _sha256_file(source_dir / PLACES_SNAPSHOT_FILENAME) != expected_places_sha256:
        raise RecoveryArtifactError(
            "Source recovery artifact places_snapshot.jsonl changed on disk between "
            "verification and staging."
        )
    if _sha256_file(source_dir / DUPLICATES_SNAPSHOT_FILENAME) != expected_duplicates_sha256:
        raise RecoveryArtifactError(
            "Source recovery artifact duplicate_candidates_snapshot.jsonl changed on disk "
            "between verification and staging."
        )


def _rebind_record_paths(record: RecoveryArtifactRecord, source_dir: Path) -> RecoveryArtifactRecord:
    """Point a verified RecoveryArtifactRecord's paths back to the canonical source directory."""
    return RecoveryArtifactRecord(
        artifact_dir=source_dir,
        manifest_path=source_dir / MANIFEST_FILENAME,
        manifest_checksum_path=source_dir / MANIFEST_CHECKSUM_FILENAME,
        places_snapshot_path=source_dir / PLACES_SNAPSHOT_FILENAME,
        duplicate_candidates_snapshot_path=source_dir / DUPLICATES_SNAPSHOT_FILENAME,
        artifact_format=record.artifact_format,
        source_import_run_id=record.source_import_run_id,
        source_publication_operation_id=record.source_publication_operation_id,
        source_boundary_version_id=record.source_boundary_version_id,
        source_boundary_checksum_sha256=record.source_boundary_checksum_sha256,
        active_place_count=record.active_place_count,
        duplicate_candidate_count=record.duplicate_candidate_count,
        active_osm_identities_sha256=record.active_osm_identities_sha256,
        content_sha256=record.content_sha256,
        manifest_sha256=record.manifest_sha256,
        manifest=record.manifest,
    )


def _parse_verified_place_records(
    places_snapshot_path: Path,
    *,
    extraction_scope: str,
) -> tuple[RecoveryArtifactPlaceRecord, ...]:
    """Parse already-checksummed places_snapshot.jsonl and verify M2 canonical invariants."""
    parsed_places: list[RecoveryArtifactPlaceRecord] = []
    with places_snapshot_path.open("rb") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            rec = json.loads(raw_line.decode("utf-8"))
            osm_type = str(rec["osm_type"])
            if extraction_scope == EXTRACTION_SCOPE_NODE_ONLY and osm_type != "node":
                raise RecoveryArtifactError(
                    f"Line {line_no}: non-node place ({osm_type!r}) found in 'node_only' "
                    "recovery artifact."
                )

            name = rec["name"]
            name_search = rec["name_search"]
            if name is not None:
                expected_ns = normalize_name_search(name)
                if expected_ns != name_search:
                    raise RecoveryArtifactError(
                        f"Line {line_no}: name_search {name_search!r} does not match canonical "
                        f"M2 normalization {expected_ns!r} for name {name!r}."
                    )

            osm_version = rec["osm_version"]
            if osm_version is not None:
                if isinstance(osm_version, bool) or not isinstance(osm_version, int) or osm_version <= 0:
                    raise RecoveryArtifactError(
                        f"Line {line_no}: invalid osm_version {osm_version!r}."
                    )

            osm_changeset = rec["osm_changeset"]
            if osm_changeset is not None:
                if (
                    isinstance(osm_changeset, bool)
                    or not isinstance(osm_changeset, int)
                    or osm_changeset <= 0
                ):
                    raise RecoveryArtifactError(
                        f"Line {line_no}: invalid osm_changeset {osm_changeset!r}."
                    )

            osm_ts = _parse_iso_datetime(
                rec["osm_timestamp"], field_name="osm_timestamp", line_no=line_no
            )
            created_at = _parse_iso_datetime(
                rec["created_at"], field_name="created_at", line_no=line_no
            )
            updated_at = _parse_iso_datetime(
                rec["updated_at"], field_name="updated_at", line_no=line_no
            )

            parsed_places.append(
                RecoveryArtifactPlaceRecord(
                    place_id=int(rec["place_id"]),
                    osm_type=osm_type,
                    osm_id=int(rec["osm_id"]),
                    name=name,
                    name_search=name_search,
                    category=str(rec["category"]),
                    address=rec["address"],
                    longitude=float(rec["longitude"]),
                    latitude=float(rec["latitude"]),
                    location_srid=int(rec["location_srid"]),
                    location_ewkb_hex=str(rec["location_ewkb_hex"]),
                    raw_tags=dict(rec["raw_tags"]),
                    last_seen_import_id=int(rec["last_seen_import_id"]),
                    is_active=bool(rec["is_active"]),
                    osm_version=int(osm_version) if osm_version is not None else None,
                    osm_changeset=int(osm_changeset) if osm_changeset is not None else None,
                    osm_timestamp=osm_ts,
                    created_at=created_at,
                    updated_at=updated_at,
                )
            )
    return tuple(parsed_places)


def _verify_historical_artifact_from_dir(
    conn: Connection,
    *,
    working_dir: Path,
    canonical_source_dir: Path,
    publication_mode: PublicationMode,
) -> VerifiedHistoricalRecoveryArtifact:
    """Verify a historical source artifact against disk digests, PostGIS, and DB provenance."""
    raw_record = verify_recovery_artifact(working_dir)
    if raw_record.active_place_count <= 0:
        raise RecoveryArtifactError(
            "Historical recovery artifact contains 0 active places and cannot be restored."
        )

    manifest = raw_record.manifest
    src_pub = manifest["source_publication"]
    bv_meta = manifest["boundary_version"]
    files_meta = manifest["files"]

    if str(src_pub.get("status")) != "PUBLISHED":
        raise RecoveryArtifactError(
            f"Recovery artifact source_publication.status must be 'PUBLISHED', "
            f"got {src_pub.get('status')!r}."
        )

    src_pub_ev = (
        dict(src_pub.get("publication_evidence"))
        if isinstance(src_pub.get("publication_evidence"), dict)
        else {}
    )
    source_extraction_scope = (
        src_pub.get("extraction_scope") or src_pub_ev.get("extraction_scope")
    )
    if not isinstance(source_extraction_scope, str) or not source_extraction_scope.strip():
        raise RecoveryArtifactError(
            "Recovery artifact manifest is missing required source_publication.extraction_scope."
        )
    source_extraction_scope = source_extraction_scope.strip()

    source_coverage_restriction = src_pub_ev.get("coverage_restriction")
    if source_coverage_restriction is not None:
        source_coverage_restriction = str(source_coverage_restriction)
    elif source_extraction_scope == EXTRACTION_SCOPE_NODE_ONLY:
        source_coverage_restriction = NODE_ONLY_COVERAGE_RESTRICTION

    source_publication_mode = (
        str(src_pub.get("publication_mode"))
        if src_pub.get("publication_mode") is not None
        else (
            str(src_pub_ev.get("publication_mode"))
            if src_pub_ev.get("publication_mode") is not None
            else None
        )
    )

    # 1. Verify historical source import_runs provenance in PostgreSQL (may be SUPERSEDED or PUBLISHED).
    db_run = conn.execute(
        text(
            """
            SELECT
                id,
                boundary_version_id,
                run_type,
                status,
                publication_operation_id,
                published_at,
                source_name,
                source_checksum_sha256,
                source_fingerprint,
                pipeline_version,
                normalizer_version,
                config_digest
            FROM import_runs
            WHERE id = :run_id
            """
        ),
        {"run_id": raw_record.source_import_run_id},
    ).one_or_none()
    if db_run is None:
        raise RecoveryArtifactError(
            f"Historical source ImportRun id={raw_record.source_import_run_id} referenced by "
            "recovery artifact does not exist in database."
        )
    if db_run.status not in ("PUBLISHED", "SUPERSEDED") or db_run.published_at is None:
        raise RecoveryArtifactError(
            f"Historical source ImportRun id={db_run.id} is not a committed publication "
            f"(status={db_run.status!r}, published_at={db_run.published_at!r})."
        )
    if db_run.publication_operation_id != raw_record.source_publication_operation_id:
        raise RecoveryArtifactError(
            "Recovery artifact source_publication.publication_operation_id does not match "
            f"historical ImportRun id={db_run.id}."
        )
    if int(db_run.boundary_version_id) != raw_record.source_boundary_version_id:
        raise RecoveryArtifactError(
            "Recovery artifact boundary_version.id does not match historical "
            f"ImportRun id={db_run.id}."
        )
    for field_name, db_val, manifest_val in (
        ("run_type", str(db_run.run_type), str(src_pub.get("run_type"))),
        ("source_name", str(db_run.source_name), str(src_pub.get("source_name"))),
        (
            "source_checksum_sha256",
            str(db_run.source_checksum_sha256),
            str(src_pub.get("source_checksum_sha256")),
        ),
        (
            "source_fingerprint",
            str(db_run.source_fingerprint),
            str(src_pub.get("source_fingerprint")),
        ),
        ("pipeline_version", str(db_run.pipeline_version), str(src_pub.get("pipeline_version"))),
        (
            "normalizer_version",
            str(db_run.normalizer_version),
            str(src_pub.get("normalizer_version")),
        ),
        ("config_digest", str(db_run.config_digest), str(src_pub.get("config_digest"))),
        ("published_at", _iso_utc(db_run.published_at), src_pub.get("published_at")),
    ):
        if db_val != manifest_val:
            raise RecoveryArtifactError(
                f"Recovery artifact historical provenance mismatch on source_publication."
                f"{field_name} (db={db_val!r}, artifact={manifest_val!r})."
            )

    # 2. Verify historical source boundary_versions record, approval status, and EWKB geometry digest.
    db_bv = conn.execute(
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
                ST_SRID(geom) AS geom_srid,
                ST_AsEWKB(geom) AS geom_ewkb
            FROM boundary_versions
            WHERE id = :bv_id
            """
        ),
        {"bv_id": raw_record.source_boundary_version_id},
    ).one_or_none()
    if db_bv is None:
        raise RecoveryArtifactError(
            f"Source BoundaryVersion id={raw_record.source_boundary_version_id} does not exist."
        )
    if (
        str(db_bv.approval_status) != "APPROVED"
        or not db_bv.validated_by
        or db_bv.validated_at is None
        or not isinstance(db_bv.validation_evidence, dict)
        or not db_bv.approved_by
        or db_bv.approved_at is None
        or db_bv.approved_at < db_bv.validated_at
    ):
        raise PublicationEligibilityError(
            f"Source BoundaryVersion id={db_bv.id} is not APPROVED with complete provenance "
            f"(approval_status={db_bv.approval_status!r}).",
            status=EligibilityStatus.BOUNDARY_NOT_APPROVED,
            reasons=(
                f"BoundaryVersion id={db_bv.id} approval_status={db_bv.approval_status!r} "
                "is not eligible for recovery publication.",
            ),
        )
    if not bool(db_bv.geom_valid) or not bool(db_bv.geom_non_empty) or int(db_bv.geom_srid) != 4326:
        raise RecoveryArtifactError(
            f"Source BoundaryVersion id={db_bv.id} geometry is invalid, empty, or non-4326."
        )
    if str(db_bv.version_code) != str(bv_meta.get("version_code")):
        raise RecoveryArtifactError(
            "Recovery artifact boundary version_code does not match database boundary_versions."
        )
    if str(db_bv.source_name) != str(bv_meta.get("source_name")):
        raise RecoveryArtifactError(
            "Recovery artifact boundary source_name does not match database boundary_versions."
        )
    if str(db_bv.checksum_sha256) != raw_record.source_boundary_checksum_sha256:
        raise RecoveryArtifactError(
            "Recovery artifact boundary checksum_sha256 does not match database boundary_versions."
        )
    db_geom_sha = _sha256_bytes(bytes(db_bv.geom_ewkb))
    if db_geom_sha != str(bv_meta.get("geom_ewkb_sha256")):
        raise RecoveryArtifactError(
            "Recovery artifact boundary geom_ewkb_sha256 does not match database PostGIS geometry."
        )

    # 3. Enforce publication mode and coverage/boundary authorization guards fail-closed.
    if publication_mode == PublicationMode.AUTHORITATIVE:
        if (
            source_extraction_scope == EXTRACTION_SCOPE_NODE_ONLY
            or source_coverage_restriction == NODE_ONLY_COVERAGE_RESTRICTION
        ):
            reason = (
                "Refusing AUTHORITATIVE recovery from incomplete 'node_only' artifact "
                f"(extraction_scope={source_extraction_scope!r}, "
                f"coverage_restriction={source_coverage_restriction!r})."
            )
            raise PublicationEligibilityError(
                reason,
                status=EligibilityStatus.COVERAGE_INCOMPATIBLE,
                reasons=(reason,),
            )
        auth_ok, auth_reasons = _is_authoritative_boundary_evidence(db_bv.validation_evidence)
        if not auth_ok:
            raise PublicationEligibilityError(
                "Refusing AUTHORITATIVE recovery for non-authoritative boundary: "
                + "; ".join(auth_reasons),
                status=EligibilityStatus.BOUNDARY_NOT_APPROVED,
                reasons=tuple(auth_reasons),
            )
    elif publication_mode == PublicationMode.CONTROLLED_TEST_NODE_ONLY:
        try:
            verify_controlled_test_database_isolation(conn)
        except UnsafeDatabaseConfigError as exc:
            reason = (
                "Controlled-test Node-only recovery is forbidden against an unauthorized "
                f"database/environment: {exc}"
            )
            raise PublicationEligibilityError(
                reason,
                status=EligibilityStatus.COVERAGE_INCOMPATIBLE,
                reasons=(reason,),
            ) from exc
        if source_extraction_scope != EXTRACTION_SCOPE_NODE_ONLY:
            reason = (
                f"CONTROLLED_TEST_NODE_ONLY recovery requires extraction_scope='node_only', "
                f"found {source_extraction_scope!r}."
            )
            raise PublicationEligibilityError(
                reason,
                status=EligibilityStatus.COVERAGE_INCOMPATIBLE,
                reasons=(reason,),
            )
    else:
        raise PublicationEligibilityError(
            f"Unsupported publication_mode: {publication_mode!r}.",
            status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
            reasons=(f"Unsupported publication_mode: {publication_mode!r}.",),
        )

    if source_extraction_scope != EXTRACTION_SCOPE_NODE_ONLY:
        raise PublicationEligibilityError(
            f"Current staging schema supports extraction_scope='node_only'; "
            f"artifact extraction_scope={source_extraction_scope!r} is not yet enabled.",
            status=EligibilityStatus.COVERAGE_INCOMPATIBLE,
            reasons=(
                f"Unsupported staging extraction_scope={source_extraction_scope!r} in current milestone.",
            ),
        )

    # 4. Parse place records & verify M2 normalization + existing stable places.id + PostGIS EWKB/ST_Covers.
    places = _parse_verified_place_records(
        raw_record.places_snapshot_path,
        extraction_scope=source_extraction_scope,
    )

    osm_types = [p.osm_type for p in places]
    osm_ids = [p.osm_id for p in places]
    existing_rows = conn.execute(
        text(
            """
            WITH req AS (
                SELECT osm_type, osm_id
                FROM unnest(
                    CAST(:osm_types AS text[]),
                    CAST(:osm_ids AS bigint[])
                ) AS t(osm_type, osm_id)
            )
            SELECT p.osm_type, p.osm_id, p.id
            FROM places p
            JOIN req r ON r.osm_type = p.osm_type AND r.osm_id = p.osm_id
            """
        ),
        {"osm_types": osm_types, "osm_ids": osm_ids},
    ).fetchall()
    existing_id_map = {(str(r.osm_type), int(r.osm_id)): int(r.id) for r in existing_rows}

    for p in places:
        existing_pid = existing_id_map.get((p.osm_type, p.osm_id))
        if existing_pid is not None and existing_pid != p.place_id:
            raise RecoveryArtifactError(
                f"Recovery artifact stable places.id mismatch for ({p.osm_type}, {p.osm_id}): "
                f"artifact has place_id={p.place_id}, database has places.id={existing_pid}."
            )

    # Validate EWKB point geometry, coordinate parity, and ST_Covers against source boundary.
    lons = [p.longitude for p in places]
    lats = [p.latitude for p in places]
    ewkbs = [p.location_ewkb_hex for p in places]
    geo_check_rows = conn.execute(
        text(
            """
            WITH pts AS (
                SELECT
                    t.osm_type,
                    t.osm_id,
                    t.lon,
                    t.lat,
                    ST_GeomFromEWKB(decode(t.ewkb_hex, 'hex')) AS g
                FROM unnest(
                    CAST(:osm_types AS text[]),
                    CAST(:osm_ids AS bigint[]),
                    CAST(:lons AS double precision[]),
                    CAST(:lats AS double precision[]),
                    CAST(:ewkbs AS text[])
                ) AS t(osm_type, osm_id, lon, lat, ewkb_hex)
            )
            SELECT
                pts.osm_type,
                pts.osm_id,
                GeometryType(pts.g) AS geom_type,
                ST_SRID(pts.g) AS geom_srid,
                ST_X(pts.g) AS gx,
                ST_Y(pts.g) AS gy,
                pts.lon,
                pts.lat,
                ST_Covers(b.geom, pts.g) AS covered
            FROM pts
            JOIN boundary_versions b ON b.id = :bv_id
            WHERE GeometryType(pts.g) <> 'POINT'
               OR ST_SRID(pts.g) <> 4326
               OR ST_X(pts.g) IS DISTINCT FROM pts.lon
               OR ST_Y(pts.g) IS DISTINCT FROM pts.lat
               OR NOT ST_Covers(b.geom, pts.g)
            LIMIT 5
            """
        ),
        {
            "osm_types": osm_types,
            "osm_ids": osm_ids,
            "lons": lons,
            "lats": lats,
            "ewkbs": ewkbs,
            "bv_id": raw_record.source_boundary_version_id,
        },
    ).fetchall()
    if geo_check_rows:
        bad = geo_check_rows[0]
        raise RecoveryArtifactError(
            f"Recovery artifact geometry/boundary check failed for ({bad.osm_type}, {bad.osm_id}): "
            f"geom_type={bad.geom_type!r}, srid={bad.geom_srid}, "
            f"ewkb_xy=({bad.gx}, {bad.gy}), json_xy=({bad.lon}, {bad.lat}), covered={bad.covered}."
        )

    rebound_record = _rebind_record_paths(raw_record, canonical_source_dir)
    return VerifiedHistoricalRecoveryArtifact(
        record=rebound_record,
        places=places,
        source_extraction_scope=source_extraction_scope,
        source_coverage_restriction=source_coverage_restriction,
        source_publication_mode=source_publication_mode,
        source_run_status_in_db=str(db_run.status),
        source_boundary_version_code=str(db_bv.version_code),
        manifest_sha256=rebound_record.manifest_sha256,
        places_file_sha256=str(files_meta[PLACES_SNAPSHOT_FILENAME]["sha256"]),
        duplicates_file_sha256=str(files_meta[DUPLICATES_SNAPSHOT_FILENAME]["sha256"]),
    )


def verify_historical_source_artifact(
    conn: Connection,
    *,
    artifact_dir: str | Path,
    publication_mode: PublicationMode = PublicationMode.AUTHORITATIVE,
) -> VerifiedHistoricalRecoveryArtifact:
    """Verify a historical source artifact on disk and against PostgreSQL provenance (read-only)."""
    source_path = Path(artifact_dir)
    with _isolated_artifact_copy(source_path) as isolated_dir:
        verified = _verify_historical_artifact_from_dir(
            conn,
            working_dir=isolated_dir,
            canonical_source_dir=source_path,
            publication_mode=publication_mode,
        )
        _verify_source_dir_unchanged(
            source_path,
            expected_manifest_sha256=verified.manifest_sha256,
            expected_places_sha256=verified.places_file_sha256,
            expected_duplicates_sha256=verified.duplicates_file_sha256,
        )
        return verified


def _build_recovery_evidence(
    verified: VerifiedHistoricalRecoveryArtifact,
    approval: RecoveryApproval,
) -> dict[str, Any]:
    src_pub = verified.record.manifest["source_publication"]
    return {
        "source_artifact": verified.record.to_evidence_dict(),
        "source_publication": {
            "import_run_id": verified.record.source_import_run_id,
            "run_type": str(src_pub["run_type"]),
            "publication_operation_id": str(verified.record.source_publication_operation_id),
            "published_at": src_pub.get("published_at"),
            "source_name": str(src_pub["source_name"]),
            "source_checksum_sha256": str(src_pub["source_checksum_sha256"]),
            "source_fingerprint": str(src_pub["source_fingerprint"]),
            "pipeline_version": str(src_pub["pipeline_version"]),
            "normalizer_version": str(src_pub["normalizer_version"]),
            "config_digest": str(src_pub["config_digest"]),
        },
        "approval": approval.to_evidence_dict(),
        "expected_active_run_id": approval.expected_active_run_id,
    }


def _artifact_crypto_identity(artifact_meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "artifact_format": artifact_meta.get("artifact_format"),
        "source_import_run_id": artifact_meta.get("source_import_run_id"),
        "source_publication_operation_id": artifact_meta.get("source_publication_operation_id"),
        "source_boundary_version_id": artifact_meta.get("source_boundary_version_id"),
        "source_boundary_checksum_sha256": artifact_meta.get("source_boundary_checksum_sha256"),
        "active_place_count": artifact_meta.get("active_place_count"),
        "active_osm_identities_sha256": artifact_meta.get("active_osm_identities_sha256"),
        "content_sha256": artifact_meta.get("content_sha256"),
        "manifest_sha256": artifact_meta.get("manifest_sha256"),
    }


def _assert_recovery_retry_matches_existing(
    conn: Connection,
    *,
    existing_run_id: int,
    op_uuid: uuid.UUID,
    verified: VerifiedHistoricalRecoveryArtifact,
    approval: RecoveryApproval,
) -> tuple[dict[str, Any], int, str]:
    """Ensure a retry on an existing operation UUID matches the original artifact and approval."""
    row = conn.execute(
        text(
            """
            SELECT
                r.id,
                r.run_type,
                r.status,
                r.boundary_version_id,
                r.publication_evidence,
                cp.staged_candidate_count,
                cp.readiness_evidence
            FROM import_runs r
            LEFT JOIN staging_run_checkpoints cp ON cp.import_run_id = r.id
            WHERE r.id = :run_id
            """
        ),
        {"run_id": existing_run_id},
    ).one()

    if str(row.run_type) != "RECOVERY":
        reason = (
            f"publication_operation_id={op_uuid} belongs to non-RECOVERY ImportRun "
            f"id={row.id} (run_type={row.run_type!r}); cannot repurpose for recovery."
        )
        raise PublicationConflictError(
            reason,
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            reasons=(reason,),
        )

    recorded_recovery: dict[str, Any] | None = None
    if isinstance(row.readiness_evidence, dict) and isinstance(
        row.readiness_evidence.get("recovery"), dict
    ):
        recorded_recovery = dict(row.readiness_evidence["recovery"])
    elif isinstance(row.publication_evidence, dict) and isinstance(
        row.publication_evidence.get("recovery"), dict
    ):
        recorded_recovery = dict(row.publication_evidence["recovery"])

    if recorded_recovery is None:
        reason = (
            f"Existing RECOVERY ImportRun id={row.id} for publication_operation_id={op_uuid} "
            "lacks durable recovery provenance metadata."
        )
        raise PublicationConflictError(
            reason,
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            reasons=(reason,),
        )

    incoming_recovery = _build_recovery_evidence(verified, approval)
    existing_art = recorded_recovery.get("source_artifact")
    existing_approval = recorded_recovery.get("approval")
    if (
        not isinstance(existing_art, dict)
        or not isinstance(existing_approval, dict)
        or _artifact_crypto_identity(existing_art)
        != _artifact_crypto_identity(incoming_recovery["source_artifact"])
        or existing_approval != incoming_recovery["approval"]
        or int(row.boundary_version_id) != verified.record.source_boundary_version_id
    ):
        reason = (
            f"Refusing to repurpose recovery publication_operation_id={op_uuid} "
            f"(ImportRun id={row.id}): source artifact or manual approval differs from the "
            "originally registered recovery operation."
        )
        raise PublicationConflictError(
            reason,
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            reasons=(reason,),
        )

    staged_count = (
        int(row.staged_candidate_count)
        if row.staged_candidate_count is not None
        else verified.record.active_place_count
    )
    return recorded_recovery, staged_count, str(row.status)


def prepare_recovery_run(
    engine: Engine,
    *,
    source_artifact_dir: str | Path,
    publication_operation_id: uuid.UUID | str,
    approval: RecoveryApproval | None,
    publication_mode: PublicationMode = PublicationMode.AUTHORITATIVE,
    fault_hooks: RecoveryFaultHooks | None = None,
) -> PreparedRecoveryRun:
    """Verify source artifact + approval and atomically prepare (or resume) a RECOVERY run.

    Guarantees:
    - Does NOT modify live `places` or active-slot metadata.
    - Rejects missing/corrupt/mismatched artifacts, ineligible boundaries, or missing/mismatched
      manual approval before creating any `import_runs` row.
    - Uses an isolated copy of the verified artifact so staging consumes the exact verified
      bytes, preserves the source artifact untouched, and rejects on-disk tampering between
      verification and staging.
    - Binds `publication_operation_id` atomically to the new `RECOVERY` run; retries with the
      exact same UUID, artifact, and approval reuse the existing `PREPARED` (or already
      committed) run without creating a duplicate run or permitting UUID repurposing.
    """
    hooks = fault_hooks or RecoveryFaultHooks()
    op_uuid = parse_publication_operation_id(publication_operation_id)
    validated_approval = validate_recovery_approval(
        approval,
        expected_publication_operation_id=op_uuid,
    )
    source_path = Path(source_artifact_dir)

    with _isolated_artifact_copy(source_path) as isolated_dir:
        with engine.connect() as conn:
            verified = _verify_historical_artifact_from_dir(
                conn,
                working_dir=isolated_dir,
                canonical_source_dir=source_path,
                publication_mode=publication_mode,
            )
            validate_recovery_approval(
                validated_approval,
                expected_publication_operation_id=op_uuid,
                source_artifact=verified.record,
            )

            existing_op = inspect_publication_operation(conn, publication_operation_id=op_uuid)
            if existing_op is not None:
                recorded_recovery, staged_count, existing_status = (
                    _assert_recovery_retry_matches_existing(
                        conn,
                        existing_run_id=existing_op.import_run_id,
                        op_uuid=op_uuid,
                        verified=verified,
                        approval=validated_approval,
                    )
                )
                if existing_op.is_committed_publication:
                    return PreparedRecoveryRun(
                        import_run_id=existing_op.import_run_id,
                        publication_operation_id=op_uuid,
                        source_artifact=verified.record,
                        approval=validated_approval,
                        expected_active_run_id=validated_approval.expected_active_run_id,
                        boundary_version_id=existing_op.boundary_version_id,
                        staged_place_count=staged_count,
                        recovery_evidence=recorded_recovery,
                        reused_existing_run=True,
                        already_committed=True,
                    )
                if existing_status != "PREPARED":
                    reason = (
                        f"Existing RECOVERY ImportRun id={existing_op.import_run_id} for "
                        f"publication_operation_id={op_uuid} is in non-resumable status "
                        f"{existing_status!r}."
                    )
                    raise PublicationConflictError(
                        reason,
                        status=EligibilityStatus.PUBLICATION_CONFLICT,
                        reasons=(reason,),
                    )
                # Ensure source artifact has not been tampered before returning resumable run.
                if hooks.on_after_source_verification is not None:
                    hooks.on_after_source_verification(source_path)
                _verify_source_dir_unchanged(
                    source_path,
                    expected_manifest_sha256=verified.manifest_sha256,
                    expected_places_sha256=verified.places_file_sha256,
                    expected_duplicates_sha256=verified.duplicates_file_sha256,
                )
                return PreparedRecoveryRun(
                    import_run_id=existing_op.import_run_id,
                    publication_operation_id=op_uuid,
                    source_artifact=verified.record,
                    approval=validated_approval,
                    expected_active_run_id=validated_approval.expected_active_run_id,
                    boundary_version_id=existing_op.boundary_version_id,
                    staged_place_count=staged_count,
                    recovery_evidence=recorded_recovery,
                    reused_existing_run=True,
                    already_committed=False,
                )

            # Preflight expected active baseline check BEFORE creating any new RECOVERY run.
            current_active_id = conn.execute(
                text("SELECT id FROM import_runs WHERE active_slot = 1")
            ).scalar_one_or_none()
            current_active = int(current_active_id) if current_active_id is not None else None
            if current_active != validated_approval.expected_active_run_id:
                reason = (
                    f"Recovery approval active-run mismatch: approval expects active run "
                    f"{validated_approval.expected_active_run_id!r}, but current active run is "
                    f"{current_active!r}."
                )
                raise PublicationConflictError(
                    reason,
                    status=EligibilityStatus.PUBLICATION_CONFLICT,
                    reasons=(reason,),
                )

        # Deterministic TOCTOU test hook: fires AFTER verification, BEFORE staging.
        if hooks.on_after_source_verification is not None:
            hooks.on_after_source_verification(source_path)

        # Re-verify original source artifact on disk has not changed since isolated copy/verification.
        _verify_source_dir_unchanged(
            source_path,
            expected_manifest_sha256=verified.manifest_sha256,
            expected_places_sha256=verified.places_file_sha256,
            expected_duplicates_sha256=verified.duplicates_file_sha256,
        )

        # Build deterministic recovery provenance metadata.
        src_pub = verified.record.manifest["source_publication"]
        normalizer_version = str(src_pub["normalizer_version"])
        config_digest = _compute_recovery_config_digest(
            extraction_scope=verified.source_extraction_scope,
            artifact_format=verified.record.artifact_format,
        )
        source_fingerprint = _compute_recovery_source_fingerprint(
            source_artifact=verified.record,
            config_digest=config_digest,
            normalizer_version=normalizer_version,
        )
        artifact_id = str(
            verified.record.manifest.get("artifact_id") or verified.record.artifact_dir.name
        )
        source_name = f"recovery_artifact:{artifact_id}"
        source_checksum_sha256 = verified.record.content_sha256
        recovery_evidence = _build_recovery_evidence(verified, validated_approval)

        # Single atomic preparation transaction: create RECOVERY run -> stage verified places ->
        # verify ST_Covers & EWKB -> mark checkpoint VERIFIED & run PREPARED.
        try:
            with engine.begin() as conn:
                run_id = int(
                    conn.execute(
                        text(
                            """
                            INSERT INTO import_runs (
                                boundary_version_id,
                                run_type,
                                source_name,
                                source_checksum_sha256,
                                source_fingerprint,
                                pipeline_version,
                                normalizer_version,
                                config_digest,
                                status,
                                publication_operation_id
                            )
                            VALUES (
                                :boundary_version_id,
                                'RECOVERY',
                                :source_name,
                                :source_checksum_sha256,
                                :source_fingerprint,
                                :pipeline_version,
                                :normalizer_version,
                                :config_digest,
                                'PROCESSING',
                                :publication_operation_id
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "boundary_version_id": verified.record.source_boundary_version_id,
                            "source_name": source_name,
                            "source_checksum_sha256": source_checksum_sha256,
                            "source_fingerprint": source_fingerprint,
                            "pipeline_version": RECOVERY_PIPELINE_VERSION,
                            "normalizer_version": normalizer_version,
                            "config_digest": config_digest,
                            "publication_operation_id": op_uuid,
                        },
                    ).scalar_one()
                )

                conn.execute(
                    text(
                        """
                        INSERT INTO staging_run_checkpoints (
                            import_run_id,
                            extraction_scope,
                            staging_state,
                            source_fingerprint,
                            source_checksum_sha256
                        )
                        VALUES (
                            :run_id,
                            :extraction_scope,
                            'IN_PROGRESS',
                            :source_fingerprint,
                            :source_checksum_sha256
                        )
                        """
                    ),
                    {
                        "run_id": run_id,
                        "extraction_scope": verified.source_extraction_scope,
                        "source_fingerprint": source_fingerprint,
                        "source_checksum_sha256": source_checksum_sha256,
                    },
                )

                staged_count = 0
                last_osm_id: int | None = None
                for place in verified.places:
                    row_prov = {
                        "source_fingerprint": source_fingerprint,
                        "source_checksum_sha256": source_checksum_sha256,
                        "extraction_scope": verified.source_extraction_scope,
                        "recovery_source_import_run_id": verified.record.source_import_run_id,
                        "recovery_source_place_id": place.place_id,
                        "recovery_source_artifact_content_sha256": verified.record.content_sha256,
                    }
                    cand = CanonicalPlaceCandidate(
                        osm_type=place.osm_type,
                        osm_id=place.osm_id,
                        name=place.name,
                        name_search=place.name_search,
                        category=place.category,
                        address=place.address,
                        longitude=place.longitude,
                        latitude=place.latitude,
                        raw_tags=place.raw_tags,
                        osm_version=place.osm_version,
                        osm_changeset=place.osm_changeset,
                        osm_timestamp=place.osm_timestamp,
                        source_provenance=row_prov,
                    )
                    content_digest = compute_candidate_content_digest(cand)

                    conn.execute(
                        text(
                            """
                            INSERT INTO staging_place_candidates (
                                import_run_id,
                                osm_type,
                                osm_id,
                                name,
                                name_search,
                                category,
                                address,
                                location,
                                raw_tags,
                                osm_version,
                                osm_changeset,
                                osm_timestamp,
                                content_digest,
                                source_provenance,
                                last_seen_pass
                            )
                            VALUES (
                                :run_id,
                                :osm_type,
                                :osm_id,
                                :name,
                                :name_search,
                                :category,
                                :address,
                                CAST(ST_GeomFromEWKB(decode(:ewkb_hex, 'hex')) AS geography),
                                CAST(:raw_tags_json AS jsonb),
                                :osm_version,
                                :osm_changeset,
                                :osm_timestamp,
                                :content_digest,
                                CAST(:source_provenance_json AS jsonb),
                                1
                            )
                            """
                        ),
                        {
                            "run_id": run_id,
                            "osm_type": place.osm_type,
                            "osm_id": place.osm_id,
                            "name": place.name,
                            "name_search": place.name_search,
                            "category": place.category,
                            "address": place.address,
                            "ewkb_hex": place.location_ewkb_hex,
                            "raw_tags_json": json.dumps(
                                place.raw_tags, sort_keys=True, ensure_ascii=False
                            ),
                            "osm_version": place.osm_version,
                            "osm_changeset": place.osm_changeset,
                            "osm_timestamp": place.osm_timestamp,
                            "content_digest": content_digest,
                            "source_provenance_json": json.dumps(
                                row_prov, sort_keys=True, ensure_ascii=False
                            ),
                        },
                    )
                    staged_count += 1
                    last_osm_id = place.osm_id

                # Re-verify PostGIS ST_Covers for all staged recovery rows inside the prepare tx.
                uncovered = int(
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
                        {"run_id": run_id, "bv_id": verified.record.source_boundary_version_id},
                    ).scalar_one()
                )
                if uncovered != 0:
                    raise RecoveryArtifactError(
                        f"Staged recovery dataset has {uncovered} row(s) outside approved source "
                        f"BoundaryVersion id={verified.record.source_boundary_version_id}."
                    )

                # Confirm source artifact on disk remained unchanged through staging.
                _verify_source_dir_unchanged(
                    source_path,
                    expected_manifest_sha256=verified.manifest_sha256,
                    expected_places_sha256=verified.places_file_sha256,
                    expected_duplicates_sha256=verified.duplicates_file_sha256,
                )

                readiness_evidence: dict[str, Any] = {
                    "import_run_id": run_id,
                    "run_type": "RECOVERY",
                    "extraction_scope": verified.source_extraction_scope,
                    "coverage_restriction": verified.source_coverage_restriction,
                    "boundary_version_id": verified.record.source_boundary_version_id,
                    "boundary_version_code": verified.source_boundary_version_code,
                    "boundary_approval_status": "APPROVED",
                    "boundary_filter_verified": True,
                    "uncovered_staged_count": 0,
                    "stale_staged_count": 0,
                    "source_verified_post_parse": True,
                    "source_fingerprint": source_fingerprint,
                    "source_checksum_sha256": source_checksum_sha256,
                    "current_pass_number": 1,
                    "resume_count": 0,
                    "staged_candidate_count": staged_count,
                    "pass_inserted_count": staged_count,
                    "outside_boundary_count": 0,
                    "duplicate_identical_count": 0,
                    "pass_conflict_count": 0,
                    "conflict_count": 0,
                    "issue_count": 0,
                    "total_nodes_seen": staged_count,
                    "supported_nodes_seen": staged_count,
                    "unsupported_nodes_seen": 0,
                    "valid_candidates_emitted": staged_count,
                    "invalid_candidates_emitted": 0,
                    "batches_committed": 1,
                    "recovery": recovery_evidence,
                }

                prep_ts = conn.execute(text("SELECT now()")).scalar_one()
                conn.execute(
                    text(
                        """
                        UPDATE staging_run_checkpoints
                        SET staging_state = 'VERIFIED',
                            source_verified_post_parse = true,
                            current_pass_number = 1,
                            resume_count = 0,
                            batches_committed = 1,
                            total_nodes_seen = :staged_count,
                            supported_nodes_seen = :staged_count,
                            unsupported_nodes_seen = 0,
                            valid_candidates_emitted = :staged_count,
                            invalid_candidates_emitted = 0,
                            pass_inserted_count = :staged_count,
                            outside_boundary_count = 0,
                            duplicate_identical_count = 0,
                            pass_conflict_count = 0,
                            staged_candidate_count = :staged_count,
                            conflict_count = 0,
                            issue_count = 0,
                            last_osm_id = :last_osm_id,
                            extraction_completed_at = :prep_ts,
                            verified_at = :prep_ts,
                            readiness_evidence = CAST(:readiness_json AS jsonb),
                            updated_at = :prep_ts
                        WHERE import_run_id = :run_id
                        """
                    ),
                    {
                        "staged_count": staged_count,
                        "last_osm_id": last_osm_id,
                        "prep_ts": prep_ts,
                        "readiness_json": json.dumps(
                            readiness_evidence, sort_keys=True, ensure_ascii=False
                        ),
                        "run_id": run_id,
                    },
                )

                conn.execute(
                    text(
                        """
                        UPDATE import_runs
                        SET status = 'PREPARED',
                            completed_at = :prep_ts
                        WHERE id = :run_id
                          AND status = 'PROCESSING'
                        """
                    ),
                    {"prep_ts": prep_ts, "run_id": run_id},
                )

                if hooks.on_before_prepare_commit is not None:
                    hooks.on_before_prepare_commit(conn)

        except IntegrityError as exc:
            orig = exc.orig
            sqlstate = getattr(orig, "sqlstate", None)
            diag = getattr(orig, "diag", None)
            constraint_name = getattr(diag, "constraint_name", None)
            if sqlstate == "23505" and constraint_name == "uq_import_runs_publication_operation_id":
                # Another caller concurrently created a run for op_uuid; verify whether it matches.
                with engine.connect() as conn:
                    winner = inspect_publication_operation(conn, publication_operation_id=op_uuid)
                    if winner is None:
                        raise PublicationConflictError(
                            f"Concurrent conflict on publication_operation_id={op_uuid}.",
                            status=EligibilityStatus.PUBLICATION_CONFLICT,
                            reasons=(f"Concurrent conflict on {op_uuid}.",),
                        ) from exc
                    recorded_recovery, winner_staged_count, winner_status = (
                        _assert_recovery_retry_matches_existing(
                            conn,
                            existing_run_id=winner.import_run_id,
                            op_uuid=op_uuid,
                            verified=verified,
                            approval=validated_approval,
                        )
                    )
                    if winner.is_committed_publication or winner_status == "PREPARED":
                        return PreparedRecoveryRun(
                            import_run_id=winner.import_run_id,
                            publication_operation_id=op_uuid,
                            source_artifact=verified.record,
                            approval=validated_approval,
                            expected_active_run_id=validated_approval.expected_active_run_id,
                            boundary_version_id=winner.boundary_version_id,
                            staged_place_count=winner_staged_count,
                            recovery_evidence=recorded_recovery,
                            reused_existing_run=True,
                            already_committed=winner.is_committed_publication,
                        )
                    reason = (
                        f"Concurrent RECOVERY ImportRun id={winner.import_run_id} for "
                        f"publication_operation_id={op_uuid} is in status {winner_status!r}."
                    )
                    raise PublicationConflictError(
                        reason,
                        status=EligibilityStatus.PUBLICATION_CONFLICT,
                        reasons=(reason,),
                    ) from exc
            raise

        return PreparedRecoveryRun(
            import_run_id=run_id,
            publication_operation_id=op_uuid,
            source_artifact=verified.record,
            approval=validated_approval,
            expected_active_run_id=validated_approval.expected_active_run_id,
            boundary_version_id=verified.record.source_boundary_version_id,
            staged_place_count=staged_count,
            recovery_evidence=recovery_evidence,
            reused_existing_run=False,
            already_committed=False,
        )


def execute_controlled_recovery(
    engine: Engine,
    *,
    source_artifact_dir: str | Path,
    publication_operation_id: uuid.UUID | str,
    approval: RecoveryApproval | None,
    publication_mode: PublicationMode = PublicationMode.AUTHORITATIVE,
    protection_artifact_dir: str | Path | None = None,
    protection_artifact_root: str | Path | None = None,
    duplicate_candidates: DuplicateCandidateInput | None = None,
    upsert_batch_size: int = 500,
    fault_hooks: RecoveryFaultHooks | None = None,
) -> RecoveryResult:
    """Internal operator entry point for M5 controlled recovery from an immutable artifact.

    Explicitly separates the two artifact roles:
    - `source_artifact_dir`: Historical V1 artifact supplying the canonical dataset to restore
      (its source publication may be `SUPERSEDED`).
    - `protection_artifact_dir` / `protection_artifact_root`: Active V2 protection artifact
      capturing the currently active dataset before replacement using M3's protection protocol.

    Workflow:
    1. Verify historical source artifact, manual approval, and active baseline; atomically
       prepare (or idempotently resume) the `RECOVERY` run bound to `publication_operation_id`.
    2. If `publication_operation_id` was already committed (even if later `SUPERSEDED`), return
       `ALREADY_PUBLISHED` without republishing or mutating live state.
    3. Publish the prepared `RECOVERY` run through M3's guarded atomic `publish_prepared_run`,
       rechecking `expected_active_run_id` under `pg_advisory_xact_lock`, protecting active V2
       with the V2 protection artifact, preserving stable `places.id`, updating
       `last_seen_import_id` to the new recovery run, softly deactivating V2-only places, and
       recording durable recovery & approval evidence atomically at COMMIT.
    """
    hooks = fault_hooks or RecoveryFaultHooks()
    source_path = Path(source_artifact_dir)

    # Guard against accidentally passing the exact same artifact directory for both roles
    # when restoring a historical (non-active) source run.
    if (
        protection_artifact_dir is not None
        and approval is not None
        and approval.source_import_run_id != approval.expected_active_run_id
        and Path(protection_artifact_dir).resolve() == source_path.resolve()
    ):
        raise RecoveryArtifactError(
            "source_artifact_dir (historical V1 source) and protection_artifact_dir "
            "(active V2 baseline protection) must refer to distinct artifacts."
        )

    prepared = prepare_recovery_run(
        engine,
        source_artifact_dir=source_path,
        publication_operation_id=publication_operation_id,
        approval=approval,
        publication_mode=publication_mode,
        fault_hooks=hooks,
    )

    if prepared.already_committed:
        pub_result = publish_prepared_run(
            engine,
            import_run_id=prepared.import_run_id,
            publication_operation_id=prepared.publication_operation_id,
            publication_mode=publication_mode,
            expected_active_run_id=prepared.expected_active_run_id,
        )
        return RecoveryResult(
            publication_result=pub_result,
            source_artifact=prepared.source_artifact,
            protection_artifact=None,
            approval=prepared.approval,
            reused_prepared_run=True,
        )

    if hooks.on_after_prepare_before_publish is not None:
        hooks.on_after_prepare_before_publish(
            prepared.import_run_id,
            prepared.publication_operation_id,
        )

    dup_input = duplicate_candidates or DuplicateCandidateInput(
        state=DuplicateDetectionState.NOT_EXECUTED
    )

    pub_result = publish_prepared_run(
        engine,
        import_run_id=prepared.import_run_id,
        publication_operation_id=prepared.publication_operation_id,
        publication_mode=publication_mode,
        expected_active_run_id=prepared.expected_active_run_id,
        recovery_artifact_dir=protection_artifact_dir,
        recovery_artifact_root=protection_artifact_root,
        duplicate_candidates=dup_input,
        upsert_batch_size=upsert_batch_size,
        fault_hooks=hooks.publication_hooks,
        additional_publication_evidence={"recovery": prepared.recovery_evidence},
    )

    return RecoveryResult(
        publication_result=pub_result,
        source_artifact=prepared.source_artifact,
        protection_artifact=pub_result.recovery_artifact,
        approval=prepared.approval,
        reused_prepared_run=prepared.reused_existing_run,
    )
