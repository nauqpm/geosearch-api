"""M3.2 / M3.7 immutable pre-publication recovery artifact preparation and full verification.

Architecture contract:
- Captures a consistent snapshot of the previously active canonical dataset using a single
  PostgreSQL `REPEATABLE READ READ ONLY` transaction (never assembled from unrelated
  READ COMMITTED queries).
- Uses ONE shared canonical JSONL serialization format for both snapshot export and live
  baseline verification so content hashes cannot drift between writer and verifier.
- Verifies complete canonical place content (identity, stable places.id, name, name_search,
  category, address, PostGIS EWKB location + SRID 4326 + WGS84 coordinates, raw_tags,
  last_seen_import_id, OSM version/changeset/timestamp, audit timestamps), complete
  duplicate_candidates rows (canonical pair, detector_version, score, distance, evidence),
  boundary geometry EWKB + checksum, and source publication provenance.
- Writes to a temporary sibling directory, fsyncs writable descriptors, verifies all files
  and checksums, atomically renames to the immutable target directory without overwriting
  existing artifacts, and performs best-effort parent directory fsync on POSIX.
- Filesystem durability limitation: local `chmod 0o444` guards only against accidental local
  modification by unprivileged callers and does NOT provide production WORM / object-lock
  immutability (the owning OS user or administrator can change permissions or unlink files
  in a writable parent directory). Production deployments require dedicated immutable/WORM
  storage.
- Does NOT claim cross-system atomicity between PostgreSQL and external filesystem writes;
  instead, the publication transaction locks live baseline rows under `pg_advisory_xact_lock`
  and re-verifies full canonical content hashes against live PostgreSQL state before writing.
- Does NOT implement M5 controlled restore execution.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

from sqlalchemy import Connection, Engine, text

from geosearch.db.models import OSM_TYPES, PLACE_CATEGORIES
from geosearch.db.types import InvalidCoordinateError, validate_wgs84_coordinates
from geosearch.publication.models import (
    RECOVERY_ARTIFACT_FORMAT_VERSION,
    RecoveryArtifactError,
    RecoveryArtifactRecord,
)

PLACES_SNAPSHOT_FILENAME: Final[str] = "places_snapshot.jsonl"
DUPLICATES_SNAPSHOT_FILENAME: Final[str] = "duplicate_candidates_snapshot.jsonl"
MANIFEST_FILENAME: Final[str] = "manifest.json"
MANIFEST_CHECKSUM_FILENAME: Final[str] = "manifest.sha256"
_STREAM_FETCH_BATCH_SIZE: Final[int] = 1000
_SHA256_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")
_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]+$")


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total_bytes = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(65536)
            if not chunk:
                break
            digest.update(chunk)
            total_bytes += len(chunk)
    return digest.hexdigest(), total_bytes


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _compute_content_sha256(manifest_body: dict[str, Any]) -> str:
    """Compute canonical SHA-256 over all immutable provenance, membership, and file digests."""
    core = {
        "artifact_format": manifest_body["artifact_format"],
        "source_publication": manifest_body["source_publication"],
        "boundary_version": manifest_body["boundary_version"],
        "membership_summary": manifest_body["membership_summary"],
        "files": manifest_body["files"],
    }
    return _sha256_bytes(_canonical_json_bytes(core))


def _write_and_fsync_bytes(path: Path, data: bytes) -> None:
    """Write bytes and fsync on the writable file descriptor (cross-platform Windows/POSIX)."""
    with path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory_best_effort(directory: Path) -> None:
    """Best-effort parent directory fsync after atomic rename on POSIX filesystems."""
    o_directory = getattr(os, "O_DIRECTORY", None)
    if o_directory is None:
        return
    try:
        dir_fd = os.open(str(directory), os.O_RDONLY | o_directory)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


def _set_file_read_only(path: Path) -> None:
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH)
    except OSError:
        pass


def _active_places_snapshot_query(*, for_update: bool) -> str:
    lock_clause = "FOR UPDATE" if for_update else ""
    return f"""
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
            ST_SRID(location::geometry) AS location_srid,
            encode(ST_AsEWKB(location::geometry), 'hex') AS location_ewkb_hex,
            raw_tags,
            last_seen_import_id,
            is_active,
            osm_version,
            osm_changeset,
            osm_timestamp,
            created_at,
            updated_at
        FROM places
        WHERE is_active = true
        ORDER BY osm_type, osm_id
        {lock_clause}
    """


def _active_duplicates_snapshot_query(*, for_share: bool) -> str:
    lock_clause = "FOR SHARE" if for_share else ""
    return f"""
        SELECT
            id,
            import_run_id,
            place_a_id,
            place_b_id,
            detector_version,
            confidence_score,
            distance_meters,
            evidence,
            created_at
        FROM duplicate_candidates
        WHERE import_run_id = :run_id
        ORDER BY place_a_id, place_b_id
        {lock_clause}
    """


def _serialize_place_row(row: Any) -> tuple[dict[str, Any], bytes]:
    """Single canonical serializer shared by snapshot export and live baseline verification."""
    record: dict[str, Any] = {
        "place_id": int(row.id),
        "osm_type": str(row.osm_type),
        "osm_id": int(row.osm_id),
        "name": row.name,
        "name_search": row.name_search,
        "category": str(row.category),
        "address": row.address,
        "longitude": float(row.lon),
        "latitude": float(row.lat),
        "location_srid": int(row.location_srid),
        "location_ewkb_hex": str(row.location_ewkb_hex),
        "raw_tags": dict(row.raw_tags),
        "last_seen_import_id": int(row.last_seen_import_id),
        "is_active": bool(row.is_active),
        "osm_version": int(row.osm_version) if row.osm_version is not None else None,
        "osm_changeset": int(row.osm_changeset) if row.osm_changeset is not None else None,
        "osm_timestamp": _iso_utc(row.osm_timestamp),
        "created_at": _iso_utc(row.created_at),
        "updated_at": _iso_utc(row.updated_at),
    }
    return record, _canonical_json_bytes(record) + b"\n"


def _serialize_duplicate_row(row: Any) -> tuple[dict[str, Any], bytes]:
    """Single canonical serializer shared by duplicate export and live baseline verification."""
    record: dict[str, Any] = {
        "id": int(row.id),
        "import_run_id": int(row.import_run_id),
        "place_a_id": int(row.place_a_id),
        "place_b_id": int(row.place_b_id),
        "detector_version": str(row.detector_version),
        "confidence_score": (
            str(row.confidence_score) if row.confidence_score is not None else None
        ),
        "distance_meters": (
            float(row.distance_meters) if row.distance_meters is not None else None
        ),
        "evidence": dict(row.evidence),
        "created_at": _iso_utc(row.created_at),
    }
    return record, _canonical_json_bytes(record) + b"\n"


def _validate_place_snapshot_record(
    rec: Any,
    *,
    line_no: int,
    prev_osm_key: tuple[str, int] | None,
) -> tuple[str, int, int]:
    """Validate structure, field types, and canonical invariants of a places_snapshot.jsonl line."""
    if not isinstance(rec, dict):
        raise RecoveryArtifactError(
            f"Invalid JSON record on line {line_no} in '{PLACES_SNAPSHOT_FILENAME}': expected object."
        )
    required_keys = (
        "place_id",
        "osm_type",
        "osm_id",
        "name",
        "name_search",
        "category",
        "address",
        "longitude",
        "latitude",
        "location_srid",
        "location_ewkb_hex",
        "raw_tags",
        "last_seen_import_id",
        "is_active",
        "osm_version",
        "osm_changeset",
        "osm_timestamp",
        "created_at",
        "updated_at",
    )
    missing = [k for k in required_keys if k not in rec]
    if missing:
        raise RecoveryArtifactError(
            f"Incomplete place record on line {line_no} in '{PLACES_SNAPSHOT_FILENAME}': "
            f"missing keys {missing!r}."
        )

    place_id = rec["place_id"]
    osm_type = rec["osm_type"]
    osm_id = rec["osm_id"]
    category = rec["category"]
    name = rec["name"]
    name_search = rec["name_search"]
    address = rec["address"]
    location_srid = rec["location_srid"]
    ewkb_hex = rec["location_ewkb_hex"]
    raw_tags = rec["raw_tags"]
    last_seen = rec["last_seen_import_id"]
    is_active = rec["is_active"]

    if isinstance(place_id, bool) or not isinstance(place_id, int) or place_id <= 0:
        raise RecoveryArtifactError(f"Line {line_no}: invalid place_id {place_id!r}.")
    if not isinstance(osm_type, str) or osm_type not in OSM_TYPES:
        raise RecoveryArtifactError(f"Line {line_no}: invalid osm_type {osm_type!r}.")
    if isinstance(osm_id, bool) or not isinstance(osm_id, int) or osm_id <= 0:
        raise RecoveryArtifactError(f"Line {line_no}: invalid osm_id {osm_id!r}.")
    if not isinstance(category, str) or category not in PLACE_CATEGORIES:
        raise RecoveryArtifactError(f"Line {line_no}: invalid category {category!r}.")
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise RecoveryArtifactError(f"Line {line_no}: blank or non-string name {name!r}.")
    if category != "atm" and name is None:
        raise RecoveryArtifactError(f"Line {line_no}: non-ATM place missing required name.")
    if (name is None) != (name_search is None):
        raise RecoveryArtifactError(f"Line {line_no}: inconsistent name/name_search nullability.")
    if name_search is not None and (not isinstance(name_search, str) or not name_search.strip()):
        raise RecoveryArtifactError(f"Line {line_no}: invalid name_search {name_search!r}.")
    if address is not None and (not isinstance(address, str) or not address.strip()):
        raise RecoveryArtifactError(f"Line {line_no}: invalid address {address!r}.")
    try:
        validate_wgs84_coordinates(rec["longitude"], rec["latitude"])
    except (InvalidCoordinateError, TypeError, ValueError) as exc:
        raise RecoveryArtifactError(f"Line {line_no}: invalid coordinates: {exc}") from exc
    if isinstance(location_srid, bool) or location_srid != 4326:
        raise RecoveryArtifactError(f"Line {line_no}: expected location_srid=4326, got {location_srid!r}.")
    if not isinstance(ewkb_hex, str) or not ewkb_hex or _HEX_RE.fullmatch(ewkb_hex) is None:
        raise RecoveryArtifactError(f"Line {line_no}: invalid location_ewkb_hex.")
    if not isinstance(raw_tags, dict):
        raise RecoveryArtifactError(f"Line {line_no}: raw_tags must be a JSON object.")
    if isinstance(last_seen, bool) or not isinstance(last_seen, int) or last_seen <= 0:
        raise RecoveryArtifactError(f"Line {line_no}: invalid last_seen_import_id {last_seen!r}.")
    if is_active is not True:
        raise RecoveryArtifactError(
            f"Line {line_no}: active places snapshot record must have is_active=true."
        )

    osm_key = (osm_type, osm_id)
    if prev_osm_key is not None and osm_key <= prev_osm_key:
        raise RecoveryArtifactError(
            f"Line {line_no}: places snapshot is not strictly sorted/unique by (osm_type, osm_id)."
        )
    return osm_type, osm_id, place_id


def _validate_duplicate_snapshot_record(
    rec: Any,
    *,
    line_no: int,
    expected_run_id: int,
    prev_pair: tuple[int, int] | None,
) -> tuple[int, int]:
    """Validate structure and canonical invariants of a duplicate_candidates_snapshot.jsonl line."""
    if not isinstance(rec, dict):
        raise RecoveryArtifactError(
            f"Invalid JSON record on line {line_no} in '{DUPLICATES_SNAPSHOT_FILENAME}': expected object."
        )
    required_keys = (
        "id",
        "import_run_id",
        "place_a_id",
        "place_b_id",
        "detector_version",
        "confidence_score",
        "distance_meters",
        "evidence",
        "created_at",
    )
    missing = [k for k in required_keys if k not in rec]
    if missing:
        raise RecoveryArtifactError(
            f"Incomplete duplicate candidate record on line {line_no}: missing keys {missing!r}."
        )
    rec_id = rec["id"]
    run_id = rec["import_run_id"]
    place_a = rec["place_a_id"]
    place_b = rec["place_b_id"]
    det_ver = rec["detector_version"]
    evidence = rec["evidence"]

    if isinstance(rec_id, bool) or not isinstance(rec_id, int) or rec_id <= 0:
        raise RecoveryArtifactError(f"Duplicate line {line_no}: invalid id {rec_id!r}.")
    if isinstance(run_id, bool) or run_id != expected_run_id:
        raise RecoveryArtifactError(
            f"Duplicate line {line_no}: import_run_id={run_id!r} != {expected_run_id}."
        )
    if (
        isinstance(place_a, bool)
        or isinstance(place_b, bool)
        or not isinstance(place_a, int)
        or not isinstance(place_b, int)
        or place_a <= 0
        or place_b <= 0
        or not (place_a < place_b)
    ):
        raise RecoveryArtifactError(
            f"Duplicate line {line_no}: invalid canonical pair ({place_a!r}, {place_b!r})."
        )
    if not isinstance(det_ver, str) or not det_ver.strip():
        raise RecoveryArtifactError(f"Duplicate line {line_no}: blank detector_version.")
    if not isinstance(evidence, dict):
        raise RecoveryArtifactError(f"Duplicate line {line_no}: evidence must be a JSON object.")

    pair = (place_a, place_b)
    if prev_pair is not None and pair <= prev_pair:
        raise RecoveryArtifactError(
            f"Duplicate line {line_no}: snapshot is not strictly sorted/unique by (place_a_id, place_b_id)."
        )
    return pair


def _export_active_snapshot_in_tx(
    conn: Connection,
    *,
    tmp_dir: Path,
    artifact_id: str,
    on_snapshot_opened: Callable[[Connection], None] | None = None,
) -> dict[str, Any] | None:
    """Export active publication snapshot inside an already-open REPEATABLE READ READ ONLY tx."""
    iso_level = str(conn.execute(text("SHOW transaction_isolation")).scalar_one()).lower()
    read_only = str(conn.execute(text("SHOW transaction_read_only")).scalar_one()).lower()
    if iso_level != "repeatable read" or read_only != "on":
        raise RecoveryArtifactError(
            "Recovery artifact export requires PostgreSQL REPEATABLE READ READ ONLY "
            f"(got isolation={iso_level!r}, read_only={read_only!r})."
        )

    active_run = conn.execute(
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
                publication_operation_id,
                published_at,
                publication_evidence
            FROM import_runs
            WHERE active_slot = 1
            """
        )
    ).one_or_none()

    if active_run is None:
        return None

    if on_snapshot_opened is not None:
        on_snapshot_opened(conn)

    if active_run.status != "PUBLISHED" or active_run.publication_operation_id is None:
        raise RecoveryArtifactError(
            f"Active run id={active_run.id} is in unexpected state "
            f"(status={active_run.status!r}, op={active_run.publication_operation_id!r})."
        )

    boundary = conn.execute(
        text(
            """
            SELECT
                id,
                version_code,
                source_name,
                checksum_sha256,
                approval_status,
                ST_AsEWKB(geom) AS geom_ewkb
            FROM boundary_versions
            WHERE id = :bv_id
            """
        ),
        {"bv_id": int(active_run.boundary_version_id)},
    ).one_or_none()
    if boundary is None:
        raise RecoveryArtifactError(
            f"Active run id={active_run.id} references missing boundary_version_id="
            f"{active_run.boundary_version_id}."
        )

    geom_ewkb_sha256 = _sha256_bytes(bytes(boundary.geom_ewkb))

    places_path = tmp_dir / PLACES_SNAPSHOT_FILENAME
    dups_path = tmp_dir / DUPLICATES_SNAPSHOT_FILENAME

    # Stream active canonical places ordered deterministically by (osm_type, osm_id).
    places_hasher = hashlib.sha256()
    identities_hasher = hashlib.sha256()
    places_count = 0
    places_bytes = 0
    active_osm_types: set[str] = set()

    places_result = conn.execute(text(_active_places_snapshot_query(for_update=False)))
    with places_path.open("wb") as places_file:
        while True:
            batch = places_result.fetchmany(_STREAM_FETCH_BATCH_SIZE)
            if not batch:
                break
            for row in batch:
                record, line = _serialize_place_row(row)
                places_file.write(line)
                places_hasher.update(line)
                places_bytes += len(line)
                places_count += 1
                active_osm_types.add(record["osm_type"])
                identities_hasher.update(
                    f"{record['osm_type']}:{record['osm_id']}:{record['place_id']}\n".encode(
                        "utf-8"
                    )
                )
        places_file.flush()
        os.fsync(places_file.fileno())

    # Stream run-scoped duplicate candidates for the active publication.
    dups_hasher = hashlib.sha256()
    dups_count = 0
    dups_bytes = 0
    dups_result = conn.execute(
        text(_active_duplicates_snapshot_query(for_share=False)),
        {"run_id": int(active_run.id)},
    )
    with dups_path.open("wb") as dups_file:
        while True:
            batch = dups_result.fetchmany(_STREAM_FETCH_BATCH_SIZE)
            if not batch:
                break
            for row in batch:
                _record, line = _serialize_duplicate_row(row)
                dups_file.write(line)
                dups_hasher.update(line)
                dups_bytes += len(line)
                dups_count += 1
        dups_file.flush()
        os.fsync(dups_file.fileno())

    pub_ev = (
        dict(active_run.publication_evidence)
        if isinstance(active_run.publication_evidence, dict)
        else {}
    )

    places_content_sha256 = places_hasher.hexdigest()
    dups_content_sha256 = dups_hasher.hexdigest()

    manifest: dict[str, Any] = {
        "artifact_format": RECOVERY_ARTIFACT_FORMAT_VERSION,
        "artifact_id": artifact_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_isolation": "REPEATABLE READ READ ONLY",
        "source_publication": {
            "import_run_id": int(active_run.id),
            "run_type": str(active_run.run_type),
            "status": str(active_run.status),
            "publication_operation_id": str(active_run.publication_operation_id),
            "published_at": _iso_utc(active_run.published_at),
            "source_name": str(active_run.source_name),
            "source_checksum_sha256": str(active_run.source_checksum_sha256),
            "source_fingerprint": str(active_run.source_fingerprint),
            "pipeline_version": str(active_run.pipeline_version),
            "normalizer_version": str(active_run.normalizer_version),
            "config_digest": str(active_run.config_digest),
            "extraction_scope": pub_ev.get("extraction_scope"),
            "publication_mode": pub_ev.get("publication_mode"),
            "publication_evidence": pub_ev,
        },
        "boundary_version": {
            "id": int(boundary.id),
            "version_code": str(boundary.version_code),
            "source_name": str(boundary.source_name),
            "checksum_sha256": str(boundary.checksum_sha256),
            "approval_status": str(boundary.approval_status),
            "geom_ewkb_sha256": geom_ewkb_sha256,
        },
        "membership_summary": {
            "active_place_count": places_count,
            "active_osm_identities_sha256": identities_hasher.hexdigest(),
            "active_places_content_sha256": places_content_sha256,
            "active_osm_types": sorted(active_osm_types),
            "duplicate_candidate_count": dups_count,
            "duplicate_candidates_content_sha256": dups_content_sha256,
        },
        "files": {
            PLACES_SNAPSHOT_FILENAME: {
                "sha256": places_content_sha256,
                "size_bytes": places_bytes,
                "record_count": places_count,
            },
            DUPLICATES_SNAPSHOT_FILENAME: {
                "sha256": dups_content_sha256,
                "size_bytes": dups_bytes,
                "record_count": dups_count,
            },
        },
    }
    manifest["content_sha256"] = _compute_content_sha256(manifest)
    return manifest


def prepare_recovery_artifact(
    engine: Engine,
    *,
    artifact_root: str | Path,
    artifact_name: str | None = None,
    on_snapshot_opened: Callable[[Connection], None] | None = None,
    on_before_finalize: Callable[[Path], None] | None = None,
) -> RecoveryArtifactRecord | None:
    """Export and atomically finalize an immutable recovery artifact for the active publication.

    Returns None explicitly when no active publication exists (first publication).
    Never fabricates a previous dataset and never overwrites an existing finalized artifact.
    """
    root_path = Path(artifact_root)
    root_path.mkdir(parents=True, exist_ok=True)

    artifact_id = uuid.uuid4().hex
    tmp_dir = root_path / f".tmp_recovery_{artifact_id}"
    tmp_dir.mkdir(parents=False, exist_ok=False)

    try:
        manifest: dict[str, Any] | None
        with engine.connect() as conn:
            with conn.begin():
                conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                manifest = _export_active_snapshot_in_tx(
                    conn,
                    tmp_dir=tmp_dir,
                    artifact_id=artifact_id,
                    on_snapshot_opened=on_snapshot_opened,
                )

        if manifest is None:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None

        source_run_id = int(manifest["source_publication"]["import_run_id"])
        final_name = (
            artifact_name
            if artifact_name is not None
            else f"recovery_run_{source_run_id}_{artifact_id[:12]}"
        )
        if "/" in final_name or "\\" in final_name or final_name in (".", ".."):
            raise RecoveryArtifactError(f"Unsafe artifact_name: {final_name!r}.")

        final_dir = root_path / final_name
        if final_dir.exists():
            raise RecoveryArtifactError(
                f"Refusing to overwrite existing finalized recovery artifact at '{final_dir}'."
            )

        manifest_bytes = _canonical_json_bytes(manifest)
        manifest_sha256 = _sha256_bytes(manifest_bytes)

        manifest_path = tmp_dir / MANIFEST_FILENAME
        _write_and_fsync_bytes(manifest_path, manifest_bytes)

        checksum_path = tmp_dir / MANIFEST_CHECKSUM_FILENAME
        _write_and_fsync_bytes(
            checksum_path,
            f"{manifest_sha256}  {MANIFEST_FILENAME}\n".encode("utf-8"),
        )

        # Verify the staged artifact completely BEFORE atomic rename.
        verify_recovery_artifact(tmp_dir)

        if on_before_finalize is not None:
            on_before_finalize(tmp_dir)

        # Accidental-overwrite guard only; production immutability requires WORM/object-lock storage.
        for fname in (
            PLACES_SNAPSHOT_FILENAME,
            DUPLICATES_SNAPSHOT_FILENAME,
            MANIFEST_FILENAME,
            MANIFEST_CHECKSUM_FILENAME,
        ):
            _set_file_read_only(tmp_dir / fname)

        # Atomic directory rename on same filesystem; refuses to overwrite existing final_dir.
        try:
            os.rename(tmp_dir, final_dir)
        except FileExistsError as exc:
            raise RecoveryArtifactError(
                f"Refusing to overwrite existing finalized recovery artifact at '{final_dir}'."
            ) from exc

        _fsync_directory_best_effort(root_path)

        return verify_recovery_artifact(final_dir)
    except Exception:
        if tmp_dir.exists():
            for child in tmp_dir.iterdir():
                try:
                    os.chmod(child, stat.S_IWRITE | stat.S_IREAD)
                except OSError:
                    pass
            shutil.rmtree(tmp_dir, ignore_errors=True)
        raise


def verify_recovery_artifact(
    artifact_dir: str | Path,
    *,
    expected_source_run_id: int | None = None,
    expected_publication_operation_id: uuid.UUID | None = None,
    expected_boundary_version_id: int | None = None,
    expected_boundary_checksum_sha256: str | None = None,
    expected_source_fingerprint: str | None = None,
) -> RecoveryArtifactRecord:
    """Verify artifact files, manifest checksum, content checksums, JSONL schemas, and baseline."""
    directory = Path(artifact_dir)
    if not directory.exists() or not directory.is_dir():
        raise RecoveryArtifactError(f"Recovery artifact directory does not exist: '{directory}'.")

    manifest_path = directory / MANIFEST_FILENAME
    checksum_path = directory / MANIFEST_CHECKSUM_FILENAME
    places_path = directory / PLACES_SNAPSHOT_FILENAME
    dups_path = directory / DUPLICATES_SNAPSHOT_FILENAME

    for required in (manifest_path, checksum_path, places_path, dups_path):
        if not required.is_file():
            raise RecoveryArtifactError(
                f"Recovery artifact is missing required file '{required.name}' in '{directory}'."
            )

    checksum_raw = checksum_path.read_text(encoding="utf-8").strip()
    if not checksum_raw:
        raise RecoveryArtifactError("Recovery artifact manifest.sha256 is empty.")
    expected_manifest_sha256 = checksum_raw.split()[0]

    manifest_bytes = manifest_path.read_bytes()
    actual_manifest_sha256 = _sha256_bytes(manifest_bytes)
    if actual_manifest_sha256 != expected_manifest_sha256:
        raise RecoveryArtifactError(
            "Recovery artifact manifest.json checksum mismatch: "
            f"expected '{expected_manifest_sha256}', actual '{actual_manifest_sha256}'."
        )

    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryArtifactError(f"Recovery artifact manifest.json is corrupt: {exc}") from exc

    if not isinstance(manifest, dict):
        raise RecoveryArtifactError("Recovery artifact manifest.json must be a JSON object.")

    if manifest.get("artifact_format") != RECOVERY_ARTIFACT_FORMAT_VERSION:
        raise RecoveryArtifactError(
            f"Unsupported recovery artifact format: {manifest.get('artifact_format')!r}."
        )

    for required_key in (
        "source_publication",
        "boundary_version",
        "membership_summary",
        "files",
        "content_sha256",
    ):
        if required_key not in manifest or not isinstance(
            manifest[required_key], dict if required_key != "content_sha256" else str
        ):
            raise RecoveryArtifactError(
                f"Recovery artifact manifest missing or invalid key '{required_key}'."
            )

    expected_content_sha256 = str(manifest["content_sha256"])
    recomputed_content_sha256 = _compute_content_sha256(manifest)
    if recomputed_content_sha256 != expected_content_sha256:
        raise RecoveryArtifactError(
            "Recovery artifact content_sha256 mismatch: "
            f"manifest='{expected_content_sha256}', recomputed='{recomputed_content_sha256}'."
        )

    src_pub = manifest["source_publication"]
    bv = manifest["boundary_version"]

    try:
        source_run_id = int(src_pub["import_run_id"])
        source_pub_op_id = uuid.UUID(str(src_pub["publication_operation_id"]))
        source_bv_id = int(bv["id"])
        source_bv_checksum = str(bv["checksum_sha256"])
        source_bv_geom_sha = str(bv["geom_ewkb_sha256"])
        source_bv_approval = str(bv["approval_status"])
        source_fp = str(src_pub["source_fingerprint"])
        source_checksum = str(src_pub["source_checksum_sha256"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RecoveryArtifactError(
            f"Recovery artifact manifest has invalid baseline provenance fields: {exc}"
        ) from exc

    if source_bv_approval != "APPROVED":
        raise RecoveryArtifactError(
            f"Recovery artifact boundary approval_status must be 'APPROVED', got {source_bv_approval!r}."
        )
    if _SHA256_RE.fullmatch(source_bv_checksum) is None:
        raise RecoveryArtifactError("Recovery artifact boundary checksum_sha256 is invalid.")
    if _SHA256_RE.fullmatch(source_bv_geom_sha) is None:
        raise RecoveryArtifactError("Recovery artifact boundary geom_ewkb_sha256 is invalid.")
    if _SHA256_RE.fullmatch(source_checksum) is None:
        raise RecoveryArtifactError("Recovery artifact source_checksum_sha256 is invalid.")
    if not source_fp.strip():
        raise RecoveryArtifactError("Recovery artifact source_fingerprint is blank.")

    files_meta = manifest["files"]
    if PLACES_SNAPSHOT_FILENAME not in files_meta or DUPLICATES_SNAPSHOT_FILENAME not in files_meta:
        raise RecoveryArtifactError("Recovery artifact manifest missing snapshot file entries.")

    # Verify places_snapshot.jsonl hash, byte size, canonical JSON schema, and identity digest.
    places_meta = files_meta[PLACES_SNAPSHOT_FILENAME]
    actual_places_sha, actual_places_size = _sha256_file(places_path)
    if actual_places_sha != places_meta.get("sha256") or actual_places_size != int(
        places_meta.get("size_bytes", -1)
    ):
        raise RecoveryArtifactError(
            f"Recovery artifact '{PLACES_SNAPSHOT_FILENAME}' checksum or size mismatch "
            f"(actual sha={actual_places_sha}, size={actual_places_size})."
        )

    identities_hasher = hashlib.sha256()
    parsed_places_count = 0
    prev_osm_key: tuple[str, int] | None = None
    with places_path.open("rb") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            if not raw_line.endswith(b"\n"):
                raise RecoveryArtifactError(
                    f"Truncated line {line_no} in '{PLACES_SNAPSHOT_FILENAME}'."
                )
            try:
                rec = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RecoveryArtifactError(
                    f"Corrupt JSON on line {line_no} in '{PLACES_SNAPSHOT_FILENAME}': {exc}"
                ) from exc
            osm_type, osm_id, place_id = _validate_place_snapshot_record(
                rec, line_no=line_no, prev_osm_key=prev_osm_key
            )
            prev_osm_key = (osm_type, osm_id)
            identities_hasher.update(f"{osm_type}:{osm_id}:{place_id}\n".encode("utf-8"))
            parsed_places_count += 1

    membership = manifest["membership_summary"]
    if parsed_places_count != int(places_meta.get("record_count", -1)) or parsed_places_count != int(
        membership.get("active_place_count", -1)
    ):
        raise RecoveryArtifactError(
            f"Recovery artifact place record count mismatch: parsed={parsed_places_count}, "
            f"manifest={membership.get('active_place_count')}."
        )
    if identities_hasher.hexdigest() != membership.get("active_osm_identities_sha256"):
        raise RecoveryArtifactError(
            "Recovery artifact active_osm_identities_sha256 mismatch against snapshot contents."
        )
    if membership.get("active_places_content_sha256") != actual_places_sha:
        raise RecoveryArtifactError(
            "Recovery artifact active_places_content_sha256 mismatch against snapshot file hash."
        )

    # Verify duplicate_candidates_snapshot.jsonl hash, byte size, and canonical JSON schema.
    dups_meta = files_meta[DUPLICATES_SNAPSHOT_FILENAME]
    actual_dups_sha, actual_dups_size = _sha256_file(dups_path)
    if actual_dups_sha != dups_meta.get("sha256") or actual_dups_size != int(
        dups_meta.get("size_bytes", -1)
    ):
        raise RecoveryArtifactError(
            f"Recovery artifact '{DUPLICATES_SNAPSHOT_FILENAME}' checksum or size mismatch."
        )

    parsed_dups_count = 0
    prev_pair: tuple[int, int] | None = None
    with dups_path.open("rb") as handle:
        for line_no, raw_line in enumerate(handle, start=1):
            if not raw_line.endswith(b"\n"):
                raise RecoveryArtifactError(
                    f"Truncated line {line_no} in '{DUPLICATES_SNAPSHOT_FILENAME}'."
                )
            try:
                rec = json.loads(raw_line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RecoveryArtifactError(
                    f"Corrupt JSON on line {line_no} in '{DUPLICATES_SNAPSHOT_FILENAME}': {exc}"
                ) from exc
            prev_pair = _validate_duplicate_snapshot_record(
                rec,
                line_no=line_no,
                expected_run_id=source_run_id,
                prev_pair=prev_pair,
            )
            parsed_dups_count += 1

    if parsed_dups_count != int(dups_meta.get("record_count", -1)) or parsed_dups_count != int(
        membership.get("duplicate_candidate_count", -1)
    ):
        raise RecoveryArtifactError(
            f"Recovery artifact duplicate candidate count mismatch: parsed={parsed_dups_count}."
        )
    if membership.get("duplicate_candidates_content_sha256") != actual_dups_sha:
        raise RecoveryArtifactError(
            "Recovery artifact duplicate_candidates_content_sha256 mismatch against snapshot file hash."
        )

    # Verify expected baseline if supplied.
    if expected_source_run_id is not None and source_run_id != expected_source_run_id:
        raise RecoveryArtifactError(
            f"Recovery artifact baseline run mismatch: artifact captures run "
            f"id={source_run_id}, expected active run id={expected_source_run_id}."
        )
    if (
        expected_publication_operation_id is not None
        and source_pub_op_id != expected_publication_operation_id
    ):
        raise RecoveryArtifactError(
            f"Recovery artifact publication_operation_id mismatch: artifact has "
            f"{source_pub_op_id}, expected {expected_publication_operation_id}."
        )
    if expected_boundary_version_id is not None and source_bv_id != expected_boundary_version_id:
        raise RecoveryArtifactError(
            f"Recovery artifact boundary_version_id mismatch: artifact has "
            f"{source_bv_id}, expected {expected_boundary_version_id}."
        )
    if (
        expected_boundary_checksum_sha256 is not None
        and source_bv_checksum != expected_boundary_checksum_sha256
    ):
        raise RecoveryArtifactError(
            f"Recovery artifact boundary checksum mismatch: artifact has "
            f"{source_bv_checksum}, expected {expected_boundary_checksum_sha256}."
        )
    if expected_source_fingerprint is not None and source_fp != expected_source_fingerprint:
        raise RecoveryArtifactError(
            f"Recovery artifact source_fingerprint mismatch: artifact has "
            f"{source_fp}, expected {expected_source_fingerprint}."
        )

    return RecoveryArtifactRecord(
        artifact_dir=directory,
        manifest_path=manifest_path,
        manifest_checksum_path=checksum_path,
        places_snapshot_path=places_path,
        duplicate_candidates_snapshot_path=dups_path,
        artifact_format=RECOVERY_ARTIFACT_FORMAT_VERSION,
        source_import_run_id=source_run_id,
        source_publication_operation_id=source_pub_op_id,
        source_boundary_version_id=source_bv_id,
        source_boundary_checksum_sha256=source_bv_checksum,
        active_place_count=parsed_places_count,
        duplicate_candidate_count=parsed_dups_count,
        active_osm_identities_sha256=str(membership["active_osm_identities_sha256"]),
        content_sha256=expected_content_sha256,
        manifest_sha256=actual_manifest_sha256,
        manifest=manifest,
    )


def verify_artifact_matches_live_baseline(
    conn: Connection,
    *,
    artifact: RecoveryArtifactRecord,
    lock_live_rows: bool = False,
) -> None:
    """Re-verify full canonical content of active places, duplicates, boundary, and run against live DB.

    When `lock_live_rows=True` (used inside the guarded publication transaction after acquiring
    `pg_advisory_xact_lock` and `import_runs` row locks), locks the active boundary row (`FOR SHARE`),
    all active `places` rows (`FOR UPDATE` in deterministic `(osm_type, osm_id)` order), and active
    `duplicate_candidates` rows (`FOR SHARE`) so no concurrent SQL writer can mutate the verified
    baseline between verification and UPSERT/reconciliation.
    """
    boundary_lock_clause = "FOR SHARE OF b" if lock_live_rows else ""
    active_run = conn.execute(
        text(
            f"""
            SELECT
                r.id,
                r.run_type,
                r.status,
                r.boundary_version_id,
                r.publication_operation_id,
                r.published_at,
                r.source_name,
                r.source_checksum_sha256,
                r.source_fingerprint,
                r.pipeline_version,
                r.normalizer_version,
                r.config_digest,
                b.version_code AS boundary_version_code,
                b.source_name AS boundary_source_name,
                b.checksum_sha256 AS boundary_checksum_sha256,
                b.approval_status AS boundary_approval_status,
                ST_AsEWKB(b.geom) AS boundary_geom_ewkb
            FROM import_runs r
            JOIN boundary_versions b ON b.id = r.boundary_version_id
            WHERE r.active_slot = 1
            {boundary_lock_clause}
            """
        )
    ).one_or_none()

    if active_run is None:
        raise RecoveryArtifactError(
            "Recovery artifact references an active baseline run "
            f"(id={artifact.source_import_run_id}), but no active publication exists in database."
        )

    src_pub = artifact.manifest["source_publication"]
    bv_meta = artifact.manifest["boundary_version"]
    files_meta = artifact.manifest["files"]

    # 1. Verify active run identity & complete source provenance.
    if int(active_run.id) != artifact.source_import_run_id:
        raise RecoveryArtifactError(
            f"Stale recovery artifact: artifact captures run id={artifact.source_import_run_id}, "
            f"but live active run under lock is id={active_run.id}."
        )
    if active_run.publication_operation_id != artifact.source_publication_operation_id:
        raise RecoveryArtifactError(
            "Stale recovery artifact: publication_operation_id does not match live active run."
        )
    for field_name, live_val, manifest_val in (
        ("run_type", str(active_run.run_type), str(src_pub.get("run_type"))),
        ("source_name", str(active_run.source_name), str(src_pub.get("source_name"))),
        (
            "source_checksum_sha256",
            str(active_run.source_checksum_sha256),
            str(src_pub.get("source_checksum_sha256")),
        ),
        (
            "source_fingerprint",
            str(active_run.source_fingerprint),
            str(src_pub.get("source_fingerprint")),
        ),
        (
            "pipeline_version",
            str(active_run.pipeline_version),
            str(src_pub.get("pipeline_version")),
        ),
        (
            "normalizer_version",
            str(active_run.normalizer_version),
            str(src_pub.get("normalizer_version")),
        ),
        ("config_digest", str(active_run.config_digest), str(src_pub.get("config_digest"))),
        ("published_at", _iso_utc(active_run.published_at), src_pub.get("published_at")),
    ):
        if live_val != manifest_val:
            raise RecoveryArtifactError(
                f"Stale recovery artifact: source_publication.{field_name} mismatch "
                f"(live={live_val!r}, artifact={manifest_val!r})."
            )

    # 2. Verify active boundary provenance AND actual PostGIS geometry EWKB digest.
    if int(active_run.boundary_version_id) != artifact.source_boundary_version_id:
        raise RecoveryArtifactError(
            "Stale recovery artifact: boundary_version_id does not match live active run."
        )
    if str(active_run.boundary_version_code) != str(bv_meta.get("version_code")):
        raise RecoveryArtifactError(
            "Stale recovery artifact: boundary version_code does not match live active boundary."
        )
    if str(active_run.boundary_checksum_sha256) != artifact.source_boundary_checksum_sha256:
        raise RecoveryArtifactError(
            "Stale recovery artifact: boundary checksum does not match live active run."
        )
    if str(active_run.boundary_approval_status) != "APPROVED":
        raise RecoveryArtifactError(
            "Stale recovery artifact: live active boundary is not APPROVED."
        )
    live_geom_sha = _sha256_bytes(bytes(active_run.boundary_geom_ewkb))
    if live_geom_sha != str(bv_meta.get("geom_ewkb_sha256")):
        raise RecoveryArtifactError(
            "Stale recovery artifact: live active boundary PostGIS geometry EWKB hash differs "
            "from artifact manifest."
        )

    # 3. Verify complete active places membership AND full canonical content hash (shared serializer).
    live_identities_hasher = hashlib.sha256()
    live_places_content_hasher = hashlib.sha256()
    live_places_count = 0
    live_places_bytes = 0

    rows = conn.execute(text(_active_places_snapshot_query(for_update=lock_live_rows)))
    while True:
        batch = rows.fetchmany(_STREAM_FETCH_BATCH_SIZE)
        if not batch:
            break
        for row in batch:
            record, line = _serialize_place_row(row)
            live_places_content_hasher.update(line)
            live_places_bytes += len(line)
            live_identities_hasher.update(
                f"{record['osm_type']}:{record['osm_id']}:{record['place_id']}\n".encode("utf-8")
            )
            live_places_count += 1

    expected_places_meta = files_meta[PLACES_SNAPSHOT_FILENAME]
    if live_places_count != artifact.active_place_count:
        raise RecoveryArtifactError(
            f"Stale recovery artifact: live active place count ({live_places_count}) != "
            f"artifact active_place_count ({artifact.active_place_count})."
        )
    if live_identities_hasher.hexdigest() != artifact.active_osm_identities_sha256:
        raise RecoveryArtifactError(
            "Stale recovery artifact: live active OSM identity set hash differs from artifact."
        )
    if (
        live_places_content_hasher.hexdigest() != str(expected_places_meta["sha256"])
        or live_places_bytes != int(expected_places_meta["size_bytes"])
    ):
        raise RecoveryArtifactError(
            "Stale recovery artifact: live active places canonical content hash differs from "
            "artifact snapshot (name, category, address, coordinates, raw_tags, or provenance changed)."
        )

    # 4. Verify complete duplicate_candidates membership AND immutable evidence content hash.
    live_dups_content_hasher = hashlib.sha256()
    live_dups_count = 0
    live_dups_bytes = 0
    dup_rows = conn.execute(
        text(_active_duplicates_snapshot_query(for_share=lock_live_rows)),
        {"run_id": int(active_run.id)},
    )
    while True:
        batch = dup_rows.fetchmany(_STREAM_FETCH_BATCH_SIZE)
        if not batch:
            break
        for row in batch:
            _record, line = _serialize_duplicate_row(row)
            live_dups_content_hasher.update(line)
            live_dups_bytes += len(line)
            live_dups_count += 1

    expected_dups_meta = files_meta[DUPLICATES_SNAPSHOT_FILENAME]
    if live_dups_count != artifact.duplicate_candidate_count:
        raise RecoveryArtifactError(
            f"Stale recovery artifact: live duplicate_candidates count ({live_dups_count}) != "
            f"artifact count ({artifact.duplicate_candidate_count})."
        )
    if (
        live_dups_content_hasher.hexdigest() != str(expected_dups_meta["sha256"])
        or live_dups_bytes != int(expected_dups_meta["size_bytes"])
    ):
        raise RecoveryArtifactError(
            "Stale recovery artifact: live duplicate_candidates canonical content/evidence hash "
            "differs from artifact snapshot."
        )
