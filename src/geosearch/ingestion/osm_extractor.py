"""Streaming Pyosmium Node extraction adapter and verified source snapshots (M2.3/M2.6).

Scope note:
- This module implements deterministic, bounded-memory Node-only extraction
  (`extraction_scope="node_only"`).
- Way and Relation geometry extraction requires additional location-cache / area
  assembly and is intentionally deferred to a later incremental ingestion task.
- Extraction statistics explicitly record Node-only counts and never claim full
  Node+Way+Relation coverage.
"""

from __future__ import annotations

import hashlib
import os
import stat
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Final

import osmium

from geosearch.ingestion.classification import normalize_osm_object
from geosearch.ingestion.models import (
    EXTRACTION_SCOPE_NODE_ONLY,
    ExtractionStats,
    NormalizationOutcome,
    NormalizationOutcomeKind,
    RawOsmObject,
)

SHA256_CHUNK_SIZE: Final[int] = 65_536
_OSM_EPOCH_UTC: Final[datetime] = datetime(1970, 1, 1, 0, 0, tzinfo=timezone.utc)
_RELEVANT_TAG_KEYS: Final[tuple[str, ...]] = ("amenity", "shop")


class OsmSourceError(RuntimeError):
    """Raised when an OSM source file is missing, unreadable, mutated, or structurally corrupt."""

    def __init__(
        self,
        message: str,
        *,
        source_path: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.source_path = source_path
        self.details = dict(details) if details is not None else {}


@dataclass(frozen=True, slots=True)
class VerifiedSourceSnapshot:
    """Isolated read-only copy of an OSM source file with verified SHA-256 and stat metadata."""

    original_path: Path
    snapshot_path: Path
    checksum_sha256: str
    size_bytes: int
    source_dev: int
    source_ino: int
    source_mtime_ns: int


def validate_osm_source_path(source_path: str | Path) -> Path:
    """Resolve and validate that source_path points to an existing non-empty regular file."""
    path = Path(source_path)
    if not path.exists():
        raise OsmSourceError(
            f"OSM source file does not exist: {path}",
            source_path=str(path),
        )
    if not path.is_file():
        raise OsmSourceError(
            f"OSM source path is not a regular file: {path}",
            source_path=str(path),
        )
    if path.stat().st_size == 0:
        raise OsmSourceError(
            f"OSM source file is empty (0 bytes): {path}",
            source_path=str(path),
        )
    return path


def compute_file_sha256(source_path: str | Path) -> str:
    """Compute a deterministic lowercase SHA-256 hex digest using bounded chunk streaming."""
    path = validate_osm_source_path(source_path)
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(SHA256_CHUNK_SIZE):
                digest.update(chunk)
    except OSError as exc:
        raise OsmSourceError(
            f"Failed to read OSM source file '{path}': {exc}",
            source_path=str(path),
        ) from exc
    return digest.hexdigest()


@contextmanager
def open_verified_source_snapshot(
    source_path: str | Path,
) -> Iterator[VerifiedSourceSnapshot]:
    """Stream source_path into an isolated read-only snapshot while computing its SHA-256.

    Guarantees:
    - Bounded 64 KiB streaming copy; never modifies the original source file.
    - The hash is computed over the exact bytes written to `snapshot_path`, which Pyosmium
      subsequently parses.
    - Detects concurrent mutation during the snapshot copy via pre/post stat comparison.
    - Safely cleans up the temporary snapshot directory on exit.
    """
    original_path = validate_osm_source_path(source_path)
    pre_stat = original_path.stat()

    tmp_dir_mgr = tempfile.TemporaryDirectory(
        prefix="geosearch_snap_",
        ignore_cleanup_errors=True,
    )
    snapshot_path = Path(tmp_dir_mgr.name) / original_path.name
    try:
        digest = hashlib.sha256()
        bytes_copied = 0
        try:
            with original_path.open("rb") as src, snapshot_path.open("wb") as dst:
                while chunk := src.read(SHA256_CHUNK_SIZE):
                    digest.update(chunk)
                    dst.write(chunk)
                    bytes_copied += len(chunk)
        except OSError as exc:
            raise OsmSourceError(
                f"Failed to create isolated snapshot for '{original_path}': {exc}",
                source_path=str(original_path),
            ) from exc

        if bytes_copied == 0:
            raise OsmSourceError(
                f"OSM source file became empty during snapshot copy: {original_path}",
                source_path=str(original_path),
            )

        post_stat = original_path.stat()
        if (
            post_stat.st_size != pre_stat.st_size
            or post_stat.st_size != bytes_copied
            or post_stat.st_mtime_ns != pre_stat.st_mtime_ns
            or post_stat.st_ino != pre_stat.st_ino
            or post_stat.st_dev != pre_stat.st_dev
        ):
            raise OsmSourceError(
                f"OSM source file changed while creating ingestion snapshot: {original_path}",
                source_path=str(original_path),
                details={
                    "reason": "source_mutated_during_snapshot",
                    "pre_size": pre_stat.st_size,
                    "post_size": post_stat.st_size,
                    "bytes_copied": bytes_copied,
                },
            )

        try:
            os.chmod(snapshot_path, stat.S_IREAD)
        except OSError:
            pass

        yield VerifiedSourceSnapshot(
            original_path=original_path,
            snapshot_path=snapshot_path,
            checksum_sha256=digest.hexdigest(),
            size_bytes=bytes_copied,
            source_dev=pre_stat.st_dev,
            source_ino=pre_stat.st_ino,
            source_mtime_ns=pre_stat.st_mtime_ns,
        )
    finally:
        if snapshot_path.exists():
            try:
                os.chmod(snapshot_path, stat.S_IWRITE | stat.S_IREAD)
            except OSError:
                pass
        tmp_dir_mgr.cleanup()


def verify_source_snapshot_integrity(
    snapshot: VerifiedSourceSnapshot,
    *,
    expected_checksum_sha256: str | None = None,
    verify_original_unchanged: bool = True,
) -> None:
    """Verify both the isolated snapshot and (optionally) the original source file integrity."""
    target_checksum = (
        expected_checksum_sha256
        if expected_checksum_sha256 is not None
        else snapshot.checksum_sha256
    )

    actual_snapshot_checksum = compute_file_sha256(snapshot.snapshot_path)
    if actual_snapshot_checksum != target_checksum:
        raise OsmSourceError(
            "Isolated ingestion snapshot checksum mismatch detected.",
            source_path=str(snapshot.snapshot_path),
            details={
                "reason": "snapshot_checksum_mismatch",
                "expected_checksum_sha256": target_checksum,
                "actual_checksum_sha256": actual_snapshot_checksum,
            },
        )

    if verify_original_unchanged:
        try:
            current_stat = snapshot.original_path.stat()
        except OSError as exc:
            raise OsmSourceError(
                f"Original OSM source file is no longer accessible: {snapshot.original_path}",
                source_path=str(snapshot.original_path),
                details={"reason": "source_missing_or_unreadable"},
            ) from exc

        actual_source_checksum = compute_file_sha256(snapshot.original_path)
        if (
            actual_source_checksum != target_checksum
            or current_stat.st_size != snapshot.size_bytes
        ):
            raise OsmSourceError(
                f"OSM source file '{snapshot.original_path}' was modified, replaced, or "
                "truncated after checksum verification.",
                source_path=str(snapshot.original_path),
                details={
                    "reason": "source_modified_after_checksum",
                    "expected_checksum_sha256": target_checksum,
                    "actual_checksum_sha256": actual_source_checksum,
                    "expected_size_bytes": snapshot.size_bytes,
                    "actual_size_bytes": current_stat.st_size,
                },
            )


def _extract_optional_osm_metadata(
    node: Any,
) -> tuple[int | None, int | None, datetime | None]:
    """Convert Pyosmium sentinel metadata values (0 / Unix epoch) to canonical None."""
    raw_version = node.version
    osm_version = raw_version if isinstance(raw_version, int) and raw_version > 0 else None

    raw_changeset = node.changeset
    osm_changeset = (
        raw_changeset if isinstance(raw_changeset, int) and raw_changeset > 0 else None
    )

    raw_timestamp = node.timestamp
    osm_timestamp = (
        raw_timestamp
        if isinstance(raw_timestamp, datetime) and raw_timestamp != _OSM_EPOCH_UTC
        else None
    )
    return osm_version, osm_changeset, osm_timestamp


def iter_osm_node_outcomes(
    source_path: str | Path,
    *,
    stats: ExtractionStats | None = None,
    source_provenance: Mapping[str, Any] | None = None,
) -> Iterator[NormalizationOutcome]:
    """Stream OSM Node objects from a PBF or OSM XML file and yield relevant outcomes.

    Memory behavior:
    - Uses Pyosmium FileProcessor with EntityFilter(NODE) to stream incrementally.
    - Filters nodes lacking 'amenity' or 'shop' tags before allocating Python tag dicts.
    - Yields only relevant outcomes (VALID, INVALID, AMBIGUOUS); ordinary UNSUPPORTED
      nodes are counted in `stats` and skipped without accumulating in memory.
    - Never holds references to Pyosmium C++ node objects past the current loop iteration.
    """
    path = validate_osm_source_path(source_path)
    active_stats = stats if stats is not None else ExtractionStats()
    base_provenance: dict[str, Any] = {
        "source_file": path.name,
        "extraction_scope": EXTRACTION_SCOPE_NODE_ONLY,
    }
    if source_provenance:
        base_provenance.update(source_provenance)

    fp_iter: Any = None
    try:
        processor = osmium.FileProcessor(str(path)).with_filter(
            osmium.filter.EntityFilter(osmium.osm.NODE)
        )
        fp_iter = iter(processor)
        for node in fp_iter:
            active_stats.total_nodes_seen += 1

            # Skip deleted/invisible nodes cleanly without fabricating candidates.
            if node.deleted or not node.visible:
                active_stats.unsupported_nodes_seen += 1
                continue

            # Early C++-level tag check avoids Python dict allocation for non-POI nodes.
            tags_view = node.tags
            if not any(key in tags_view for key in _RELEVANT_TAG_KEYS):
                active_stats.unsupported_nodes_seen += 1
                continue

            # Copy all scalar/tag values out of the Pyosmium buffer immediately.
            tags_dict: dict[str, str] = {tag.k: tag.v for tag in tags_view}
            if node.location.valid():
                lon: float | None = float(node.location.lon)
                lat: float | None = float(node.location.lat)
            else:
                lon = None
                lat = None

            osm_version, osm_changeset, osm_timestamp = _extract_optional_osm_metadata(node)
            source_occurrence_id = f"node_stream_index:{active_stats.total_nodes_seen}"
            node_provenance = {
                **base_provenance,
                "source_occurrence_id": source_occurrence_id,
            }
            raw_obj = RawOsmObject(
                osm_type="node",
                osm_id=int(node.id),
                tags=tags_dict,
                longitude=lon,
                latitude=lat,
                osm_version=osm_version,
                osm_changeset=osm_changeset,
                osm_timestamp=osm_timestamp,
                source_provenance=node_provenance,
                source_occurrence_id=source_occurrence_id,
            )

            outcome = normalize_osm_object(raw_obj)
            if outcome.kind == NormalizationOutcomeKind.UNSUPPORTED:
                active_stats.unsupported_nodes_seen += 1
                continue

            active_stats.supported_nodes_seen += 1
            if outcome.kind == NormalizationOutcomeKind.VALID:
                active_stats.valid_candidates += 1
            else:
                active_stats.invalid_candidates += 1
                if outcome.issue is not None:
                    active_stats.record_issue(outcome.issue.issue_code)

            yield outcome
    except OsmSourceError:
        raise
    except Exception as exc:
        raise OsmSourceError(
            f"Error while streaming OSM source file '{path}': {exc}",
            source_path=str(path),
            details={"reason": "osm_parse_error"},
        ) from exc
    finally:
        if fp_iter is not None and hasattr(fp_iter, "close"):
            fp_iter.close()
