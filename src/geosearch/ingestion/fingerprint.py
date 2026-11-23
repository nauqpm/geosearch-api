"""Deterministic source identity, config digest, and candidate content fingerprinting (M2.4)."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import timezone
from typing import Any, Final

from geosearch.ingestion.classification import get_approved_category_rules_spec
from geosearch.ingestion.models import (
    ADDRESS_STRATEGY_VERSION,
    EXTRACTION_SCOPE_NODE_ONLY,
    NORMALIZER_VERSION,
    PIPELINE_VERSION,
    CanonicalPlaceCandidate,
    NormalizationIssue,
)

SHA256_HEX_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")


def _canonical_json_sha256(payload: dict[str, Any]) -> str:
    """Hash a JSON-serializable payload canonically with sorted keys and compact separators."""
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_non_blank(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-blank string, got {value!r}.")
    return value.strip()


def _require_sha256_hex(name: str, value: str) -> str:
    if not isinstance(value, str) or SHA256_HEX_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a 64-character lowercase SHA-256 hex digest.")
    return value


def compute_config_digest(
    *,
    extraction_scope: str = EXTRACTION_SCOPE_NODE_ONLY,
    address_strategy: str = ADDRESS_STRATEGY_VERSION,
    apply_boundary_filter: bool = True,
) -> str:
    """Compute a deterministic digest of semantic extraction and normalization configuration.

    Operational settings such as batch size or local paths are intentionally excluded so two
    runs with identical semantic behavior produce the same config_digest.
    """
    payload = {
        "schema": "geosearch.ingestion.config.v1",
        "extraction_scope": _require_non_blank("extraction_scope", extraction_scope),
        "address_strategy": _require_non_blank("address_strategy", address_strategy),
        "apply_boundary_filter": bool(apply_boundary_filter),
        "category_rules": get_approved_category_rules_spec(),
    }
    return _canonical_json_sha256(payload)


def compute_source_fingerprint(
    *,
    source_checksum_sha256: str,
    source_snapshot_id: str,
    boundary_version_code: str,
    boundary_checksum_sha256: str,
    config_digest: str,
    pipeline_version: str = PIPELINE_VERSION,
    normalizer_version: str = NORMALIZER_VERSION,
) -> str:
    """Compute a reproducible canonical source fingerprint independent of timestamps or UUIDs."""
    payload = {
        "schema": "geosearch.ingestion.fingerprint.v1",
        "source_checksum_sha256": _require_sha256_hex(
            "source_checksum_sha256", source_checksum_sha256
        ),
        "source_snapshot_id": _require_non_blank("source_snapshot_id", source_snapshot_id),
        "boundary_version_code": _require_non_blank(
            "boundary_version_code", boundary_version_code
        ),
        "boundary_checksum_sha256": _require_sha256_hex(
            "boundary_checksum_sha256", boundary_checksum_sha256
        ),
        "pipeline_version": _require_non_blank("pipeline_version", pipeline_version),
        "normalizer_version": _require_non_blank("normalizer_version", normalizer_version),
        "config_digest": _require_non_blank("config_digest", config_digest),
    }
    return _canonical_json_sha256(payload)


def compute_candidate_content_digest(candidate: CanonicalPlaceCandidate) -> str:
    """Compute a deterministic SHA-256 digest of a canonical candidate's semantic fields."""
    timestamp_iso = (
        candidate.osm_timestamp.astimezone(timezone.utc).isoformat()
        if candidate.osm_timestamp is not None
        else None
    )
    payload = {
        "osm_type": candidate.osm_type,
        "osm_id": candidate.osm_id,
        "name": candidate.name,
        "name_search": candidate.name_search,
        "category": candidate.category,
        "address": candidate.address,
        "longitude": repr(float(candidate.longitude)),
        "latitude": repr(float(candidate.latitude)),
        "raw_tags": dict(sorted(candidate.raw_tags.items())),
        "osm_version": candidate.osm_version,
        "osm_changeset": candidate.osm_changeset,
        "osm_timestamp": timestamp_iso,
    }
    return _canonical_json_sha256(payload)


def compute_issue_fingerprint(
    *,
    import_run_id: int,
    issue: NormalizationIssue,
) -> str:
    """Compute a deterministic SHA-256 fingerprint for idempotent run-scoped issue storage.

    Includes run ID, stage, issue_code, severity, nullable OSM identity, deterministic
    source_occurrence_id (when present), message, and canonical details so repeated replays
    of the same source occurrence do not duplicate import_issues while two genuinely distinct
    malformed source occurrences with identical invalid payloads are never silently merged.
    """
    payload = {
        "schema": "geosearch.ingestion.issue.v1",
        "import_run_id": int(import_run_id),
        "stage": issue.stage,
        "issue_code": issue.issue_code,
        "severity": issue.severity,
        "osm_type": issue.osm_type,
        "osm_id": issue.osm_id,
        "source_occurrence_id": issue.source_occurrence_id,
        "message": issue.message,
        "details": issue.details,
    }
    return _canonical_json_sha256(payload)
