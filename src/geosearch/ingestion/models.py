"""Typed canonical data models for GeoSearch M2 OSM validation and ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Final

PIPELINE_VERSION: Final[str] = "0.2.0"
NORMALIZER_VERSION: Final[str] = "0.2.0"
EXTRACTION_SCOPE_NODE_ONLY: Final[str] = "node_only"
ADDRESS_STRATEGY_VERSION: Final[str] = "minimal_osm_addr_v1"
DEFAULT_STAGING_BATCH_SIZE: Final[int] = 500


class ClassificationStatus(str, Enum):
    """Pure category classification outcome for an OSM tag dictionary."""

    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    AMBIGUOUS = "AMBIGUOUS"
    INVALID_INPUT = "INVALID_INPUT"


class NormalizationOutcomeKind(str, Enum):
    """End-to-end normalization outcome for a raw OSM object."""

    VALID = "VALID"
    UNSUPPORTED = "UNSUPPORTED"
    INVALID = "INVALID"
    AMBIGUOUS = "AMBIGUOUS"


class IssueSeverity(str, Enum):
    """Approved import_issues severity values matching M1 schema constraints."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class PipelineStage(str, Enum):
    """Explicit M2 ingestion pipeline stages for issue traceability."""

    EXTRACT = "EXTRACT"
    CLASSIFY = "CLASSIFY"
    VALIDATE = "VALIDATE"
    STAGE = "STAGE"
    VERIFY = "VERIFY"


@dataclass(frozen=True, slots=True)
class OsmIdentity:
    """Validated canonical OpenStreetMap object identity."""

    osm_type: str
    osm_id: int


@dataclass(frozen=True, slots=True)
class RawOsmObject:
    """Unvalidated input representation of an OSM object before normalization."""

    osm_type: Any
    osm_id: Any
    tags: Any
    longitude: Any
    latitude: Any
    osm_version: Any = None
    osm_changeset: Any = None
    osm_timestamp: datetime | None = None
    source_provenance: dict[str, Any] = field(default_factory=dict)
    source_occurrence_id: str | None = None


@dataclass(frozen=True, slots=True)
class CanonicalPlaceCandidate:
    """Validated, normalized place candidate compatible with M1 places constraints."""

    osm_type: str
    osm_id: int
    name: str | None
    name_search: str | None
    category: str
    address: str | None
    longitude: float
    latitude: float
    raw_tags: dict[str, str]
    osm_version: int | None = None
    osm_changeset: int | None = None
    osm_timestamp: datetime | None = None
    source_provenance: dict[str, Any] = field(default_factory=dict)

    @property
    def identity(self) -> OsmIdentity:
        """Return the typed OSM identity tuple."""
        return OsmIdentity(osm_type=self.osm_type, osm_id=self.osm_id)


@dataclass(frozen=True, slots=True)
class NormalizationIssue:
    """Traceable issue record compatible with M1 import_issues constraints."""

    severity: str
    stage: str
    issue_code: str
    message: str
    osm_type: str | None = None
    osm_id: int | None = None
    details: dict[str, Any] | None = None
    source_occurrence_id: str | None = None


@dataclass(frozen=True, slots=True)
class CategoryClassification:
    """Deterministic result of classifying an OSM tag dictionary."""

    status: ClassificationStatus
    category: str | None
    matched_categories: tuple[str, ...] = ()
    matched_tags: dict[str, str] = field(default_factory=dict)
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class NormalizationOutcome:
    """Result of running a RawOsmObject through the M2 normalization pipeline."""

    kind: NormalizationOutcomeKind
    candidate: CanonicalPlaceCandidate | None = None
    issue: NormalizationIssue | None = None
    classification: CategoryClassification | None = None


@dataclass(slots=True)
class ExtractionStats:
    """Deterministic counters produced by streaming OSM Node extraction."""

    total_nodes_seen: int = 0
    supported_nodes_seen: int = 0
    unsupported_nodes_seen: int = 0
    valid_candidates: int = 0
    invalid_candidates: int = 0
    issue_counts_by_code: dict[str, int] = field(default_factory=dict)

    def record_issue(self, issue_code: str) -> None:
        """Increment deterministic counter for a specific issue code."""
        self.issue_counts_by_code[issue_code] = self.issue_counts_by_code.get(issue_code, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-serializable summary of extraction counters."""
        return {
            "total_nodes_seen": self.total_nodes_seen,
            "supported_nodes_seen": self.supported_nodes_seen,
            "unsupported_nodes_seen": self.unsupported_nodes_seen,
            "valid_candidates": self.valid_candidates,
            "invalid_candidates": self.invalid_candidates,
            "issue_counts_by_code": dict(sorted(self.issue_counts_by_code.items())),
        }
