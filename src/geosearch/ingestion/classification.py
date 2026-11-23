"""Pure deterministic OSM category mapping and object normalization pipeline (M2.2)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from geosearch.ingestion.models import (
    CanonicalPlaceCandidate,
    CategoryClassification,
    ClassificationStatus,
    IssueSeverity,
    NormalizationIssue,
    NormalizationOutcome,
    NormalizationOutcomeKind,
    PipelineStage,
    RawOsmObject,
)
from geosearch.ingestion.validation import (
    ValidationError,
    extract_address_from_tags,
    validate_coordinates,
    validate_name,
    validate_osm_identity,
    validate_osm_metadata,
    validate_raw_tags,
    validate_source_provenance,
)

# Approved MVP OSM tag -> canonical category mapping (strictly five categories).
APPROVED_OSM_CATEGORY_RULES: Final[tuple[tuple[str, str, str], ...]] = (
    ("amenity", "atm", "atm"),
    ("amenity", "cafe", "cafe"),
    ("amenity", "pharmacy", "pharmacy"),
    ("amenity", "restaurant", "restaurant"),
    ("shop", "supermarket", "supermarket"),
)

_RULES_BY_KEY: Final[dict[str, dict[str, str]]] = {
    "amenity": {
        "atm": "atm",
        "cafe": "cafe",
        "pharmacy": "pharmacy",
        "restaurant": "restaurant",
    },
    "shop": {
        "supermarket": "supermarket",
    },
}


def classify_osm_tags(raw_tags: Any) -> CategoryClassification:
    """Deterministically classify an OSM tag dictionary into the five approved categories.

    Outcomes:
    - SUPPORTED: Exactly one approved tag rule matches cleanly (single exact value).
    - UNSUPPORTED: Zero approved tag rules match.
    - AMBIGUOUS: Multiple approved categories match simultaneously, or a semicolon-delimited
      multi-value tag contains an approved category without an approved precedence rule.
    - INVALID_INPUT: raw_tags is missing or not a valid string-to-string mapping.
    """
    try:
        validated_tags = validate_raw_tags(raw_tags)
    except ValidationError as exc:
        return CategoryClassification(
            status=ClassificationStatus.INVALID_INPUT,
            category=None,
            matched_categories=(),
            matched_tags={},
            reason=str(exc),
        )

    matched_categories: list[str] = []
    matched_tags: dict[str, str] = {}
    has_semicolon_ambiguity = False

    # Iterate in canonical sorted key order for determinism independent of input dict order.
    for tag_key in sorted(_RULES_BY_KEY):
        raw_value = validated_tags.get(tag_key)
        if raw_value is None:
            continue

        value_map = _RULES_BY_KEY[tag_key]
        trimmed_value = raw_value.strip()

        if ";" in trimmed_value:
            tokens = [token.strip() for token in trimmed_value.split(";") if token.strip()]
            token_matches = sorted({value_map[tok] for tok in tokens if tok in value_map})
            if token_matches:
                matched_tags[tag_key] = raw_value
                matched_categories.extend(token_matches)
                # Any semicolon-delimited tag matching an approved token (including multi-value,
                # repeated, or trailing-delimiter values like 'cafe;' or 'cafe;cafe') lacks an
                # approved precedence/parsing rule and is deterministically quarantined.
                has_semicolon_ambiguity = True
        else:
            canonical = value_map.get(trimmed_value)
            if canonical is not None:
                matched_tags[tag_key] = raw_value
                matched_categories.append(canonical)

    unique_categories = tuple(sorted(set(matched_categories)))
    ordered_matched_tags = dict(sorted(matched_tags.items()))

    if not unique_categories:
        return CategoryClassification(
            status=ClassificationStatus.UNSUPPORTED,
            category=None,
            matched_categories=(),
            matched_tags={},
            reason=None,
        )

    if len(unique_categories) > 1 or has_semicolon_ambiguity:
        return CategoryClassification(
            status=ClassificationStatus.AMBIGUOUS,
            category=None,
            matched_categories=unique_categories,
            matched_tags=ordered_matched_tags,
            reason=(
                "Conflicting or multi-value OSM category tags matched without an approved "
                f"precedence rule: matched={list(unique_categories)}, tags={ordered_matched_tags}."
            ),
        )

    return CategoryClassification(
        status=ClassificationStatus.SUPPORTED,
        category=unique_categories[0],
        matched_categories=unique_categories,
        matched_tags=ordered_matched_tags,
        reason=None,
    )


def _try_valid_identity_pair(osm_type: Any, osm_id: Any) -> tuple[str | None, int | None]:
    """Return a valid (osm_type, osm_id) pair for import_issues or (None, None) if invalid."""
    try:
        identity = validate_osm_identity(osm_type, osm_id)
    except ValidationError:
        return None, None
    return identity.osm_type, identity.osm_id


def normalize_osm_object(raw_obj: RawOsmObject) -> NormalizationOutcome:
    """Run an unvalidated OSM object through the explicit M2 normalization pipeline.

    Pipeline:
      Raw OSM Tags
      -> Category Classification (filtering ordinary unsupported objects early)
      -> OSM Identity Validation
      -> Ambiguity Quarantine Check
      -> Field Extraction
      -> Name & Address Normalization
      -> Coordinate Validation
      -> OSM Metadata & Provenance Validation
      -> CanonicalPlaceCandidate or NormalizationIssue
    """
    # Step 1: Validate raw_tags structure (never substitute {} for missing/malformed tags).
    try:
        validated_tags = validate_raw_tags(raw_obj.tags)
    except ValidationError as exc:
        issue_osm_type, issue_osm_id = _try_valid_identity_pair(
            raw_obj.osm_type, raw_obj.osm_id
        )
        return NormalizationOutcome(
            kind=NormalizationOutcomeKind.INVALID,
            issue=NormalizationIssue(
                severity=IssueSeverity.ERROR.value,
                stage=PipelineStage.VALIDATE.value,
                issue_code=exc.issue_code,
                message=str(exc),
                osm_type=issue_osm_type,
                osm_id=issue_osm_id,
                details=exc.details,
                source_occurrence_id=raw_obj.source_occurrence_id,
            ),
            classification=CategoryClassification(
                status=ClassificationStatus.INVALID_INPUT,
                category=None,
                reason=str(exc),
            ),
        )

    # Step 2: Pure category classification (filter unsupported objects without error noise).
    classification = classify_osm_tags(validated_tags)
    if classification.status == ClassificationStatus.UNSUPPORTED:
        return NormalizationOutcome(
            kind=NormalizationOutcomeKind.UNSUPPORTED,
            classification=classification,
        )

    # Step 3: Validate OSM identity for all relevant (supported or ambiguous) objects.
    try:
        identity = validate_osm_identity(raw_obj.osm_type, raw_obj.osm_id)
    except ValidationError as exc:
        # M1 import_issues CHECK requires osm_type/osm_id to be valid or both NULL.
        return NormalizationOutcome(
            kind=NormalizationOutcomeKind.INVALID,
            issue=NormalizationIssue(
                severity=IssueSeverity.ERROR.value,
                stage=PipelineStage.VALIDATE.value,
                issue_code=exc.issue_code,
                message=str(exc),
                osm_type=None,
                osm_id=None,
                details={
                    **exc.details,
                    "matched_categories": list(classification.matched_categories),
                    "matched_tags": classification.matched_tags,
                },
                source_occurrence_id=raw_obj.source_occurrence_id,
            ),
            classification=classification,
        )

    # Step 4: Quarantine ambiguous/conflicting category tags deterministically.
    if classification.status == ClassificationStatus.AMBIGUOUS:
        return NormalizationOutcome(
            kind=NormalizationOutcomeKind.AMBIGUOUS,
            issue=NormalizationIssue(
                severity=IssueSeverity.WARNING.value,
                stage=PipelineStage.CLASSIFY.value,
                issue_code="AMBIGUOUS_CATEGORY",
                message=classification.reason or "Ambiguous OSM category tags.",
                osm_type=identity.osm_type,
                osm_id=identity.osm_id,
                details={
                    "matched_categories": list(classification.matched_categories),
                    "matched_tags": classification.matched_tags,
                },
                source_occurrence_id=raw_obj.source_occurrence_id,
            ),
            classification=classification,
        )

    assert classification.category is not None
    category = classification.category

    # Step 5: Field extraction, name/address normalization, coordinate & metadata validation.
    raw_name = validated_tags.get("name")
    try:
        cleaned_name, name_search = validate_name(raw_name, category)
        address = extract_address_from_tags(validated_tags)
        lon, lat = validate_coordinates(raw_obj.longitude, raw_obj.latitude)
        version, changeset, timestamp = validate_osm_metadata(
            osm_version=raw_obj.osm_version,
            osm_changeset=raw_obj.osm_changeset,
            osm_timestamp=raw_obj.osm_timestamp,
        )
        provenance = validate_source_provenance(raw_obj.source_provenance)
    except ValidationError as exc:
        return NormalizationOutcome(
            kind=NormalizationOutcomeKind.INVALID,
            issue=NormalizationIssue(
                severity=IssueSeverity.ERROR.value,
                stage=PipelineStage.VALIDATE.value,
                issue_code=exc.issue_code,
                message=str(exc),
                osm_type=identity.osm_type,
                osm_id=identity.osm_id,
                details={
                    "category": category,
                    **exc.details,
                },
                source_occurrence_id=raw_obj.source_occurrence_id,
            ),
            classification=classification,
        )

    # Sort raw_tags keys for deterministic serialization while preserving all original keys/values.
    canonical_raw_tags = dict(sorted(validated_tags.items()))
    candidate = CanonicalPlaceCandidate(
        osm_type=identity.osm_type,
        osm_id=identity.osm_id,
        name=cleaned_name,
        name_search=name_search,
        category=category,
        address=address,
        longitude=lon,
        latitude=lat,
        raw_tags=canonical_raw_tags,
        osm_version=version,
        osm_changeset=changeset,
        osm_timestamp=timestamp,
        source_provenance=provenance,
    )
    return NormalizationOutcome(
        kind=NormalizationOutcomeKind.VALID,
        candidate=candidate,
        classification=classification,
    )


def get_approved_category_rules_spec() -> list[dict[str, str]]:
    """Return a deterministic serializable representation of approved category rules."""
    return [
        {"tag_key": tag_key, "tag_value": tag_value, "category": category}
        for tag_key, tag_value, category in APPROVED_OSM_CATEGORY_RULES
    ]
