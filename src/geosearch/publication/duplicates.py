"""M3.3 Step 7 & M3.4: Canonical duplicate candidate resolution and immutable persistence.

Architecture rules:
- Keep both canonical OSM places; NEVER automatically merge POIs.
- Resolve OSM identities to stable internal `places.id` after UPSERT.
- Enforce canonical ordering `place_a_id < place_b_id` and `UNIQUE(import_run_id, place_a_id, place_b_id)`.
- Reject self-pairs, unknown identities, blank detector versions, or non-object evidence.
- Distinguish `NOT_EXECUTED` from `VERIFIED_EMPTY` and `CANDIDATES_PROVIDED` explicitly.
- Preserve immutable detection evidence; retries never duplicate or overwrite existing rows.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from sqlalchemy import Connection, text

from geosearch.db.models import OSM_TYPES
from geosearch.publication.models import (
    DuplicateCandidateInput,
    DuplicateDetectionState,
    PublicationInvariantError,
)


@dataclass(frozen=True, slots=True)
class DuplicateResolutionResult:
    """Structured outcome of run-scoped duplicate candidate resolution."""

    state: DuplicateDetectionState
    detector_version: str | None
    resolved_pairs: frozenset[tuple[int, int]]
    inserted_count: int
    existing_preserved_count: int

    def to_evidence_dict(self) -> dict[str, object]:
        """Serialize duplicate resolution provenance for import_runs.publication_evidence."""
        return {
            "state": self.state.value,
            "detector_version": self.detector_version,
            "resolved_pair_count": len(self.resolved_pairs),
            "inserted_count": self.inserted_count,
            "existing_preserved_count": self.existing_preserved_count,
        }


def resolve_and_record_duplicate_candidates(
    conn: Connection,
    *,
    import_run_id: int,
    duplicate_input: DuplicateCandidateInput,
    on_during_duplicate_resolution: Callable[[Connection], None] | None = None,
) -> DuplicateResolutionResult:
    """Resolve staged OSM duplicate pairs to canonical places.id and persist immutably."""
    if on_during_duplicate_resolution is not None:
        on_during_duplicate_resolution(conn)

    if duplicate_input.state == DuplicateDetectionState.NOT_EXECUTED:
        if duplicate_input.candidates:
            raise PublicationInvariantError(
                "DuplicateCandidateInput has state=NOT_EXECUTED but non-empty candidates."
            )
        return DuplicateResolutionResult(
            state=DuplicateDetectionState.NOT_EXECUTED,
            detector_version=None,
            resolved_pairs=frozenset(),
            inserted_count=0,
            existing_preserved_count=0,
        )

    if duplicate_input.state == DuplicateDetectionState.VERIFIED_EMPTY:
        if duplicate_input.candidates:
            raise PublicationInvariantError(
                "DuplicateCandidateInput has state=VERIFIED_EMPTY but non-empty candidates."
            )
        if not duplicate_input.detector_version or not duplicate_input.detector_version.strip():
            raise PublicationInvariantError(
                "DuplicateCandidateInput with state=VERIFIED_EMPTY requires non-blank detector_version."
            )
        return DuplicateResolutionResult(
            state=DuplicateDetectionState.VERIFIED_EMPTY,
            detector_version=duplicate_input.detector_version.strip(),
            resolved_pairs=frozenset(),
            inserted_count=0,
            existing_preserved_count=0,
        )

    if duplicate_input.state != DuplicateDetectionState.CANDIDATES_PROVIDED:
        raise PublicationInvariantError(
            f"Unsupported DuplicateDetectionState: {duplicate_input.state!r}."
        )

    if not duplicate_input.candidates:
        raise PublicationInvariantError(
            "DuplicateCandidateInput with state=CANDIDATES_PROVIDED requires at least one candidate."
        )

    # Gather all distinct OSM identities referenced by staged duplicate candidates.
    needed_identities: set[tuple[str, int]] = set()
    for idx, cand in enumerate(duplicate_input.candidates):
        if cand.osm_type_a not in OSM_TYPES or cand.osm_type_b not in OSM_TYPES:
            raise PublicationInvariantError(
                f"Duplicate candidate #{idx} has invalid osm_type "
                f"({cand.osm_type_a!r}, {cand.osm_type_b!r})."
            )
        if cand.osm_id_a <= 0 or cand.osm_id_b <= 0:
            raise PublicationInvariantError(
                f"Duplicate candidate #{idx} has non-positive osm_id."
            )
        if (cand.osm_type_a, cand.osm_id_a) == (cand.osm_type_b, cand.osm_id_b):
            raise PublicationInvariantError(
                f"Duplicate candidate #{idx} is an invalid self-pair "
                f"({cand.osm_type_a}, {cand.osm_id_a})."
            )
        if not isinstance(cand.detector_version, str) or not cand.detector_version.strip():
            raise PublicationInvariantError(
                f"Duplicate candidate #{idx} has blank detector_version."
            )
        if not isinstance(cand.evidence, dict):
            raise PublicationInvariantError(
                f"Duplicate candidate #{idx} evidence must be a JSON object."
            )
        if cand.confidence_score is not None:
            score = Decimal(cand.confidence_score)
            if score < Decimal("0") or score > Decimal("1"):
                raise PublicationInvariantError(
                    f"Duplicate candidate #{idx} confidence_score {score} out of [0, 1]."
                )
        if cand.distance_meters is not None and cand.distance_meters < 0:
            raise PublicationInvariantError(
                f"Duplicate candidate #{idx} distance_meters {cand.distance_meters} is negative."
            )
        needed_identities.add((cand.osm_type_a, int(cand.osm_id_a)))
        needed_identities.add((cand.osm_type_b, int(cand.osm_id_b)))

    # Resolve OSM identities to canonical places.id.
    osm_types_param = [t for t, _ in sorted(needed_identities)]
    osm_ids_param = [i for _, i in sorted(needed_identities)]
    resolved_rows = conn.execute(
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
        {"osm_types": osm_types_param, "osm_ids": osm_ids_param},
    ).fetchall()
    place_id_by_osm: dict[tuple[str, int], int] = {
        (str(r.osm_type), int(r.osm_id)): int(r.id) for r in resolved_rows
    }

    missing = needed_identities - set(place_id_by_osm.keys())
    if missing:
        raise PublicationInvariantError(
            f"Duplicate candidate resolution failed: missing canonical place(s) in `places` "
            f"for OSM identities {sorted(missing)!r}."
        )

    # Canonicalize pairs (place_a_id < place_b_id) and deduplicate within this call.
    canonical_rows: dict[tuple[int, int], tuple[str, Decimal | None, float | None, str]] = {}
    for cand in duplicate_input.candidates:
        id_a = place_id_by_osm[(cand.osm_type_a, int(cand.osm_id_a))]
        id_b = place_id_by_osm[(cand.osm_type_b, int(cand.osm_id_b))]
        if id_a == id_b:
            raise PublicationInvariantError(
                f"Duplicate candidate resolved to invalid self-pair place_id={id_a}."
            )
        low_id, high_id = (id_a, id_b) if id_a < id_b else (id_b, id_a)
        pair = (low_id, high_id)
        evidence_json = json.dumps(cand.evidence, sort_keys=True, ensure_ascii=False)
        entry = (
            cand.detector_version.strip(),
            Decimal(cand.confidence_score) if cand.confidence_score is not None else None,
            float(cand.distance_meters) if cand.distance_meters is not None else None,
            evidence_json,
        )
        if pair in canonical_rows:
            if canonical_rows[pair] != entry:
                raise PublicationInvariantError(
                    f"Conflicting duplicate candidate evidence for canonical pair {pair} "
                    f"in ImportRun id={import_run_id}."
                )
            continue
        canonical_rows[pair] = entry

    inserted_count = 0
    existing_preserved_count = 0
    for (place_a_id, place_b_id), (
        det_version,
        conf_score,
        dist_m,
        evidence_json,
    ) in sorted(canonical_rows.items()):
        inserted_id = conn.execute(
            text(
                """
                INSERT INTO duplicate_candidates (
                    import_run_id,
                    place_a_id,
                    place_b_id,
                    detector_version,
                    confidence_score,
                    distance_meters,
                    evidence
                )
                VALUES (
                    :run_id,
                    :place_a_id,
                    :place_b_id,
                    :detector_version,
                    :confidence_score,
                    :distance_meters,
                    CAST(:evidence_json AS jsonb)
                )
                ON CONFLICT (import_run_id, place_a_id, place_b_id) DO NOTHING
                RETURNING id
                """
            ),
            {
                "run_id": import_run_id,
                "place_a_id": place_a_id,
                "place_b_id": place_b_id,
                "detector_version": det_version,
                "confidence_score": conf_score,
                "distance_meters": dist_m,
                "evidence_json": evidence_json,
            },
        ).scalar_one_or_none()
        if inserted_id is not None:
            inserted_count += 1
        else:
            # Verify existing immutable row matches incoming replay exactly; reject conflicting evidence.
            existing_row = conn.execute(
                text(
                    """
                    SELECT detector_version, confidence_score, distance_meters, evidence
                    FROM duplicate_candidates
                    WHERE import_run_id = :run_id
                      AND place_a_id = :place_a_id
                      AND place_b_id = :place_b_id
                    """
                ),
                {
                    "run_id": import_run_id,
                    "place_a_id": place_a_id,
                    "place_b_id": place_b_id,
                },
            ).one()
            existing_evidence_json = json.dumps(
                dict(existing_row.evidence), sort_keys=True, ensure_ascii=False
            )
            existing_score = (
                Decimal(existing_row.confidence_score)
                if existing_row.confidence_score is not None
                else None
            )
            existing_dist = (
                float(existing_row.distance_meters)
                if existing_row.distance_meters is not None
                else None
            )
            if (
                str(existing_row.detector_version) != det_version
                or existing_score != conf_score
                or existing_dist != dist_m
                or existing_evidence_json != evidence_json
            ):
                raise PublicationInvariantError(
                    f"Conflicting duplicate candidate evidence on replay for canonical pair "
                    f"({place_a_id}, {place_b_id}) in ImportRun id={import_run_id}; existing "
                    "immutable evidence preserved."
                )
            existing_preserved_count += 1

    detector_versions = sorted({entry[0] for entry in canonical_rows.values()})
    resolved_detector = (
        duplicate_input.detector_version.strip()
        if duplicate_input.detector_version and duplicate_input.detector_version.strip()
        else (detector_versions[0] if len(detector_versions) == 1 else ",".join(detector_versions))
    )

    return DuplicateResolutionResult(
        state=DuplicateDetectionState.CANDIDATES_PROVIDED,
        detector_version=resolved_detector,
        resolved_pairs=frozenset(canonical_rows.keys()),
        inserted_count=inserted_count,
        existing_preserved_count=existing_preserved_count,
    )
