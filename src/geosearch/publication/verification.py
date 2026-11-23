"""M3.3 Step 8: In-transaction pre-COMMIT and post-switch invariant verification."""

from __future__ import annotations

import uuid

from sqlalchemy import Connection, text

from geosearch.publication.duplicates import DuplicateResolutionResult
from geosearch.publication.eligibility import compute_staging_dataset_digest
from geosearch.publication.models import (
    DuplicateDetectionState,
    PublicationInvariantError,
    PublicationMode,
)
from geosearch.publication.reconciliation import PrePublishPlaceState


def verify_pre_commit_invariants(
    conn: Connection,
    *,
    import_run_id: int,
    boundary_version_id: int,
    publication_mode: PublicationMode,
    publication_operation_id: uuid.UUID,
    expected_previous_active_run_id: int | None,
    pre_state: PrePublishPlaceState,
    duplicate_result: DuplicateResolutionResult,
    expected_staging_digest: str | None = None,
) -> int:
    """Verify every live dataset invariant inside the open publication transaction before slot switch.

    Returns the verified active place count within the authorized coverage scope.
    Raises PublicationInvariantError (triggering transaction rollback) on any violation.
    """
    if expected_staging_digest is not None:
        actual_staging_digest = compute_staging_dataset_digest(
            conn, import_run_id=import_run_id, lock_rows=False
        )
        if actual_staging_digest != expected_staging_digest:
            raise PublicationInvariantError(
                "Pre-COMMIT staging integrity violation: staging_place_candidates digest changed "
                f"during publication transaction (expected={expected_staging_digest}, "
                f"actual={actual_staging_digest})."
            )
    scope_node_only = publication_mode == PublicationMode.CONTROLLED_TEST_NODE_ONLY
    scope_predicate = "AND p.osm_type = 'node'" if scope_node_only else ""

    # 1. Bidirectional SQL EXCEPT over active identities AND full canonical content.
    live_minus_staged = conn.execute(
        text(
            f"""
            SELECT count(*)
            FROM (
                SELECT
                    p.osm_type,
                    p.osm_id,
                    p.category,
                    p.name,
                    p.name_search,
                    p.address,
                    ST_AsEWKB(p.location::geometry) AS location_ewkb,
                    p.raw_tags,
                    p.osm_version,
                    p.osm_changeset,
                    p.osm_timestamp
                FROM places p
                WHERE p.is_active = true
                  {scope_predicate}
                EXCEPT
                SELECT
                    s.osm_type,
                    s.osm_id,
                    s.category,
                    s.name,
                    s.name_search,
                    s.address,
                    ST_AsEWKB(s.location::geometry) AS location_ewkb,
                    s.raw_tags,
                    s.osm_version,
                    s.osm_changeset,
                    s.osm_timestamp
                FROM staging_place_candidates s
                WHERE s.import_run_id = :run_id
            ) AS diff_live_minus_staged
            """
        ),
        {"run_id": import_run_id},
    ).scalar_one()

    staged_minus_live = conn.execute(
        text(
            f"""
            SELECT count(*)
            FROM (
                SELECT
                    s.osm_type,
                    s.osm_id,
                    s.category,
                    s.name,
                    s.name_search,
                    s.address,
                    ST_AsEWKB(s.location::geometry) AS location_ewkb,
                    s.raw_tags,
                    s.osm_version,
                    s.osm_changeset,
                    s.osm_timestamp
                FROM staging_place_candidates s
                WHERE s.import_run_id = :run_id
                EXCEPT
                SELECT
                    p.osm_type,
                    p.osm_id,
                    p.category,
                    p.name,
                    p.name_search,
                    p.address,
                    ST_AsEWKB(p.location::geometry) AS location_ewkb,
                    p.raw_tags,
                    p.osm_version,
                    p.osm_changeset,
                    p.osm_timestamp
                FROM places p
                WHERE p.is_active = true
                  {scope_predicate}
            ) AS diff_staged_minus_live
            """
        ),
        {"run_id": import_run_id},
    ).scalar_one()

    if int(live_minus_staged) != 0 or int(staged_minus_live) != 0:
        raise PublicationInvariantError(
            "Pre-COMMIT set/content verification failed: active live places in scope do not "
            f"match staged candidates (live_minus_staged={live_minus_staged}, "
            f"staged_minus_live={staged_minus_live})."
        )

    # 2. Verify last_seen_import_id matches the new run for every active member in scope.
    mismatched_provenance = int(
        conn.execute(
            text(
                f"""
                SELECT count(*)
                FROM places p
                WHERE p.is_active = true
                  {scope_predicate}
                  AND p.last_seen_import_id <> :run_id
                """
            ),
            {"run_id": import_run_id},
        ).scalar_one()
    )
    if mismatched_provenance != 0:
        raise PublicationInvariantError(
            f"Pre-COMMIT provenance verification failed: {mismatched_provenance} active place(s) "
            f"do not have last_seen_import_id={import_run_id}."
        )

    # 3. Verify stable internal places.id was preserved for all pre-existing OSM identities.
    if pre_state.existing_ids_for_staged:
        current_rows = conn.execute(
            text(
                """
                SELECT p.osm_type, p.osm_id, p.id
                FROM places p
                JOIN staging_place_candidates s
                  ON s.osm_type = p.osm_type
                 AND s.osm_id = p.osm_id
                WHERE s.import_run_id = :run_id
                """
            ),
            {"run_id": import_run_id},
        ).fetchall()
        current_by_osm = {(str(r.osm_type), int(r.osm_id)): int(r.id) for r in current_rows}
        for osm_key, (old_id, _was_active) in pre_state.existing_ids_for_staged.items():
            new_id = current_by_osm.get(osm_key)
            if new_id != old_id:
                raise PublicationInvariantError(
                    f"Stable places.id violated for OSM identity {osm_key}: "
                    f"expected id={old_id}, found id={new_id}."
                )

    # 4. Verify no hard deletions occurred on places.
    total_places_after = int(conn.execute(text("SELECT count(*) FROM places")).scalar_one())
    if total_places_after < pre_state.total_places_before:
        raise PublicationInvariantError(
            f"Historical place rows were deleted: before={pre_state.total_places_before}, "
            f"after={total_places_after}."
        )

    # 5. For controlled node_only scope, verify zero Way/Relation active rows were deactivated.
    if scope_node_only and pre_state.active_non_node_ids_before:
        still_active_non_node = {
            int(pid)
            for pid in conn.execute(
                text(
                    """
                    SELECT id
                    FROM places
                    WHERE is_active = true
                      AND osm_type <> 'node'
                    """
                )
            ).scalars().all()
        }
        deactivated_non_nodes = pre_state.active_non_node_ids_before - still_active_non_node
        if deactivated_non_nodes:
            raise PublicationInvariantError(
                "Scope isolation violation: controlled node_only publication deactivated "
                f"non-node place IDs {sorted(deactivated_non_nodes)!r}."
            )

    # 6. Verify every active place in scope satisfies PostGIS ST_Covers against the approved boundary.
    uncovered_active = int(
        conn.execute(
            text(
                f"""
                SELECT count(*)
                FROM places p
                JOIN boundary_versions b ON b.id = :bv_id
                WHERE p.is_active = true
                  {scope_predicate}
                  AND NOT ST_Covers(b.geom, p.location::geometry)
                """
            ),
            {"bv_id": boundary_version_id},
        ).scalar_one()
    )
    if uncovered_active != 0:
        raise PublicationInvariantError(
            f"Pre-COMMIT spatial verification failed: {uncovered_active} active place(s) "
            f"fail ST_Covers against boundary_version_id={boundary_version_id}."
        )

    # 7. Verify exact duplicate candidate pair set equality for this run.
    persisted_dup_pairs = {
        (int(r.place_a_id), int(r.place_b_id))
        for r in conn.execute(
            text(
                """
                SELECT place_a_id, place_b_id
                FROM duplicate_candidates
                WHERE import_run_id = :run_id
                """
            ),
            {"run_id": import_run_id},
        ).fetchall()
    }
    for place_a_id, place_b_id in persisted_dup_pairs:
        if not (place_a_id < place_b_id):
            raise PublicationInvariantError(
                f"Duplicate candidate canonical ordering violated: ({place_a_id}, {place_b_id})."
            )
    if duplicate_result.state in (
        DuplicateDetectionState.NOT_EXECUTED,
        DuplicateDetectionState.VERIFIED_EMPTY,
    ):
        if persisted_dup_pairs:
            raise PublicationInvariantError(
                f"Unexpected duplicate_candidates rows persisted for ImportRun id={import_run_id} "
                f"under state={duplicate_result.state.value}: {sorted(persisted_dup_pairs)!r}."
            )
    elif duplicate_result.state == DuplicateDetectionState.CANDIDATES_PROVIDED:
        if persisted_dup_pairs != duplicate_result.resolved_pairs:
            missing_pairs = duplicate_result.resolved_pairs - persisted_dup_pairs
            extra_pairs = persisted_dup_pairs - duplicate_result.resolved_pairs
            raise PublicationInvariantError(
                f"Duplicate candidate set mismatch for ImportRun id={import_run_id}: "
                f"missing={sorted(missing_pairs)!r}, unexpected_extra={sorted(extra_pairs)!r}."
            )

    # 8. Verify target run still holds the expected operation UUID and PREPARED status.
    run_check = conn.execute(
        text(
            """
            SELECT status, active_slot, publication_operation_id
            FROM import_runs
            WHERE id = :run_id
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()
    if (
        run_check is None
        or run_check.status != "PREPARED"
        or run_check.active_slot is not None
        or run_check.publication_operation_id != publication_operation_id
    ):
        raise PublicationInvariantError(
            f"Target ImportRun id={import_run_id} state drifted before slot switch: {run_check!r}."
        )

    # 9. Verify the currently active run is still the expected previous active run.
    active_ids = [
        int(rid)
        for rid in conn.execute(
            text("SELECT id FROM import_runs WHERE active_slot = 1")
        ).scalars().all()
    ]
    expected_list = (
        [] if expected_previous_active_run_id is None else [expected_previous_active_run_id]
    )
    if active_ids != expected_list:
        raise PublicationInvariantError(
            f"Active publication baseline drifted inside transaction: expected {expected_list!r}, "
            f"found {active_ids!r}."
        )

    active_count = int(
        conn.execute(
            text(f"SELECT count(*) FROM places p WHERE p.is_active = true {scope_predicate}")
        ).scalar_one()
    )
    return active_count


def verify_post_switch_active_slot(
    conn: Connection,
    *,
    new_active_run_id: int,
    previous_active_run_id: int | None,
    publication_operation_id: uuid.UUID,
) -> None:
    """Verify active_slot == 1 ownership and supersession integrity immediately before COMMIT."""
    active_rows = conn.execute(
        text(
            """
            SELECT id, status, active_slot, publication_operation_id, published_at, superseded_at
            FROM import_runs
            WHERE active_slot = 1
            """
        )
    ).fetchall()
    if len(active_rows) != 1:
        raise PublicationInvariantError(
            f"Expected exactly 1 active publication after slot switch, found {len(active_rows)}."
        )
    active = active_rows[0]
    if (
        int(active.id) != new_active_run_id
        or active.status != "PUBLISHED"
        or active.publication_operation_id != publication_operation_id
        or active.published_at is None
        or active.superseded_at is not None
    ):
        raise PublicationInvariantError(
            f"Newly activated run metadata verification failed: {active!r}."
        )

    if previous_active_run_id is not None:
        prev = conn.execute(
            text(
                """
                SELECT id, status, active_slot, publication_operation_id,
                       published_at, superseded_at
                FROM import_runs
                WHERE id = :prev_id
                """
            ),
            {"prev_id": previous_active_run_id},
        ).one_or_none()
        if (
            prev is None
            or prev.status != "SUPERSEDED"
            or prev.active_slot is not None
            or prev.publication_operation_id is None
            or prev.published_at is None
            or prev.superseded_at is None
            or prev.superseded_at < prev.published_at
        ):
            raise PublicationInvariantError(
                f"Superseded run id={previous_active_run_id} verification failed: {prev!r}."
            )
