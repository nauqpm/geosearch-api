"""M3.3 Steps 5-6: Conflict-aware places UPSERT and scope-aware membership reconciliation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy import Connection, text

from geosearch.publication.models import PublicationMode


@dataclass(frozen=True, slots=True)
class PrePublishPlaceState:
    """Snapshot of relevant live places state captured under lock before UPSERT."""

    existing_ids_for_staged: dict[tuple[str, int], tuple[int, bool]]
    total_places_before: int
    active_non_node_ids_before: frozenset[int]


@dataclass(frozen=True, slots=True)
class UpsertOutcome:
    """Deterministic counters from conflict-aware staging-to-places UPSERT."""

    staged_count: int
    inserted_count: int
    updated_count: int
    reactivated_count: int


def capture_pre_publish_place_state(
    conn: Connection,
    *,
    import_run_id: int,
) -> PrePublishPlaceState:
    """Capture pre-UPSERT stable IDs, total row count, and active non-node IDs under lock."""
    matched_rows = conn.execute(
        text(
            """
            SELECT p.osm_type, p.osm_id, p.id, p.is_active
            FROM places p
            JOIN staging_place_candidates s
              ON s.osm_type = p.osm_type
             AND s.osm_id = p.osm_id
            WHERE s.import_run_id = :run_id
            ORDER BY p.osm_type, p.osm_id
            """
        ),
        {"run_id": import_run_id},
    ).fetchall()
    existing_ids = {
        (str(r.osm_type), int(r.osm_id)): (int(r.id), bool(r.is_active))
        for r in matched_rows
    }

    total_before = int(conn.execute(text("SELECT count(*) FROM places")).scalar_one())

    non_node_ids = conn.execute(
        text(
            """
            SELECT id
            FROM places
            WHERE is_active = true
              AND osm_type <> 'node'
            ORDER BY id
            """
        )
    ).scalars().all()

    return PrePublishPlaceState(
        existing_ids_for_staged=existing_ids,
        total_places_before=total_before,
        active_non_node_ids_before=frozenset(int(pid) for pid in non_node_ids),
    )


def upsert_staged_places(
    conn: Connection,
    *,
    import_run_id: int,
    pre_state: PrePublishPlaceState,
    upsert_batch_size: int = 500,
    on_during_upsert: Callable[[Connection, int], None] | None = None,
) -> UpsertOutcome:
    """UPSERT validated staged candidates into live places preserving stable places.id.

    Guarantees:
    - Uses `ON CONFLICT (osm_type, osm_id) DO UPDATE` — never deletes and reinserts.
    - Preserves `places.id` and `places.created_at`.
    - Updates `last_seen_import_id = :run_id` and `is_active = true` for EVERY staged member,
      including records whose canonical content did not change.
    - Processes staged rows in deterministic `(osm_type, osm_id)` order in bounded batches.
    - Never issues hidden COMMITs; executes strictly inside caller's transaction.
    """
    if upsert_batch_size <= 0:
        raise ValueError(f"upsert_batch_size must be positive, got {upsert_batch_size!r}.")

    staged_ids = [
        int(row_id)
        for row_id in conn.execute(
            text(
                """
                SELECT id
                FROM staging_place_candidates
                WHERE import_run_id = :run_id
                ORDER BY osm_type, osm_id
                """
            ),
            {"run_id": import_run_id},
        ).scalars().all()
    ]

    batch_index = 0
    for offset in range(0, len(staged_ids), upsert_batch_size):
        batch_ids = staged_ids[offset : offset + upsert_batch_size]
        batch_index += 1

        conn.execute(
            text(
                """
                INSERT INTO places (
                    osm_type,
                    osm_id,
                    name,
                    name_search,
                    category,
                    address,
                    location,
                    raw_tags,
                    last_seen_import_id,
                    is_active,
                    osm_version,
                    osm_changeset,
                    osm_timestamp,
                    updated_at
                )
                SELECT
                    s.osm_type,
                    s.osm_id,
                    s.name,
                    s.name_search,
                    s.category,
                    s.address,
                    s.location,
                    s.raw_tags,
                    :run_id,
                    true,
                    s.osm_version,
                    s.osm_changeset,
                    s.osm_timestamp,
                    now()
                FROM staging_place_candidates s
                WHERE s.import_run_id = :run_id
                  AND s.id = ANY(:batch_ids)
                ORDER BY s.osm_type, s.osm_id
                ON CONFLICT (osm_type, osm_id) DO UPDATE
                SET name = EXCLUDED.name,
                    name_search = EXCLUDED.name_search,
                    category = EXCLUDED.category,
                    address = EXCLUDED.address,
                    location = EXCLUDED.location,
                    raw_tags = EXCLUDED.raw_tags,
                    last_seen_import_id = EXCLUDED.last_seen_import_id,
                    is_active = true,
                    osm_version = EXCLUDED.osm_version,
                    osm_changeset = EXCLUDED.osm_changeset,
                    osm_timestamp = EXCLUDED.osm_timestamp,
                    updated_at = now()
                """
            ),
            {"run_id": import_run_id, "batch_ids": batch_ids},
        )

        if on_during_upsert is not None:
            on_during_upsert(conn, batch_index)

    existing_active = sum(
        1 for _, was_active in pre_state.existing_ids_for_staged.values() if was_active
    )
    existing_inactive = sum(
        1 for _, was_active in pre_state.existing_ids_for_staged.values() if not was_active
    )
    inserted_count = len(staged_ids) - len(pre_state.existing_ids_for_staged)

    return UpsertOutcome(
        staged_count=len(staged_ids),
        inserted_count=inserted_count,
        updated_count=existing_active,
        reactivated_count=existing_inactive,
    )


def reconcile_place_membership(
    conn: Connection,
    *,
    import_run_id: int,
    publication_mode: PublicationMode,
    on_during_reconciliation: Callable[[Connection], None] | None = None,
    on_after_deactivation: Callable[[Connection], None] | None = None,
) -> int:
    """Deactivate active places absent from the newly approved staging set within coverage scope.

    Safety rules:
    - Never hard-deletes rows from `places`.
    - Preserves `last_seen_import_id` on deactivated historical rows.
    - Under `PublicationMode.CONTROLLED_TEST_NODE_ONLY`, restricts deactivation strictly to
      `osm_type = 'node'` so Way/Relation POIs are never deactivated by a Node-only staging set.
    - Under `PublicationMode.AUTHORITATIVE`, reconciles across all active OSM types.
    """
    if on_during_reconciliation is not None:
        on_during_reconciliation(conn)

    if publication_mode == PublicationMode.CONTROLLED_TEST_NODE_ONLY:
        scope_clause = "AND p.osm_type = 'node'"
    elif publication_mode == PublicationMode.AUTHORITATIVE:
        scope_clause = ""
    else:
        raise ValueError(f"Unsupported publication_mode: {publication_mode!r}.")

    result = conn.execute(
        text(
            f"""
            UPDATE places p
            SET is_active = false,
                updated_at = now()
            WHERE p.is_active = true
              {scope_clause}
              AND NOT EXISTS (
                  SELECT 1
                  FROM staging_place_candidates s
                  WHERE s.import_run_id = :run_id
                    AND s.osm_type = p.osm_type
                    AND s.osm_id = p.osm_id
              )
            """
        ),
        {"run_id": import_run_id},
    )
    deactivated_count = int(result.rowcount or 0)

    if on_after_deactivation is not None:
        on_after_deactivation(conn)

    return deactivated_count
