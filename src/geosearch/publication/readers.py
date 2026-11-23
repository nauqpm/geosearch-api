"""M3.5 database read consistency helpers matching the approved adaptive isolation policy.

Approved policy (ARCHITECTURE_BASELINE.md Section 15):
- Single SQL statement reads: PostgreSQL `READ COMMITTED`. A single statement sees one
  consistent committed snapshot without partial publication states.
- Multi-statement reads requiring a shared publication snapshot: PostgreSQL
  `REPEATABLE READ READ ONLY` inside one short transaction on a single connection.
- Separate HTTP requests / transactions do NOT implicitly share a snapshot.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text


@dataclass(frozen=True, slots=True)
class ActivePublicationSnapshotView:
    """Consistent multi-statement view of active publication metadata and active places."""

    active_run_id: int | None
    boundary_version_id: int | None
    active_place_identities: tuple[tuple[str, int, int], ...]  # (osm_type, osm_id, place_id)
    active_place_names: dict[tuple[str, int], str | None]


def read_active_place_identities_single_statement(
    engine: Engine,
) -> tuple[tuple[str, int, int, int], ...]:
    """Read active places + active run in ONE SQL statement under READ COMMITTED.

    Returns tuples of `(osm_type, osm_id, place_id, active_run_id)` observed atomically.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT p.osm_type, p.osm_id, p.id AS place_id, r.id AS active_run_id
                FROM import_runs r
                JOIN places p ON p.is_active = true
                WHERE r.active_slot = 1
                ORDER BY p.osm_type, p.osm_id
                """
            )
        ).fetchall()
        return tuple(
            (str(r.osm_type), int(r.osm_id), int(r.place_id), int(r.active_run_id))
            for r in rows
        )


@contextmanager
def repeatable_read_snapshot(engine: Engine) -> Iterator[Connection]:
    """Open a short PostgreSQL `REPEATABLE READ READ ONLY` transaction on a fresh connection."""
    with engine.connect() as conn:
        with conn.begin():
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            yield conn


def read_publication_snapshot_in_tx(conn: Connection) -> ActivePublicationSnapshotView:
    """Execute multiple SQL statements inside caller's REPEATABLE READ READ ONLY transaction."""
    active_run = conn.execute(
        text(
            """
            SELECT id, boundary_version_id
            FROM import_runs
            WHERE active_slot = 1
            """
        )
    ).one_or_none()

    place_rows = conn.execute(
        text(
            """
            SELECT osm_type, osm_id, id, name
            FROM places
            WHERE is_active = true
            ORDER BY osm_type, osm_id
            """
        )
    ).fetchall()

    return ActivePublicationSnapshotView(
        active_run_id=int(active_run.id) if active_run is not None else None,
        boundary_version_id=int(active_run.boundary_version_id) if active_run is not None else None,
        active_place_identities=tuple(
            (str(r.osm_type), int(r.osm_id), int(r.id)) for r in place_rows
        ),
        active_place_names={
            (str(r.osm_type), int(r.osm_id)): r.name for r in place_rows
        },
    )
