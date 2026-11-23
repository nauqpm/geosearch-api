"""Durable publication operation UUID inspection and guarded registration (M3.1/M3.4).

Transaction boundary contract:
- `register_publication_operation(conn, ...)` executes inside the caller's open transaction
  without emitting hidden COMMIT statements.
- `register_publication_operation_durable(engine_or_conn, ...)` commits ONLY the durable
  binding of `import_runs.publication_operation_id` while `status` remains `'PREPARED'`,
  `published_at IS NULL`, and `active_slot IS NULL`.
- Durable registration NEVER implies that live publication committed. On unknown COMMIT
  outcome, callers must inspect `PublicationOperationEvidence.is_committed_publication`
  (`status IN ('PUBLISHED', 'SUPERSEDED')` and `published_at IS NOT NULL`).
- Immutability trade-off (Case E): Under the approved five-table schema,
  `import_runs.publication_operation_id` is permanently immutable once registered (enforced by
  `trg_import_runs_guard`). If a worker registers UUID U2 on PREPARED run R2 and then loses a
  concurrent baseline race against another run R1, R2 remains `PREPARED` bound to U2 and cannot
  silently mutate its registered UUID; operators must either explicitly retry R2 with U2 against
  the newly verified baseline R1 (plus a fresh recovery artifact for R1) or stage a new ImportRun.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError

from geosearch.publication.models import (
    EligibilityStatus,
    PublicationConflictError,
    PublicationEligibilityError,
    PublicationOperationEvidence,
)

NIL_UUID = uuid.UUID("00000000-0000-0000-0000-000000000000")


def parse_publication_operation_id(value: uuid.UUID | str) -> uuid.UUID:
    """Validate and normalize a non-nil publication operation UUID."""
    if isinstance(value, uuid.UUID):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = uuid.UUID(value.strip())
        except ValueError as exc:
            raise PublicationEligibilityError(
                f"Invalid publication_operation_id UUID: {value!r}.",
                status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
                reasons=(f"Invalid publication_operation_id UUID: {value!r}.",),
            ) from exc
    else:
        raise PublicationEligibilityError(
            f"publication_operation_id must be a valid non-nil UUID, got {value!r}.",
            status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
            reasons=("publication_operation_id must be a valid non-nil UUID.",),
        )

    if parsed == NIL_UUID:
        raise PublicationEligibilityError(
            "Nil UUID (00000000-0000-0000-0000-000000000000) is not a valid publication_operation_id.",
            status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
            reasons=("Nil UUID is forbidden as publication_operation_id.",),
        )
    return parsed


def _row_to_evidence(row: object) -> PublicationOperationEvidence:
    return PublicationOperationEvidence(
        import_run_id=int(getattr(row, "id")),
        status=str(getattr(row, "status")),
        active_slot=(
            int(getattr(row, "active_slot"))
            if getattr(row, "active_slot") is not None
            else None
        ),
        boundary_version_id=int(getattr(row, "boundary_version_id")),
        publication_operation_id=getattr(row, "publication_operation_id"),
        published_at=getattr(row, "published_at"),
        superseded_at=getattr(row, "superseded_at"),
        publication_evidence=(
            dict(getattr(row, "publication_evidence"))
            if isinstance(getattr(row, "publication_evidence"), dict)
            else None
        ),
    )


def inspect_publication_operation(
    conn: Connection,
    *,
    publication_operation_id: uuid.UUID | str,
) -> PublicationOperationEvidence | None:
    """Query durable import_runs evidence by publication_operation_id."""
    op_uuid = parse_publication_operation_id(publication_operation_id)
    row = conn.execute(
        text(
            """
            SELECT
                id,
                status,
                active_slot,
                boundary_version_id,
                publication_operation_id,
                published_at,
                superseded_at,
                publication_evidence
            FROM import_runs
            WHERE publication_operation_id = :op_uuid
            """
        ),
        {"op_uuid": op_uuid},
    ).one_or_none()
    return _row_to_evidence(row) if row is not None else None


def inspect_run_publication_state(
    conn: Connection,
    *,
    import_run_id: int,
) -> PublicationOperationEvidence | None:
    """Query durable import_runs publication state by import_run_id."""
    row = conn.execute(
        text(
            """
            SELECT
                id,
                status,
                active_slot,
                boundary_version_id,
                publication_operation_id,
                published_at,
                superseded_at,
                publication_evidence
            FROM import_runs
            WHERE id = :run_id
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()
    return _row_to_evidence(row) if row is not None else None


def register_publication_operation(
    conn: Connection,
    *,
    import_run_id: int,
    publication_operation_id: uuid.UUID | str,
    on_during_registration: Callable[[Connection], None] | None = None,
) -> PublicationOperationEvidence:
    """Bind publication_operation_id to a PREPARED import run under row-level locking.

    Guarantees:
    - Serializes concurrent registrations against the same import_run_id via SELECT ... FOR UPDATE.
    - Rejects reusing an operation UUID already owned by a different run (PUBLICATION_CONFLICT).
    - Rejects assigning a second operation UUID to a run already owned by another UUID.
    - Idempotent when the same (import_run_id, publication_operation_id) is already registered.
    - Does not commit; caller controls the enclosing transaction boundary.
    """
    op_uuid = parse_publication_operation_id(publication_operation_id)

    # Acquire row lock on target import_runs row first for deterministic serialization.
    target_row = conn.execute(
        text(
            """
            SELECT
                id,
                status,
                active_slot,
                boundary_version_id,
                publication_operation_id,
                published_at,
                superseded_at,
                publication_evidence
            FROM import_runs
            WHERE id = :run_id
            FOR UPDATE
            """
        ),
        {"run_id": import_run_id},
    ).one_or_none()

    if target_row is None:
        raise PublicationEligibilityError(
            f"ImportRun id={import_run_id} does not exist.",
            status=EligibilityStatus.NOT_PREPARED,
            reasons=(f"ImportRun id={import_run_id} does not exist.",),
        )

    if on_during_registration is not None:
        on_during_registration(conn)

    # Check if another run already owns op_uuid.
    uuid_owner = conn.execute(
        text(
            """
            SELECT id, status
            FROM import_runs
            WHERE publication_operation_id = :op_uuid
            """
        ),
        {"op_uuid": op_uuid},
    ).one_or_none()

    if uuid_owner is not None and int(uuid_owner.id) != import_run_id:
        reason = (
            f"publication_operation_id={op_uuid} is already owned by "
            f"ImportRun id={uuid_owner.id} (status={uuid_owner.status!r})."
        )
        raise PublicationConflictError(
            reason,
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            reasons=(reason,),
        )

    existing_op_id: uuid.UUID | None = target_row.publication_operation_id
    if existing_op_id is not None:
        if existing_op_id != op_uuid:
            reason = (
                f"ImportRun id={import_run_id} is already owned by a different "
                f"publication_operation_id={existing_op_id} (incoming={op_uuid})."
            )
            raise PublicationConflictError(
                reason,
                status=EligibilityStatus.PUBLICATION_CONFLICT,
                reasons=(reason,),
            )
        evidence = _row_to_evidence(target_row)
        if evidence.is_committed_publication or evidence.status == "PREPARED":
            return evidence
        reason = (
            f"ImportRun id={import_run_id} with registered operation {op_uuid} "
            f"is in non-publishable status {evidence.status!r}."
        )
        raise PublicationEligibilityError(
            reason,
            status=EligibilityStatus.NOT_PREPARED,
            reasons=(reason,),
        )

    if target_row.status in ("PUBLISHED", "SUPERSEDED"):
        reason = (
            f"ImportRun id={import_run_id} is already in status {target_row.status!r} "
            "and cannot accept a new publication_operation_id."
        )
        raise PublicationConflictError(
            reason,
            status=EligibilityStatus.ALREADY_PUBLISHED,
            reasons=(reason,),
        )

    if target_row.status != "PREPARED":
        reason = (
            f"Cannot register publication operation on ImportRun id={import_run_id}: "
            f"status must be 'PREPARED', found {target_row.status!r}."
        )
        raise PublicationEligibilityError(
            reason,
            status=EligibilityStatus.NOT_PREPARED,
            reasons=(reason,),
        )

    try:
        # Use a savepoint so a concurrent unique violation on uq_import_runs_publication_operation_id
        # cleanly rolls back just the conflicting UPDATE when called inside an outer transaction.
        with conn.begin_nested():
            updated = conn.execute(
                text(
                    """
                    UPDATE import_runs
                    SET publication_operation_id = :op_uuid
                    WHERE id = :run_id
                      AND status = 'PREPARED'
                      AND publication_operation_id IS NULL
                    RETURNING
                        id,
                        status,
                        active_slot,
                        boundary_version_id,
                        publication_operation_id,
                        published_at,
                        superseded_at,
                        publication_evidence
                    """
                ),
                {"op_uuid": op_uuid, "run_id": import_run_id},
            ).one_or_none()
    except IntegrityError as exc:
        orig = exc.orig
        sqlstate = getattr(orig, "sqlstate", None)
        diag = getattr(orig, "diag", None)
        constraint_name = getattr(diag, "constraint_name", None)
        if sqlstate == "23505" and constraint_name == "uq_import_runs_publication_operation_id":
            reason = (
                f"Concurrent registration conflict: publication_operation_id={op_uuid} "
                "was claimed by another ImportRun."
            )
            raise PublicationConflictError(
                reason,
                status=EligibilityStatus.PUBLICATION_CONFLICT,
                reasons=(reason,),
            ) from exc
        raise

    if updated is None:
        reason = (
            f"Failed to register publication_operation_id={op_uuid} on "
            f"ImportRun id={import_run_id}; row state changed concurrently."
        )
        raise PublicationConflictError(
            reason,
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            reasons=(reason,),
        )

    return _row_to_evidence(updated)


@contextmanager
def _short_registration_tx(engine_or_conn: Engine | Connection) -> Iterator[Connection]:
    if isinstance(engine_or_conn, Engine):
        with engine_or_conn.begin() as conn:
            yield conn
    else:
        with engine_or_conn.begin_nested():
            yield engine_or_conn


def register_publication_operation_durable(
    engine_or_conn: Engine | Connection,
    *,
    import_run_id: int,
    publication_operation_id: uuid.UUID | str,
    on_during_registration: Callable[[Connection], None] | None = None,
) -> PublicationOperationEvidence:
    """Persist durable publication operation ownership in a dedicated short transaction.

    When passed an Engine, this commits ONLY `import_runs.publication_operation_id` while
    the run stays `PREPARED` (`published_at IS NULL`, `active_slot IS NULL`).
    """
    with _short_registration_tx(engine_or_conn) as conn:
        return register_publication_operation(
            conn,
            import_run_id=import_run_id,
            publication_operation_id=publication_operation_id,
            on_during_registration=on_during_registration,
        )
