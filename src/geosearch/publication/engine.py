"""M3.3 & M3.4 guarded atomic transactional publication engine.

Transaction & locking protocol:
1. Durable operation evidence inspection (resolves already-committed retries and lost COMMIT
   acknowledgements before starting any write transaction).
2. Read-only preflight eligibility check (M3.1) outside write locks.
3. Pre-publication immutable recovery artifact verification or preparation (M3.2) outside
   the publication advisory lock using a single `REPEATABLE READ READ ONLY` snapshot.
4. Short guarded durable registration transaction binding `import_runs.publication_operation_id`
   while `status` remains `'PREPARED'` (`published_at IS NULL`, `active_slot IS NULL`).
5. Single explicit PostgreSQL `READ COMMITTED` publication transaction:
   - Acquire transaction-scoped `pg_advisory_xact_lock(PUBLICATION_ADVISORY_LOCK_KEY)`.
   - Revalidate operation ownership, expected active baseline, `PREPARED` run, `VERIFIED`
     staging checkpoint, fingerprints, `APPROVED` boundary, `ST_Covers`, dataset coverage
     compatibility, and live baseline match for the recovery artifact.
   - Lock target & previous active `import_runs` rows in deterministic ID order (`FOR UPDATE`).
   - Capture pre-UPSERT stable `places.id` map and active non-node IDs.
   - Conflict-aware `ON CONFLICT (osm_type, osm_id) DO UPDATE` UPSERT preserving `places.id`.
   - Scope-aware membership reconciliation (soft deactivation only; never hard deletes;
     never deactivates Way/Relation rows under `CONTROLLED_TEST_NODE_ONLY`).
   - Resolve & persist run-scoped `duplicate_candidates` without auto-merge.
   - Run pre-COMMIT bidirectional SQL `EXCEPT`, provenance, stable ID, spatial `ST_Covers`,
     and baseline invariants.
   - Atomically supersede old active run (`PUBLISHED -> SUPERSEDED`, `active_slot = NULL`)
     and activate new run (`PREPARED -> PUBLISHED`, `active_slot = 1`, `published_at = now()`).
   - Verify post-switch single active slot.
   - COMMIT atomically (or ROLLBACK completely on any pre-COMMIT failure).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import Connection, Engine, text

from geosearch.publication.duplicates import resolve_and_record_duplicate_candidates
from geosearch.publication.eligibility import (
    UNSET_BASELINE,
    compute_staging_dataset_digest,
    validate_publication_eligibility,
)
from geosearch.publication.models import (
    PUBLICATION_ADVISORY_LOCK_KEY,
    DuplicateCandidateInput,
    DuplicateDetectionState,
    EligibilityStatus,
    PublicationConflictError,
    PublicationEligibilityError,
    PublicationEligibilityResult,
    PublicationFaultHooks,
    PublicationMode,
    PublicationOperationEvidence,
    PublicationOutcomeStatus,
    PublicationResult,
    RecoveryArtifactError,
    RecoveryArtifactRecord,
)
from geosearch.publication.operations import (
    inspect_publication_operation,
    inspect_run_publication_state,
    parse_publication_operation_id,
    register_publication_operation_durable,
)
from geosearch.publication.reconciliation import (
    capture_pre_publish_place_state,
    reconcile_place_membership,
    upsert_staged_places,
)
from geosearch.publication.recovery_artifact import (
    prepare_recovery_artifact,
    verify_artifact_matches_live_baseline,
    verify_recovery_artifact,
)
from geosearch.publication.verification import (
    verify_post_switch_active_slot,
    verify_pre_commit_invariants,
)


def _raise_for_eligibility(result: PublicationEligibilityResult) -> None:
    message = (
        f"Publication blocked for ImportRun id={result.import_run_id} "
        f"[{result.status.value}]: " + "; ".join(result.reasons)
    )
    if result.status == EligibilityStatus.PUBLICATION_CONFLICT:
        raise PublicationConflictError(
            message,
            status=result.status,
            reasons=result.reasons,
            eligibility_result=result,
        )
    raise PublicationEligibilityError(
        message,
        status=result.status,
        reasons=result.reasons,
        eligibility_result=result,
    )


def _build_already_published_result(
    evidence: PublicationOperationEvidence,
    *,
    fallback_mode: PublicationMode,
) -> PublicationResult:
    """Build an idempotent ALREADY_PUBLISHED result purely from durable database evidence."""
    assert evidence.publication_operation_id is not None
    assert evidence.published_at is not None
    pub_ev: dict[str, Any] = dict(evidence.publication_evidence or {})

    raw_mode = pub_ev.get("publication_mode")
    resolved_mode = (
        PublicationMode(raw_mode)
        if isinstance(raw_mode, str) and raw_mode in {m.value for m in PublicationMode}
        else fallback_mode
    )

    dup_meta = pub_ev.get("duplicate_candidates")
    if isinstance(dup_meta, dict) and dup_meta.get("state") in {
        s.value for s in DuplicateDetectionState
    }:
        dup_state = DuplicateDetectionState(str(dup_meta["state"]))
        dup_count = int(dup_meta.get("resolved_pair_count", 0))
    else:
        dup_state = DuplicateDetectionState.NOT_EXECUTED
        dup_count = 0

    return PublicationResult(
        outcome=PublicationOutcomeStatus.ALREADY_PUBLISHED,
        import_run_id=evidence.import_run_id,
        publication_operation_id=evidence.publication_operation_id,
        boundary_version_id=evidence.boundary_version_id,
        publication_mode=resolved_mode,
        previous_active_run_id=(
            int(pub_ev["previous_active_run_id"])
            if pub_ev.get("previous_active_run_id") is not None
            else None
        ),
        published_at=evidence.published_at,
        active_place_count=int(pub_ev.get("active_place_count", 0)),
        inserted_place_count=int(pub_ev.get("inserted_place_count", 0)),
        updated_place_count=int(pub_ev.get("updated_place_count", 0)),
        reactivated_place_count=int(pub_ev.get("reactivated_place_count", 0)),
        deactivated_place_count=int(pub_ev.get("deactivated_place_count", 0)),
        duplicate_candidate_count=dup_count,
        duplicate_detection_state=dup_state,
        recovery_artifact=None,
        publication_evidence=pub_ev,
        already_committed=True,
    )


def _lock_runs_in_deterministic_order(conn: Connection, run_ids: list[int]) -> None:
    """Acquire row-level FOR UPDATE locks on import_runs in ascending ID order."""
    unique_sorted = sorted(set(run_ids))
    if not unique_sorted:
        return
    conn.execute(
        text(
            """
            SELECT id
            FROM import_runs
            WHERE id = ANY(:run_ids)
            ORDER BY id
            FOR UPDATE
            """
        ),
        {"run_ids": unique_sorted},
    ).fetchall()


def _resolve_preflight_recovery_artifact(
    engine: Engine,
    *,
    preflight: PublicationEligibilityResult,
    recovery_artifact_dir: str | Path | None,
    recovery_artifact_root: str | Path | None,
) -> RecoveryArtifactRecord | None:
    """Prepare or verify the pre-publication recovery artifact outside the write lock."""
    if preflight.is_first_publication:
        if recovery_artifact_dir is not None:
            raise RecoveryArtifactError(
                "A pre-publication recovery artifact was supplied, but no active publication "
                "exists in the database (first publication)."
            )
        return None

    assert preflight.current_active_run_id is not None

    if recovery_artifact_dir is not None:
        return verify_recovery_artifact(
            recovery_artifact_dir,
            expected_source_run_id=preflight.current_active_run_id,
        )

    if recovery_artifact_root is not None:
        artifact = prepare_recovery_artifact(
            engine,
            artifact_root=recovery_artifact_root,
        )
        if artifact is None:
            raise RecoveryArtifactError(
                f"Expected active run id={preflight.current_active_run_id} disappeared while "
                "preparing recovery artifact."
            )
        return artifact

    raise PublicationEligibilityError(
        f"Replacement publication over active ImportRun id={preflight.current_active_run_id} "
        "requires either an existing verified `recovery_artifact_dir` or `recovery_artifact_root` "
        "to generate an immutable pre-publication recovery artifact.",
        status=EligibilityStatus.MISSING_RECOVERY_ARTIFACT,
        reasons=(
            f"Missing required pre-publication recovery artifact for active ImportRun "
            f"id={preflight.current_active_run_id}.",
        ),
        eligibility_result=preflight,
    )


def resolve_unknown_commit_outcome(
    engine: Engine,
    *,
    publication_operation_id: uuid.UUID | str,
    import_run_id: int | None = None,
) -> PublicationOperationEvidence | None:
    """Inspect durable database evidence after an unknown COMMIT acknowledgement.

    Callers experiencing a dropped connection or missing acknowledgement MUST inspect
    durable evidence via this function (or retry `publish_prepared_run` with the exact same
    `publication_operation_id`) rather than generating a fresh operation UUID.
    """
    op_uuid = parse_publication_operation_id(publication_operation_id)
    with engine.connect() as conn:
        evidence = inspect_publication_operation(conn, publication_operation_id=op_uuid)
    if (
        evidence is not None
        and import_run_id is not None
        and evidence.import_run_id != import_run_id
    ):
        raise PublicationConflictError(
            f"publication_operation_id={op_uuid} belongs to ImportRun id={evidence.import_run_id}, "
            f"not requested ImportRun id={import_run_id}.",
            status=EligibilityStatus.PUBLICATION_CONFLICT,
            reasons=(
                f"Operation UUID {op_uuid} is owned by ImportRun id={evidence.import_run_id}.",
            ),
        )
    return evidence


def publish_prepared_run(
    engine: Engine,
    *,
    import_run_id: int,
    publication_operation_id: uuid.UUID | str,
    publication_mode: PublicationMode = PublicationMode.AUTHORITATIVE,
    expected_active_run_id: int | None | object = UNSET_BASELINE,
    recovery_artifact_dir: str | Path | None = None,
    recovery_artifact_root: str | Path | None = None,
    duplicate_candidates: DuplicateCandidateInput | None = None,
    upsert_batch_size: int = 500,
    fault_hooks: PublicationFaultHooks | None = None,
    additional_publication_evidence: dict[str, Any] | None = None,
) -> PublicationResult:
    """Atomically publish a verified PREPARED staging run into the live `places` table."""
    hooks = fault_hooks or PublicationFaultHooks()
    dup_input = duplicate_candidates or DuplicateCandidateInput(
        state=DuplicateDetectionState.NOT_EXECUTED
    )
    op_uuid = parse_publication_operation_id(publication_operation_id)

    # Step 0: Durable operation evidence lookup before any write or mutation.
    with engine.connect() as conn:
        existing_by_op = inspect_publication_operation(conn, publication_operation_id=op_uuid)
        if existing_by_op is not None:
            if existing_by_op.import_run_id != import_run_id:
                reason = (
                    f"publication_operation_id={op_uuid} is already owned by "
                    f"ImportRun id={existing_by_op.import_run_id} "
                    f"(status={existing_by_op.status!r})."
                )
                raise PublicationConflictError(
                    reason,
                    status=EligibilityStatus.PUBLICATION_CONFLICT,
                    reasons=(reason,),
                )
            if existing_by_op.is_committed_publication:
                return _build_already_published_result(
                    existing_by_op, fallback_mode=publication_mode
                )

    # Step 1: Preflight eligibility validation (read-only, fail-closed).
    with engine.connect() as conn:
        preflight = validate_publication_eligibility(
            conn,
            import_run_id=import_run_id,
            publication_operation_id=op_uuid,
            publication_mode=publication_mode,
            expected_active_run_id=expected_active_run_id,
            require_recovery_artifact_for_replacement=False,
            verify_content_digests=True,
        )

    if (
        preflight.status == EligibilityStatus.ALREADY_PUBLISHED
        and preflight.existing_operation_evidence is not None
        and preflight.existing_operation_evidence.publication_operation_id == op_uuid
        and preflight.existing_operation_evidence.is_committed_publication
    ):
        return _build_already_published_result(
            preflight.existing_operation_evidence,
            fallback_mode=publication_mode,
        )

    if not preflight.eligible:
        _raise_for_eligibility(preflight)

    # Fail closed BEFORE operation registration if replacement publication has neither an existing
    # artifact nor an artifact root configured (avoids burning an immutable UUID registration).
    if (
        not preflight.is_first_publication
        and recovery_artifact_dir is None
        and recovery_artifact_root is None
    ):
        raise PublicationEligibilityError(
            f"Replacement publication over active ImportRun id={preflight.current_active_run_id} "
            "requires either an existing verified `recovery_artifact_dir` or "
            "`recovery_artifact_root` to generate an immutable pre-publication recovery artifact.",
            status=EligibilityStatus.MISSING_RECOVERY_ARTIFACT,
            reasons=(
                f"Missing required pre-publication recovery artifact for active ImportRun "
                f"id={preflight.current_active_run_id}.",
            ),
            eligibility_result=preflight,
        )

    # Lock in the baseline observed during preflight so concurrent publications cannot silently
    # rebase this operation even if caller did not explicitly pass expected_active_run_id.
    effective_expected_active: int | None = (
        preflight.current_active_run_id
        if expected_active_run_id is UNSET_BASELINE
        else (int(expected_active_run_id) if expected_active_run_id is not None else None)
    )

    # Step 2: Durable pre-publication operation UUID registration (short dedicated transaction).
    # Runs BEFORE exporting external recovery artifacts so conflicting registrations do not create
    # orphan artifact directories. Commits ONLY `import_runs.publication_operation_id` while status
    # remains `PREPARED` (`published_at IS NULL`, `active_slot IS NULL`).
    reg_evidence = register_publication_operation_durable(
        engine,
        import_run_id=import_run_id,
        publication_operation_id=op_uuid,
        on_during_registration=hooks.on_during_registration,
    )
    if reg_evidence.is_committed_publication:
        return _build_already_published_result(reg_evidence, fallback_mode=publication_mode)

    # Step 3: Prepare or verify pre-publication recovery artifact OUTSIDE the write lock.
    prepared_artifact = _resolve_preflight_recovery_artifact(
        engine,
        preflight=preflight,
        recovery_artifact_dir=recovery_artifact_dir,
        recovery_artifact_root=recovery_artifact_root,
    )

    # Fault checkpoint 1: before acquiring the publication lock.
    if hooks.on_before_lock is not None:
        hooks.on_before_lock()

    # Step 4: Guarded atomic live publication transaction (PostgreSQL READ COMMITTED).
    with engine.connect() as conn:
        with conn.begin():
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")

            # Acquire transaction-scoped advisory lock; automatically released on COMMIT/ROLLBACK.
            conn.execute(
                text("SELECT pg_advisory_xact_lock(:lock_key)"),
                {"lock_key": PUBLICATION_ADVISORY_LOCK_KEY},
            )

            # Fault checkpoint 2: immediately after acquiring publication lock.
            if hooks.on_after_lock is not None:
                hooks.on_after_lock(conn)

            # Re-check under lock whether the exact same operation already committed concurrently.
            under_lock_state = inspect_run_publication_state(conn, import_run_id=import_run_id)
            if (
                under_lock_state is not None
                and under_lock_state.publication_operation_id == op_uuid
                and under_lock_state.is_committed_publication
            ):
                return _build_already_published_result(
                    under_lock_state, fallback_mode=publication_mode
                )

            # Acquire deterministic row-level FOR UPDATE locks on involved import_runs rows AND
            # target staging_run_checkpoints BEFORE revalidating eligibility so no concurrent
            # writer can slip state changes in during lock-guarded revalidation.
            current_active_row_id = conn.execute(
                text("SELECT id FROM import_runs WHERE active_slot = 1")
            ).scalar_one_or_none()
            lock_targets = [import_run_id]
            if current_active_row_id is not None:
                lock_targets.append(int(current_active_row_id))
            _lock_runs_in_deterministic_order(conn, lock_targets)

            conn.execute(
                text(
                    """
                    SELECT import_run_id
                    FROM staging_run_checkpoints
                    WHERE import_run_id = :run_id
                    FOR UPDATE
                    """
                ),
                {"run_id": import_run_id},
            ).one_or_none()

            # Revalidate ALL eligibility preconditions under lock against live state.
            reval = validate_publication_eligibility(
                conn,
                import_run_id=import_run_id,
                publication_operation_id=op_uuid,
                publication_mode=publication_mode,
                expected_active_run_id=effective_expected_active,
                require_recovery_artifact_for_replacement=True,
                recovery_artifact_dir=(
                    prepared_artifact.artifact_dir if prepared_artifact is not None else None
                ),
                verify_content_digests=True,
            )
            if not reval.eligible:
                _raise_for_eligibility(reval)

            assert reval.boundary_version_id is not None
            previous_active_run_id = reval.current_active_run_id

            # Lock staged candidate rows FOR SHARE and compute canonical staging digest to prove
            # staging remains bit-for-bit identical between revalidation and pre-COMMIT verification.
            expected_staging_digest = compute_staging_dataset_digest(
                conn,
                import_run_id=import_run_id,
                lock_rows=True,
            )

            # Re-verify immutable recovery artifact integrity & full canonical content against
            # locked live baseline rows (active boundary FOR SHARE, active places FOR UPDATE,
            # active duplicate_candidates FOR SHARE).
            verified_artifact: RecoveryArtifactRecord | None = None
            if previous_active_run_id is not None:
                if prepared_artifact is None:
                    raise PublicationEligibilityError(
                        f"Missing required recovery artifact for active run "
                        f"id={previous_active_run_id}.",
                        status=EligibilityStatus.MISSING_RECOVERY_ARTIFACT,
                        reasons=("Missing recovery artifact under publication lock.",),
                        eligibility_result=reval,
                    )
                verified_artifact = verify_recovery_artifact(
                    prepared_artifact.artifact_dir,
                    expected_source_run_id=previous_active_run_id,
                )
                verify_artifact_matches_live_baseline(
                    conn,
                    artifact=verified_artifact,
                    lock_live_rows=True,
                )
            elif prepared_artifact is not None:
                raise RecoveryArtifactError(
                    "Unexpected recovery artifact present while publishing with no active run."
                )

            # Fault checkpoint 3: after run/boundary/staging/artifact revalidation under lock.
            if hooks.on_after_revalidation is not None:
                hooks.on_after_revalidation(conn)

            # Capture pre-UPSERT stable places.id and active non-node rows.
            pre_state = capture_pre_publish_place_state(conn, import_run_id=import_run_id)

            # Fault checkpoint 4: immediately before UPSERT.
            if hooks.on_before_upsert is not None:
                hooks.on_before_upsert(conn)

            # Step 5: Conflict-aware UPSERT preserving stable places.id.
            upsert_outcome = upsert_staged_places(
                conn,
                import_run_id=import_run_id,
                pre_state=pre_state,
                upsert_batch_size=upsert_batch_size,
                on_during_upsert=hooks.on_during_upsert,
            )

            # Fault checkpoint 6: immediately after UPSERT.
            if hooks.on_after_upsert is not None:
                hooks.on_after_upsert(conn)

            # Step 6: Scope-aware dataset membership reconciliation (soft deactivation only).
            deactivated_count = reconcile_place_membership(
                conn,
                import_run_id=import_run_id,
                publication_mode=publication_mode,
                on_during_reconciliation=hooks.on_during_reconciliation,
                on_after_deactivation=hooks.on_after_deactivation,
            )

            # Step 7: Resolve & persist canonical duplicate candidates without merging.
            dup_result = resolve_and_record_duplicate_candidates(
                conn,
                import_run_id=import_run_id,
                duplicate_input=dup_input,
                on_during_duplicate_resolution=hooks.on_during_duplicate_resolution,
            )

            # Fault checkpoint 10: during pre-COMMIT verification.
            if hooks.on_during_pre_commit_verification is not None:
                hooks.on_during_pre_commit_verification(conn)

            # Step 8: Pre-COMMIT live set/content/provenance/spatial/staging invariant verification.
            active_place_count = verify_pre_commit_invariants(
                conn,
                import_run_id=import_run_id,
                boundary_version_id=reval.boundary_version_id,
                publication_mode=publication_mode,
                publication_operation_id=op_uuid,
                expected_previous_active_run_id=previous_active_run_id,
                pre_state=pre_state,
                duplicate_result=dup_result,
                expected_staging_digest=expected_staging_digest,
            )

            # Step 9: Switch active publication metadata atomically inside the same transaction.
            pub_ts: datetime = conn.execute(text("SELECT now()")).scalar_one()

            publication_evidence: dict[str, Any] = {
                "publication_mode": publication_mode.value,
                "eligibility_tier": reval.tier.value,
                "extraction_scope": reval.extraction_scope,
                "coverage_restriction": reval.coverage_restriction,
                "is_first_publication": previous_active_run_id is None,
                "previous_active_run_id": previous_active_run_id,
                "boundary_version_id": reval.boundary_version_id,
                "active_place_count": active_place_count,
                "inserted_place_count": upsert_outcome.inserted_count,
                "updated_place_count": upsert_outcome.updated_count,
                "reactivated_place_count": upsert_outcome.reactivated_count,
                "deactivated_place_count": deactivated_count,
                "duplicate_candidates": dup_result.to_evidence_dict(),
                "recovery_artifact": (
                    verified_artifact.to_evidence_dict() if verified_artifact is not None else None
                ),
            }
            if additional_publication_evidence is not None:
                if not isinstance(additional_publication_evidence, dict):
                    raise PublicationEligibilityError(
                        "additional_publication_evidence must be a dictionary.",
                        status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
                        reasons=("additional_publication_evidence must be a JSON object.",),
                    )
                collisions = sorted(
                    set(additional_publication_evidence.keys()) & set(publication_evidence.keys())
                )
                if collisions:
                    raise PublicationEligibilityError(
                        "additional_publication_evidence collides with reserved keys: "
                        f"{collisions!r}.",
                        status=EligibilityStatus.OTHER_BLOCKING_FAILURE,
                        reasons=(f"Reserved publication_evidence key collision: {collisions!r}.",),
                    )
                publication_evidence.update(additional_publication_evidence)
            else:
                # For direct publish_prepared_run calls on RECOVERY runs, carry forward durable
                # recovery provenance & approval metadata recorded in readiness_evidence.
                recovery_meta = conn.execute(
                    text(
                        """
                        SELECT cp.readiness_evidence -> 'recovery' AS rec_meta
                        FROM import_runs r
                        JOIN staging_run_checkpoints cp ON cp.import_run_id = r.id
                        WHERE r.id = :run_id
                          AND r.run_type = 'RECOVERY'
                        """
                    ),
                    {"run_id": import_run_id},
                ).scalar_one_or_none()
                if isinstance(recovery_meta, dict):
                    publication_evidence["recovery"] = recovery_meta

            if previous_active_run_id is not None:
                supersession_meta = {
                    "superseded_by_run_id": import_run_id,
                    "superseded_by_operation_id": str(op_uuid),
                }
                superseded_rows = conn.execute(
                    text(
                        """
                        UPDATE import_runs
                        SET status = 'SUPERSEDED',
                            active_slot = NULL,
                            superseded_at = GREATEST(:pub_ts, published_at),
                            publication_evidence = COALESCE(publication_evidence, '{}'::jsonb)
                                                   || CAST(:supersession_meta AS jsonb)
                        WHERE id = :prev_id
                          AND status = 'PUBLISHED'
                          AND active_slot = 1
                        """
                    ),
                    {
                        "pub_ts": pub_ts,
                        "supersession_meta": json.dumps(supersession_meta, sort_keys=True),
                        "prev_id": previous_active_run_id,
                    },
                ).rowcount
                if superseded_rows != 1:
                    raise PublicationConflictError(
                        f"Expected to supersede active run id={previous_active_run_id}, "
                        f"updated {superseded_rows} row(s).",
                        status=EligibilityStatus.PUBLICATION_CONFLICT,
                        reasons=("Failed to supersede previous active run atomically.",),
                    )

            # Fault checkpoint 11: mid active-slot switch (after superseding old, before activating new).
            if hooks.on_during_active_slot_switch is not None:
                hooks.on_during_active_slot_switch(conn)

            activated_rows = conn.execute(
                text(
                    """
                    UPDATE import_runs
                    SET status = 'PUBLISHED',
                        active_slot = 1,
                        published_at = :pub_ts,
                        publication_evidence = CAST(:pub_evidence_json AS jsonb)
                    WHERE id = :new_id
                      AND status = 'PREPARED'
                      AND active_slot IS NULL
                      AND publication_operation_id = :op_uuid
                    """
                ),
                {
                    "pub_ts": pub_ts,
                    "pub_evidence_json": json.dumps(
                        publication_evidence, sort_keys=True, ensure_ascii=False
                    ),
                    "new_id": import_run_id,
                    "op_uuid": op_uuid,
                },
            ).rowcount
            if activated_rows != 1:
                raise PublicationConflictError(
                    f"Expected to activate PREPARED run id={import_run_id}, "
                    f"updated {activated_rows} row(s).",
                    status=EligibilityStatus.PUBLICATION_CONFLICT,
                    reasons=("Failed to activate target PREPARED run atomically.",),
                )

            verify_post_switch_active_slot(
                conn,
                new_active_run_id=import_run_id,
                previous_active_run_id=previous_active_run_id,
                publication_operation_id=op_uuid,
            )

            # Fault checkpoint 12: immediately before COMMIT.
            if hooks.on_before_commit is not None:
                hooks.on_before_commit(conn)

            # Step 10: COMMIT occurs cleanly upon exiting `with conn.begin():`.

    # Fault checkpoint 13: AFTER successful COMMIT, before returning acknowledgement to caller.
    if hooks.on_after_commit_before_ack is not None:
        hooks.on_after_commit_before_ack(import_run_id, op_uuid)

    return PublicationResult(
        outcome=PublicationOutcomeStatus.PUBLISHED,
        import_run_id=import_run_id,
        publication_operation_id=op_uuid,
        boundary_version_id=reval.boundary_version_id,
        publication_mode=publication_mode,
        previous_active_run_id=previous_active_run_id,
        published_at=pub_ts,
        active_place_count=active_place_count,
        inserted_place_count=upsert_outcome.inserted_count,
        updated_place_count=upsert_outcome.updated_count,
        reactivated_place_count=upsert_outcome.reactivated_count,
        deactivated_place_count=deactivated_count,
        duplicate_candidate_count=len(dup_result.resolved_pairs),
        duplicate_detection_state=dup_result.state,
        recovery_artifact=verified_artifact,
        publication_evidence=publication_evidence,
        already_committed=False,
    )
