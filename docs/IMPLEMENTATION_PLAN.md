# GeoSearch API — Implementation Plan

Status: IN PROGRESS (M1–M4 closed on synthetic fixtures; M5 controlled recovery & reliability implemented and verified on synthetic fixtures, pending M5 sign-off review before M6)

## Working Principles

- Implement one milestone at a time.
- Every milestone must produce reviewable code and tests.
- Do not skip database constraints or implementation verification.
- Do not declare a milestone complete without executed checks.
- Preserve approved architecture decisions.

## M0 — Repository Bootstrap

Deliverables:
- README.md
- AGENTS.md
- Architecture baseline
- Implementation plan
- Acceptance policy
- Minimal project directories and tooling placeholders if needed

Acceptance:
- Documentation is consistent
- No unapproved implementation
- No committed credentials or generated runtime data
- Initial repository structure is clean

## M1 — Database Foundation

Deliverables:
- PostgreSQL/PostGIS Docker configuration
- Python project configuration
- SQLAlchemy connectivity
- Alembic initialization
- Initial versioned schema for the five primary tables
- Database smoke tests and constraint tests

Acceptance:
- Clean database upgrade succeeds
- PostGIS is available
- Required constraints exist
- Negative database tests reject invalid data
- No destructive operations on non-test databases

Implement M1 in small tasks:
- M1.1: PostgreSQL/PostGIS and Python tooling (completed)
- M1.2: SQLAlchemy models and schema contracts (completed)
- M1.3: Alembic migrations (completed)
- M1.4: Constraint and migration tests (completed)
- M1.5: Architecture compliance remediation — boundary `DRAFT -> VALIDATED -> APPROVED` lifecycle, database safety hardening, Alembic/PostGIS reflection and autogenerate fixes, core-only packaging fix, and documentation alignment (completed; pending review before M2)

## M2 — Data Validation & OSM Ingestion

Deliverables:
- Canonical data models (`src/geosearch/ingestion/models.py`) — completed (M2.1)
- Name/category/coordinate validators (`src/geosearch/ingestion/validation.py`, `classification.py`) — completed (M2.1–M2.2)
- OSM Node extraction (`src/geosearch/ingestion/osm_extractor.py`) — completed (M2.3, Node-only)
- Deterministic staging (`src/geosearch/db/staging.py`, migration `0002_m2_staging_tables`, `src/geosearch/ingestion/fingerprint.py`, `staging.py`, `pipeline.py`) — completed (M2.4)
- Import issues and quarantine (`import_issues` integration & conflict quarantine) — completed (M2.4)
- Verified synthetic ingestion fixtures (`tests/fixtures/sample_synthetic_nodes.osm`, dynamic `.osm.pbf` fixtures, `tests/test_m2_*.py`) — completed (M2.5)
- M2.6 Data Integrity & Compliance Remediation (idempotent `staging_issue_fingerprints` registry, separated pass vs cumulative counters, isolated verified source snapshots + pre/post checksum verification, strict `ST_Covers` + non-empty + zero-stale-row readiness verification, `FOR UPDATE` per-run row locking, Unicode `Cf` & semicolon fixes, deterministic concurrency & SQL CHECK tests) — completed (M2.6)

Acceptance:
- Invalid data does not enter canonical staging or live places (verified)
- Source provenance and reproducible fingerprints are retained (verified)
- Same OSM identity remains unique per run without overwriting conflicts (verified)
- Incomplete imports never pass readiness checks or modify live places (verified)
- Independently verified post-2025 Ho Chi Minh City geographic boundary and reference fixtures remain pending external authoritative data (not fabricated via synthetic fixtures)

Ways and Relations are later incremental tasks.

## M3 — Transactional Publication

Deliverables:
- Prepared-run readiness & fail-closed eligibility (`src/geosearch/publication/eligibility.py`, `models.py`) — completed (M3.1)
- Durable publication UUID registration & evidence inspection (`src/geosearch/publication/operations.py`) — completed (M3.1/M3.4)
- Immutable pre-publication recovery artifacts (`src/geosearch/publication/recovery_artifact.py`) — completed (M3.2; restore execution deferred to M5)
- PostgreSQL trigger guards (`alembic/versions/20261123_0003_m3_publication_guards.py`, `20261123_0004_m3_staging_immutability_guards.py`) — completed (M3.1/M3.4/M3.7)
- Advisory locking, guarded atomic publication, conflict-aware UPSERT, scope-aware membership reconciliation, canonical duplicate candidate resolution, active-slot switching, and bidirectional SQL `EXCEPT` + staging digest pre-commit verification (`src/geosearch/publication/engine.py`, `reconciliation.py`, `duplicates.py`, `verification.py`) — completed (M3.3–M3.4/M3.7)
- Read consistency helpers, 13-checkpoint fault-injection, deterministic concurrency, reader snapshot isolation, and M3.7 remediation integration tests (`src/geosearch/publication/readers.py`, `tests/test_m3_*.py`, `tests/test_m37_remediation.py`) — completed (M3.5–M3.7)

Acceptance:
- V1 to V2 publication succeeds atomically on synthetic fixtures (verified)
- Failure before COMMIT leaves previous publication unchanged across all 12 pre-COMMIT checkpoints (verified)
- Retry does not duplicate committed publication and resolves lost COMMIT acknowledgements via durable evidence (verified)
- Concurrency does not cause stale overwrites or duplicate active slots (verified)
- Incomplete `node_only` runs and synthetic boundaries remain blocked from `AUTHORITATIVE` publication (verified)
- Authoritative real-world HCMC boundary publication, Way/Relation full coverage, and M5 controlled restore execution remain deferred

## M4 — Search API

Deliverables:
- FastAPI application factory, configuration validation (`DATABASE_URL` isolated from `TEST_DATABASE_URL`), engine lifespan disposal, and leak-free JSON error handling (`src/geosearch/api/app.py`, `config.py`, `dependencies.py`, `errors.py`) — completed (M4.1/M4.4)
- Health endpoint `GET /health` with bounded read-only PostgreSQL readiness probe (`src/geosearch/api/routes/health.py`) — completed (M4.1)
- Place details `GET /api/v1/places/{id}` with active-only single-statement `READ COMMITTED READ ONLY` semantics and non-leaking public schema (`src/geosearch/api/routes/places.py`, `schemas/places.py`, `services/place_queries.py`) — completed (M4.2)
- Category and Vietnamese-normalized keyword search `GET /api/v1/places` reusing M2 `normalize_name_search`, literal SQL `LIKE` escaping, 5-category enforcement, bounded deterministic pagination, and `REPEATABLE READ READ ONLY` snapshot consistency — completed (M4.2/M4.4)
- PostGIS WGS84 spheroid nearby radius search `GET /api/v1/places/nearby` (`ST_DWithin` / `ST_Distance` in meters, WGS84 finiteness & bounds validation, closest-first + `id` tie-breaking, `REPEATABLE READ READ ONLY` snapshot consistency) — completed (M4.3/M4.4)
- Real PostgreSQL/PostGIS HTTP integration, spatial, fault-injection, and publication concurrency tests (`tests/test_m4_health.py`, `test_m4_places_detail.py`, `test_m4_places_search.py`, `test_m4_nearby.py`, `test_m4_api_consistency.py`) — completed (M4.5)
- README API contract reference and OpenAPI contract verification — completed (M4.6)

Acceptance:
- Query results match validated synthetic fixtures on real PostgreSQL 16 / PostGIS 3.4 (verified)
- Coordinate, radius, category, and pagination parameters are strictly validated (verified)
- API reads obey adaptive isolation policy (`READ COMMITTED READ ONLY` single-statement and `REPEATABLE READ READ ONLY` multi-statement) with zero uncommitted publication leakage and zero V1/V2 snapshot mixing (verified)
- No schema changes, no new migrations beyond `0004_m3_staging_immutability`, and no unapproved indexing strategy introduced (verified)
- **M4 Sign-Off**: Explicitly accepted (`IMPLEMENTATION_ACCEPTED` for the synthetic PostgreSQL/PostGIS test scope) on commit `bab6c0f260abeb01151082bb0e31256af44c29bc` (61 M4 tests / 449 total repository tests passed on PostgreSQL 16.4 / PostGIS 3.4.3).
- Authoritative post-2025 HCMC boundary publication, Way/Relation extraction, and M6 CI aggregation remain deferred

## M5 — Recovery & Reliability

Deliverables:
- Historical source artifact verification (`verify_historical_source_artifact`) with TOCTOU-safe isolated snapshot copying, manifest/format/checksum/JSONL/M2 `name_search`/PostGIS EWKB & `ST_Covers`/historical `import_runs` & `APPROVED` boundary provenance/stable `places.id` verification (`src/geosearch/publication/recovery.py`) — completed
- Traceable manual approval (`RecoveryApproval`, `build_recovery_approval_from_artifact`, `validate_recovery_approval`) persisted in `staging_run_checkpoints.readiness_evidence["recovery"]` and `import_runs.publication_evidence["recovery"]` under the existing five-table schema with zero new migrations (`alembic head = 0004_m3_staging_immutability`; reviewer authentication external, no approver allowlist or dual-control requirement) — completed
- Atomic recovery run preparation (`prepare_recovery_run`) and guarded controlled recovery execution (`execute_controlled_recovery`) reusing M3 `publish_prepared_run`, separating V1 source and V2 protection artifacts, preserving prior runs/UUIDs/`published_at`, supporting idempotent retry & lost COMMIT ack resolution, and blocking UUID repurposing (`src/geosearch/publication/recovery.py`, `models.py`, `eligibility.py`, `engine.py`) — completed
- Approved duplicate-candidate scope (Option 2 + Option A): automated heuristic matching deferred until distance, name-matching, and scoring rules are approved; run-scoped canonical pair ordering (`place_a_id < place_b_id`), uniqueness on `(import_run_id, place_a_id, place_b_id)`, trigger-enforced immutability, retry replay safety, historical retention on original V1/V2 runs, `NOT_EXECUTED` default on recovery runs without copying historical pairs or reporting `VERIFIED_EMPTY`, and preservation of separate canonical OSM identities (no auto-merge) verified — completed
- Isolated PostgreSQL/PostGIS recovery acceptance, 14-checkpoint pre-COMMIT fault-injection, stale approval lock-race, concurrent FastAPI `REPEATABLE READ` snapshot, and duplicate-evidence tests (`tests/test_m5_recovery_verification_and_approval.py`, `tests/test_m5_recovery_e2e_and_history.py`, `tests/test_m5_faults_concurrency_and_duplicates.py`) — completed
- Operator recovery procedure documentation in `README.md` — completed

Acceptance:
- Artifact verification and manual approval are mandatory and fail closed without altering live state (verified)
- Recovery restores exact canonical V1 content, membership, active boundary, stable `places.id`, and `last_seen_import_id` with distinct V1 source and V2 protection artifacts (verified)
- Recovery uses a new `RECOVERY` run and operation UUID, preserves history, handles retries, lost COMMIT acks, and retry after supersession safely (verified)
- Faults and stale approvals under lock leave live state consistent (verified)
- Concurrent API readers see consistent committed snapshots with zero uncommitted leakage and zero V2/V1 mixing (verified)
- Approved duplicate-evidence resolver and Option A recovery retention invariants verified; automated heuristic matching (Option 2) explicitly deferred pending formal matching-rule approval

## M6 — CI & Final Acceptance

Deliverables:
- S1–S5 test suites
- G1–G6 migration gates
- Acceptance Registry
- Evidence aggregation
- Deterministic concurrency tests
- Final acceptance report

Acceptance:
- All blocking requirements are technically verified
- Test evidence is tied to the correct commit and environment
- Explicit sign-off is completed

## Milestone Completion Report

Each completed task must report:

1. Implemented behavior
2. Changed files
3. Executed commands
4. Actual test results
5. Remaining risks
6. Architecture impact
7. Next bounded task

## Current Task

M5 — Recovery & Reliability (implemented and technically verified on isolated synthetic PostgreSQL 16 / PostGIS 3.4 fixtures with approved Option 2 / Option A duplicate scope; awaiting M5 sign-off review before M6).

Do not implement M6 until M5 is reviewed and M6 is explicitly assigned.
