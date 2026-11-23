# GeoSearch API — Implementation Plan

Status: IN PROGRESS (M1.1–M1.5, M2.1–M2.6, and M3.1–M3.7 implemented and verified on synthetic fixtures; ready for M3 sign-off review before M4)

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
- Health endpoint
- Place details
- Category and keyword search
- Nearby radius search
- Request validation and error handling
- API integration tests

Acceptance:
- Query results match validated fixtures
- Coordinate and radius parameters are validated
- API reads obey adaptive isolation policy

## M5 — Recovery & Reliability

Deliverables:
- Immutable recovery artifacts
- Manifests and content checksums
- Controlled recovery publication
- Historical audit preservation
- Duplicate candidate detection and evidence
- Recovery and fault-injection tests

Acceptance:
- Artifact verification is mandatory
- Recovery restores correct canonical content
- Recovery uses a new run and operation ID
- Concurrent readers see consistent snapshots

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

M3.7 — Transactional Integrity & Compliance Remediation (M3.1–M3.7 complete and verified on synthetic fixtures; awaiting sign-off review before starting M4).

Do not implement M4 or later milestones until M3 is reviewed and M4 is explicitly assigned.
