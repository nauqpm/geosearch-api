# GeoSearch API — Implementation Plan

Status: IN PROGRESS (M1.1–M1.5 implemented and verified; awaiting review before M2)

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
- Canonical data models
- Name/category/coordinate validators
- OSM Node extraction
- Deterministic staging
- Import issues and quarantine
- Verified synthetic ingestion fixtures

Acceptance:
- Invalid data does not enter canonical places
- Source provenance is retained
- Same OSM identity remains unique
- Incomplete imports never deactivate live data

Ways and Relations are later incremental tasks.

## M3 — Transactional Publication

Deliverables:
- Prepared-run readiness checks
- Publication UUID registration
- Advisory locking and guarded state transitions
- Conflict-aware UPSERT
- Full staging membership reconciliation
- Active slot switching
- Pre-commit verification

Acceptance:
- V1 to V2 publication succeeds atomically
- Failure before COMMIT leaves previous publication unchanged
- Retry does not duplicate committed publication
- Concurrency does not cause stale overwrite

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

M1.5 — Architecture Compliance Remediation (complete; awaiting sign-off review before starting M2).

Do not implement M2 or later milestones until M1 is reviewed and M2 is explicitly assigned.
