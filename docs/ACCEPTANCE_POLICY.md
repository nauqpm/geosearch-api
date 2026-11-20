# GeoSearch API — Acceptance Policy

Status: APPROVED SPECIFICATION
Implementation verification: NOT STARTED

## 1. Acceptance Philosophy

A successful test run is not sufficient evidence that all architecture requirements have been verified.

Each blocking requirement must map to:

Requirement -> Scenario -> Test -> Evidence -> Result

Tests may satisfy multiple scenarios, provided the actual assertions prove each expected outcome.

## 2. Approved Acceptance Scopes

| Scope | Scenario memberships |
|---|---:|
| Fault Injection & Recovery | 18 |
| Unique Constraints & Idempotency | 26 |
| Cross-table Integrity & Recovery | 28 |
| Migration & Schema | 15 |

The combined total of 87 represents scenario memberships across scopes.

It is NOT a requirement to implement exactly 87 independent test functions.

Column validation, JSONB, spatial correctness, and independently verified boundary fixtures remain mandatory even when not included in the above total.

## 3. Core Invariants

### Column and Spatial Integrity

- Category belongs to the approved five-category domain.
- Name requirements are enforced consistently.
- Name-search normalization is validated.
- Raw tags are valid JSONB objects.
- Coordinates and geometry satisfy approved spatial rules.
- Invalid data does not silently enter canonical places.

### Uniqueness and Idempotency

- One canonical row per OSM identity.
- Stable internal place ID on UPSERT.
- Immutable run fingerprint during guarded resume.
- Durable and unique publication operation UUID.
- Retry does not apply committed publication again.
- Superseded publication is not automatically reactivated.
- Canonical duplicate pairs are unique within each import run.
- Concurrency does not cause stale overwrites.

### Cross-table and Recovery Integrity

- At most one active publication.
- Exactly one active publication after successful publication.
- Active boundary matches the published run.
- Active places match the validated staging membership.
- Active content and provenance match the published dataset.
- Publication changes commit or roll back atomically.
- Recovery restores verified artifact content.
- Publication history is preserved.
- API readers see a consistent publication snapshot.

### Migration Integrity

- Clean upgrades succeed.
- Actual PostgreSQL schema matches the approved contract.
- Required constraints are present and validated.
- Legacy invalid data is not silently destroyed.
- Failed migrations are not reported as completed.
- Downgrade safety is verified when applicable.

## 4. Required Test Suites

- S1: Unit
- S2: Database Integration
- S3: Migration
- S4: Publication & Recovery Workflow
- S5: Race Conditions & Fault Injection

Use actual PostgreSQL/PostGIS for integration tests.

Tests requiring durable COMMIT or multiple transactions must use isolated databases and independent connections.

## 5. Migration Gates

- G1: Clean Upgrade
- G2: Schema Contract Verification
- G3: Schema Drift Detection
- G4: Existing-data Compatibility
- G5: Migration Failure Safety
- G6: Conditional Downgrade/Re-upgrade

Non-reversible migrations require documented and approved recovery or roll-forward alternatives.

## 6. Blocking Result Rules

Accepted as technical evidence:

- PASS with correct environment and valid evidence.

Not accepted as a completed blocking requirement:

- FAIL
- SKIPPED
- XFAIL
- FLAKY
- MISSING
- UNVERIFIED

NOT_APPLICABLE may be accepted only where an explicit and approved applicability rule exists.

Rerunning a failed test must not silently erase the original failure.

## 7. Evidence Requirements

Each acceptance run should record:

- Git commit SHA
- Architecture specification version
- Acceptance Registry version or checksum
- PostgreSQL/PostGIS versions
- Alembic revision
- Fixture identities and checksums
- JUnit test results
- Schema verification report
- Fault-injection evidence
- Recovery verification evidence
- Final technical verification result

Do not include passwords or secret connection strings.

## 8. Completion States

### SPECIFICATION_DONE

Approved architecture decisions and acceptance criteria are defined.

### IMPLEMENTED

Code and test infrastructure exist but have not necessarily passed all required verification.

### TECHNICALLY_VERIFIED

All blocking requirements pass with valid evidence for a specific commit and environment.

### IMPLEMENTATION_ACCEPTED

Technical verification has been reviewed and explicitly signed off.

## 9. Change Control

Any change to an approved invariant or acceptance criterion must be documented and reviewed.

Do not reduce test coverage merely to make CI green.

## 10. Current Status

Database integrity specification: DONE.

Implementation and technical verification: NOT STARTED.

No acceptance result should be claimed until tests and evidence are available.
