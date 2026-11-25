# GeoSearch API

A location-based place discovery service powered by Python, FastAPI, PostgreSQL/PostGIS, and OpenStreetMap data.

> Status: M1–M5 (Database Foundation, OSM Node-only Ingestion, Transactional Publication, Read-Only Search API, and Controlled Recovery & Reliability) and M6 (Acceptance Registry, S1–S5 suites, G1–G6 migration gates, GitHub Actions CI workflow, and evidence aggregation) are implemented and locally verified against PostgreSQL 16 / PostGIS 3.4 using synthetic fixtures (remote GitHub Actions CI run and explicit `IMPLEMENTATION_ACCEPTED` sign-off pending). Automated heuristic duplicate-matching thresholds, independently verified post-2025 Ho Chi Minh City boundary fixtures, Way/Relation geometry extraction, and dedicated indexing optimization remain deferred.

## Overview

GeoSearch API is a backend engineering project focused on building reliable geospatial search capabilities using OpenStreetMap data.

The service exposes a read-only REST API supporting keyword search (with Vietnamese diacritic normalization), approved category filtering, WGS84 PostGIS nearby radius discovery, and stable place details over the currently published canonical dataset.

Beyond REST API development, the project enforces database integrity, deterministic data ingestion, idempotency, transactional publication, controlled recovery preparation, and automated PostgreSQL/PostGIS integration testing.

## Technology Stack

- Python 3.12 and FastAPI / Pydantic v2 / Uvicorn
- SQLAlchemy 2.x, Psycopg 3, and Alembic
- PostgreSQL 16 with PostGIS 3.4
- Pytest and HTTPX TestClient
- Docker Compose
- OpenStreetMap data and Pyosmium
- GitHub Actions CI (`.github/workflows/ci-acceptance.yml`)

## API Endpoints (M4 — Implemented, Read-Only)

| Method | Endpoint | Purpose | Isolation Mode |
|---|---|---|---|
| GET | `/health` | Process liveness and lightweight PostgreSQL readiness | `READ COMMITTED READ ONLY` |
| GET | `/api/v1/places` | List and search active places by Vietnamese keyword and category with bounded pagination | `REPEATABLE READ READ ONLY` |
| GET | `/api/v1/places/nearby` | Find active places within a WGS84 geodesic radius in meters using PostGIS | `REPEATABLE READ READ ONLY` |
| GET | `/api/v1/places/{id}` | Retrieve public details of one active place by stable internal `places.id` | `READ COMMITTED READ ONLY` |

Interactive OpenAPI documentation is served by FastAPI at `/docs` and `/openapi.json`.

All public endpoints are strictly **read-only** (`GET` only, PostgreSQL `READ ONLY` transactions). The API never runs migrations on startup, never ingests OSM files automatically, never exposes staging tables, and never mutates canonical place or publication state.

## MVP Geographic Scope & Dataset Limitations

- **Approved Geographic Scope**: Ho Chi Minh City, Vietnam, using the administrative scope following the 2025 reorganization (including the formerly separate Binh Duong and Ba Ria–Vung Tau areas and Con Dao). Boundary data must be independently verified before authoritative publication.
- **Current Dataset Limitations (Explicitly Distinguished from API Completeness)**:
  - M4 API implementation and PostgreSQL/PostGIS verification use controlled synthetic `node_only` fixtures in isolated disposable test databases.
  - Authoritative post-2025 Ho Chi Minh City boundary verification and Way/Relation geometry extraction remain deferred (`node_only_incomplete_osm_coverage`), so production-wide `AUTHORITATIVE` publication remains blocked by M3 safeguards.
  - Dedicated search/spatial indexing optimization (e.g., GIST, GIN, trigram, partial indexes) requires a separate approved indexing strategy and is intentionally not included in M4.

## Approved Place Categories

Exactly five canonical MVP categories are accepted:

- `cafe`
- `restaurant`
- `atm`
- `pharmacy`
- `supermarket`

Unsupported category values are rejected with HTTP `422` (never silently mapped to a fallback).

## Architecture & Governance Documents

- [Architecture Baseline](docs/ARCHITECTURE_BASELINE.md) — approved engineering decisions
- [Implementation Plan](docs/IMPLEMENTATION_PLAN.md) — milestone roadmap and status
- [Acceptance Policy](docs/ACCEPTANCE_POLICY.md) — verification and sign-off rules
- [Acceptance Registry](docs/acceptance/acceptance_registry.json) — versioned requirement-to-evidence traceability (`18 + 26 + 28 + 15 = 87` policy memberships plus column/JSONB, spatial/API, and deferred items)

## Current Status

- [x] Database integrity architecture specification
- [x] Repository bootstrap (M0)
- [x] Database foundation (M1.1–M1.5: PostGIS Docker setup, five SQLAlchemy 2.x canonical models, Alembic initial migration `0001`, constraint & G1–G6 migration integration tests)
- [x] Data ingestion and validation (M2.1–M2.6: canonical models, Vietnamese `name_search` normalization, deterministic 5-category mapping, streaming Pyosmium Node-only extractor with verified source snapshots, technical staging schema `0002`, idempotent issue fingerprinting, `ST_Covers` readiness verification)
- [x] Transactional publication (M3.1–M3.7: fail-closed eligibility tiers, immutable pre-publication recovery artifacts, trigger guards `0003` & `0004`, guarded atomic `pg_advisory_xact_lock` publication, stable `places.id` UPSERT, scope-aware soft deactivation, bidirectional SQL `EXCEPT` verification, fault-injection, concurrency, and reader isolation tests)
- [x] Search API (M4.1–M4.6: FastAPI application factory, `/health`, `/api/v1/places`, `/api/v1/places/nearby`, `/api/v1/places/{id}`, Vietnamese search reuse, PostGIS WGS84 spheroid distance, adaptive `READ COMMITTED` / `REPEATABLE READ READ ONLY` snapshot consistency, leak-free error handling, and PostgreSQL/PostGIS HTTP integration & concurrency tests)
- [x] Controlled recovery execution & duplicate evidence reliability (M5: historical source artifact verification with TOCTOU isolation, traceable `RecoveryApproval`, atomic `RECOVERY` run staging & guarded publication via `execute_controlled_recovery`, retry & unknown COMMIT outcome resolution, fault-injection, concurrent API snapshot tests, and run-scoped duplicate-evidence preservation; automated duplicate-matching thresholds remain pending approval)
- [x] Full CI & acceptance evidence aggregation (M6: Acceptance Registry `docs/acceptance/acceptance_registry.json`, S1–S5 suite selectors, executable G1–G6 migration gates, `.github/workflows/ci-acceptance.yml`, and unified local/CI policy evaluator `python -m geosearch.acceptance`)

## Local Setup, Running the API & Isolated Testing

### Environment Variables: `DATABASE_URL` vs `TEST_DATABASE_URL`

GeoSearch strictly separates runtime/development and test database targets:

- **`DATABASE_URL`**: Required by Alembic CLI migrations and the FastAPI runtime (`geosearch.api.app:create_app`). Defaults in `.env.example` to `postgres_dev` (`127.0.0.1:55432/geosearch_dev`). The API **never** falls back to `TEST_DATABASE_URL`.
- **`TEST_DATABASE_URL`**: Used exclusively by `pytest` and test fixtures. Defaults in `.env.example` to the ephemeral `tmpfs`-backed `postgres_test` container (`127.0.0.1:55433/geosearch_test`).
- **Precedence**: When `python-dotenv` is installed, local `.env` is loaded with `override=False`, so explicitly exported shell environment variables always take precedence. Never commit `.env` or real credentials.
- **Optional API runtime tuning environment variables** (bounded positive integers):
  - `GEOSEARCH_DB_POOL_SIZE` (default: `5`, range: `1..100`)
  - `GEOSEARCH_DB_MAX_OVERFLOW` (default: `10`, range: `0..100`)
  - `GEOSEARCH_DB_POOL_TIMEOUT` (default: `10` seconds, range: `1..120`)
  - `GEOSEARCH_DB_CONNECT_TIMEOUT` (default: `5` seconds, range: `1..120`)
  - `GEOSEARCH_DB_STATEMENT_TIMEOUT_MS` (default: `5000` ms, range: `100..60000`)
  - `GEOSEARCH_HEALTH_CHECK_TIMEOUT_MS` (default: `2000` ms, range: `100..60000`)

### Setup & Verification Steps

1. Create and activate a Python 3.12 virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   # Windows PowerShell: .\.venv\Scripts\Activate.ps1
   # Linux/macOS:        source .venv/bin/activate
   pip install -e ".[dev]"
   ```
   > Core-only installation (`pip install .`) supports Alembic and FastAPI when `DATABASE_URL` is exported directly (`python-dotenv` is optional).
2. Copy `.env.example` to `.env` (or export `DATABASE_URL` and `TEST_DATABASE_URL` in your shell).
3. Start PostgreSQL/PostGIS containers (`postgis/postgis:16-3.4`, localhost-only ports `55432` for dev and `55433` for test):
   ```bash
   docker compose up -d --wait
   ```
4. Apply and verify Alembic migrations against `DATABASE_URL`:
   ```bash
   alembic upgrade head
   alembic current
   alembic check
   ```
5. Start the FastAPI server against `DATABASE_URL` using the factory entry point:
   ```bash
   uvicorn geosearch.api.app:create_app --factory --host 127.0.0.1 --port 8000
   ```
6. Run the full unit and isolated PostgreSQL/PostGIS integration test suite against `TEST_DATABASE_URL`:
   ```bash
   pytest -v
   ```
7. Run the unified M6 local acceptance verification pipeline (validates the Acceptance Registry, executes S1–S5 suites with JUnit XMLs, runs G1–G6 migration gates on isolated disposable databases, evaluates policy compliance, and writes `artifacts/acceptance/evidence_bundle.json` and `artifacts/acceptance/acceptance_report.md`):
   ```bash
   python -m geosearch.acceptance run-local
   ```

### Acceptance Test Suites (S1–S5) & Migration Gates (G1–G6)

Every test in `tests/` is deterministically assigned to exactly one suite marker (`s1`–`s5`) via `src/geosearch/acceptance/suites.py` and `tests/conftest.py`:

| Suite | Command | Database Required | Scope |
|---|---|---|---|
| S1 — Unit | `pytest -m s1 -v` | No | Pure Python validation, normalization, classification, DB URL guards, and M6 policy evaluation |
| S2 — Database Integration | `pytest -m s2 -v` | Yes (`TEST_DATABASE_URL`) | Real PostgreSQL/PostGIS constraints, staging integration, spatial queries, and API query endpoints |
| S3 — Migration | `pytest -m s3 -v` | Yes (`TEST_DATABASE_URL`) | Alembic G1–G6 lifecycle, catalog contract, drift, and PostGIS reflection checks |
| S4 — Workflow | `pytest -m s4 -v` | Yes (`TEST_DATABASE_URL`) | Publication, controlled recovery, durable evidence, history, and duplicate-candidate workflows |
| S5 — Race & Fault Injection | `pytest -m s5 -v` | Yes (`TEST_DATABASE_URL`) | Independent connections, deterministic checkpoints, rollback, concurrency, and snapshot isolation |

Standalone verification subcommands (shared identically by local verification and `.github/workflows/ci-acceptance.yml`):

```bash
python -m geosearch.acceptance validate-registry
python -m geosearch.acceptance run-migration-gates --output-dir artifacts/acceptance
python -m geosearch.acceptance evaluate --output-dir artifacts/acceptance
```

Generated local acceptance artifacts are written under `artifacts/acceptance/` (ignored by Git):
- `artifacts/acceptance/evidence_bundle.json` — machine-readable evidence bundle with revision, spec/registry/fixture checksums, S1–S5 JUnit summaries, G1–G6 results, blocking status, deferred items, and append-only attempt history
- `artifacts/acceptance/acceptance_report.md` — concise human-readable technical acceptance report
- `artifacts/acceptance/migration_gates.json` & `artifacts/acceptance/schema_verification.json` — live PostgreSQL/PostGIS G1–G6 and catalog contract outputs
- `artifacts/acceptance/junit/s1_unit.xml` .. `s5_race_fault.xml` — JUnit XML results with SHA-256 and commit-bound metadata

`TECHNICALLY_VERIFIED` is emitted only when all blocking requirements for the declared `SYNTHETIC_MVP_VERIFICATION` scope pass with fresh, non-stale evidence. `IMPLEMENTATION_ACCEPTED` is never auto-assigned and requires explicit project-owner or reviewer sign-off. When rerunning `.github/workflows/ci-acceptance.yml` in GitHub Actions, choose **Re-run all jobs** so all S1–S5 and G1–G6 artifacts are uploaded under the current `github.run_attempt`.

## Search API Contract Reference

> **Contract note**: Endpoint paths, the 5 approved categories, active-only canonical dataset semantics, Vietnamese normalization rules, WGS84 coordinate bounds, and adaptive `READ COMMITTED` / `REPEATABLE READ READ ONLY` isolation come from [Architecture Baseline](docs/ARCHITECTURE_BASELINE.md). Parameter names (`q`, `category`, `page`, `limit`, `lon`, `lat`, `radius_m`), pagination bounds, maximum radius (`50000` m), and JSON envelope shapes below are conservative M4 implementation defaults.

### Coordinate Order & Distance Semantics

- Coordinates are always explicit WGS84 fields: **`longitude` = X** in `[-180, 180]` and **`latitude` = Y** in `[-90, 90]`. Non-finite values (`NaN`, `Infinity`, `-Infinity`) and out-of-range coordinates are rejected with HTTP `422`.
- Nearby queries use PostGIS `GEOGRAPHY(Point, 4326)` with **WGS84 spheroidal distance** (`use_spheroid = true`) in **meters** consistently across `ST_DWithin` membership, returned `distance_m` (`ST_Distance`), and closest-first sorting.

### 1. `GET /health`

Runs a bounded read-only `SELECT 1` check against PostgreSQL.

- **200 OK**:
  ```json
  {
    "status": "ok",
    "service": "geosearch-api",
    "database": "ok"
  }
  ```
- **503 Service Unavailable** (database unreachable or timed out; no credentials or SQL leaked):
  ```json
  {
    "status": "degraded",
    "service": "geosearch-api",
    "database": "unavailable"
  }
  ```

### 2. `GET /api/v1/places`

List and search active canonical places in the currently published dataset.

| Parameter | Type | Default | Constraints / Notes |
|---|---|---|---|
| `q` | string (optional) | `null` | Max 200 chars; normalized with M2 Vietnamese normalizer (diacritics, `đ/Đ -> d`, NFC/NFD, case folding, whitespace, invisible `Cf` removal); `%`, `_`, `\` treated literally; blank/whitespace treated as no filter |
| `category` | string[] (optional) | `null` | Repeatable parameter (`?category=cafe&category=atm`); strictly limited to the 5 approved categories |
| `page` | integer | `1` | `1 <= page <= 100000` |
| `limit` | integer | `20` | `1 <= limit <= 100` |

Ordering: deterministic `name_search ASC NULLS LAST, id ASC`.

Example request:
```bash
curl "http://127.0.0.1:8000/api/v1/places?q=ca+phe&category=cafe&page=1&limit=20"
```

Example response (`200 OK`):
```json
{
  "items": [
    {
      "id": 1,
      "osm_type": "node",
      "osm_id": 1001,
      "name": "Cà Phê Sài Gòn",
      "category": "cafe",
      "address": "12 Lê Lợi, Quận 1",
      "longitude": 106.7011,
      "latitude": 10.7771
    }
  ],
  "total": 1,
  "page": 1,
  "limit": 20,
  "has_more": false
}
```

### 3. `GET /api/v1/places/nearby`

Find active canonical places within a geodesic radius in meters.

| Parameter | Type | Default | Constraints / Notes |
|---|---|---|---|
| `lon` | float (required) | — | Finite WGS84 longitude (X) in `[-180, 180]` |
| `lat` | float (required) | — | Finite WGS84 latitude (Y) in `[-90, 90]` |
| `radius_m` | float | `1000.0` | Finite meters, `0 < radius_m <= 50000` |
| `q` | string (optional) | `null` | Same Vietnamese normalization and escaping as `/api/v1/places` |
| `category` | string[] (optional) | `null` | Repeatable approved category filter |
| `page` | integer | `1` | `1 <= page <= 100000` |
| `limit` | integer | `20` | `1 <= limit <= 100` |

Ordering: closest-first `distance_m ASC`, with deterministic tie-breaking `id ASC`.

Example request:
```bash
curl "http://127.0.0.1:8000/api/v1/places/nearby?lon=106.7009&lat=10.7769&radius_m=1500&category=cafe"
```

Example response (`200 OK`):
```json
{
  "items": [
    {
      "id": 1,
      "osm_type": "node",
      "osm_id": 1001,
      "name": "Cà Phê Sài Gòn",
      "category": "cafe",
      "address": "12 Lê Lợi, Quận 1",
      "longitude": 106.7011,
      "latitude": 10.7771,
      "distance_m": 31.21
    }
  ],
  "total": 1,
  "page": 1,
  "limit": 20,
  "has_more": false,
  "center": {
    "longitude": 106.7009,
    "latitude": 10.7769
  },
  "radius_m": 1500.0
}
```

### 4. `GET /api/v1/places/{id}`

Retrieve public details of a single active place by stable internal `places.id` (`1 <= id <= 9223372036854775807`). Returns `404` for nonexistent places, inactive places, staging-only rows, or when no active publication exists. Public responses exclude internal columns such as `raw_tags`, `name_search`, `is_active`, and `last_seen_import_id`.

Example request:
```bash
curl "http://127.0.0.1:8000/api/v1/places/1"
```

Example response (`200 OK`):
```json
{
  "id": 1,
  "osm_type": "node",
  "osm_id": 1001,
  "name": "Cà Phê Sài Gòn",
  "category": "cafe",
  "address": "12 Lê Lợi, Quận 1",
  "longitude": 106.7011,
  "latitude": 10.7771,
  "osm_version": 3,
  "osm_timestamp": "2026-01-20T00:00:00Z"
}
```

### Error Responses

All non-health API errors return a consistent JSON envelope without exposing SQL statements, credentials, connection strings, or stack traces:

```json
{
  "error": {
    "code": "PLACE_NOT_FOUND",
    "message": "Place not found."
  }
}
```

| HTTP Status | Error Code | Meaning |
|---|---|---|
| `404` | `PLACE_NOT_FOUND` / `NOT_FOUND` | Requested active place or route not found |
| `405` | `METHOD_NOT_ALLOWED` | Non-`GET` method on read-only endpoint |
| `422` | `VALIDATION_ERROR` | Invalid path/query parameter, coordinate, radius, pagination, or category (includes sanitized `details`) |
| `503` | `DATABASE_UNAVAILABLE` | PostgreSQL connection or pool unavailable |
| `504` | `DATABASE_TIMEOUT` | PostgreSQL query exceeded bounded `statement_timeout` |
| `500` | `INTERNAL_SERVER_ERROR` | Masked unexpected server failure |

### Read Consistency Guarantees

- **`GET /api/v1/places/{id}` & `GET /health`**: Single SQL statement under PostgreSQL `READ COMMITTED, READ ONLY`.
- **`GET /api/v1/places` & `GET /api/v1/places/nearby`**: Multi-statement (`COUNT(*)` + paginated `SELECT`) executed inside one short PostgreSQL `REPEATABLE READ, READ ONLY` transaction on a single connection, guaranteeing `total` and `items` always reflect the exact same committed publication snapshot even during concurrent atomic V1 -> V2 publication.
- **No cross-request snapshot guarantee**: Separate HTTP requests open separate transactions; deterministic `ORDER BY` does not guarantee cross-request pagination stability across a concurrent publication switch.
- Ordinary API reads rely on PostgreSQL MVCC and never acquire M3 publication advisory locks.

## OSM Data Ingestion (M2 — Node-Only Staging)

- **Supplying a real OSM source file later**: Download a Geofabrik Vietnam `.osm.pbf` extract locally (never commit `*.osm.pbf` files to Git; they are ignored in `.gitignore`), verify its SHA-256 checksum via `geosearch.ingestion.compute_file_sha256`, and supply the file path and an existing `boundary_versions.id` to `geosearch.ingestion.ingest_osm_nodes_to_staging(...)`.
- **Current ingestion scope**: M2 performs streaming, bounded-memory **Node-only** extraction (`extraction_scope="node_only"`) from an isolated verified source snapshot into run-scoped technical staging tables (`staging_place_candidates`, `staging_issue_fingerprints`, `staging_run_checkpoints`) and `import_issues` without modifying the live `places` table.
- **Deferred items**:
  - Way and Relation geometry extraction (requires node-location caching / polygon centroid or representative-point design).
  - Authoritative post-2025 Ho Chi Minh City boundary polygon (including former Binh Duong, former Ba Ria–Vung Tau, and Con Dao) and independently verified geographic reference fixtures.
  - Approved Vietnamese address formatting rules (M2 uses a minimal lossless extraction from `addr:full` / present `addr:*` tags while preserving all original tags in `raw_tags`).
  - Automated heuristic duplicate-detection matching thresholds and recovery duplicate-copying policy (awaiting formal business-rule approval; M3/M5 enforce canonical pair ordering, per-run uniqueness, immutability, historical retention, and no auto-merge).
  - Dedicated indexing optimization strategy (explicitly out of scope until approved).

## Controlled Recovery Procedure (M5 — Operator Reference)

Controlled recovery restores a previously published canonical dataset (e.g., `V1`) while a newer publication (e.g., `V2`) is active, without `TRUNCATE`, hard POI deletion, historical rewrites, or database resets. The public Search API remains strictly read-only.

### 1. Internal Operator Entry Points

Module: `geosearch.publication` (`src/geosearch/publication/recovery.py`)

- `verify_historical_source_artifact(conn, artifact_dir=..., publication_mode=...)` — read-only verification of a historical artifact's manifest, format version (`geosearch.recovery_artifact.v1`), SHA-256 digests, canonical JSONL records, M2 `name_search` parity, PostGIS EWKB & `ST_Covers` parity, historical `import_runs` provenance (`PUBLISHED` or `SUPERSEDED`), `APPROVED` boundary provenance + geometry EWKB hash, and existing stable `places.id` integrity.
- `build_recovery_approval_from_artifact(source_artifact, publication_operation_id=..., expected_active_run_id=..., approved_by=..., approval_reason=...)` — creates a typed `RecoveryApproval` bound cryptographically to the verified artifact, operation UUID, and expected active run.
- `prepare_recovery_run(engine, source_artifact_dir=..., publication_operation_id=..., approval=..., publication_mode=...)` — copies the source artifact to an isolated read-only snapshot, verifies it (and confirms the source directory on disk did not change before staging), and atomically creates (or idempotently resumes) a `PREPARED` `RECOVERY` run bound to `publication_operation_id`.
- `execute_controlled_recovery(engine, source_artifact_dir=..., publication_operation_id=..., approval=..., publication_mode=..., protection_artifact_dir=... | protection_artifact_root=...)` — end-to-end internal entry point combining `prepare_recovery_run` and M3's guarded `publish_prepared_run`.

### 2. Distinguishing the Two Artifact Roles

Every replacement recovery explicitly separates two artifacts:

| Role | Parameter | Purpose & Verification |
|---|---|---|
| **V1 Source Artifact** | `source_artifact_dir` | Historical immutable artifact supplying canonical content and membership to restore. Its source `import_runs` row may be `SUPERSEDED`. Verified against historical `import_runs` + `boundary_versions` provenance and preserved untouched on disk. |
| **V2 Protection Artifact** | `protection_artifact_dir` or `protection_artifact_root` | Pre-publication protection snapshot of the **currently active** run (`V2`) captured/verified via M3's `prepare_recovery_artifact` / `verify_artifact_matches_live_baseline` under `pg_advisory_xact_lock` before replacing `V2`. |

Supplying the V1 source artifact as the V2 protection artifact is rejected fail-closed.

### 3. Recording Manual Approval & Trust Assumptions

- Every recovery requires a `RecoveryApproval` containing non-blank `approved_by`, timezone-aware `approved_at`, non-blank `approval_reason`, exact `publication_operation_id`, `source_import_run_id`, `source_artifact_manifest_sha256`, `source_artifact_content_sha256`, and `expected_active_run_id`.
- Approval metadata and source provenance are persisted durably in `staging_run_checkpoints.readiness_evidence["recovery"]` (frozen at `PREPARED` by `0004` triggers) and atomically copied into `import_runs.publication_evidence["recovery"]` at `PUBLISHED` (preserved across `SUPERSEDED`).
- `expected_active_run_id` is checked at preflight and re-checked under PostgreSQL `pg_advisory_xact_lock`; if another publication changed the active run in the meantime, the stale recovery is rejected with `PublicationConflictError`.
- **Trust assumption**: No separate approval table or reviewer authentication service is introduced under the approved 5-table schema. Human reviewer identity verification and authorization for `approved_by` are external to this internal Python library entry point.

- **Duplicate evidence during recovery (approved Option A)**: V1 and V2 `duplicate_candidates` remain on their original historical `import_run_id` rows (even after supersession or POI deactivation). By default, when detection did not run and no candidates are supplied, the new `RECOVERY` run records `duplicate_detection_state = NOT_EXECUTED` (`resolved_pair_count = 0`) without copying historical pairs into the new run or falsely reporting `VERIFIED_EMPTY`. Distinct OSM identities always remain separate canonical POIs (never auto-merged). Automated heuristic duplicate matching remains deferred pending approved distance, name-matching, and scoring rules.

### 4. Retries & Resolving an Unknown COMMIT Outcome

1. **Never generate a fresh `publication_operation_id` after an ambiguous failure or dropped connection.**
2. Inspect durable PostgreSQL evidence first:
   ```python
   from geosearch.publication import resolve_unknown_commit_outcome

   evidence = resolve_unknown_commit_outcome(
       engine,
       publication_operation_id=op_uuid,
   )
   ```
   - If `evidence is not None and evidence.is_committed_publication` (`status IN ('PUBLISHED', 'SUPERSEDED')` and `published_at IS NOT NULL`): the recovery **already committed**. Do not re-run with a new UUID.
   - If `evidence is not None and evidence.status == "PREPARED"`: the recovery preparation committed, but live publication rolled back before COMMIT. Re-invoke `execute_controlled_recovery` with the **exact same** `publication_operation_id`, `source_artifact_dir`, and `approval` to resume that prepared recovery run without creating a duplicate run.
   - If `evidence is None`: preparation rolled back completely; safe to retry with the same `publication_operation_id`.
3. Retrying `execute_controlled_recovery` with an already-committed (or subsequently `SUPERSEDED`) `publication_operation_id` returns `PublicationOutcomeStatus.ALREADY_PUBLISHED` without republishing or reactivating a superseded run. Attempting to reuse the UUID with a different artifact or approval raises `PublicationConflictError`.

## Data Attribution

This project plans to use OpenStreetMap data.

© OpenStreetMap contributors.

OpenStreetMap data is available under the Open Database License (ODbL). Data attribution, licensing, and distribution obligations must be reviewed before publishing derived datasets.

## License

The source-code license has not yet been selected.
