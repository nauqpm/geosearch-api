# GeoSearch API — Architecture Baseline

Status: APPROVED SPECIFICATION  
Implementation status: NOT VERIFIED  
Scope: MVP backend and database integrity

## 1. Purpose

Build a reliable location-based place discovery API using OpenStreetMap data and PostgreSQL/PostGIS.

The current architecture baseline is approved. Implementation may refine technical details but must not silently change the decisions below.

## 2. Technology

- Python
- FastAPI
- SQLAlchemy
- Alembic
- PostgreSQL + PostGIS
- Pytest
- Docker Compose
- Pyosmium for OSM ingestion

## 3. API Scope

Planned endpoints:

- GET /health
- GET /api/v1/places
- GET /api/v1/places/nearby
- GET /api/v1/places/{id}

Support keyword search, category filtering, geographic radius search, and place details.

## 4. Geographic Scope

Use the post-2025 Ho Chi Minh City administrative scope, including the formerly separate Binh Duong and Ba Ria–Vung Tau areas and Con Dao.

Requirements:

- Independently verify authoritative boundary references.
- Store immutable boundary versions.
- Require approval before a boundary can be published.
- Test synthetic boundaries and independently verified geographic points.
- Use ST_Covers for geographic membership, including boundary points.

Do not assume unverified boundary coordinates are authoritative.

## 5. OSM Category Mapping

Only the following mappings are in the MVP:

- amenity=cafe -> cafe
- amenity=restaurant -> restaurant
- amenity=atm -> atm
- amenity=pharmacy -> pharmacy
- shop=supermarket -> supermarket

Do not silently extend this category domain.

## 6. Canonical Tables

Exactly five primary logical tables:

1. places
2. boundary_versions
3. import_runs
4. import_issues
5. duplicate_candidates

Technical staging tables and external immutable artifacts are allowed.

Do not add a sixth primary business table without architecture review.

## 7. Places

Requirements:

- Internal stable primary key: places.id
- OSM identity: (osm_type, osm_id)
- UNIQUE(osm_type, osm_id)
- Required category from the approved domain
- Name nullable only for ATM
- name_search follows name nullability
- Nullable address
- location GEOGRAPHY(Point,4326) NOT NULL
- raw_tags JSONB NOT NULL containing a JSON object
- last_seen_import_id linked to import provenance
- is_active for logical activation/deactivation
- Audit and source metadata where available

Do not invent missing names or addresses.

Normalize name_search in Python using Unicode casefolding, combining-mark removal, d -> d handling for Vietnamese đ, and whitespace normalization. Specifically, convert both đ and Đ to d in the canonical search representation.

Validate coordinate finiteness and latitude/longitude ranges before construction.

Use ST_MakePoint(longitude, latitude).

Do not hard-delete historical POIs during publication.

## 8. OSM Ingestion

Initial source: OpenStreetMap Vietnam PBF.

Initial extraction focuses on Nodes; Ways and Relations follow.

Principles:

- Deterministic parsing and category mapping
- Verified source checksum
- Canonical staging before publication
- Quarantine malformed or ambiguous records
- Idempotent UPSERT by OSM identity
- Preserve original decoded OSM tags
- Never write incomplete ETL output directly to live places

## 9. Boundary Versions

- Geometry type: GEOMETRY(MultiPolygon,4326)
- Canonical geometry must be valid and non-empty
- Geometry and source/version provenance are immutable
- Approval is required before publication
- Invalid geometry is rejected or reviewed; not silently repaired

Boundary version selection must be tied to the import run.

## 10. Import Runs

Each independent import execution has a unique run ID.

- Source fingerprint is not a unique run key
- Same source can be re-imported in a new run
- Resume may reuse the same run only when fingerprint and safe checkpoint match
- A published run must not return to PROCESSING
- Publication operation UUID is unique
- Publication evidence remains durable after SUPERSEDED

Fingerprint components include source snapshot, boundary version, pipeline/normalizer version, and configuration.

## 11. Publication

Use Transactional In-place Publish.

Approved protocol:

1. Acquire publication lock
2. Revalidate prepared run and approved boundary
3. Verify staging completeness and fingerprints
4. UPSERT canonical places
5. Reconcile full dataset membership
6. Deactivate identities absent from the new canonical set
7. Resolve and record duplicate candidates
8. Verify live content, membership, and boundary
9. Switch active publication metadata
10. COMMIT atomically

Use a single active slot in import_runs.

The database must prevent two active slots. The service must ensure exactly one active run after successful publication.

Record published_at in the same transaction as the live publication changes.

On unknown COMMIT outcome, inspect durable operation evidence before retrying.

Do not TRUNCATE places or destroy historical identity.

## 12. Dataset Membership

The validated staging dataset is authoritative for publication membership.

After successful publication:

active OSM identities == prepared canonical OSM identities.

Update last_seen_import_id for every place included in the newly published dataset, including places with unchanged content.

Reject incomplete or unverified staging before deactivation.

## 13. Duplicate Candidates

Suspected real-world duplicates are not equivalent to duplicate OSM identities.

- Keep both canonical OSM objects
- Do not automatically merge
- Canonical pair ordering: place_a_id < place_b_id
- UNIQUE(import_run_id, place_a_id, place_b_id)
- Keep immutable detection evidence
- Retry within a run must not duplicate or silently overwrite evidence
- Candidates from different import runs can coexist
- Inactive POIs do not erase historical evidence

Candidate detection in staging may use OSM identities before internal place IDs are available.

## 14. Recovery

Use Immutable Recovery Artifact + Controlled Restore.

- Capture canonical POI content and membership
- Preserve source publication and boundary provenance
- Validate artifact manifest and checksums
- Require manual recovery approval
- Create a new recovery run and publication UUID
- Restore using guarded atomic publication
- Keep previous runs and publication evidence immutable

A committed publication is not undone with SQL ROLLBACK.

## 15. API Read Consistency

Use adaptive isolation:

- Single-statement reads: READ COMMITTED
- Multi-query reads requiring a shared publication snapshot:
  REPEATABLE READ READ ONLY

Do not claim consistency across separate HTTP requests without an explicit version-pinning design.

## 16. Data Integrity Principles

- Strict database constraints
- Layered Python and PostgreSQL validation
- No silent data loss
- Stable identity
- Deterministic staging
- Atomic publication
- Durable operation evidence
- Controlled recovery
- Reproducible integration testing

## 17. Out of Scope for Current Implementation

- Full production scaling
- Automatic duplicate merging
- Unapproved additional business tables
- Multi-city publication
- A dedicated indexing optimization strategy
- Production-grade chaos infrastructure

## 18. Architecture Change Policy

An approved decision must not change silently.

If implementation reveals a genuine technical conflict, create a proposal explaining:

- Existing decision
- Identified conflict
- Options and trade-offs
- Recommended minimal change
- Expected migration and test impact

Obtain approval before modifying the architecture baseline.
