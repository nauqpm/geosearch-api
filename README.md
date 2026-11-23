# GeoSearch API

A location-based place discovery service powered by Python, FastAPI, PostgreSQL/PostGIS, and OpenStreetMap data.

> Status: M1 — Database Foundation (including M1.5 architecture compliance remediation) implemented and verified against PostgreSQL 16 / PostGIS 3.4, pending review before M2. Later milestones (ingestion, publication, API, recovery) are not yet implemented.

## Overview

GeoSearch API is a backend engineering project focused on building reliable geospatial search capabilities using real-world OpenStreetMap data.

The service aims to support keyword search, category filtering, nearby place discovery, and place details within the defined Ho Chi Minh City geographic scope.

Beyond REST API development, the project explores database integrity, deterministic data ingestion, idempotency, transactional publication, controlled recovery, and automated testing.

## Planned Technology Stack

- Python and FastAPI
- SQLAlchemy and Alembic
- PostgreSQL with PostGIS
- Pytest
- Docker Compose
- OpenStreetMap data and Pyosmium
- GitHub Actions

## Planned API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Service health |
| GET | `/api/v1/places` | Search by keyword and category |
| GET | `/api/v1/places/nearby` | Find places within a radius |
| GET | `/api/v1/places/{id}` | Retrieve place details |

These endpoints are planned, not yet implemented.

## MVP Geographic Scope

Ho Chi Minh City, Vietnam, using the administrative scope following the 2025 reorganization, including the areas formerly associated with Binh Duong and Ba Ria–Vung Tau, and Con Dao.

Boundary data must be independently verified before being accepted for publication.

## Initial Place Categories

- cafe
- restaurant
- atm
- pharmacy
- supermarket

## Engineering Goals

1. Build reliable REST APIs for geospatial place discovery.
2. Ingest and normalize OpenStreetMap data deterministically.
3. Preserve stable OSM identities during repeated imports.
4. Publish validated datasets atomically.
5. Protect historical audit and enable controlled recovery.
6. Verify correctness with PostgreSQL/PostGIS integration tests.
7. Maintain traceability between invariants, tests, and CI evidence.

## Architecture

See [Architecture Baseline](docs/ARCHITECTURE_BASELINE.md) for approved engineering decisions.

## Development Roadmap

See [Implementation Plan](docs/IMPLEMENTATION_PLAN.md).

## Testing and Acceptance

See [Acceptance Policy](docs/ACCEPTANCE_POLICY.md).

## Current Status

- [x] Database integrity architecture specification
- [x] Repository bootstrap
- [x] Database foundation (M1.1–M1.5: PostGIS Docker setup, five SQLAlchemy 2.x models, Alembic initial migration, constraint & migration integration tests, compliance remediation)
- [ ] Data ingestion and validation (M2)
- [ ] Transactional publication (M3)
- [ ] Search endpoints (M4)
- [ ] Recovery implementation (M5)
- [ ] Full CI acceptance (M6)

## Local Database Development & Testing (M1)

### Environment Variables: `DATABASE_URL` vs `TEST_DATABASE_URL`

GeoSearch strictly separates development and test database configuration:

- **`DATABASE_URL`**: Target database for `alembic` CLI commands and application runtime. Defaults in `.env.example` to the persistent `postgres_dev` container (`127.0.0.1:55432/geosearch_dev`).
- **`TEST_DATABASE_URL`**: Target database used exclusively by `pytest` and test fixtures. Defaults in `.env.example` to the ephemeral `tmpfs`-backed `postgres_test` container (`127.0.0.1:55433/geosearch_test`).
- Test guards fail closed if `TEST_DATABASE_URL` is unset, targets a non-local host, uses an unrecognized database name, or targets the same PostgreSQL server `(host, port)` as `DATABASE_URL` (including `localhost` / `127.0.0.1` / `::1` aliases and default port `5432`).

### Setup & Verification Steps

1. Create and activate a Python 3.12 virtual environment, then install the package with development dependencies:
   ```bash
   python -m venv .venv
   # Windows PowerShell: .\.venv\Scripts\Activate.ps1
   # Linux/macOS:        source .venv/bin/activate
   pip install -e ".[dev]"
   ```
   > Core-only installation (`pip install .`) supports the Alembic migration workflow when `DATABASE_URL` is exported directly in the environment (`python-dotenv` is optional).
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
5. Run the PostgreSQL/PostGIS integration test suite against `TEST_DATABASE_URL`:
   ```bash
   pytest -v
   ```

## Data Attribution

This project plans to use OpenStreetMap data.

© OpenStreetMap contributors.

OpenStreetMap data is available under the Open Database License (ODbL). Data attribution, licensing, and distribution obligations must be reviewed before publishing derived datasets.

## License

The source-code license has not yet been selected.
