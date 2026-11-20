# GeoSearch API

A location-based place discovery service powered by Python, FastAPI, PostgreSQL/PostGIS, and OpenStreetMap data.

> Status: Architecture baseline completed for database integrity. Implementation not started.

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
- [ ] Repository bootstrap
- [ ] Database foundation
- [ ] Data ingestion and validation
- [ ] Transactional publication
- [ ] Search endpoints
- [ ] Recovery implementation
- [ ] Full CI acceptance

## Data Attribution

This project plans to use OpenStreetMap data.

© OpenStreetMap contributors.

OpenStreetMap data is available under the Open Database License (ODbL). Data attribution, licensing, and distribution obligations must be reviewed before publishing derived datasets.

## License

The source-code license has not yet been selected.
