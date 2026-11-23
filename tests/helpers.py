"""Shared helpers and valid fixture factories for M1 PostgreSQL/PostGIS tests."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, text
from sqlalchemy.exc import DBAPIError

from geosearch.db import validate_test_database_url

REPO_ROOT = Path(__file__).resolve().parents[1]
ALEMBIC_INI_PATH = REPO_ROOT / "alembic.ini"

VALID_SHA256 = "a" * 64
VALID_MULTIPOLYGON_WKT = "MULTIPOLYGON(((106.6 10.7, 106.8 10.7, 106.8 10.9, 106.6 10.9, 106.6 10.7)))"
DEFAULT_VALIDATED_AT = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
DEFAULT_APPROVED_AT = datetime(2026, 1, 1, 1, 0, tzinfo=timezone.utc)
DEFAULT_VALIDATION_EVIDENCE_JSON = '{"method": "synthetic-check", "points_verified": 4}'
_DEFAULT: Any = object()


def make_alembic_config(database_url: str) -> Config:
    """Create an Alembic Config bound strictly to a validated test database URL."""
    validate_test_database_url(database_url)
    cfg = Config(str(ALEMBIC_INI_PATH))
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


def upgrade_test_db_to_head(database_url: str) -> None:
    """Run Alembic upgrade head on a validated test database URL."""
    command.upgrade(make_alembic_config(database_url), "head")


def assert_pg_error(
    exc_info: pytest.ExceptionInfo[DBAPIError],
    *,
    sqlstate: str,
    constraint_name: str | None = None,
    column_name: str | None = None,
) -> None:
    """Assert exact PostgreSQL SQLSTATE and constraint/column metadata on DBAPIError."""
    orig = exc_info.value.orig
    assert orig is not None, "Expected underlying psycopg error on DBAPIError.orig"
    actual_sqlstate = getattr(orig, "sqlstate", None)
    assert actual_sqlstate == sqlstate, (
        f"Expected SQLSTATE {sqlstate!r}, got {actual_sqlstate!r} ({exc_info.value})"
    )
    diag = getattr(orig, "diag", None)
    if constraint_name is not None:
        actual_constraint = getattr(diag, "constraint_name", None)
        assert actual_constraint == constraint_name, (
            f"Expected constraint_name {constraint_name!r}, got {actual_constraint!r}"
        )
    if column_name is not None:
        actual_column = getattr(diag, "column_name", None)
        assert actual_column == column_name, (
            f"Expected column_name {column_name!r}, got {actual_column!r}"
        )


def insert_boundary_version(
    conn: Connection,
    *,
    version_code: str | None = None,
    geom_wkt: str = VALID_MULTIPOLYGON_WKT,
    srid: int = 4326,
    source_name: str = "synthetic-test-source",
    checksum_sha256: str = VALID_SHA256,
    approval_status: str = "APPROVED",
    validated_by: Any = _DEFAULT,
    validated_at: Any = _DEFAULT,
    validation_evidence_json: Any = _DEFAULT,
    approved_by: Any = _DEFAULT,
    approved_at: Any = _DEFAULT,
) -> int:
    """Insert a boundary_versions row and return its generated ID."""
    resolved_code = version_code or f"bv-{uuid.uuid4().hex[:10]}"

    if validated_by is _DEFAULT:
        validated_by = "validator@test" if approval_status in ("VALIDATED", "APPROVED") else None
    if validated_at is _DEFAULT:
        validated_at = DEFAULT_VALIDATED_AT if approval_status in ("VALIDATED", "APPROVED") else None
    if validation_evidence_json is _DEFAULT:
        validation_evidence_json = (
            DEFAULT_VALIDATION_EVIDENCE_JSON
            if approval_status in ("VALIDATED", "APPROVED")
            else None
        )
    if approved_by is _DEFAULT:
        approved_by = "reviewer@test" if approval_status == "APPROVED" else None
    if approved_at is _DEFAULT:
        approved_at = DEFAULT_APPROVED_AT if approval_status == "APPROVED" else None

    return conn.execute(
        text(
            """
            INSERT INTO boundary_versions (
                version_code, geom, source_name, checksum_sha256,
                approval_status, validated_by, validated_at, validation_evidence,
                approved_by, approved_at
            )
            VALUES (
                :version_code,
                ST_GeomFromText(:geom_wkt, :srid),
                :source_name,
                :checksum_sha256,
                :approval_status,
                :validated_by,
                :validated_at,
                CAST(:validation_evidence_json AS jsonb),
                :approved_by,
                :approved_at
            )
            RETURNING id
            """
        ),
        {
            "version_code": resolved_code,
            "geom_wkt": geom_wkt,
            "srid": srid,
            "source_name": source_name,
            "checksum_sha256": checksum_sha256,
            "approval_status": approval_status,
            "validated_by": validated_by,
            "validated_at": validated_at,
            "validation_evidence_json": validation_evidence_json,
            "approved_by": approved_by,
            "approved_at": approved_at,
        },
    ).scalar_one()


def insert_import_run(
    conn: Connection,
    boundary_version_id: int,
    *,
    run_type: str = "IMPORT",
    source_name: str = "vietnam-latest.osm.pbf",
    source_checksum_sha256: str = VALID_SHA256,
    source_fingerprint: str = "fp-default-v1",
    pipeline_version: str = "0.1.0",
    normalizer_version: str = "0.1.0",
    config_digest: str = "cfg-v1",
    status: str = "PREPARED",
    publication_operation_id: uuid.UUID | None = None,
    published_at: datetime | None = None,
    superseded_at: datetime | None = None,
    publication_evidence_json: str | None = None,
    active_slot: int | None = None,
) -> int:
    """Insert an import_runs row and return its generated ID."""
    return conn.execute(
        text(
            """
            INSERT INTO import_runs (
                boundary_version_id, run_type, source_name, source_checksum_sha256,
                source_fingerprint, pipeline_version, normalizer_version, config_digest,
                status, publication_operation_id, published_at, superseded_at,
                publication_evidence, active_slot
            )
            VALUES (
                :boundary_version_id, :run_type, :source_name, :source_checksum_sha256,
                :source_fingerprint, :pipeline_version, :normalizer_version, :config_digest,
                :status, :publication_operation_id, :published_at, :superseded_at,
                CAST(:publication_evidence_json AS jsonb), :active_slot
            )
            RETURNING id
            """
        ),
        {
            "boundary_version_id": boundary_version_id,
            "run_type": run_type,
            "source_name": source_name,
            "source_checksum_sha256": source_checksum_sha256,
            "source_fingerprint": source_fingerprint,
            "pipeline_version": pipeline_version,
            "normalizer_version": normalizer_version,
            "config_digest": config_digest,
            "status": status,
            "publication_operation_id": publication_operation_id,
            "published_at": published_at,
            "superseded_at": superseded_at,
            "publication_evidence_json": publication_evidence_json,
            "active_slot": active_slot,
        },
    ).scalar_one()


def insert_place(
    conn: Connection,
    import_run_id: int,
    *,
    osm_type: str = "node",
    osm_id: int = 100001,
    name: str | None = "Ca Phe Sai Gon",
    name_search: str | None = "ca phe sai gon",
    category: str = "cafe",
    address: str | None = "1 Le Loi, District 1",
    lon: float = 106.7009,
    lat: float = 10.7769,
    raw_tags_json: str = '{"amenity": "cafe", "name": "Ca Phe Sai Gon"}',
    is_active: bool = True,
    osm_version: int | None = 1,
    osm_changeset: int | None = 12345,
) -> int:
    """Insert a places row with explicit PostGIS geography point and return its ID."""
    return conn.execute(
        text(
            """
            INSERT INTO places (
                osm_type, osm_id, name, name_search, category, address,
                location, raw_tags, last_seen_import_id, is_active,
                osm_version, osm_changeset
            )
            VALUES (
                :osm_type, :osm_id, :name, :name_search, :category, :address,
                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                CAST(:raw_tags_json AS jsonb),
                :last_seen_import_id, :is_active, :osm_version, :osm_changeset
            )
            RETURNING id
            """
        ),
        {
            "osm_type": osm_type,
            "osm_id": osm_id,
            "name": name,
            "name_search": name_search,
            "category": category,
            "address": address,
            "lon": lon,
            "lat": lat,
            "raw_tags_json": raw_tags_json,
            "last_seen_import_id": import_run_id,
            "is_active": is_active,
            "osm_version": osm_version,
            "osm_changeset": osm_changeset,
        },
    ).scalar_one()
