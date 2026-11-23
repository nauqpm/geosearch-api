"""M1.4 synthetic spatial integrity tests using real PostGIS SQL.

NOTE: These deterministic synthetic MultiPolygon fixtures verify PostGIS spatial
predicates (ST_Covers, ST_IsValid, hole/island semantics) on the M1 schema.
They do NOT verify or represent the real Ho Chi Minh City administrative boundary,
which must be independently sourced and verified in a later milestone.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Connection, text

from tests.helpers import insert_boundary_version, insert_import_run, insert_place

# Deterministic synthetic MultiPolygon (SRID 4326):
# - Polygon 1: outer square [0, 10] x [0, 10] with interior hole [3, 7] x [3, 7]
# - Polygon 2: separate island square [20, 24] x [0, 4]
SYNTHETIC_MULTIPOLYGON_WKT = (
    "MULTIPOLYGON("
    "((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 3 7, 7 7, 7 3, 3 3)),"
    "((20 0, 24 0, 24 4, 20 4, 20 0))"
    ")"
)


@pytest.mark.parametrize(
    ("case_label", "lon", "lat", "expected_covers", "expected_contains"),
    [
        ("interior_point", 1.5, 5.0, True, True),
        ("outer_boundary_edge", 0.0, 5.0, True, False),
        ("outer_boundary_vertex", 10.0, 10.0, True, False),
        ("polygon_hole_interior", 5.0, 5.0, False, False),
        # In OGC Simple Features, the inner ring boundary is part of the polygon boundary:
        # ST_Covers includes boundary points (True), whereas ST_Contains excludes them (False).
        ("hole_boundary_edge", 3.0, 5.0, True, False),
        ("island_interior", 22.0, 2.0, True, True),
        ("island_boundary_edge", 20.0, 2.0, True, False),
        ("outside_between_main_and_island", 15.0, 5.0, False, False),
        ("outside_west", -1.0, 5.0, False, False),
    ],
)
def test_synthetic_multipolygon_spatial_membership(
    migrated_conn: Connection,
    case_label: str,
    lon: float,
    lat: float,
    expected_covers: bool,
    expected_contains: bool,
) -> None:
    """Verify ST_Covers and ST_Contains against persisted boundary_versions and places rows."""
    bv_id = insert_boundary_version(
        migrated_conn,
        version_code=f"synthetic-{case_label}",
        geom_wkt=SYNTHETIC_MULTIPOLYGON_WKT,
    )
    run_id = insert_import_run(migrated_conn, bv_id)
    place_id = insert_place(
        migrated_conn,
        run_id,
        osm_id=80001,
        lon=lon,
        lat=lat,
        name=f"Point {case_label}",
        name_search=f"point {case_label}",
    )

    row = migrated_conn.execute(
        text(
            """
            SELECT
                ST_IsValid(b.geom) AS geom_valid,
                NOT ST_IsEmpty(b.geom) AS geom_non_empty,
                ST_NumGeometries(b.geom) AS num_parts,
                ST_Covers(b.geom, p.location::geometry) AS covers,
                ST_Contains(b.geom, p.location::geometry) AS contains
            FROM boundary_versions b
            JOIN places p ON p.id = :place_id
            WHERE b.id = :bv_id
            """
        ),
        {"bv_id": bv_id, "place_id": place_id},
    ).one()

    assert row.geom_valid is True
    assert row.geom_non_empty is True
    assert row.num_parts == 2
    assert row.covers is expected_covers
    assert row.contains is expected_contains
