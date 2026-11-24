"""M4.3 tests (Group E): GET /api/v1/places/nearby PostGIS WGS84 spheroid spatial search."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import osmium
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from geosearch.api import create_app
from geosearch.db import create_test_engine
from geosearch.ingestion import ingest_osm_nodes_to_staging
from geosearch.publication import PublicationMode, publish_prepared_run
from tests.helpers import insert_boundary_version, upgrade_test_db_to_head


def _write_pbf(path: Path, nodes: list[osmium.osm.mutable.Node]) -> Path:
    writer = osmium.SimpleWriter(str(path))
    try:
        for node in nodes:
            writer.add_node(node)
    finally:
        writer.close()
    return path


def _node(
    osm_id: int,
    *,
    lon: float,
    lat: float,
    tags: dict[str, str],
) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=1,
        changeset=100,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags=tags,
    )


def test_m43_nearby_postgis_distance_boundary_ties_filtering_pagination_and_v1_v2(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Verifies real PostGIS ST_DWithin/ST_Distance, exact center, boundary, ties, filters, and V1->V2."""
    center_lon = 106.7000
    center_lat = 10.7800

    with disposable_database("m43_nearby") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m43-nearby")

            # Construct controlled spatial layout around (106.7000, 10.7800):
            # - 3001 & 3002: exact center (distance == 0.0m, same-coordinate tie-break)
            # - 3003 & 3004: symmetric +/-0.002 deg longitude (~218.7m, exact equal distance tie-break!)
            # - 3005: +0.005 deg longitude (~546.8m, inside 600m)
            # - 3006: +0.010 deg longitude (~1093.6m, outside 1000m, inside 1200m)
            # - 3007: +0.020 deg longitude (~2187.2m, outside 1200m)
            pbf_v1 = _write_pbf(
                work_dir / "nearby_v1.osm.pbf",
                [
                    _node(3001, lon=center_lon, lat=center_lat, tags={"amenity": "cafe", "name": "Center Cafe A"}),
                    _node(3002, lon=center_lon, lat=center_lat, tags={"amenity": "atm"}),
                    _node(3003, lon=center_lon + 0.002, lat=center_lat, tags={"amenity": "pharmacy", "name": "East Pharmacy"}),
                    _node(3004, lon=center_lon - 0.002, lat=center_lat, tags={"amenity": "restaurant", "name": "West Restaurant"}),
                    _node(3005, lon=center_lon + 0.005, lat=center_lat, tags={"amenity": "cafe", "name": "Mid Cafe Đồng Khởi"}),
                    _node(3006, lon=center_lon + 0.010, lat=center_lat, tags={"shop": "supermarket", "name": "Farther Supermarket"}),
                    _node(3007, lon=center_lon + 0.020, lat=center_lat, tags={"amenity": "cafe", "name": "Outer Cafe"}),
                ],
            )
            ing_v1 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v1, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing_v1.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )

            # Query PostGIS directly for reference WGS84 spheroid distances.
            with engine.connect() as conn:
                ref_distances = {
                    int(r.osm_id): float(r.dist)
                    for r in conn.execute(
                        text(
                            """
                            SELECT
                                osm_id,
                                ST_Distance(
                                    location,
                                    ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                                    true
                                ) AS dist
                            FROM places
                            """
                        ),
                        {"lon": center_lon, "lat": center_lat},
                    ).fetchall()
                }
            # Sanity check real geodesic meter distances (not degrees!).
            assert ref_distances[3001] == 0.0
            assert ref_distances[3002] == 0.0
            assert 215.0 < ref_distances[3003] < 225.0
            assert ref_distances[3003] == pytest.approx(ref_distances[3004], rel=0, abs=1e-9)
            assert 540.0 < ref_distances[3005] < 555.0
            assert 1085.0 < ref_distances[3006] < 1105.0
            assert 2170.0 < ref_distances[3007] < 2205.0

            app = create_app(engine=engine)
            with TestClient(app) as client:
                # Default radius (1000m): includes 3001..3005 (5 places), excludes 3006 & 3007.
                resp = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat},
                )
                assert resp.status_code == 200
                body = resp.json()
                assert body["total"] == 5
                assert body["center"] == {"longitude": center_lon, "latitude": center_lat}
                assert body["radius_m"] == 1000.0
                assert body["has_more"] is False
                items = body["items"]
                assert len(items) == 5

                # Closest-first sorting + deterministic tie-breaking by ascending id for equal distances.
                assert {items[0]["osm_id"], items[1]["osm_id"]} == {3001, 3002}
                assert items[0]["distance_m"] == 0.0
                assert items[1]["distance_m"] == 0.0
                assert items[0]["id"] < items[1]["id"]

                assert {items[2]["osm_id"], items[3]["osm_id"]} == {3003, 3004}
                assert items[2]["distance_m"] == pytest.approx(ref_distances[3003], abs=1e-6)
                assert items[3]["distance_m"] == pytest.approx(ref_distances[3004], abs=1e-6)
                assert items[2]["id"] < items[3]["id"]

                assert items[4]["osm_id"] == 3005
                assert items[4]["distance_m"] == pytest.approx(ref_distances[3005], abs=1e-6)

                # Exact radius boundary check around 3006 (~1093.6m):
                # radius = exact_dist - 0.5m excludes 3006; radius = exact_dist + 0.5m includes 3006!
                r_just_outside = client.get(
                    "/api/v1/places/nearby",
                    params={
                        "lon": center_lon,
                        "lat": center_lat,
                        "radius_m": ref_distances[3006] - 0.5,
                    },
                )
                assert r_just_outside.status_code == 200
                assert 3006 not in [it["osm_id"] for it in r_just_outside.json()["items"]]

                r_just_inside = client.get(
                    "/api/v1/places/nearby",
                    params={
                        "lon": center_lon,
                        "lat": center_lat,
                        "radius_m": ref_distances[3006] + 0.5,
                    },
                )
                assert r_just_inside.status_code == 200
                assert 3006 in [it["osm_id"] for it in r_just_inside.json()["items"]]
                assert r_just_inside.json()["total"] == 6

                # Category filtering (multiple categories) + Vietnamese text filter inside radius.
                r_cat = client.get(
                    "/api/v1/places/nearby",
                    params=[
                        ("lon", str(center_lon)),
                        ("lat", str(center_lat)),
                        ("radius_m", "1500"),
                        ("category", "cafe"),
                        ("category", "supermarket"),
                    ],
                )
                assert r_cat.status_code == 200
                assert [it["osm_id"] for it in r_cat.json()["items"]] == [3001, 3005, 3006]

                r_text = client.get(
                    "/api/v1/places/nearby",
                    params={
                        "lon": center_lon,
                        "lat": center_lat,
                        "radius_m": 1500,
                        "q": "dong khoi",
                    },
                )
                assert r_text.status_code == 200
                assert [it["osm_id"] for it in r_text.json()["items"]] == [3005]

                # Bounded pagination on nearby search.
                r_p1 = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": 2500, "page": 1, "limit": 3},
                )
                r_p2 = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": 2500, "page": 2, "limit": 3},
                )
                r_p3 = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": 2500, "page": 3, "limit": 3},
                )
                assert r_p1.json()["total"] == 7 and r_p1.json()["has_more"] is True
                assert r_p2.json()["total"] == 7 and r_p2.json()["has_more"] is True
                assert r_p3.json()["total"] == 7 and r_p3.json()["has_more"] is False
                all_paged_osm = [
                    it["osm_id"]
                    for resp_p in (r_p1, r_p2, r_p3)
                    for it in resp_p.json()["items"]
                ]
                assert len(all_paged_osm) == 7
                assert len(set(all_paged_osm)) == 7

                # Empty result far from active places (still valid WGS84 coordinates).
                r_far = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 180.0, "lat": -89.5, "radius_m": 500},
                )
                assert r_far.status_code == 200
                assert r_far.json()["total"] == 0
                assert r_far.json()["items"] == []

                # Publish V2 deactivating 3001 and 3003 -> inactive records must be excluded.
                pbf_v2 = _write_pbf(
                    work_dir / "nearby_v2.osm.pbf",
                    [
                        _node(3002, lon=center_lon, lat=center_lat, tags={"amenity": "atm"}),
                        _node(3004, lon=center_lon - 0.002, lat=center_lat, tags={"amenity": "restaurant", "name": "West Restaurant"}),
                    ],
                )
                ing_v2 = ingest_osm_nodes_to_staging(engine, source_path=pbf_v2, boundary_version_id=bv_id)
                publish_prepared_run(
                    engine,
                    import_run_id=ing_v2.import_run.id,
                    publication_operation_id=uuid.uuid4(),
                    publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                    expected_active_run_id=ing_v1.import_run.id,
                    recovery_artifact_root=work_dir / "artifacts",
                )

                r_v2 = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": 5000},
                )
                assert r_v2.status_code == 200
                assert [it["osm_id"] for it in r_v2.json()["items"]] == [3002, 3004]
        finally:
            engine.dispose()


def test_m43_nearby_distinguishes_geodesic_meters_from_planar_degrees(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Identical 0.01 deg longitude delta is ~1093m at 10.78N (outside 800m) vs ~558m at 60N (inside 800m)."""
    high_lat_and_low_lat_boundary = (
        "MULTIPOLYGON("
        "((106.6 10.7, 106.8 10.7, 106.8 10.9, 106.6 10.9, 106.6 10.7)),"
        "((10.0 59.9, 10.2 59.9, 10.2 60.1, 10.0 60.1, 10.0 59.9))"
        ")"
    )
    with disposable_database("m43_meters") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(
                    conn,
                    version_code="bv-m43-lat-test",
                    geom_wkt=high_lat_and_low_lat_boundary,
                )

            pbf = _write_pbf(
                work_dir / "lat_compare.osm.pbf",
                [
                    _node(4001, lon=106.71, lat=10.78, tags={"amenity": "cafe", "name": "Low Lat Cafe"}),
                    _node(4002, lon=10.11, lat=60.00, tags={"amenity": "cafe", "name": "High Lat Cafe"}),
                ],
            )
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )

            app = create_app(engine=engine)
            with TestClient(app) as client:
                # 0.01 deg east of (106.70, 10.78) is ~1093.6m -> NOT within 800m!
                r_low = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 106.70, "lat": 10.78, "radius_m": 800},
                )
                assert r_low.status_code == 200
                assert r_low.json()["total"] == 0

                # 0.01 deg east of (10.10, 60.00) is ~558.0m -> WITHIN 800m!
                r_high = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": 10.10, "lat": 60.00, "radius_m": 800},
                )
                assert r_high.status_code == 200
                assert r_high.json()["total"] == 1
                item = r_high.json()["items"][0]
                assert item["osm_id"] == 4002
                assert 550.0 < item["distance_m"] < 565.0
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    "params",
    [
        {"lat": 10.78},  # missing lon
        {"lon": 106.70},  # missing lat
        {"lon": -180.1, "lat": 10.78},
        {"lon": 180.1, "lat": 10.78},
        {"lon": 106.70, "lat": -90.1},
        {"lon": 106.70, "lat": 90.1},
        {"lon": 10.78, "lat": 106.70},  # swapped lat/lon fails lat <= 90
        {"lon": "nan", "lat": 10.78},
        {"lon": 106.70, "lat": "nan"},
        {"lon": "inf", "lat": 10.78},
        {"lon": 106.70, "lat": "-inf"},
        {"lon": "abc", "lat": 10.78},
        {"lon": 106.70, "lat": 10.78, "radius_m": 0},
        {"lon": 106.70, "lat": 10.78, "radius_m": -50},
        {"lon": 106.70, "lat": 10.78, "radius_m": 50000.1},
        {"lon": 106.70, "lat": 10.78, "radius_m": "nan"},
        {"lon": 106.70, "lat": 10.78, "radius_m": "inf"},
        {"lon": 106.70, "lat": 10.78, "category": "museum"},
        {"lon": 106.70, "lat": 10.78, "limit": 0},
        {"lon": 106.70, "lat": 10.78, "limit": 101},
        {"lon": 106.70, "lat": 10.78, "page": 0},
    ],
)
def test_m43_nearby_rejects_invalid_coordinates_radius_and_filters(
    migrated_test_engine,
    params: dict[str, object],
) -> None:
    """Out-of-range WGS84 bounds, NaN/Inf, invalid radius, and unsupported categories return 422."""
    app = create_app(engine=migrated_test_engine)
    with TestClient(app) as client:
        resp = client.get("/api/v1/places/nearby", params=params)
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


def test_m47_ac4_exact_postgis_spheroidal_radius_boundary_equality(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC4: Full-precision PostGIS WGS84 spheroid distance is included at equality, excluded below, included above."""
    center_lon = 106.7000
    center_lat = 10.7800

    with disposable_database("m47_ac4_exact") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m47-ac4")

            pbf = _write_pbf(
                work_dir / "ac4_exact.osm.pbf",
                [
                    _node(
                        3101,
                        lon=106.7100,
                        lat=10.7800,
                        tags={"amenity": "cafe", "name": "Boundary Equality Cafe"},
                    ),
                ],
            )
            ing = ingest_osm_nodes_to_staging(engine, source_path=pbf, boundary_version_id=bv_id)
            publish_prepared_run(
                engine,
                import_run_id=ing.import_run.id,
                publication_operation_id=uuid.uuid4(),
                publication_mode=PublicationMode.CONTROLLED_TEST_NODE_ONLY,
                expected_active_run_id=None,
            )

            with engine.connect() as conn:
                exact_dist = float(
                    conn.execute(
                        text(
                            """
                            SELECT ST_Distance(
                                location,
                                ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)::geography,
                                true
                            )
                            FROM places
                            WHERE osm_id = 3101
                            """
                        ),
                        {"lon": center_lon, "lat": center_lat},
                    ).scalar_one()
                )
            assert 1085.0 < exact_dist < 1105.0

            app = create_app(engine=engine)
            with TestClient(app) as client:
                # Exact equality: POI must be included.
                r_eq = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": repr(exact_dist)},
                )
                assert r_eq.status_code == 200
                eq_body = r_eq.json()
                assert eq_body["total"] == 1
                assert [it["osm_id"] for it in eq_body["items"]] == [3101]
                assert eq_body["items"][0]["distance_m"] == pytest.approx(exact_dist, rel=0, abs=1e-9)

                # Just below boundary: POI must be excluded.
                r_below = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": repr(exact_dist - 0.01)},
                )
                assert r_below.status_code == 200
                assert r_below.json()["total"] == 0
                assert r_below.json()["items"] == []

                # Just above boundary: POI must be included.
                r_above = client.get(
                    "/api/v1/places/nearby",
                    params={"lon": center_lon, "lat": center_lat, "radius_m": repr(exact_dist + 0.01)},
                )
                assert r_above.status_code == 200
                assert r_above.json()["total"] == 1
                assert [it["osm_id"] for it in r_above.json()["items"]] == [3101]
        finally:
            engine.dispose()
