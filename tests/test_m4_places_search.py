"""M4.2 tests (Groups C & D): GET /api/v1/places text search, categories, and pagination."""

from __future__ import annotations

import unicodedata
import uuid
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path

import osmium
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from geosearch.api import create_app
from geosearch.api.services.place_queries import escape_like_literal
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
    tags: dict[str, str],
    lon: float = 106.7009,
    lat: float = 10.7769,
) -> osmium.osm.mutable.Node:
    return osmium.osm.mutable.Node(
        id=osm_id,
        version=1,
        changeset=100,
        timestamp="2026-01-20T00:00:00Z",
        location=(lon, lat),
        tags=tags,
    )


def test_m42_escape_like_literal_unit() -> None:
    """LIKE escaping treats %, _, and backslash as literal characters."""
    assert escape_like_literal("100%_cafe\\test") == r"100\%\_cafe\\test"


def test_m42_search_vietnamese_normalization_wildcards_categories_and_pagination(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """Exercises Vietnamese search equivalence, LIKE escaping, 5 categories, and deterministic pagination."""
    with disposable_database("m42_search") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m42-search")

            app = create_app(engine=engine)
            with TestClient(app) as client:
                # Before any publication exists: empty collection (200), never 500.
                empty_resp = client.get("/api/v1/places")
                assert empty_resp.status_code == 200
                assert empty_resp.json() == {
                    "items": [],
                    "total": 0,
                    "page": 1,
                    "limit": 20,
                    "has_more": False,
                }

                # Seed and publish controlled dataset covering all 5 categories, Vietnamese names,
                # equal normalized names (for tie-break testing), LIKE meta chars, and unnamed ATMs.
                pbf = _write_pbf(
                    work_dir / "search_v1.osm.pbf",
                    [
                        _node(2001, tags={"amenity": "cafe", "name": "Cà Phê Đồng Nai"}),
                        _node(2002, tags={"amenity": "cafe", "name": "Cà Phê 100% Nguyên Chất"}),
                        _node(2003, tags={"amenity": "cafe", "name": "Quán_Góc Phố"}),
                        _node(2004, tags={"amenity": "restaurant", "name": "Phở Hòa"}),
                        _node(2005, tags={"amenity": "restaurant", "name": "Phở Hòa"}),  # Tie on name_search
                        _node(2006, tags={"amenity": "pharmacy", "name": "Nhà Thuốc Long Châu"}),
                        _node(2007, tags={"shop": "supermarket", "name": "Siêu Thị Co.opmart"}),
                        _node(2008, tags={"amenity": "atm", "name": "ATM Vietcombank"}),
                        _node(2009, tags={"amenity": "atm"}),  # Unnamed ATM #1
                        _node(2010, tags={"amenity": "atm"}),  # Unnamed ATM #2
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

                # Vietnamese diacritic + đ/Đ + case + NFC/NFD + whitespace + Cf format chars equivalence.
                for query_variant in (
                    "cà phê đồng nai",
                    "ca phe dong nai",
                    "CA PHE DONG NAI",
                    "  Cà   Phê   ĐỒNG   Nai  ",
                    unicodedata.normalize("NFD", "Cà Phê Đồng Nai"),
                    unicodedata.normalize("NFC", "Cà Phê Đồng Nai"),
                    "cà\u200b phê \ufeffđồng nai",
                ):
                    r = client.get("/api/v1/places", params={"q": query_variant})
                    assert r.status_code == 200, query_variant
                    body = r.json()
                    assert body["total"] == 1
                    assert [item["osm_id"] for item in body["items"]] == [2001]

                # Empty or whitespace-only or Cf-only search returns all 10 active places.
                for blank_q in ("", "   ", "\t\n", "\u200b\ufeff"):
                    r = client.get("/api/v1/places", params={"q": blank_q})
                    assert r.status_code == 200
                    assert r.json()["total"] == 10

                # LIKE wildcard literal escaping: '%' only matches literal '%', '_' only matches literal '_'.
                r_pct = client.get("/api/v1/places", params={"q": "%"})
                assert r_pct.status_code == 200
                assert [item["osm_id"] for item in r_pct.json()["items"]] == [2002]

                r_underscore = client.get("/api/v1/places", params={"q": "_"})
                assert r_underscore.status_code == 200
                assert [item["osm_id"] for item in r_underscore.json()["items"]] == [2003]

                # SQL injection payload treated strictly as a literal string -> 0 matches, 200 OK.
                r_sqli = client.get("/api/v1/places", params={"q": "' OR 1=1 --"})
                assert r_sqli.status_code == 200
                assert r_sqli.json()["total"] == 0
                assert r_sqli.json()["items"] == []

                # Category filtering: all 5 approved categories individually.
                expected_counts = {
                    "cafe": 3,
                    "restaurant": 2,
                    "pharmacy": 1,
                    "supermarket": 1,
                    "atm": 3,
                }
                for cat, expected_count in expected_counts.items():
                    r_cat = client.get("/api/v1/places", params={"category": cat})
                    assert r_cat.status_code == 200
                    assert r_cat.json()["total"] == expected_count
                    assert all(item["category"] == cat for item in r_cat.json()["items"])

                # Multi-category filtering.
                r_multi = client.get(
                    "/api/v1/places",
                    params=[("category", "pharmacy"), ("category", "supermarket")],
                )
                assert r_multi.status_code == 200
                assert r_multi.json()["total"] == 2
                assert {item["osm_id"] for item in r_multi.json()["items"]} == {2006, 2007}

                # Combined text search + category filtering.
                r_combo = client.get(
                    "/api/v1/places",
                    params={"q": "ca phe", "category": "cafe"},
                )
                assert r_combo.status_code == 200
                assert {item["osm_id"] for item in r_combo.json()["items"]} == {2001, 2002}

                r_combo_miss = client.get(
                    "/api/v1/places",
                    params={"q": "ca phe", "category": "pharmacy"},
                )
                assert r_combo_miss.status_code == 200
                assert r_combo_miss.json()["total"] == 0
                assert r_combo_miss.json()["items"] == []

                # Deterministic pagination across all 10 items with limit=3:
                # 4 pages (3 + 3 + 3 + 1), zero duplicates, zero omissions, accurate metadata.
                seen_ids: list[int] = []
                for page_num, expected_len, expected_has_more in [
                    (1, 3, True),
                    (2, 3, True),
                    (3, 3, True),
                    (4, 1, False),
                ]:
                    rp = client.get("/api/v1/places", params={"page": page_num, "limit": 3})
                    assert rp.status_code == 200
                    pbody = rp.json()
                    assert pbody["page"] == page_num
                    assert pbody["limit"] == 3
                    assert pbody["total"] == 10
                    assert pbody["has_more"] is expected_has_more
                    assert len(pbody["items"]) == expected_len
                    seen_ids.extend(item["id"] for item in pbody["items"])

                assert len(seen_ids) == 10
                assert len(set(seen_ids)) == 10

                # Empty page beyond end still reports accurate total and has_more=False.
                r_beyond = client.get("/api/v1/places", params={"page": 5, "limit": 3})
                assert r_beyond.status_code == 200
                assert r_beyond.json() == {
                    "items": [],
                    "total": 10,
                    "page": 5,
                    "limit": 3,
                    "has_more": False,
                }

                # Equal name_search tie-break uses ascending stable internal places.id,
                # and unnamed ATMs appear NULLS LAST ordered by ascending places.id.
                r_ties = client.get("/api/v1/places", params={"q": "pho hoa"})
                assert r_ties.status_code == 200
                tie_ids = [item["id"] for item in r_ties.json()["items"]]
                assert tie_ids == sorted(tie_ids) and len(tie_ids) == 2

                r_atms = client.get("/api/v1/places", params={"category": "atm"})
                atm_items = r_atms.json()["items"]
                assert atm_items[0]["name"] == "ATM Vietcombank"
                assert atm_items[1]["name"] is None
                assert atm_items[2]["name"] is None
                assert atm_items[1]["id"] < atm_items[2]["id"]
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    "params",
    [
        {"category": "bar"},
        {"category": "hospital"},
        {"category": ""},
        [("category", "cafe"), ("category", "invalid_cat")],
        {"page": 0},
        {"page": -1},
        {"page": 100_001},
        {"limit": 0},
        {"limit": -5},
        {"limit": 101},
        {"q": "a" * 201},
        {"q": "bad\x00query"},
    ],
)
def test_m42_search_rejects_invalid_parameters_with_422(
    migrated_test_engine,
    params: dict[str, object] | list[tuple[str, str]],
) -> None:
    """Unsupported categories, invalid pagination bounds, oversized q, and NUL bytes return 422."""
    app = create_app(engine=migrated_test_engine)
    with TestClient(app) as client:
        resp = client.get("/api/v1/places", params=params)
        assert resp.status_code == 422
        assert resp.json()["error"]["code"] == "VALIDATION_ERROR"


def test_m47_ac5_literal_backslash_search_via_parameterized_like_escape(
    disposable_database: Callable[[str], AbstractContextManager[str]],
    work_dir: Path,
) -> None:
    """AC5: Searching for a literal backslash matches only the POI containing '\\' in real PostgreSQL."""
    with disposable_database("m47_ac5_bslash") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            with engine.begin() as conn:
                bv_id = insert_boundary_version(conn, version_code="bv-m47-ac5")

            pbf = _write_pbf(
                work_dir / "ac5_backslash.osm.pbf",
                [
                    _node(2101, tags={"amenity": "cafe", "name": r"Cafe A\B Corner"}),
                    _node(2102, tags={"amenity": "cafe", "name": "Cafe AB Corner"}),
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
                r_bs = client.get("/api/v1/places", params={"q": "\\"})
                assert r_bs.status_code == 200
                body_bs = r_bs.json()
                assert body_bs["total"] == 1
                assert [item["osm_id"] for item in body_bs["items"]] == [2101]

                r_sub = client.get("/api/v1/places", params={"q": r"a\b"})
                assert r_sub.status_code == 200
                body_sub = r_sub.json()
                assert body_sub["total"] == 1
                assert [item["osm_id"] for item in body_sub["items"]] == [2101]
        finally:
            engine.dispose()
