"""M4.1 tests: FastAPI foundation, configuration safety, lifecycle, OpenAPI, and /health."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from geosearch.api import ApiSettings, create_app, load_api_settings
from geosearch.db import UnsafeDatabaseConfigError, create_db_engine, create_test_engine
from tests.helpers import upgrade_test_db_to_head


def test_m41_load_api_settings_rejects_missing_database_url_without_test_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """API config must fail closed when DATABASE_URL is absent and never fall back to TEST_DATABASE_URL."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "TEST_DATABASE_URL",
        "postgresql+psycopg://user:test_password@127.0.0.1:55433/geosearch_test",
    )

    with pytest.raises(UnsafeDatabaseConfigError, match="DATABASE_URL"):
        load_api_settings(load_dotenv_file=False)


def test_m41_load_api_settings_rejects_invalid_database_url_and_invalid_tuning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed URLs, non-psycopg dialects, and out-of-range numeric settings fail closed."""
    with pytest.raises(UnsafeDatabaseConfigError, match="Unsupported database dialect"):
        ApiSettings(database_url="sqlite:///./nope.db")

    with pytest.raises(UnsafeDatabaseConfigError, match="Malformed database URL"):
        ApiSettings(database_url="not-a-valid-url")

    with pytest.raises(ValueError, match="db_pool_size"):
        ApiSettings(
            database_url="postgresql+psycopg://user:pass@127.0.0.1:55432/geosearch_dev",
            db_pool_size=0,
        )

    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://user:pass@127.0.0.1:55432/geosearch_dev",
    )
    monkeypatch.setenv("GEOSEARCH_DB_STATEMENT_TIMEOUT_MS", "not-an-int")
    with pytest.raises(ValueError, match="GEOSEARCH_DB_STATEMENT_TIMEOUT_MS"):
        load_api_settings(load_dotenv_file=False)


def test_m41_load_api_settings_preserves_explicit_env_over_dotenv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicitly exported DATABASE_URL is never overwritten by `.env` loading."""
    explicit_url = "postgresql+psycopg://explicit_user:explicit_pw@127.0.0.1:55499/explicit_db"
    monkeypatch.setenv("DATABASE_URL", explicit_url)
    monkeypatch.setenv("GEOSEARCH_DB_POOL_SIZE", "7")

    settings = load_api_settings(load_dotenv_file=True)
    assert settings.database_url == explicit_url
    assert settings.db_pool_size == 7


def test_m41_app_lifespan_disposes_owned_engine_only() -> None:
    """Owned engines are disposed on shutdown; caller-injected engines are not disposed."""
    owned_disposed: list[bool] = []
    settings = ApiSettings(
        database_url="postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test",
        db_pool_size=1,
        db_max_overflow=0,
    )
    app_owned = create_app(settings=settings)
    original_dispose = app_owned.state.engine.dispose

    def _track_owned_dispose() -> None:
        owned_disposed.append(True)
        original_dispose()

    app_owned.state.engine.dispose = _track_owned_dispose  # type: ignore[method-assign]
    with TestClient(app_owned):
        assert owned_disposed == []
    assert owned_disposed == [True]

    injected_engine = create_db_engine(
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test",
        pool_size=1,
        max_overflow=0,
    )
    injected_disposed: list[bool] = []
    orig_inj_dispose = injected_engine.dispose

    def _track_injected_dispose() -> None:
        injected_disposed.append(True)
        orig_inj_dispose()

    injected_engine.dispose = _track_injected_dispose  # type: ignore[method-assign]
    try:
        app_injected = create_app(engine=injected_engine)
        with TestClient(app_injected):
            pass
        assert injected_disposed == []
    finally:
        orig_inj_dispose()


def test_m41_health_endpoint_ok_and_openapi_registration(
    disposable_database: Callable[[str], AbstractContextManager[str]],
) -> None:
    """GET /health succeeds with real PostgreSQL, does not mutate DB, and registers OpenAPI."""
    with disposable_database("m41_health") as db_url:
        upgrade_test_db_to_head(db_url)
        engine = create_test_engine(db_url, pool_size=2, max_overflow=0)
        try:
            app = create_app(engine=engine)
            with TestClient(app) as client:
                # Reuses the single engine instance across requests.
                first_engine: Engine = app.state.engine

                resp = client.get("/health")
                assert resp.status_code == 200
                assert resp.json() == {
                    "status": "ok",
                    "service": "geosearch-api",
                    "database": "ok",
                }
                assert app.state.engine is first_engine

                openapi_resp = client.get("/openapi.json")
                assert openapi_resp.status_code == 200
                spec = openapi_resp.json()
                assert spec["info"]["title"] == "GeoSearch API"
                expected_paths = {
                    "/health",
                    "/api/v1/places",
                    "/api/v1/places/nearby",
                    "/api/v1/places/{id}",
                }
                assert set(spec["paths"].keys()) == expected_paths
                for route_path, operations in spec["paths"].items():
                    assert list(operations.keys()) == ["get"], (
                        f"Public route {route_path} must only expose GET, got {list(operations.keys())}"
                    )
                assert "503" in spec["paths"]["/health"]["get"]["responses"]
                nearby_params = {
                    p["name"]: p for p in spec["paths"]["/api/v1/places/nearby"]["get"]["parameters"]
                }
                assert nearby_params["lon"]["required"] is True
                assert nearby_params["lat"]["required"] is True
                assert nearby_params["radius_m"]["required"] is False
                assert "404" in spec["paths"]["/api/v1/places/{id}"]["get"]["responses"]

            # Health check never creates records or alters schema.
            with engine.connect() as conn:
                places_count = conn.execute(text("SELECT count(*) FROM places")).scalar_one()
                runs_count = conn.execute(text("SELECT count(*) FROM import_runs")).scalar_one()
                assert places_count == 0
                assert runs_count == 0
        finally:
            engine.dispose()


def test_m41_health_endpoint_returns_503_without_leaking_credentials() -> None:
    """When PostgreSQL is unreachable, /health returns 503 without leaking DSN, password, or SQL."""
    secret_password = "super_secret_do_not_leak_999"
    unreachable_engine = create_db_engine(
        f"postgresql+psycopg://secret_user:{secret_password}@127.0.0.1:1/geosearch_unreachable",
        pool_size=1,
        max_overflow=0,
        connect_timeout=1,
    )
    try:
        app = create_app(engine=unreachable_engine, health_check_timeout_ms=500)
        with TestClient(app) as client:
            resp = client.get("/health")
            assert resp.status_code == 503
            body = resp.json()
            assert body == {
                "status": "degraded",
                "service": "geosearch-api",
                "database": "unavailable",
            }
            raw_text = resp.text
            assert secret_password not in raw_text
            assert "secret_user" not in raw_text
            assert "127.0.0.1" not in raw_text
            assert "postgresql" not in raw_text.lower()
            assert "traceback" not in raw_text.lower()
    finally:
        unreachable_engine.dispose()
