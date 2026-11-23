"""M1.1/M1.5 smoke tests for PostgreSQL/PostGIS connectivity and fail-closed test safety."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import Connection, text
from sqlalchemy.engine import make_url

from geosearch.db import (
    UnsafeDatabaseConfigError,
    build_disposable_test_database_name,
    create_test_engine,
    is_recognized_test_database_name,
    validate_test_database_prefix,
    validate_test_database_url,
    verify_connected_test_database,
)
from tests.conftest import _managed_disposable_database


class _FakeScalarResult:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one(self) -> Any:
        return self._value


class _FakeConnection:
    """Minimal Connection stub for deterministic negative tests of verify_connected_test_database."""

    def __init__(self, current_database: Any, engine_url: str | None = None) -> None:
        self._current_database = current_database
        self.engine = (
            SimpleNamespace(url=make_url(engine_url)) if engine_url is not None else None
        )

    def execute(self, statement: Any) -> _FakeScalarResult:
        return _FakeScalarResult(self._current_database)


def test_select_one(db_connection: Connection) -> None:
    """Verify basic database connectivity and SELECT 1 execution."""
    result = db_connection.execute(text("SELECT 1")).scalar_one()
    assert result == 1


def test_database_identity(db_connection: Connection) -> None:
    """Verify the active connection targets a recognized dedicated test database."""
    db_name = verify_connected_test_database(db_connection)
    assert is_recognized_test_database_name(db_name)


def test_postgis_extension_available(db_connection: Connection) -> None:
    """Verify PostGIS extension is installed and reports a version string."""
    ext_version = db_connection.execute(
        text("SELECT extversion FROM pg_extension WHERE extname = 'postgis'")
    ).scalar_one_or_none()
    assert ext_version is not None
    postgis_version = db_connection.execute(text("SELECT PostGIS_Version()")).scalar_one()
    assert isinstance(postgis_version, str)
    assert len(postgis_version) > 0


def test_postgis_spatial_function_execution(db_connection: Connection) -> None:
    """Verify PostGIS point construction, geography distance, and ST_Covers execution."""
    distance_m = db_connection.execute(
        text(
            """
            SELECT ST_Distance(
                ST_SetSRID(ST_MakePoint(106.7009, 10.7769), 4326)::geography,
                ST_SetSRID(ST_MakePoint(106.7009, 10.7859), 4326)::geography
            )
            """
        )
    ).scalar_one()
    # ~0.009 degrees latitude is roughly 995 meters on WGS84 ellipsoid.
    assert 990.0 < distance_m < 1005.0

    covers = db_connection.execute(
        text(
            """
            SELECT ST_Covers(
                ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))', 4326),
                ST_SetSRID(ST_MakePoint(5, 5), 4326)
            )
            """
        )
    ).scalar_one()
    assert covers is True


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "",
        "   ",
        "sqlite:///:memory:",
        "postgresql://user:pass@127.0.0.1:55433/geosearch_test",
        "postgresql+psycopg://user:dev_password@127.0.0.1:55432/geosearch_dev",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/postgres",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_prod",
        "postgresql+psycopg://user:pass@db.example.com:5432/geosearch_test",
        "postgresql+psycopg://user:pass@postgres_dev:5432/geosearch_test",
        "postgresql+psycopg:///geosearch_test",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test_",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test__double",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test_UPPER",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test_with-dash",
        "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test_bad;drop",
        f"postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test_{'a' * 55}",
    ],
)
def test_unsafe_test_database_urls_rejected(unsafe_url: str) -> None:
    """Verify fail-closed rejection of non-test, remote, malformed, or wrong-dialect URLs."""
    with pytest.raises(UnsafeDatabaseConfigError):
        validate_test_database_url(unsafe_url, dev_database_url=None)


@pytest.mark.parametrize(
    ("test_url", "dev_url"),
    [
        # Identical endpoint
        (
            "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test",
            "postgresql+psycopg://user:pass@127.0.0.1:55433/geosearch_test",
        ),
        # Same dev server (host, port) with a DIFFERENT database name
        (
            "postgresql+psycopg://user:test_pw@127.0.0.1:55432/geosearch_test",
            "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_dev",
        ),
        # Loopback host alias mismatch: localhost vs 127.0.0.1
        (
            "postgresql+psycopg://user:test_pw@localhost:55432/geosearch_test",
            "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_dev",
        ),
        # Loopback host alias mismatch: IPv6 ::1 vs 127.0.0.1
        (
            "postgresql+psycopg://user:test_pw@[::1]:55432/geosearch_test",
            "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_dev",
        ),
        # Loopback host alias mismatch: IPv6 ::1 vs localhost
        (
            "postgresql+psycopg://user:test_pw@[::1]:55432/geosearch_test_ci",
            "postgresql+psycopg://user:dev_pw@LOCALHOST:55432/geosearch_dev",
        ),
        # Default PostgreSQL port 5432 normalization (omitted vs explicit :5432)
        (
            "postgresql+psycopg://user:test_pw@localhost:5432/geosearch_test",
            "postgresql+psycopg://user:dev_pw@127.0.0.1/geosearch_dev",
        ),
        (
            "postgresql+psycopg://user:test_pw@127.0.0.1/geosearch_test",
            "postgresql+psycopg://user:dev_pw@[::1]:5432/geosearch_dev",
        ),
        # Malformed dev DATABASE_URL fails closed rather than bypassing isolation
        (
            "postgresql+psycopg://user:test_pw@127.0.0.1:55433/geosearch_test",
            "not-a-valid-url://[broken",
        ),
    ],
)
def test_dev_server_collision_and_loopback_aliases_rejected(
    test_url: str, dev_url: str
) -> None:
    """Verify test URLs targeting the dev PostgreSQL server or loopback/port aliases fail closed."""
    with pytest.raises(UnsafeDatabaseConfigError):
        validate_test_database_url(test_url, dev_database_url=dev_url)


@pytest.mark.parametrize(
    ("test_url", "dev_url"),
    [
        (
            "postgresql+psycopg://user:test_pw@127.0.0.1:55433/geosearch_test",
            "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_dev",
        ),
        (
            "postgresql+psycopg://user:test_pw@localhost:55433/geosearch_test_schema_abcdef1234",
            "postgresql+psycopg://user:dev_pw@127.0.0.1:55432/geosearch_dev",
        ),
        (
            "postgresql+psycopg://user:test_pw@[::1]:55433/geosearch_test",
            "postgresql+psycopg://user:dev_pw@localhost:55432/geosearch_dev",
        ),
        (
            "postgresql+psycopg://user:test_pw@postgres_test:5432/geosearch_test",
            "postgresql+psycopg://user:dev_pw@postgres_dev:5432/geosearch_dev",
        ),
    ],
)
def test_isolated_test_database_urls_accepted(test_url: str, dev_url: str) -> None:
    """Verify properly isolated local test PostgreSQL servers are accepted."""
    parsed = validate_test_database_url(test_url, dev_database_url=dev_url)
    assert is_recognized_test_database_name(parsed.database)


@pytest.mark.parametrize(
    "unsafe_prefix",
    [
        "",
        "   ",
        "UpperCase",
        "bad-hyphen",
        "bad_trailing_",
        "_leading",
        "double__underscore",
        'inject"; DROP DATABASE geosearch_dev; --',
        "a" * 33,
    ],
)
def test_unsafe_disposable_prefixes_rejected(unsafe_prefix: str) -> None:
    """Verify disposable test database prefix validation rejects unsafe or oversized names."""
    with pytest.raises(UnsafeDatabaseConfigError):
        validate_test_database_prefix(unsafe_prefix)
    with pytest.raises(UnsafeDatabaseConfigError):
        build_disposable_test_database_name(unsafe_prefix)


def test_disposable_database_fixture_rejects_unsafe_prefix_before_connecting() -> None:
    """Verify _managed_disposable_database fails closed on unsafe prefix before any DB call."""
    with pytest.raises(UnsafeDatabaseConfigError):
        with _managed_disposable_database(
            "postgresql+psycopg://user:test_password@127.0.0.1:55433/geosearch_test",
            'bad";DROP',
        ):
            pass


@pytest.mark.parametrize(
    "bad_current_db",
    [
        "geosearch_dev",
        "postgres",
        "template1",
        "",
        None,
        "geosearch_test_",
        "geosearch_test_UPPER",
        'geosearch_test_bad";DROP',
    ],
)
def test_verify_connected_test_database_rejects_non_test_identity(
    bad_current_db: Any,
) -> None:
    """Verify verify_connected_test_database fails closed on non-test current_database()."""
    fake_conn = _FakeConnection(bad_current_db)
    with pytest.raises(UnsafeDatabaseConfigError):
        verify_connected_test_database(fake_conn, dev_database_url=None)  # type: ignore[arg-type]


def test_verify_connected_test_database_rejects_expected_mismatch_and_dev_server() -> None:
    """Verify live identity check rejects unexpected DB name or dev server endpoint collision."""
    fake_conn = _FakeConnection(
        "geosearch_test_other",
        engine_url="postgresql+psycopg://user:pw@127.0.0.1:55433/geosearch_test_other",
    )
    with pytest.raises(UnsafeDatabaseConfigError):
        verify_connected_test_database(
            fake_conn,  # type: ignore[arg-type]
            expected_database="geosearch_test",
            dev_database_url="postgresql+psycopg://user:pw@127.0.0.1:55432/geosearch_dev",
        )

    dev_server_conn = _FakeConnection(
        "geosearch_test",
        engine_url="postgresql+psycopg://user:pw@localhost:55432/geosearch_test",
    )
    with pytest.raises(UnsafeDatabaseConfigError):
        verify_connected_test_database(
            dev_server_conn,  # type: ignore[arg-type]
            expected_database="geosearch_test",
            dev_database_url="postgresql+psycopg://user:pw@127.0.0.1:55432/geosearch_dev",
        )


def test_create_test_engine_fails_closed_on_unsafe_url() -> None:
    """Verify create_test_engine rejects non-test database URLs before connecting."""
    with pytest.raises(UnsafeDatabaseConfigError):
        create_test_engine(
            "postgresql+psycopg://user:dev_password@127.0.0.1:55432/geosearch_dev"
        )


def test_missing_test_database_url_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify missing TEST_DATABASE_URL fails closed instead of falling back."""
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    with pytest.raises(UnsafeDatabaseConfigError):
        create_test_engine()
