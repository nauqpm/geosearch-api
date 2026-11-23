"""Shared pytest fixtures implementing Hybrid Isolation on PostgreSQL/PostGIS."""

from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
import tempfile

import pytest
from dotenv import load_dotenv
from psycopg import sql as psycopg_sql
from sqlalchemy import Connection, Engine, text

from geosearch.db import (
    UnsafeDatabaseConfigError,
    build_disposable_test_database_name,
    create_test_engine,
    get_test_database_url,
    is_recognized_test_database_name,
    validate_test_database_url,
    verify_connected_test_database,
)
from tests.helpers import upgrade_test_db_to_head

# Load local .env only if present without overriding explicitly exported env vars.
load_dotenv(override=False)


def _execute_db_ddl(connection: Connection, statement: psycopg_sql.Composed) -> None:
    """Execute a safely quoted psycopg SQL DDL statement on the underlying DBAPI connection."""
    raw_conn = connection.connection.driver_connection
    if raw_conn is None:
        raise UnsafeDatabaseConfigError(
            "Unable to access underlying psycopg connection for safe DDL execution."
        )
    with raw_conn.cursor() as cursor:
        cursor.execute(statement)


@contextmanager
def _managed_disposable_database(base_test_url: str, prefix: str) -> Generator[str, None, None]:
    """Create a fresh isolated test database on the test PostgreSQL instance and drop it on exit."""
    parsed_base = validate_test_database_url(base_test_url)
    db_name = build_disposable_test_database_name(prefix)
    disposable_url = parsed_base.set(database=db_name).render_as_string(hide_password=False)
    validate_test_database_url(disposable_url)

    admin_engine = create_test_engine(base_test_url, pool_size=1, max_overflow=0)
    created = False
    try:
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            verify_connected_test_database(conn, expected_database=parsed_base.database)
            _execute_db_ddl(
                conn,
                psycopg_sql.SQL("CREATE DATABASE {}").format(
                    psycopg_sql.Identifier(db_name)
                ),
            )
            created = True
        yield disposable_url
    finally:
        if created:
            with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                verify_connected_test_database(conn, expected_database=parsed_base.database)
                if not is_recognized_test_database_name(db_name):
                    raise UnsafeDatabaseConfigError(
                        f"Refusing to drop unrecognized database '{db_name}'."
                    )
                conn.execute(
                    text(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                        "WHERE datname = :db_name AND pid <> pg_backend_pid()"
                    ),
                    {"db_name": db_name},
                )
                _execute_db_ddl(
                    conn,
                    psycopg_sql.SQL("DROP DATABASE IF EXISTS {}").format(
                        psycopg_sql.Identifier(db_name)
                    ),
                )
        admin_engine.dispose()


@pytest.fixture(scope="session")
def test_database_url() -> str:
    """Return validated TEST_DATABASE_URL; fails closed if missing or unsafe."""
    return get_test_database_url()


@pytest.fixture(scope="session")
def test_engine(test_database_url: str) -> Generator[Engine, None, None]:
    """Session-scoped engine bound strictly to the configured test database."""
    parsed = validate_test_database_url(test_database_url)
    engine = create_test_engine(test_database_url)
    try:
        with engine.connect() as conn:
            verify_connected_test_database(conn, expected_database=parsed.database)
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def db_connection(test_engine: Engine) -> Generator[Connection, None, None]:
    """Transaction-isolated connection to the primary test database (used by M1.1 smoke tests)."""
    with test_engine.connect() as conn:
        transaction = conn.begin()
        try:
            yield conn
        finally:
            transaction.rollback()


@pytest.fixture(scope="session")
def migrated_test_engine(test_database_url: str) -> Generator[Engine, None, None]:
    """Session-scoped isolated database upgraded to Alembic head for SAVEPOINT constraint tests."""
    with _managed_disposable_database(test_database_url, "schema") as disposable_url:
        parsed = validate_test_database_url(disposable_url)
        upgrade_test_db_to_head(disposable_url)
        engine = create_test_engine(disposable_url)
        try:
            with engine.connect() as conn:
                verify_connected_test_database(conn, expected_database=parsed.database)
            yield engine
        finally:
            engine.dispose()


@pytest.fixture()
def migrated_conn(migrated_test_engine: Engine) -> Generator[Connection, None, None]:
    """Connection wrapped in an outer transaction + SAVEPOINT rolled back after each test."""
    with migrated_test_engine.connect() as conn:
        outer_tx = conn.begin()
        savepoint = conn.begin_nested()
        try:
            yield conn
        finally:
            if savepoint.is_active:
                savepoint.rollback()
            if outer_tx.is_active:
                outer_tx.rollback()


@pytest.fixture()
def disposable_database(
    test_database_url: str,
) -> Callable[[str], AbstractContextManager[str]]:
    """Factory yielding fresh isolated disposable databases for COMMIT and migration tests."""

    @contextmanager
    def _factory(prefix: str = "isolated") -> Generator[str, None, None]:
        with _managed_disposable_database(test_database_url, prefix) as url:
            yield url

    return _factory


@pytest.fixture()
def work_dir() -> Generator[Path, None, None]:
    """Isolated temporary directory for test fixture files without shared basetemp ACL issues."""
    with tempfile.TemporaryDirectory(
        prefix="geosearch_test_",
        ignore_cleanup_errors=True,
    ) as tmp_dir:
        yield Path(tmp_dir)
