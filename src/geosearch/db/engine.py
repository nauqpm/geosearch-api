"""SQLAlchemy engine factory and fail-closed test database guards."""

from __future__ import annotations

import os
import re
import uuid
from typing import Final

from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

ALLOWED_DRIVERNAME: Final[str] = "postgresql+psycopg"
LOOPBACK_HOSTS: Final[frozenset[str]] = frozenset({"127.0.0.1", "localhost", "::1"})
ALLOWED_TEST_HOSTS: Final[frozenset[str]] = LOOPBACK_HOSTS | frozenset({"postgres_test"})
DEFAULT_POSTGRES_PORT: Final[int] = 5432
MAX_PG_IDENTIFIER_LENGTH: Final[int] = 63
MAX_DISPOSABLE_PREFIX_LENGTH: Final[int] = 32

TEST_DB_EXACT_NAME: Final[str] = "geosearch_test"
TEST_DB_PREFIX: Final[str] = "geosearch_test_"
TEST_DB_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^geosearch_test(?:_[a-z0-9]+(?:_[a-z0-9]+)*)?$"
)
TEST_DB_PREFIX_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
TEST_DB_SUFFIX_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[a-f0-9]{10}$")

_UNSET: Final[object] = object()


class UnsafeDatabaseConfigError(ValueError):
    """Raised when a database URL is missing, malformed, or unsafe for test execution."""


def _normalize_host(host: str | None) -> str:
    """Normalize a URL host string, stripping IPv6 brackets and lowercasing."""
    if not host:
        return ""
    cleaned = host.strip().lower()
    if cleaned.startswith("[") and cleaned.endswith("]"):
        cleaned = cleaned[1:-1]
    return cleaned


def _canonical_server_endpoint(parsed: URL) -> tuple[str, int]:
    """Return canonical (host, port) with loopback aliases normalized and default port resolved."""
    raw_host = _normalize_host(parsed.host)
    canonical_host = "127.0.0.1" if raw_host in LOOPBACK_HOSTS else raw_host
    port = DEFAULT_POSTGRES_PORT if parsed.port is None else parsed.port
    if not (1 <= port <= 65535):
        raise UnsafeDatabaseConfigError(f"Invalid PostgreSQL port: {port!r}.")
    return (canonical_host, port)


def _resolve_dev_database_url(dev_database_url: str | None | object) -> URL | None:
    """Resolve and parse dev DATABASE_URL; fail closed if configured but malformed."""
    raw_dev_url = (
        os.environ.get("DATABASE_URL") if dev_database_url is _UNSET else dev_database_url
    )
    if raw_dev_url is None:
        return None
    if not isinstance(raw_dev_url, str) or not raw_dev_url.strip():
        return None
    try:
        return make_url(raw_dev_url.strip())
    except ArgumentError as exc:
        raise UnsafeDatabaseConfigError(
            f"Malformed DATABASE_URL prevents safe dev/test server isolation check: {exc}"
        ) from exc


def is_recognized_test_database_name(database_name: str | None) -> bool:
    """Return True only for approved GeoSearch test database names."""
    if not isinstance(database_name, str) or not database_name:
        return False
    if len(database_name) > MAX_PG_IDENTIFIER_LENGTH:
        return False
    return TEST_DB_NAME_PATTERN.fullmatch(database_name) is not None


def validate_test_database_prefix(prefix: str) -> str:
    """Validate a disposable test database prefix segment against the approved pattern."""
    if not isinstance(prefix, str) or not prefix:
        raise UnsafeDatabaseConfigError(
            "Disposable test database prefix must be a non-empty lowercase identifier."
        )
    if len(prefix) > MAX_DISPOSABLE_PREFIX_LENGTH:
        raise UnsafeDatabaseConfigError(
            f"Disposable test database prefix '{prefix}' exceeds maximum length "
            f"of {MAX_DISPOSABLE_PREFIX_LENGTH} characters."
        )
    if TEST_DB_PREFIX_PATTERN.fullmatch(prefix) is None:
        raise UnsafeDatabaseConfigError(
            f"Unsafe disposable test database prefix '{prefix}'. "
            "Only lowercase ASCII letters, digits, and single underscores are allowed."
        )
    return prefix


def build_disposable_test_database_name(
    prefix: str,
    *,
    suffix: str | None = None,
) -> str:
    """Build and validate an approved disposable test database identifier."""
    validated_prefix = validate_test_database_prefix(prefix)
    resolved_suffix = uuid.uuid4().hex[:10] if suffix is None else suffix
    if (
        not isinstance(resolved_suffix, str)
        or TEST_DB_SUFFIX_PATTERN.fullmatch(resolved_suffix) is None
    ):
        raise UnsafeDatabaseConfigError(
            f"Unsafe disposable test database suffix '{resolved_suffix}'."
        )
    db_name = f"{TEST_DB_PREFIX}{validated_prefix}_{resolved_suffix}"
    if not is_recognized_test_database_name(db_name):
        raise UnsafeDatabaseConfigError(
            f"Generated disposable database name '{db_name}' is not an approved test database name."
        )
    return db_name


def validate_database_url(database_url: str) -> URL:
    """Validate that a database URL explicitly targets PostgreSQL via Psycopg 3."""
    if not isinstance(database_url, str) or not database_url.strip():
        raise UnsafeDatabaseConfigError("Database URL must be a non-empty string.")

    try:
        parsed = make_url(database_url.strip())
    except ArgumentError as exc:
        raise UnsafeDatabaseConfigError(f"Malformed database URL: {exc}") from exc

    if parsed.drivername != ALLOWED_DRIVERNAME:
        raise UnsafeDatabaseConfigError(
            f"Unsupported database dialect '{parsed.drivername}'. "
            f"Expected '{ALLOWED_DRIVERNAME}'."
        )

    if not parsed.database:
        raise UnsafeDatabaseConfigError("Database URL must specify a database name.")

    if parsed.port is not None and not (1 <= parsed.port <= 65535):
        raise UnsafeDatabaseConfigError(f"Invalid PostgreSQL port: {parsed.port!r}.")

    return parsed


def validate_test_database_url(
    database_url: str,
    *,
    dev_database_url: str | None | object = _UNSET,
) -> URL:
    """Fail closed unless database_url targets an isolated local test PostgreSQL server."""
    parsed = validate_database_url(database_url)

    host = _normalize_host(parsed.host)
    if host not in ALLOWED_TEST_HOSTS:
        raise UnsafeDatabaseConfigError(
            f"Refusing test connection to non-local host '{parsed.host}'. "
            f"Allowed hosts: {sorted(ALLOWED_TEST_HOSTS)}."
        )

    if not is_recognized_test_database_name(parsed.database):
        raise UnsafeDatabaseConfigError(
            f"Refusing test connection to unrecognized database '{parsed.database}'. "
            f"Test database name must be '{TEST_DB_EXACT_NAME}' or match "
            f"'{TEST_DB_PREFIX}<lowercase_alnum_underscore>'."
        )

    test_endpoint = _canonical_server_endpoint(parsed)
    dev_parsed = _resolve_dev_database_url(dev_database_url)
    if dev_parsed is not None:
        dev_endpoint = _canonical_server_endpoint(dev_parsed)
        if dev_endpoint == test_endpoint:
            raise UnsafeDatabaseConfigError(
                "TEST_DATABASE_URL must not target the same PostgreSQL server "
                "(host/port) as DATABASE_URL, even with a different database name."
            )

    return parsed


def verify_connected_test_database(
    connection: Connection,
    *,
    expected_database: str | None = None,
    dev_database_url: str | None | object = _UNSET,
) -> str:
    """Verify live PostgreSQL session identity and fail closed if not an approved test DB."""
    current_db = connection.execute(text("SELECT current_database()")).scalar_one()
    if not isinstance(current_db, str) or not is_recognized_test_database_name(current_db):
        raise UnsafeDatabaseConfigError(
            f"Connected database '{current_db}' is not a recognized test database."
        )

    if expected_database is not None and current_db != expected_database:
        raise UnsafeDatabaseConfigError(
            f"Connected database '{current_db}' does not match expected test database "
            f"'{expected_database}'."
        )

    dev_parsed = _resolve_dev_database_url(dev_database_url)
    if dev_parsed is not None:
        if dev_parsed.database and current_db == dev_parsed.database:
            raise UnsafeDatabaseConfigError(
                f"Connected database '{current_db}' matches development DATABASE_URL database."
            )
        engine = getattr(connection, "engine", None)
        engine_url = getattr(engine, "url", None)
        if isinstance(engine_url, URL):
            if _canonical_server_endpoint(engine_url) == _canonical_server_endpoint(dev_parsed):
                raise UnsafeDatabaseConfigError(
                    "Connected engine targets the same PostgreSQL server (host/port) "
                    "as development DATABASE_URL."
                )

    return current_db


def get_database_url(env_var: str = "DATABASE_URL") -> str:
    """Read and validate a database URL from the environment."""
    value = os.environ.get(env_var)
    if not value:
        raise UnsafeDatabaseConfigError(f"Environment variable '{env_var}' is not set.")
    validate_database_url(value)
    return value


def get_test_database_url() -> str:
    """Read and strictly validate TEST_DATABASE_URL from the environment."""
    value = os.environ.get("TEST_DATABASE_URL")
    if not value:
        raise UnsafeDatabaseConfigError(
            "Environment variable 'TEST_DATABASE_URL' is not set; refusing to guess."
        )
    validate_test_database_url(value, dev_database_url=os.environ.get("DATABASE_URL"))
    return value


def create_db_engine(
    database_url: str,
    *,
    pool_size: int = 5,
    max_overflow: int = 10,
    pool_timeout: int = 10,
    connect_timeout: int = 5,
    echo: bool = False,
) -> Engine:
    """Create a configured SQLAlchemy Engine for PostgreSQL/Psycopg 3 without global state."""
    validate_database_url(database_url)
    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        connect_args={"connect_timeout": connect_timeout},
        echo=echo,
    )


def create_test_engine(
    database_url: str | None = None,
    *,
    pool_size: int = 2,
    max_overflow: int = 4,
    pool_timeout: int = 10,
    connect_timeout: int = 5,
    echo: bool = False,
) -> Engine:
    """Create a validated SQLAlchemy Engine strictly for test databases."""
    resolved_url = database_url if database_url is not None else get_test_database_url()
    validate_test_database_url(resolved_url, dev_database_url=os.environ.get("DATABASE_URL"))
    return create_db_engine(
        resolved_url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=pool_timeout,
        connect_timeout=connect_timeout,
        echo=echo,
    )
