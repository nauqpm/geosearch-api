"""Configuration loading and validation for the GeoSearch FastAPI application.

Environment policy:
- Uses `DATABASE_URL` exclusively for application runtime connectivity.
- Never falls back to `TEST_DATABASE_URL`.
- Optional `.env` loading uses `override=False` so explicitly exported shell
  variables always take precedence.
- Core installations without `python-dotenv` work seamlessly when `DATABASE_URL`
  is exported in the environment.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from geosearch.db.engine import get_database_url, validate_database_url

DEFAULT_DB_POOL_SIZE: Final[int] = 5
DEFAULT_DB_MAX_OVERFLOW: Final[int] = 10
DEFAULT_DB_POOL_TIMEOUT: Final[int] = 10
DEFAULT_DB_CONNECT_TIMEOUT: Final[int] = 5
DEFAULT_DB_STATEMENT_TIMEOUT_MS: Final[int] = 5_000
DEFAULT_HEALTH_CHECK_TIMEOUT_MS: Final[int] = 2_000

MAX_DB_POOL_SIZE: Final[int] = 100
MAX_DB_MAX_OVERFLOW: Final[int] = 100
MAX_DB_TIMEOUT_SECONDS: Final[int] = 120
MAX_STATEMENT_TIMEOUT_MS: Final[int] = 60_000


@dataclass(frozen=True, slots=True)
class ApiSettings:
    """Validated runtime settings for the read-only GeoSearch API."""

    database_url: str
    db_pool_size: int = DEFAULT_DB_POOL_SIZE
    db_max_overflow: int = DEFAULT_DB_MAX_OVERFLOW
    db_pool_timeout: int = DEFAULT_DB_POOL_TIMEOUT
    db_connect_timeout: int = DEFAULT_DB_CONNECT_TIMEOUT
    db_statement_timeout_ms: int = DEFAULT_DB_STATEMENT_TIMEOUT_MS
    health_check_timeout_ms: int = DEFAULT_HEALTH_CHECK_TIMEOUT_MS

    def __post_init__(self) -> None:
        validate_database_url(self.database_url)
        _validate_bounded_int("db_pool_size", self.db_pool_size, 1, MAX_DB_POOL_SIZE)
        _validate_bounded_int("db_max_overflow", self.db_max_overflow, 0, MAX_DB_MAX_OVERFLOW)
        _validate_bounded_int("db_pool_timeout", self.db_pool_timeout, 1, MAX_DB_TIMEOUT_SECONDS)
        _validate_bounded_int(
            "db_connect_timeout", self.db_connect_timeout, 1, MAX_DB_TIMEOUT_SECONDS
        )
        _validate_bounded_int(
            "db_statement_timeout_ms",
            self.db_statement_timeout_ms,
            100,
            MAX_STATEMENT_TIMEOUT_MS,
        )
        _validate_bounded_int(
            "health_check_timeout_ms",
            self.health_check_timeout_ms,
            100,
            MAX_STATEMENT_TIMEOUT_MS,
        )


def _validate_bounded_int(name: str, value: int, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"Setting '{name}' must be an integer, got {type(value).__name__}.")
    if not (minimum <= value <= maximum):
        raise ValueError(
            f"Setting '{name}' must be between {minimum} and {maximum}, got {value}."
        )
    return value


def _read_optional_int_env(env_var: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(env_var)
    if raw is None or not raw.strip():
        return default
    try:
        parsed = int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"Environment variable '{env_var}' must be an integer.") from exc
    return _validate_bounded_int(env_var, parsed, minimum, maximum)


def _load_optional_dotenv() -> None:
    """Load local `.env` if `python-dotenv` is installed without overriding env vars."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(override=False)


def load_api_settings(
    *,
    database_url: str | None = None,
    load_dotenv_file: bool = True,
) -> ApiSettings:
    """Resolve and validate API settings from environment or explicit arguments.

    Explicitly exported environment variables always take precedence over `.env`.
    Never reads or falls back to `TEST_DATABASE_URL`.
    """
    if load_dotenv_file:
        _load_optional_dotenv()

    if database_url is not None:
        validate_database_url(database_url)
        resolved_url = database_url.strip()
    else:
        resolved_url = get_database_url("DATABASE_URL").strip()

    return ApiSettings(
        database_url=resolved_url,
        db_pool_size=_read_optional_int_env(
            "GEOSEARCH_DB_POOL_SIZE", DEFAULT_DB_POOL_SIZE, 1, MAX_DB_POOL_SIZE
        ),
        db_max_overflow=_read_optional_int_env(
            "GEOSEARCH_DB_MAX_OVERFLOW", DEFAULT_DB_MAX_OVERFLOW, 0, MAX_DB_MAX_OVERFLOW
        ),
        db_pool_timeout=_read_optional_int_env(
            "GEOSEARCH_DB_POOL_TIMEOUT", DEFAULT_DB_POOL_TIMEOUT, 1, MAX_DB_TIMEOUT_SECONDS
        ),
        db_connect_timeout=_read_optional_int_env(
            "GEOSEARCH_DB_CONNECT_TIMEOUT",
            DEFAULT_DB_CONNECT_TIMEOUT,
            1,
            MAX_DB_TIMEOUT_SECONDS,
        ),
        db_statement_timeout_ms=_read_optional_int_env(
            "GEOSEARCH_DB_STATEMENT_TIMEOUT_MS",
            DEFAULT_DB_STATEMENT_TIMEOUT_MS,
            100,
            MAX_STATEMENT_TIMEOUT_MS,
        ),
        health_check_timeout_ms=_read_optional_int_env(
            "GEOSEARCH_HEALTH_CHECK_TIMEOUT_MS",
            DEFAULT_HEALTH_CHECK_TIMEOUT_MS,
            100,
            MAX_STATEMENT_TIMEOUT_MS,
        ),
    )
