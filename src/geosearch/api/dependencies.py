"""FastAPI dependencies and read-only PostgreSQL transaction helpers.

Preserves the approved adaptive isolation policy (ARCHITECTURE_BASELINE.md Section 15):
- Single-statement reads: `READ COMMITTED, READ ONLY`
- Multi-statement reads requiring a shared publication snapshot:
  `REPEATABLE READ, READ ONLY` via M3's `repeatable_read_snapshot` helper.

All database interactions in the API are strictly `READ ONLY` at the PostgreSQL
transaction level and bounded by a transaction-local `statement_timeout`.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from fastapi import Depends, Request
from sqlalchemy import Connection, Engine, text

from geosearch.publication.readers import repeatable_read_snapshot


def _apply_local_statement_timeout(conn: Connection, statement_timeout_ms: int | None) -> None:
    if statement_timeout_ms is None:
        return
    conn.execute(
        text("SELECT set_config('statement_timeout', :timeout_val, true)"),
        {"timeout_val": f"{int(statement_timeout_ms)}ms"},
    )


@contextmanager
def read_committed_readonly(
    engine: Engine,
    *,
    statement_timeout_ms: int | None = None,
) -> Iterator[Connection]:
    """Open a short PostgreSQL `READ COMMITTED, READ ONLY` transaction on a pooled connection."""
    with engine.connect() as conn:
        with conn.begin():
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
            _apply_local_statement_timeout(conn, statement_timeout_ms)
            yield conn


@contextmanager
def repeatable_read_readonly(
    engine: Engine,
    *,
    statement_timeout_ms: int | None = None,
) -> Iterator[Connection]:
    """Open a short PostgreSQL `REPEATABLE READ, READ ONLY` transaction via M3 reader helper."""
    with repeatable_read_snapshot(engine) as conn:
        _apply_local_statement_timeout(conn, statement_timeout_ms)
        yield conn


def get_engine(request: Request) -> Engine:
    """Return the application-scoped SQLAlchemy Engine stored on `app.state`."""
    engine: Engine = request.app.state.engine
    return engine


def get_statement_timeout_ms(request: Request) -> int:
    """Return the configured per-request PostgreSQL statement timeout in milliseconds."""
    return int(request.app.state.statement_timeout_ms)


def get_health_check_timeout_ms(request: Request) -> int:
    """Return the configured health-check PostgreSQL statement timeout in milliseconds."""
    return int(request.app.state.health_check_timeout_ms)


def get_read_committed_conn(
    engine: Engine = Depends(get_engine),
    statement_timeout_ms: int = Depends(get_statement_timeout_ms),
) -> Iterator[Connection]:
    """FastAPI dependency yielding a `READ COMMITTED, READ ONLY` PostgreSQL connection."""
    with read_committed_readonly(engine, statement_timeout_ms=statement_timeout_ms) as conn:
        yield conn


def get_repeatable_read_conn(
    engine: Engine = Depends(get_engine),
    statement_timeout_ms: int = Depends(get_statement_timeout_ms),
) -> Iterator[Connection]:
    """FastAPI dependency yielding a `REPEATABLE READ, READ ONLY` PostgreSQL connection."""
    with repeatable_read_readonly(engine, statement_timeout_ms=statement_timeout_ms) as conn:
        yield conn
