"""Alembic migration environment configured for GeoSearch PostgreSQL/PostGIS schema."""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection

from geosearch.db import Base, create_db_engine, validate_database_url
from geosearch.db.types import (
    alembic_compare_type,
    alembic_include_object,
    alembic_render_item,
    get_default_schema,
    get_extension_managed_tables,
    get_visible_extension_managed_tables,
    make_alembic_include_object,
)

# Ensure all canonical models are registered on Base.metadata.
import geosearch.db.models  # noqa: F401


def _load_optional_dotenv() -> None:
    """Load local .env when python-dotenv is installed; remain optional for core installs."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(override=False)


_load_optional_dotenv()

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata

include_object = alembic_include_object
compare_type = alembic_compare_type
render_item = alembic_render_item


def _resolve_database_url() -> str:
    configured = config.get_main_option("sqlalchemy.url")
    if configured and configured.strip():
        validate_database_url(configured)
        return configured.strip()

    env_url = os.environ.get("DATABASE_URL")
    if not env_url or not env_url.strip():
        raise RuntimeError(
            "No database URL configured for Alembic. Set 'sqlalchemy.url' in Alembic "
            "Config or export DATABASE_URL."
        )
    validate_database_url(env_url)
    return env_url.strip()


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = _resolve_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
        compare_type=compare_type,
        compare_server_default=True,
        render_item=render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


def _configure_and_run_online(connection: Connection) -> None:
    was_in_transaction = connection.in_transaction()
    default_schema = get_default_schema(connection)
    ignored_tables = get_extension_managed_tables(connection)
    visible_ignored_tables = get_visible_extension_managed_tables(connection)
    if not was_in_transaction and connection.in_transaction():
        connection.rollback()

    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_object=make_alembic_include_object(
            ignored_tables,
            visible_ignored_tables=visible_ignored_tables,
            default_schema=default_schema,
        ),
        compare_type=compare_type,
        compare_server_default=True,
        render_item=render_item,
        transaction_per_migration=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode using an existing connection or engine factory."""
    provided_connection = config.attributes.get("connection")
    if provided_connection is not None:
        _configure_and_run_online(provided_connection)
        return

    url = _resolve_database_url()
    connectable = create_db_engine(url, pool_size=1, max_overflow=0)
    try:
        with connectable.connect() as connection:
            _configure_and_run_online(connection)
    finally:
        connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
