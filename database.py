"""
Async database layer for the Local Regulatory Change Radar (LRCR).

    engine          SQLAlchemy async engine (PostgreSQL via asyncpg, or SQLite for local dev)
    SessionLocal    async_sessionmaker
    init_db()       create tables + add columns new models gained (idempotent, safe with several instances starting at once)
    get_session()   FastAPI dependency / async context manager

Which database is used (first one that is set wins):

    LRCR_DATABASE_URL   explicit override
    DATABASE_URL        the name Render / Neon / Supabase / Heroku use
    (neither)           sqlite+aiosqlite:///./lrcr.db   -- local development ONLY

On an ephemeral host (Render free tier) SQLite is wiped on every restart, which
also wipes the FOIA send ledger. Set DATABASE_URL to a hosted PostgreSQL.

Pass the connection string exactly as the provider shows it. These are all
accepted and rewritten to the asyncpg driver automatically:

    postgres://user:pw@host/db
    postgresql://user:pw@host/db?sslmode=require&channel_binding=require    (Neon)
    postgresql://postgres.PROJECTREF:pw@aws-0-REGION.pooler.supabase.com:6543/postgres   (Supabase)
    postgresql+asyncpg://user:pw@host/db

Handled for you, because asyncpg differs from psycopg2:
    * libpq-only URL options (sslmode, channel_binding, options, ...) are
      translated or dropped instead of crashing asyncpg.
    * TLS is on by default for any non-local host (Neon and Supabase require it).
    * Prepared-statement caching is off, so PgBouncer / Supavisor transaction
      pooling (Neon "-pooler" host, Supabase port 6543) works.
    * pool_pre_ping + pool_recycle, so connections that a free-tier database
      dropped while suspended are replaced instead of raising.

Optional tuning: LRCR_DB_POOL_SIZE (5), LRCR_DB_MAX_OVERFLOW (5),
LRCR_DB_POOL_RECYCLE_S (240), LRCR_DB_CONNECT_TIMEOUT_S (30), LRCR_SQL_ECHO=1.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from sqlalchemy import event, inspect, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

logger = logging.getLogger("lrcr.db")

_DEFAULT_SQLITE_URL = "sqlite+aiosqlite:///./lrcr.db"
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1", ""}
# Query options asyncpg understands (everything else in a libpq-style URL is dropped).
_ASYNCPG_QUERY_KEYS = {"ssl", "host", "timeout", "command_timeout", "statement_cache_size", "prepared_statement_cache_size"}
_SCHEMA_LOCK_KEY = 727_001_001   # arbitrary constant: serialises create_all across instances


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        logger.warning("%s is not an integer, using %d", name, default)
        return default


def _raw_database_url() -> str:
    return (os.environ.get("LRCR_DATABASE_URL") or os.environ.get("DATABASE_URL") or "").strip() or _DEFAULT_SQLITE_URL


def prepare_database_url(raw: str) -> tuple[URL, dict[str, Any]]:
    """
    Turn whatever the provider gave us into (SQLAlchemy URL, connect_args).

    Pure function (no I/O) so it can be unit-tested. Raises ValueError for a
    URL it cannot make sense of.
    """
    try:
        url = make_url(raw)
    except Exception as exc:  # noqa: BLE001 — sqlalchemy raises ArgumentError; keep it friendly
        raise ValueError(f"DATABASE_URL is not a valid database URL: {exc}") from exc

    backend = url.get_backend_name()

    if backend == "sqlite":
        if url.drivername == "sqlite":
            url = url.set(drivername="sqlite+aiosqlite")
        return url, {"timeout": 30}

    if backend not in {"postgres", "postgresql"}:
        raise ValueError(f"unsupported database backend {backend!r}; use PostgreSQL or SQLite")

    # postgres:// and postgresql:// (and psycopg2-style URLs) -> asyncpg
    url = url.set(drivername="postgresql+asyncpg")

    query = dict(url.query)
    sslmode = str(query.get("sslmode", "")).strip().lower()
    ssl_arg = str(query.get("ssl", "")).strip().lower()
    kept = {k: v for k, v in query.items() if k in _ASYNCPG_QUERY_KEYS and k != "ssl"}
    dropped = sorted(set(query) - set(kept) - {"ssl", "sslmode"})
    if dropped:
        logger.info("ignoring URL option(s) asyncpg does not use: %s", ", ".join(dropped))
    url = url.set(query={})   # everything asyncpg needs goes through connect_args below

    connect_args: dict[str, Any] = {
        "timeout": _env_int("LRCR_DB_CONNECT_TIMEOUT_S", 30),
        # PgBouncer / Supavisor in transaction mode cannot handle server-side prepared statements.
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
        "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__",
    }
    for k, v in kept.items():                      # explicit URL values win over our defaults
        connect_args[k] = int(v) if k.endswith(("timeout", "size")) else v

    # TLS: an explicit sslmode/ssl in the URL wins; otherwise require it for any remote host.
    wanted = sslmode or ssl_arg
    if wanted in {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}:
        connect_args["ssl"] = wanted
    elif wanted in {"true", "1", "yes", "on"}:
        connect_args["ssl"] = "require"
    elif wanted in {"false", "0", "no", "off"}:
        connect_args["ssl"] = "disable"
    elif (url.host or "") not in _LOCAL_HOSTS:
        connect_args["ssl"] = "require"
    return url, connect_args


_RAW_URL = _raw_database_url()
_URL, _CONNECT_ARGS = prepare_database_url(_RAW_URL)
IS_POSTGRES: bool = _URL.get_backend_name() == "postgresql"
_IS_SQLITE: bool = not IS_POSTGRES

DATABASE_URL: str = _URL.render_as_string(hide_password=False)       # for the engine only; never log this
DATABASE_URL_SAFE: str = _URL.render_as_string(hide_password=True)   # safe for logs


class Base(DeclarativeBase):
    """Declarative base shared by every model in models.py."""


def _build_engine() -> AsyncEngine:
    echo = os.environ.get("LRCR_SQL_ECHO", "0") == "1"
    if IS_POSTGRES:
        return create_async_engine(
            _URL,
            echo=echo,
            connect_args=_CONNECT_ARGS,
            pool_size=_env_int("LRCR_DB_POOL_SIZE", 5),
            max_overflow=_env_int("LRCR_DB_MAX_OVERFLOW", 5),
            pool_pre_ping=True,                                   # replace connections the host dropped while idle
            pool_recycle=_env_int("LRCR_DB_POOL_RECYCLE_S", 240),  # below Neon's 5-minute idle suspend
            pool_timeout=30,
        )
    return create_async_engine(_URL, echo=echo, connect_args=_CONNECT_ARGS)


engine: AsyncEngine = _build_engine()

if _IS_SQLITE:

    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_pragmas(dbapi_connection, _record) -> None:  # type: ignore[no-untyped-def]
        """
        SQLite does NOT enforce foreign keys unless asked, per connection.
        WAL lets readers proceed while a pipeline run is writing.
        """
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()


SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,   # objects stay usable after commit (we read ids back)
    autoflush=False,
)


def _add_missing_columns(sync_conn: Any) -> list[str]:
    """
    create_all() creates missing TABLES but never adds columns to tables that
    already exist, so a database created by an earlier version would break as soon
    as a model gains a field. This adds every missing nullable column with a plain
    ALTER TABLE ... ADD COLUMN (works on SQLite and PostgreSQL) and returns what it
    added. Not a replacement for Alembic: it never alters, renames or drops.
    """
    insp = inspect(sync_conn)
    preparer = sync_conn.dialect.identifier_preparer
    added: list[str] = []
    for table in Base.metadata.sorted_tables:
        if not insp.has_table(table.name):
            continue
        existing = {c["name"] for c in insp.get_columns(table.name)}
        for col in table.columns:
            if col.name in existing:
                continue
            if not col.nullable and col.server_default is None:
                logger.error("cannot auto-add NOT NULL column %s.%s without a default; migrate it by hand",
                             table.name, col.name)
                continue
            ddl = (f"ALTER TABLE {preparer.quote(table.name)} ADD COLUMN {preparer.quote(col.name)} "
                   f"{col.type.compile(dialect=sync_conn.dialect)}")
            sync_conn.execute(text(ddl))
            added.append(f"{table.name}.{col.name}")
    return added


async def _create_schema() -> None:
    import models  # noqa: F401 — registers the models on Base.metadata

    async with engine.begin() as conn:
        if IS_POSTGRES:
            # Render starts the new instance before stopping the old one, so two
            # processes can run create_all together. The transaction-scoped lock
            # serialises them (and works through a transaction-mode pooler).
            await conn.execute(text(f"SELECT pg_advisory_xact_lock({_SCHEMA_LOCK_KEY})"))
        await conn.run_sync(Base.metadata.create_all)
        added = await conn.run_sync(_add_missing_columns)
        if added:
            logger.info("schema upgraded, added column(s): %s", ", ".join(added))
        if _IS_SQLITE:
            mode = (await conn.execute(text("PRAGMA journal_mode"))).scalar()
            logger.info("database ready at %s (journal_mode=%s)", DATABASE_URL_SAFE, mode)
        else:
            logger.info("database ready at %s", DATABASE_URL_SAFE)


async def init_db(*, attempts: int = 5) -> None:
    """
    Create all tables that do not exist yet. Safe to call on every startup.

    A hosted database that was suspended (Neon) or is still waking up may refuse
    the first connection, so PostgreSQL gets a few retries with backoff before
    the error is allowed to stop the app.
    """
    if _IS_SQLITE:
        await _create_schema()
        return
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            await _create_schema()
            return
        except Exception as exc:  # noqa: BLE001 — anything at connect time is worth a retry
            if attempt == attempts:
                logger.error("database unreachable after %d attempts (%s): %s", attempts, DATABASE_URL_SAFE, exc)
                raise
            logger.warning("database not ready (attempt %d/%d): %s: %s — retrying in %.0fs",
                           attempt, attempts, type(exc).__name__, exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 20.0)


async def dispose_db() -> None:
    await engine.dispose()


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Transactional scope: commit on success, rollback on any exception."""
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: `session: AsyncSession = Depends(get_session)`."""
    async with SessionLocal() as session:
        yield session
