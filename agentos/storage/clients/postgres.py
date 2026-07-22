from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg

from agentos.config.settings import Settings

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "schema.sql"


class PostgresClient:
    """Async PostgreSQL client and durable system-of-record boundary."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool: asyncpg.Pool | None = None
        self._connect_lock = asyncio.Lock()

    @staticmethod
    def _encode_json(value: Any) -> str:
        """Encode native values while accepting the repository layer's JSON text."""

        if isinstance(value, str):
            try:
                json.loads(value)
            except json.JSONDecodeError:
                return json.dumps(value, separators=(",", ":"))
            return value
        return json.dumps(value, separators=(",", ":"), default=str)

    @classmethod
    async def _initialize_connection(cls, connection: asyncpg.Connection) -> None:
        """Install codecs on every physical connection created by the pool."""

        for type_name in ("json", "jsonb"):
            await connection.set_type_codec(
                type_name,
                schema="pg_catalog",
                encoder=cls._encode_json,
                decoder=json.loads,
                format="text",
            )

    async def connect(self) -> None:
        if self.pool is not None:
            return
        async with self._connect_lock:
            if self.pool is None:
                self.pool = await asyncpg.create_pool(
                    dsn=self.settings.postgres_dsn,
                    min_size=self.settings.postgres_pool_min_size,
                    max_size=self.settings.postgres_pool_max_size,
                    command_timeout=self.settings.postgres_command_timeout_seconds,
                    init=self._initialize_connection,
                    server_settings={
                        "application_name": "agentos",
                        "timezone": "UTC",
                        "statement_timeout": str(
                            int(self.settings.postgres_command_timeout_seconds * 1000)
                        ),
                        "idle_in_transaction_session_timeout": "30000",
                    },
                )

    async def disconnect(self) -> None:
        pool, self.pool = self.pool, None
        if pool is not None:
            await pool.close()

    async def healthcheck(self) -> dict[str, Any]:
        await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            version = await connection.fetchval("SELECT current_setting('server_version')")
            read_write = await connection.fetchval("SELECT NOT pg_is_in_recovery()")
        return {
            "service": "postgres",
            "healthy": bool(version),
            "version": version,
            "writable": read_write,
        }

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                yield connection

    async def execute(self, query: str, *args: Any) -> str:
        await self.connect()
        assert self.pool is not None
        return str(await self.pool.execute(query, *args))

    async def fetch(self, query: str, *args: Any) -> list[asyncpg.Record]:
        await self.connect()
        assert self.pool is not None
        return list(await self.pool.fetch(query, *args))

    async def fetchrow(self, query: str, *args: Any) -> asyncpg.Record | None:
        await self.connect()
        assert self.pool is not None
        return await self.pool.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any) -> Any:
        await self.connect()
        assert self.pool is not None
        return await self.pool.fetchval(query, *args)

    async def initialize_schema(self) -> None:
        """Apply the idempotent schema while holding a cross-process advisory lock."""

        schema_sql = await asyncio.to_thread(SCHEMA_PATH.read_text, encoding="utf-8")
        async with self.transaction() as connection:
            await connection.execute("SELECT pg_advisory_xact_lock(hashtext('agentos-schema-v2'))")
            projects_exists = await connection.fetchval("SELECT to_regclass('public.projects')")
            if projects_exists:
                current_shape = await connection.fetchval(
                    """
                    SELECT EXISTS(
                      SELECT 1 FROM information_schema.columns
                      WHERE table_schema='public' AND table_name='projects'
                        AND column_name='project_key'
                    )
                    """
                )
                if not current_shape:
                    raise RuntimeError(
                        "legacy AgentOS schema detected; automatic destructive migration is refused. "
                        "Export the legacy data and initialize the v2 polyglot schema in a new database."
                    )
            await connection.execute(schema_sql)
