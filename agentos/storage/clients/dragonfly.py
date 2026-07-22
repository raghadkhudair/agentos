from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from agentos.config.settings import Settings


class DragonflyClient:
    """Namespaced Dragonfly client for ephemeral coordination only."""

    _RELEASE_LOCK = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.redis = Redis.from_url(
            settings.dragonfly_dsn,
            decode_responses=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            socket_timeout=10,
            retry_on_timeout=True,
        )

    def key(self, *parts: object) -> str:
        safe_parts = [str(part).strip(":") for part in parts]
        return ":".join([self.settings.dragonfly_key_prefix, *safe_parts])

    async def close(self) -> None:
        await self.redis.aclose()

    async def healthcheck(self) -> dict[str, Any]:
        healthy = bool(await self.redis.ping())
        info = await self.redis.info(section="server")
        return {
            "service": "dragonfly",
            "healthy": healthy,
            "version": info.get("dragonfly_version") or info.get("redis_version"),
        }

    async def ensure_consumer_group(self, stream: str, group: str) -> None:
        try:
            await self.redis.xgroup_create(stream, group, id="0-0", mkstream=True)
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

    @asynccontextmanager
    async def lock(
        self, name: str, *, ttl_seconds: int = 120, wait_seconds: float = 0
    ) -> AsyncIterator[bool]:
        key = self.key("lock", name)
        token = secrets.token_urlsafe(24)
        acquired = bool(await self.redis.set(key, token, ex=ttl_seconds, nx=True))
        if not acquired and wait_seconds > 0:
            lock = self.redis.lock(key, timeout=ttl_seconds, blocking_timeout=wait_seconds)
            acquired = bool(await lock.acquire())
            if acquired:
                token = await self.redis.get(key) or token
        try:
            yield acquired
        finally:
            if acquired:
                await self.redis.eval(self._RELEASE_LOCK, 1, key, token)

    async def set_json(self, key: str, payload: str, *, ttl_seconds: int | None = None) -> None:
        await self.redis.set(self.key(key), payload, ex=ttl_seconds)
