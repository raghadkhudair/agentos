from __future__ import annotations

import json

from redis.asyncio import Redis

from agentos.messaging.events import Event


class DragonflyBus:
    """Redis-compatible event helper for Dragonfly Streams.

    PostgreSQL remains the durable source of truth. Dragonfly is used for hot coordination.
    """

    def __init__(self, url: str):
        self.redis = Redis.from_url(url, decode_responses=True)

    async def publish_event(self, stream: str, event: Event) -> str:
        return await self.redis.xadd(stream, {"event": event.model_dump_json()})

    async def read_latest(self, stream: str, count: int = 10) -> list[dict]:
        items = await self.redis.xrevrange(stream, count=count)
        return [{"id": item_id, "event": json.loads(fields["event"])} for item_id, fields in items]
