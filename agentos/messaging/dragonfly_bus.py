from __future__ import annotations

import json
from typing import Any

from agentos.config.settings import Settings
from agentos.messaging.events import Event, validate_event
from agentos.storage.clients.dragonfly import DragonflyClient


class DragonflyBus:
    """Typed event transport over Dragonfly Streams.

    Durable publication is handled through PostgreSQL's event outbox; this class
    only moves already-validated messages through the hot coordination layer.
    """

    def __init__(self, settings: Settings | str):
        if isinstance(settings, str):
            settings = Settings.model_validate({"AGENTOS_ENV": "test", "DRAGONFLY_URL": settings})
        self.client = DragonflyClient(settings)
        self.redis = self.client.redis

    def stream_key(self, topic: str) -> str:
        return self.client.key("stream", topic)

    def inbox_key(self, project_id: str, agent_id: str) -> str:
        return self.client.key("project", project_id, "agent", agent_id, "inbox")

    @staticmethod
    def inbox_group(project_id: str, agent_id: str) -> str:
        return f"agent-inbox-{project_id}-{agent_id}"

    async def publish_event(
        self,
        stream: str | None,
        event: Event,
        claimed_agent_id: str | None = None,
        *,
        max_length: int = 100_000,
    ) -> str:
        ok, reason = validate_event(event, claimed_agent_id)
        if not ok:
            raise ValueError(f"communication guardrail rejected event: {reason}")
        topic = stream or event.topic or event.get_target_topic()
        if topic != event.topic:
            raise ValueError("publish topic must equal the validated event topic")
        return str(
            await self.redis.xadd(
                self.stream_key(topic),
                {"event": event.model_dump_json()},
                maxlen=max_length,
                approximate=True,
            )
        )

    async def read_latest(self, stream: str, count: int = 10) -> list[dict[str, Any]]:
        items = await self.redis.xrevrange(self.stream_key(stream), count=max(1, min(count, 1000)))
        return [
            {"id": item_id, "event": json.loads(fields["event"])}
            for item_id, fields in items
            if "event" in fields
        ]

    async def ensure_group(self, topic: str, group: str) -> None:
        await self.client.ensure_consumer_group(self.stream_key(topic), group)

    async def ensure_inbox_group(self, project_id: str, agent_id: str) -> str:
        key = self.inbox_key(project_id, agent_id)
        group = self.inbox_group(project_id, agent_id)
        await self.client.ensure_consumer_group(key, group)
        return group

    async def send_to_inbox(
        self, project_id: str, agent_id: str, event: Event, *, max_length: int = 10_000
    ) -> str:
        if event.project_id != project_id:
            raise ValueError("inbox project does not match event project")
        del max_length
        # Do not trim a stream that can contain pending, unacknowledged work. Processed
        # entries can be compacted later from durable PostgreSQL receipt state.
        message_id = await self.redis.xadd(
            self.inbox_key(project_id, agent_id), {"event": event.model_dump_json()}
        )
        await self.redis.publish(
            self.client.key("project", project_id, "agent", agent_id, "wakeup"),
            event.event_type.value,
        )
        return str(message_id)

    async def read_inbox(
        self,
        project_id: str,
        agent_id: str,
        consumer_name: str,
        *,
        count: int = 10,
        block_milliseconds: int = 1000,
    ) -> list[tuple[str, dict[str, str]]]:
        key = self.inbox_key(project_id, agent_id)
        group = self.inbox_group(project_id, agent_id)
        response = await self.redis.xreadgroup(
            groupname=group,
            consumername=consumer_name,
            streams={key: ">"},
            count=max(1, min(count, 100)),
            block=max(1, block_milliseconds),
        )
        if not response:
            return []
        return [
            (str(message_id), dict(fields)) for _, items in response for message_id, fields in items
        ]

    async def reclaim_inbox(
        self,
        project_id: str,
        agent_id: str,
        consumer_name: str,
        *,
        min_idle_milliseconds: int,
        count: int = 10,
    ) -> list[tuple[str, dict[str, str]]]:
        response = await self.redis.xautoclaim(
            self.inbox_key(project_id, agent_id),
            self.inbox_group(project_id, agent_id),
            consumer_name,
            max(1, min_idle_milliseconds),
            "0-0",
            count=max(1, min(count, 100)),
        )
        if not response:
            return []
        items = response[1] if len(response) > 1 else []
        return [(str(message_id), dict(fields)) for message_id, fields in items]

    async def acknowledge_inbox(self, project_id: str, agent_id: str, message_id: str) -> int:
        return int(
            await self.redis.xack(
                self.inbox_key(project_id, agent_id),
                self.inbox_group(project_id, agent_id),
                message_id,
            )
        )

    async def close(self) -> None:
        await self.client.close()
