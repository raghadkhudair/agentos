from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import Any

import ray
import structlog

from agentos.config.loader import runtime_tuning
from agentos.config.settings import Settings
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, validate_event
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import EventRepository

logger = structlog.get_logger()


@ray.remote(num_cpus=0.2, max_concurrency=8)  # type: ignore[call-overload]
class TriggerEngineActor:
    """Routes typed events to independent agent inboxes with consumer-group recovery."""

    SYSTEM_PRODUCERS = {
        None,
        "runtime_supervisor",
        "dod_evaluator",
        "dod_watchdog",
        "stagnation_watchdog",
        "deadlock_watchdog",
        "safety_watchdog",
        "infrastructure_agent-1",
        "outbox_dispatcher",
    }

    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.bus = DragonflyBus(self.settings)
        self.db = PostgresClient(self.settings)
        self.receipts = EventRepository(self.db)
        self.subscriptions: dict[str, set[str]] = defaultdict(set)
        self.registered_agents: set[str] = set()
        self.allowed_producers: dict[str, set[str]] = defaultdict(set)
        self.fallback_coordinator: str | None = None
        self.is_running = False
        self.tuning = runtime_tuning()

    async def register_agent(
        self,
        agent_id: str,
        event_subscriptions: list[str],
        allowed_event_types: list[str],
        *,
        coordinator: bool = False,
    ) -> None:
        self.registered_agents.add(agent_id)
        for event_type in event_subscriptions:
            self.subscriptions[event_type].add(agent_id)
        for event_type in allowed_event_types:
            self.allowed_producers[event_type].add(agent_id)
        if coordinator:
            self.fallback_coordinator = agent_id

    async def register_subscription(self, event_type: str, agent_id: str) -> None:
        self.registered_agents.add(agent_id)
        self.subscriptions[event_type].add(agent_id)

    async def register_allowed_producer(self, event_type: str, agent_id: str) -> None:
        self.allowed_producers[event_type].add(agent_id)

    @staticmethod
    def topics(project_id: str) -> list[str]:
        return [
            f"project.{project_id}.{suffix}"
            for suffix in (
                "events",
                "tasks",
                "contracts",
                "reviews",
                "tests",
                "blockers",
                "checkpoints",
                "summaries",
                "resources",
            )
        ]

    async def start_routing_loop(self, project_id: str) -> None:
        if self.is_running:
            return
        self.is_running = True
        topics = self.topics(project_id)
        group = f"trigger-engine-{project_id}"
        consumer = f"router-{ray.get_runtime_context().get_actor_id()}"
        for topic in topics:
            await self.bus.ensure_group(topic, group)
        logger.info("trigger_engine_started", project_id=project_id, topics=topics)
        block_ms = int(self.tuning["agent_inbox_loop"]["stream_block_milliseconds"])
        batch_size = int(self.tuning["agent_inbox_loop"]["batch_size"])
        claim_idle_ms = int(self.tuning["agent_inbox_loop"]["processing_lease_seconds"]) * 1000

        while self.is_running:
            try:
                reclaimed: list[tuple[str, list[tuple[str, dict[str, str]]]]] = []
                for topic in topics:
                    stream_key = self.bus.stream_key(topic)
                    claimed = await self.bus.redis.xautoclaim(
                        stream_key,
                        group,
                        consumer,
                        claim_idle_ms,
                        "0-0",
                        count=batch_size,
                    )
                    if claimed and len(claimed) > 1 and claimed[1]:
                        reclaimed.append((stream_key, claimed[1]))
                if reclaimed:
                    await self._process_stream_messages(group, reclaimed)

                streams = {self.bus.stream_key(topic): ">" for topic in topics}
                response = await self.bus.redis.xreadgroup(
                    groupname=group,
                    consumername=consumer,
                    streams=streams,
                    count=batch_size,
                    block=block_ms,
                )
                await self._process_stream_messages(group, response or [])
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                self.is_running = False
                raise
            except Exception as error:
                logger.error("trigger_engine_loop_error", error_type=type(error).__name__)
                await asyncio.sleep(float(self.tuning["agent_inbox_loop"]["error_backoff_seconds"]))

    async def _process_stream_messages(
        self,
        group: str,
        response: list[tuple[str, list[tuple[str, dict[str, str]]]]],
    ) -> None:
        for stream_key, items in response:
            for message_id, fields in items:
                raw = fields.get("event")
                if not raw:
                    await self.bus.redis.xack(stream_key, group, message_id)
                    continue
                try:
                    event = Event.model_validate(json.loads(raw))
                    await self._route_event(event)
                    await self.bus.redis.xack(stream_key, group, message_id)
                except Exception as error:
                    logger.error(
                        "event_routing_failed",
                        message_id=message_id,
                        error_type=type(error).__name__,
                    )

    async def _route_event(self, event: Event) -> None:
        valid, reason = validate_event(event)
        if not valid:
            raise ValueError(reason)
        event_type = event.event_type.value
        producer = event.producer_agent_id
        if producer not in self.SYSTEM_PRODUCERS and producer not in self.registered_agents:
            raise PermissionError("unregistered event producer")
        explicit = self.allowed_producers.get(event_type)
        if producer not in self.SYSTEM_PRODUCERS and explicit and producer not in explicit:
            raise PermissionError(f"producer cannot emit {event_type}")

        subscribers = set(self.subscriptions.get(event_type, set()))
        if event.target_agent_id:
            if event.target_agent_id not in self.registered_agents:
                raise PermissionError("target agent is not registered")
            subscribers = {event.target_agent_id}
        if producer:
            subscribers.discard(producer)
        if not subscribers and self.fallback_coordinator and producer != self.fallback_coordinator:
            subscribers.add(self.fallback_coordinator)

        for agent_id in sorted(subscribers):
            status = await self.receipts.event_receipt_status(
                event.project_id, str(event.event_id), agent_id
            )
            if status is not None:
                continue
            stream_id = await self.bus.send_to_inbox(event.project_id, agent_id, event)
            await self.receipts.record_event_delivery(
                event.project_id, str(event.event_id), agent_id, stream_id
            )
        logger.info(
            "event_dispatched",
            event_id=str(event.event_id),
            event_type=event_type,
            recipients=sorted(subscribers),
        )

    async def stop(self) -> None:
        self.is_running = False

    def status(self) -> dict[str, bool]:
        return {"running": self.is_running}
