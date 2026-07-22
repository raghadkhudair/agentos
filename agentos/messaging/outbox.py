from __future__ import annotations

import asyncio
from typing import Any

import ray
import structlog

from agentos.config.settings import Settings
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import EventRepository

logger = structlog.get_logger()


@ray.remote(num_cpus=0.1, max_concurrency=2)  # type: ignore[call-overload]
class OutboxDispatcherActor:
    """Reliably forwards durable PostgreSQL events to Dragonfly Streams."""

    def __init__(self, project_id: str, settings_payload: dict[str, Any]):
        self.project_id = project_id
        self.settings = Settings(**settings_payload)
        self.db = PostgresClient(self.settings)
        self.repository = EventRepository(self.db)
        self.bus = DragonflyBus(self.settings)
        self.running = False

    async def run(self) -> None:
        if self.running:
            return
        self.running = True
        while self.running:
            try:
                batch = await self.repository.claim_outbox(self.project_id, 100)
                if not batch:
                    await asyncio.sleep(0.25)
                    continue
                for row in batch:
                    try:
                        event = Event.model_validate(row["payload"])
                        await self.bus.publish_event(event.topic, event)
                        await self.repository.mark_outbox_published(row["id"])
                    except Exception as error:
                        await self.repository.mark_outbox_failed(
                            row["id"], type(error).__name__, row["attempt_count"] + 1
                        )
                        logger.error(
                            "outbox_publish_failed",
                            outbox_id=row["id"],
                            error_type=type(error).__name__,
                        )
            except asyncio.CancelledError:
                self.running = False
                raise
            except Exception as error:
                logger.error("outbox_dispatch_loop_failed", error_type=type(error).__name__)
                await asyncio.sleep(1)

    async def stop(self) -> None:
        self.running = False

    def status(self) -> dict[str, Any]:
        return {"running": self.running, "project_id": self.project_id}
