from __future__ import annotations

from uuid import uuid4

import ray

from agentos.checkpoints.manager import Checkpoint, CheckpointManager
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest
from agentos.memory.broker import MemoryBroker


@ray.remote(max_restarts=-1, max_task_retries=3)
class AgentWorkerActor:
    """Long-running Ray actor for a single specialized IT/development agent."""

    def __init__(self, agent_id: str, role: str, project_id: str, settings: dict):
        self.agent_id = agent_id
        self.role = role
        self.project_id = project_id
        self.settings = Settings(**settings) if settings else Settings()
        self.memory_broker = MemoryBroker()
        self.checkpoints = CheckpointManager()
        self.status = "STARTING"
        self.current_task_id: str | None = None

    async def start(self) -> dict:
        self.status = "IDLE"
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "project_id": self.project_id,
            "status": self.status,
        }

    async def heartbeat(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "project_id": self.project_id,
            "status": self.status,
            "current_task_id": self.current_task_id,
        }

    async def handle_event(self, event_id: str) -> dict:
        self.status = "CATCH_UP"
        packet = await self.memory_broker.build_catchup_packet(
            project_id=self.project_id,
            agent_id=self.agent_id,
            trigger_event_id=event_id,
        )

        self.status = "DECIDE_NEXT_ACTION"
        proposed = ActionRequest(
            project_id=self.project_id,
            agent_id=self.agent_id,
            action_type="create_summary",
            description=f"Summarize catch-up packet for event {event_id}.",
            payload={"catchup": packet.__dict__},
        )

        self.status = "CHECKPOINT"
        checkpoint = await self.checkpoints.create(
            Checkpoint(
                checkpoint_id=str(uuid4()),
                project_id=self.project_id,
                agent_id=self.agent_id,
                achievement="event_processed",
                summary=f"Processed event {event_id} and proposed next action {proposed.action_type}.",
            )
        )
        self.status = "IDLE"
        return {"agent_id": self.agent_id, "proposed_action": proposed.model_dump(), "checkpoint": checkpoint.model_dump()}
