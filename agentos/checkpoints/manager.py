from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import ray
import structlog
from pydantic import BaseModel, Field

from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.messaging.events import Event, EventType
from agentos.provider.gateway import ProviderRequest
from agentos.storage.clients.mongodb import MongoDocumentClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import CheckpointRepository, EventRepository, SummaryRepository

logger = structlog.get_logger()


class Checkpoint(BaseModel):
    checkpoint_id: str = Field(default_factory=lambda: str(uuid4()))
    project_id: str
    agent_id: str
    achievement: str
    summary: str
    task_id: str | None = None
    artifacts: list[str] = Field(default_factory=list)
    agent_state_snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


@ray.remote(num_cpus=0.1, max_concurrency=16)  # type: ignore[call-overload]
class CheckpointManagerActor:
    """Durably checkpoints achievements and mirrors resumable state to MongoDB."""

    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.db = PostgresClient(self.settings)
        self.repo = CheckpointRepository(self.db)
        self.events = EventRepository(self.db)
        self.mongo = MongoDocumentClient(self.settings)

    async def create(self, checkpoint_dict: dict[str, Any]) -> dict[str, Any]:
        checkpoint = Checkpoint.model_validate(checkpoint_dict)
        persisted_id = await self.repo.save_checkpoint(
            project_id=checkpoint.project_id,
            agent_id=checkpoint.agent_id,
            achievement=checkpoint.achievement,
            summary=checkpoint.summary,
            task_id=checkpoint.task_id,
            agent_state_snapshot=checkpoint.agent_state_snapshot,
            artifacts=checkpoint.artifacts,
        )
        await self.mongo.save_agent_state(
            project_id=checkpoint.project_id,
            agent_id=checkpoint.agent_id,
            state={
                **checkpoint.agent_state_snapshot,
                "checkpoint_id": persisted_id,
                "achievement": checkpoint.achievement,
                "updated_at": checkpoint.created_at.isoformat(),
            },
        )
        event = Event(
            project_id=checkpoint.project_id,
            event_type=EventType.CHECKPOINT_CREATED,
            producer_agent_id=checkpoint.agent_id,
            payload={
                "checkpoint_id": persisted_id,
                "achievement": checkpoint.achievement,
                "task_id": checkpoint.task_id,
            },
        )
        await self.events.save_event(checkpoint.project_id, event)
        return {**checkpoint.model_dump(mode="json"), "checkpoint_id": persisted_id}

    async def recover_agent_state(self, project_id: str, agent_id: str) -> dict[str, Any] | None:
        midterm = await self.mongo.load_agent_state(project_id=project_id, agent_id=agent_id)
        if midterm:
            return midterm
        row = await self.repo.latest(project_id, agent_id)
        if not row:
            return None
        return {
            "checkpoint_id": str(row["id"]),
            "achievement": row["achievement"],
            "summary": row["summary"],
            "task_id": str(row["task_id"]) if row["task_id"] else None,
            "agent_state_snapshot": row["state_pointer"],
            "artifacts": row["artifacts"],
        }


@ray.remote(num_cpus=0.1, max_concurrency=8)  # type: ignore[call-overload]
class SummaryManagerActor:
    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.db = PostgresClient(self.settings)
        self.repo = SummaryRepository(self.db)

    async def _summarize(
        self,
        *,
        project_id: str,
        purpose: str,
        prompt: str,
        provider_gateway: Any,
        agent_id: str,
        agent_role: str,
        complexity: TaskComplexity = TaskComplexity.LOW,
    ) -> str:
        request = ProviderRequest(
            purpose=purpose,
            messages=[{"role": "user", "content": prompt}],
            budget_key=UUID(project_id),
            agent_id=agent_id,
            agent_role=agent_role,
            complexity=complexity,
        )
        response = await provider_gateway.get_completion.remote(request.model_dump(mode="json"))
        return str(response["content"]).strip()

    async def generate_local_agent_summary(
        self, project_id: str, agent_id: str, provider_gateway: Any
    ) -> str:
        rows = await self.db.fetch(
            """
            SELECT achievement,summary,created_at FROM checkpoints
            WHERE project_id=$1 AND agent_id=$2 ORDER BY created_at DESC LIMIT 20
            """,
            __import__("uuid").UUID(project_id),
            agent_id,
        )
        history = (
            "\n".join(
                f"- {row['created_at'].isoformat()} {row['achievement']}: {row['summary']}"
                for row in rows
            )
            or "No checkpoints."
        )
        summary = await self._summarize(
            project_id=project_id,
            purpose="heartbeat_summary",
            prompt=f"Summarize this agent history as factual progress, blockers, and next work:\n{history}",
            provider_gateway=provider_gateway,
            agent_id=agent_id,
            agent_role="pm_tech_lead",
        )
        await self.repo.save_summary(project_id, "agent_local", agent_id, summary)
        return summary

    async def generate_squad_summary(
        self, project_id: str, squad_name: str, provider_gateway: Any
    ) -> str:
        rows = await self.db.fetch(
            """
            SELECT c.agent_id,c.achievement,c.summary,c.created_at
            FROM checkpoints c JOIN agents a ON a.project_id=c.project_id AND a.id=c.agent_id
            WHERE c.project_id=$1 AND a.squad=$2 ORDER BY c.created_at DESC LIMIT 30
            """,
            __import__("uuid").UUID(project_id),
            squad_name,
        )
        history = (
            "\n".join(f"- {row['agent_id']} {row['achievement']}: {row['summary']}" for row in rows)
            or "No checkpoints."
        )
        summary = await self._summarize(
            project_id=project_id,
            purpose="heartbeat_summary",
            prompt=f"Summarize squad {squad_name} progress and blockers without speculation:\n{history}",
            provider_gateway=provider_gateway,
            agent_id="summary_manager",
            agent_role="pm_tech_lead",
        )
        await self.repo.save_summary(project_id, "squad", squad_name, summary)
        return summary

    async def generate_project_summary(self, project_id: str, provider_gateway: Any) -> str:
        stats = await self.db.fetch(
            "SELECT status,count(*) count FROM tasks WHERE project_id=$1 GROUP BY status",
            __import__("uuid").UUID(project_id),
        )
        dod = await self.db.fetch(
            "SELECT criterion_id,status,evidence_summary FROM dod_checks WHERE project_id=$1 ORDER BY created_at",
            __import__("uuid").UUID(project_id),
        )
        prompt = (
            "Write a factual project status under 180 words. Do not claim completion without SATISFIED evidence.\n"
            f"Task counts: {json.dumps([dict(row) for row in stats], default=str)}\n"
            f"DoD: {json.dumps([dict(row) for row in dod], default=str)}"
        )
        summary = await self._summarize(
            project_id=project_id,
            purpose="heartbeat_summary",
            prompt=prompt,
            provider_gateway=provider_gateway,
            agent_id="summary_manager",
            agent_role="pm_tech_lead",
        )
        await self.repo.save_summary(project_id, "project", project_id, summary)
        return summary

    async def generate_stagnation_summary(
        self, project_id: str, reason: str, context_lines: list[str], provider_gateway: Any
    ) -> str:
        return await self._summarize(
            project_id=project_id,
            purpose="compress_event_history",
            prompt=f"Explain this stagnation using only evidence. Reason: {reason}\n"
            + "\n".join(context_lines),
            provider_gateway=provider_gateway,
            agent_id="summary_manager",
            agent_role="pm_tech_lead",
        )

    async def compress_event_history(
        self, raw_events: list[str], project_id: str, provider_gateway: Any
    ) -> str:
        if not raw_events:
            return "No prior events."
        return await self._summarize(
            project_id=project_id,
            purpose="compress_event_history",
            prompt="Compress this event timeline without inventing facts:\n"
            + "\n".join(raw_events),
            provider_gateway=provider_gateway,
            agent_id="summary_manager",
            agent_role="pm_tech_lead",
        )


CheckpointManager = CheckpointManagerActor

__all__ = ["Checkpoint", "CheckpointManager", "CheckpointManagerActor", "SummaryManagerActor"]
