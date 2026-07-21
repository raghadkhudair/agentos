from __future__ import annotations

import json
import re
import uuid
import ray
import structlog
from datetime import datetime, timezone
from pydantic import BaseModel, Field

from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import CheckpointRepository
from agentos.provider.gateway import ProviderRequest

logger = structlog.get_logger()

class Checkpoint(BaseModel):
    checkpoint_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    project_id: str
    agent_id: str
    achievement: str
    summary: str
    task_id: str | None = None
    artifacts: list[str] = Field(default_factory=list)
    agent_state_snapshot: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


@ray.remote(namespace="agentos")
class CheckpointManagerActor:
    """Creates durable progress markers after achievements, links artifacts, and restores agent state pointers."""

    def __init__(self, settings_payload: dict):
        from agentos.config.settings import Settings
        self.settings = Settings(**settings_payload) if settings_payload else Settings()
        self.db = DatabaseManager(self.settings)
        self.repo = CheckpointRepository(self.db)
        self._connected = False

    async def _ensure_connected(self):
        if not self._connected:
            await self.db.connect()
            self._connected = True

    async def create(self, checkpoint_dict: dict) -> dict:
        """Saves a comprehensive checkpoint linking artifacts and agent state to Postgres in a single hit."""
        await self._ensure_connected()
        checkpoint = Checkpoint(**checkpoint_dict)
        
        logger.info(
            "creating_checkpoint", 
            agent_id=checkpoint.agent_id, 
            achievement=checkpoint.achievement, 
            checkpoint_id=checkpoint.checkpoint_id
        )

        # Call the unified, all-in-one repository driver
        await self.repo.save_checkpoint(
            project_id=checkpoint.project_id,
            agent_id=checkpoint.agent_id,
            achievement=checkpoint.achievement,
            summary=checkpoint.summary,
            task_id=checkpoint.task_id,
            agent_state_snapshot=checkpoint.agent_state_snapshot,
            artifacts=checkpoint.artifacts
        )

        return checkpoint.model_dump()

    async def recover_agent_state(self, project_id: str, agent_id: str) -> dict | None:
        """Retrieves the latest checkpoint snapshot to support recovery and resume on crashes."""
        await self._ensure_connected()
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        query = """
            SELECT checkpoint_id, achievement, summary, task_id, agent_state_snapshot, artifacts
            FROM checkpoints
            WHERE project_id = $1 AND agent_id = $2
            ORDER BY created_at DESC
            LIMIT 1;
        """
        try:
            async with self.db.pool.acquire() as conn:
                row = await conn.fetchrow(query, safe_project_id, agent_id)
                if not row:
                    return None

                state_snapshot = {}
                if row["agent_state_snapshot"]:
                    state_snapshot = json.loads(row["agent_state_snapshot"]) if isinstance(row["agent_state_snapshot"], str) else row["agent_state_snapshot"]

                return {
                    "checkpoint_id": str(row["checkpoint_id"]),
                    "achievement": row["achievement"],
                    "summary": row["summary"],
                    "task_id": str(row["task_id"]) if row["task_id"] else None,
                    "agent_state_snapshot": state_snapshot,
                    "artifacts": row["artifacts"] or []
                }
        except Exception as e:
            logger.error("failed_to_recover_agent_state", error=str(e))
            return None


@ray.remote(namespace="agentos")
class SummaryManagerActor:
    """Compresses verbose agent histories and checkpoint databases into clean semantic memory buffers."""

    def __init__(self, settings_payload: dict):
        from agentos.config.settings import Settings
        self.settings = Settings(**settings_payload)
        self.db = DatabaseManager(self.settings)
        self._connected = False

    async def _ensure_connected(self):
        if not self._connected:
            await self.db.connect()
            self._connected = True

    async def generate_local_agent_summary(self, project_id: str, agent_id: str, provider_gateway: any) -> str:
        """Compresses all recent checkpoints created by a specific agent into a personal progress summary."""
        await self._ensure_connected()
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        query = """
            SELECT achievement, summary, created_at 
            FROM checkpoints 
            WHERE project_id = $1 AND agent_id = $2 
            ORDER BY created_at DESC 
            LIMIT 10;
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query, safe_project_id, agent_id)
            if not rows:
                return f"No recent checkpoints found for agent {agent_id}."

            history_lines = [f"- [{r['created_at'].isoformat()}] Achievement: {r['achievement']} | Summary: {r['summary']}" for r in rows]
            raw_history = "\n".join(history_lines)

        prompt = (
            f"You are the Summary Manager for AgentOS. Your task is to compress the following raw activity timeline "
            f"for agent '{agent_id}' into a concise, 3-sentence performance update. Describe exactly what tasks "
            f"they made progress on, what succeeded, and if anything failed.\n\n"
            f"RAW TIMELINE:\n{raw_history}"
        )

        req = ProviderRequest(
            purpose="generate_local_agent_summary",
            messages=[{"role": "user", "content": prompt}],
            budget_key=project_id
        )
        # Fetch compiled summary from provider
        response = await provider_gateway.get_completion.remote(req)
        return response["content"].strip()

    async def generate_squad_summary(self, project_id: str, squad_name: str, provider_gateway: any) -> str:
        """Aggregates checkpoints from all agents in a specific squad (e.g., backend) into a domain status report."""
        await self._ensure_connected()
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        # We query checkpoints where the agent_id matches the squad prefix (e.g., 'backend_developer-1')
        squad_pattern = f"{squad_name}%"
        query = """
            SELECT c.agent_id, c.achievement, c.summary 
            FROM checkpoints c
            JOIN agents a ON c.agent_id = a.id
            WHERE c.project_id = $1 AND a.squad = $2;
            ORDER BY created_at DESC 
            LIMIT 15;
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query, safe_project_id, squad_pattern)
            if not rows:
                return f"No recent checkpoints found for squad '{squad_name}'."

            history_lines = [f"- Agent: {r['agent_id']} | Did: {r['achievement']} | Details: {r['summary']}" for r in rows]
            raw_history = "\n".join(history_lines)

        prompt = (
            f"You are the Summary Manager. Aggregate and summarize the following squad activity "
            f"for the '{squad_name}' squad into a high-density, bulleted status report. Highlight joint team achievements "
            f"and identify any shared bottlenecks.\n\n"
            f"SQUAD LOGS:\n{raw_history}"
        )

        req = ProviderRequest(
            purpose="generate_squad_summary",
            messages=[{"role": "user", "content": prompt}],
            budget_key=project_id
        )
        response = await provider_gateway.get_completion.remote(req)
        return response["content"].strip()

    async def generate_project_summary(self, project_id: str, provider_gateway: any) -> str:
        """Creates an executive, project-wide status briefing comparing progress to the master Definition of Done."""
        await self._ensure_connected()
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        task_query = """
            SELECT status, COUNT(*) as cnt 
            FROM tasks 
            WHERE project_id = $1 
            GROUP BY status;
        """
        checkpoint_query = """
            SELECT agent_id, achievement, summary 
            FROM checkpoints 
            WHERE project_id = $1 
            ORDER BY created_at DESC 
            LIMIT 5;
        """

        async with self.db.pool.acquire() as conn:
            task_rows = await conn.fetch(task_query, safe_project_id)
            checkpoint_rows = await conn.fetch(checkpoint_query, safe_project_id)

            task_summary = ", ".join([f"{r['status']}: {r['cnt']}" for r in task_rows]) if task_rows else "No tasks scheduled."
            achievements = "\n".join([f"- {r['agent_id']}: {r['summary']}" for r in checkpoint_rows]) if checkpoint_rows else "No checkpoints recorded."

        prompt = (
            f"You are the Lead Summary Manager. Write a high-level executive project status briefing. "
            f"Keep it under 150 words. Focus on the progression toward the final Definition of Done.\n\n"
            f"TASK PROFILE STATS: {task_summary}\n"
            f"LATEST MAJOR ACHIEVEMENTS:\n{achievements}"
        )

        req = ProviderRequest(
            purpose="generate_project_summary",
            messages=[{"role": "user", "content": prompt}],
            budget_key=project_id
        )
        response = await provider_gateway.get_completion.remote(req)
        summary_text = response["content"].strip()

        try:
            await self.promote_to_global_lesson(
                project_id=project_id,
                milestone_summary=summary_text,
                owner_agent_id="summary_manager",
                provider_gateway=provider_gateway
            )
        except Exception as e:
            logger.warning("failed_auto_promotion_global_lesson", error=str(e))

        return summary_text
    

    async def generate_stagnation_summary(self, project_id: str, reason: str, context_lines: list[str], provider_gateway: any) -> str:
        """Writes a real, human-readable explanation of a detected stagnation event."""
        await self._ensure_connected()
        raw_context = "\n".join(context_lines) if context_lines else "No additional context available."

        prompt = (
            f"You are the Summary Manager for AgentOS. The Stagnation Watchdog just detected a problem: "
            f"'{reason}'.\n\n"
            f"Write a clear, 3-4 sentence explanation of what's actually going wrong, based on this evidence, "
            f"so a human or the PM agent can understand it at a glance and decide what to do next.\n\n"
            f"EVIDENCE:\n{raw_context}"
        )

        req = ProviderRequest(
            purpose="generate_stagnation_summary",
            messages=[{"role": "user", "content": prompt}],
            budget_key=project_id
        )
        response = await provider_gateway.get_completion.remote(req)
        return response["content"].strip()
    
    async def compress_event_history(self, raw_events: list[str], project_id: str, provider_gateway: any) -> str:
        """
        Takes a raw event stream list and compresses it into a high-density,
        bulleted briefing. Used by the Memory Broker to prevent prompt pollution.
        """
        if not raw_events or raw_events == ["No recent events found. Project is initializing."]:
            return "Project is initializing. No previous event history to report."

        prompt = (
            "You are the Core Summary Manager for AgentOS. Your job is to compress the following "
            "raw timeline of recent system events into a highly concise, bulleted context briefing.\n"
            "Identify key milestones, blockages, or changes. Strip out metadata, raw JSON brackets, "
            "and duplicate records.\n\n"
            f"RAW EVENT STREAM:\n" + "\n".join(raw_events)
        )

        req = ProviderRequest(
            purpose="compress_event_history",
            messages=[{"role": "user", "content": prompt}],
            budget_key=project_id
        )

        try:
            response = await provider_gateway.get_completion.remote(req)
            return response["content"].strip()
        except Exception as e:
            logger.error("failed_to_compress_event_history", error=str(e))
            return "Fallback raw event stream:\n" + "\n".join(raw_events[:3])

    async def promote_to_global_lesson(
        self, 
        project_id: str,
        milestone_summary: str, 
        owner_agent_id: str, 
        provider_gateway: any
    ) -> str | None:
        """
        Uses an LLM to extract reusable cross-project technical patterns from milestone 
        summaries and persists them as global_patterns with pgvector embeddings.
        """
        await self._ensure_connected()
        
        prompt = (
            "You are the Architectural Knowledge Manager for AgentOS.\n"
            "Analyze the following project achievement summary and extract any reusable, generalizable "
            "technical lesson, pattern, or best practice that would benefit future software engineering projects.\n"
            "Do NOT include project-specific identifiers, user credentials, or specific file paths.\n\n"
            f"MILESTONE SUMMARY:\n{milestone_summary}\n\n"
            "Respond with a JSON object containing:\n"
            "{\n"
            '  "title": "Concise descriptive pattern title",\n'
            '  "lesson_content": "Detailed explanation of the technical pattern or fix"\n'
            "}"
        )

        req = ProviderRequest(
            purpose="extract_global_lesson",
            messages=[{"role": "user", "content": prompt}],
            budget_key=project_id
        )

        try:
            response = await provider_gateway.get_completion.remote(req, response_format={"type": "json_object"})
            clean_content = response["content"].strip()
            if clean_content.startswith("```"):
                clean_content = re.sub(r"^```json\s*|^```\s*", "", clean_content, flags=re.MULTILINE)
                clean_content = re.sub(r"\s*```$", "", clean_content, flags=re.MULTILINE).strip()

            lesson_data = json.loads(clean_content)
            title = lesson_data.get("title", "Reusable Architectural Pattern")
            lesson_content = lesson_data.get("lesson_content", "")

            if not lesson_content:
                return None

            lesson_vector = await provider_gateway.get_embedding.remote(lesson_content, "global_knowledge")

            from agentos.storage.repositories import MemoryRepository
            memory_repo = MemoryRepository(self.db)

            mem_id = await memory_repo.save_global_lesson(
                owner_agent_id=owner_agent_id,
                title=title,
                lesson_content=lesson_content,
                lesson_embedding=lesson_vector
            )
            logger.info("promoted_global_lesson_learned", title=title, memory_id=mem_id)
            return mem_id
        except Exception as e:
            logger.error("failed_to_promote_global_lesson", error=str(e))
            return None
