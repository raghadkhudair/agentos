from __future__ import annotations

import re
import uuid
import json
import ray
import structlog
from dataclasses import dataclass, field

from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import TaskRepository
from agentos.provider.gateway import ProviderGateway, ProviderRequest

logger = structlog.get_logger()

@dataclass(frozen=True)
class CatchUpPacket:
    project_id: str
    agent_id: str
    trigger_event_id: str
    summary_of_recent_events: str  # Summarized to prevent prompt pollution
    active_tasks: list[str] = field(default_factory=list)
    relevant_memories: list[str] = field(default_factory=list)
    recommended_next_actions: list[str] = field(default_factory=list)


@ray.remote(namespace="agentos")
class MemoryBrokerActor:
    """Scoped memory gateway enforcing context boundaries, zero-trust ACL layers, and context summarization."""
    
    def __init__(self, settings_payload: dict):
        from agentos.config.settings import Settings
        self.settings = Settings(**settings_payload)
        self.db = DatabaseManager(self.settings)
        self.task_repo = TaskRepository(self.db)
        self._connected = False

    async def _ensure_connected(self):
        """Ensures PostgreSQL connections are active in this Ray process."""
        if not self._connected:
            await self.db.connect()
            self._connected = True

    def _scrub_secrets(self, text: str) -> str:
        """Zero-trust data-scrubbing pattern to block password/API key exposure."""
        # Redacts common credentials and API keys inside content streams
        scrubbed = re.sub(r'(?i)(api_key|password|secret|token|private_key)\s*[:=]\s*["\'][^"\']+["\']', r'\1: "[REDACTED_BY_MEMORY_BROKER]"', text)
        return scrubbed

    async def build_catchup_packet(
        self,
        *,
        project_id: str,
        agent_id: str,
        trigger_event_id: str,
        agent_allowed_scopes: list[str],
        requested_scopes: list[str] | None = None,
        provider_gateway: any = None
    ) -> dict:
        """Retrieves context safely and delegates summarization to the SummaryManagerActor."""
        await self._ensure_connected()
        
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        # 1. Enforce Memory Access Control (ACL)
        resolved_scopes = []
        if requested_scopes:
            resolved_scopes = [s for s in requested_scopes if s in agent_allowed_scopes]
        else:
            resolved_scopes = agent_allowed_scopes

        if not resolved_scopes:
            resolved_scopes = ["project"]

        logger.info("enforcing_memory_access_control", agent_id=agent_id, effective=resolved_scopes)
        
        # 2. Fetch Live Historical Events Stream
        query_events = """
            SELECT event_type, topic, payload 
            FROM events
            WHERE project_id = $1
            ORDER BY created_at DESC 
            LIMIT 10;
        """
        raw_events = []
        trigger_message = ""
        
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query_events, safe_project_id)
            for row in rows:
                raw_payload = row['payload']
                scrubbed_payload = self._scrub_secrets(
                    raw_payload if isinstance(raw_payload, str) else json.dumps(raw_payload)
                )
                
                raw_events.append(f"[{row['event_type']}] {row['topic']}: {scrubbed_payload}")
                if not trigger_message:
                    try:
                        payload_dict = json.loads(scrubbed_payload)
                        trigger_message = payload_dict.get("message", "")
                    except Exception:
                        trigger_message = scrubbed_payload
                
        if not raw_events:
            raw_events = ["No recent events found. Project is initializing."]

        # --- 3. DELEGATED SUMMARIZATION (No more duplication!) ---
        summarized_briefing = "No history to summarize."
        if provider_gateway and raw_events:
            try:
                # 1. Locate the remote Summary Manager Actor
                summary_manager = ray.get_actor("summary_manager", namespace="agentos")
                
                # 2. Delegate the compression task
                summarized_briefing = await summary_manager.compress_event_history.remote(
                    raw_events=raw_events,
                    project_id=str(project_id),
                    provider_gateway=provider_gateway
                )
            except Exception as e:
                logger.error("failed_to_delegate_summarization", error=str(e))
                # Fallback directly to the scrubbed events if lookup/call fails
                summarized_briefing = "Fallback raw timeline:\n" + "\n".join(raw_events[:3])

        # 4. Fetch Live Task Graph
        live_tasks = await self.task_repo.get_active_tasks(str(project_id))
        formatted_tasks = []
        for t in live_tasks:
            dep_list = t.get("dependencies", [])
            dep_str = ", ".join(dep_list) if dep_list else "NONE"
            
            formatted_tasks.append(
                f"- [Task ID: {t['id']}] {t['title']} (Status: {t['status']})\n"
                f"  Description: {t['description']}\n"
                f"  Blocked By Tasks: {dep_str}"
            )
            
        if not formatted_tasks:
            formatted_tasks = ["No active tasks currently assigned."]

        # 5. Semantic Memory Lookup (pgvector)
        relevant_memories = []
        if provider_gateway and trigger_message:
            try:
                query_vector = await provider_gateway.get_embedding.remote(trigger_message)
                
                query_memories = """
                    SELECT mi.title, mi.content, (me.embedding <=> $2::vector) as distance
                    FROM memory_items mi
                    JOIN memory_embeddings me ON me.memory_item_id = mi.id
                    WHERE mi.project_id = $1 AND mi.scope = ANY($3)
                    ORDER BY distance ASC
                    LIMIT 3;
                """
                async with self.db.pool.acquire() as conn:
                    mem_rows = await conn.fetch(query_memories, safe_project_id, query_vector, resolved_scopes)
                    for m in mem_rows:
                        scrubbed_mem = self._scrub_secrets(m['content'])
                        relevant_memories.append(f"🔍 [Memory Scope: {resolved_scopes}]: {m['title']} - {scrubbed_mem}")
            except Exception as e:
                logger.warning("vector_search_bypassed", reason=str(e))

        if not relevant_memories:
            relevant_memories = ["No matching vector memories found within the assigned access scopes."]

        packet = CatchUpPacket(
            project_id=str(project_id),
            agent_id=agent_id,
            trigger_event_id=trigger_event_id,
            summary_of_recent_events=summarized_briefing,
            active_tasks=formatted_tasks,
            relevant_memories=relevant_memories,
            recommended_next_actions=[
                "Verify task hierarchy sequences before generating code requests.",
                "Ensure standard verification checks pass via sandboxed execution paths."
            ],
        )
        return packet.__dict__