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
    
    async def register_agent_identity(self, identity_data: dict) -> None:
        from agentos.governance.models import AgentIdentity
        identity_obj = AgentIdentity(**identity_data)
        if not hasattr(self, "_authenticated_identities"):
            self._authenticated_identities = {}
        self._authenticated_identities[identity_obj.agent_id] = identity_obj

    async def build_catchup_packet(
    self,
    *,
    project_id: str,
    agent_id: str,
    trigger_event_id: str,
    requested_scopes: list[str] | None = None,
    provider_gateway: any = None
) -> dict:
        await self._ensure_connected()
        
        if not hasattr(self, "_authenticated_identities"):
            self._authenticated_identities = {}
            
        auth_identity = self._authenticated_identities.get(agent_id)
        if not auth_identity:
            return {"error": "Identity Registry Violation: Unknown memory requester signature."}
            
        agent_allowed_scopes = auth_identity.memory_scopes
        
        resolved_scopes = []
        if requested_scopes:
            resolved_scopes = [s for s in requested_scopes if s in agent_allowed_scopes]
        else:
            resolved_scopes = agent_allowed_scopes

        if not resolved_scopes:
            resolved_scopes = ["project"]

        logger.info("enforcing_memory_access_control", agent_id=agent_id, effective=resolved_scopes)
        
        # Fetch Live Historical Events Stream
        query_events = """
            SELECT event_type, topic, payload 
            FROM events
            WHERE project_id = $1
            ORDER BY created_at DESC 
            LIMIT 10;
        """
        raw_events = []
        trigger_message = ""
        
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
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

        summarized_briefing = "No history to summarize."
        if provider_gateway and raw_events:
            try:
                summary_manager = ray.get_actor("summary_manager", namespace="agentos")
                
                summarized_briefing = await summary_manager.compress_event_history.remote(
                    raw_events=raw_events,
                    project_id=str(project_id),
                    provider_gateway=provider_gateway
                )
            except Exception as e:
                logger.error("failed_to_delegate_summarization", error=str(e))
                summarized_briefing = "Fallback raw timeline:\n" + "\n".join(raw_events[:3])

        # Fetch Live Task Graph
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

        relevant_memories = []
        if provider_gateway and trigger_message:
            try:
                query_vector = await provider_gateway.get_embedding.remote(trigger_message, str(project_id))
                
                from agentos.storage.repositories import MemoryRepository, CodebaseMapRepository
                memory_repo = MemoryRepository(self.db)
                
                mem_rows = await memory_repo.search_hybrid_memories(
                    project_id=str(project_id),
                    agent_id=agent_id,
                    allowed_scopes=resolved_scopes,
                    query_text=trigger_message,
                    query_embedding=query_vector,
                    limit=3
                )
                
                for m in mem_rows:
                    scrubbed_mem = self._scrub_secrets(m['content'])
                    relevant_memories.append(
                        f"🔍 [Memory Scope: {m['scope']} | Type: {m['memory_type']}]: {m['title']} - {scrubbed_mem}"
                    )
                
                # Codebase Map Search
                code_repo = CodebaseMapRepository(self.db)
                code_snippets = await code_repo.search_codebase(str(project_id), query_vector, limit=2)
                for cs in code_snippets:
                    relevant_memories.append(
                        f"[Codebase Map - {cs['file_path']} ({cs['chunk_identifier']})]:\n{cs['code_snippet']}"
                    )
            
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