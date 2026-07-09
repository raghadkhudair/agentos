from __future__ import annotations

from dataclasses import dataclass, field
import uuid
import json

from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import TaskRepository

@dataclass(frozen=True)
class CatchUpPacket:
    project_id: str
    agent_id: str
    trigger_event_id: str
    relevant_events: list[str] = field(default_factory=list)
    active_tasks: list[str] = field(default_factory=list)
    relevant_memories: list[str] = field(default_factory=list)
    recommended_next_actions: list[str] = field(default_factory=list)

class MemoryBroker:
    """Scoped memory gateway enforcing context boundaries and security ACL layers."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.task_repo = TaskRepository(db_manager)

    async def build_catchup_packet(
        self,
        *,
        project_id: str,
        agent_id: str,
        trigger_event_id: str,
        allowed_scopes: list[str] | None = None,  # e.g., ["project_memory", "squad_memory"]
        provider_gateway: any = None
    ) -> CatchUpPacket:
        
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        # Default fallback to baseline context if no explicit scopes are declared
        scopes = allowed_scopes if allowed_scopes else ["project_memory", "global_patterns"]
        
        # 1. Fetch live historical events stream
        query_events = """
            SELECT event_type, topic, payload 
            FROM events
            WHERE project_id = $1
            ORDER BY created_at DESC 
            LIMIT 5;
        """
        recent_events = []
        trigger_message = ""
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query_events, safe_project_id)
            for row in rows:
                recent_events.append(f"[{row['event_type']}] {row['topic']}: {row['payload']}")
                if not trigger_message:
                    try:
                        payload_dict = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
                        trigger_message = payload_dict.get("message", "")
                    except Exception:
                        trigger_message = str(row['payload'])
                
        if not recent_events:
            recent_events = ["No recent events found. Project is initializing."]

        # 2. Fetch live task graph with relational dependencies
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

        # 3. Scoped Semantic pgvector Long-Term Memory Lookup
        relevant_memories = []
        if provider_gateway and trigger_message:
            query_vector = await provider_gateway.get_embedding(trigger_message)
            
            # Upgraded with a 'scope = ANY($3)' clause to restrict memory exposure
            query_memories = """
                SELECT mi.title, mi.content, (me.embedding <=> $2::vector) as distance
                FROM memory_items mi
                JOIN memory_embeddings me ON me.memory_item_id = mi.id
                WHERE mi.project_id = $1 AND mi.scope = ANY($3)
                ORDER BY distance ASC
                LIMIT 3;
            """
            try:
                async with self.db.pool.acquire() as conn:
                    mem_rows = await conn.fetch(query_memories, safe_project_id, query_vector, scopes)
                    for m in mem_rows:
                        relevant_memories.append(f"🔍 [Memory ({scopes})]: {m['title']} - {m['content']}")
            except Exception as e:
                relevant_memories = [f"Memory subsystem online, vector parsing issue handled: {str(e)}"]

        if not relevant_memories:
            relevant_memories = ["No matching vector memories found within the assigned access scopes."]

        return CatchUpPacket(
            project_id=str(project_id),
            agent_id=agent_id,
            trigger_event_id=trigger_event_id,
            relevant_events=recent_events,
            active_tasks=formatted_tasks,
            relevant_memories=relevant_memories,
            recommended_next_actions=[
                "Verify task hierarchy sequences before generating code requests.",
                "Ensure standard verification checks pass via sandboxed execution paths."
            ],
        )