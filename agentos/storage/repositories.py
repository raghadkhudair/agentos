import hashlib
import json
from uuid import UUID
from agentos.cli.main import status
from agentos.storage.database import DatabaseManager
from agentos.messaging.events import Event

class ProjectRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def create_project(self, name: str, request: str, dod: list) -> str:
        query = """
            INSERT INTO projects (name, request, status, dod)
            VALUES ($1, $2, 'INITIALIZED', $3)
            ON CONFLICT (name) DO UPDATE SET updated_at = now()
            RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            project_id = await conn.fetchval(query, name, request, json.dumps(dod))
            return str(project_id)

class EventRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def save_event(self, project_id: str, event: Event) -> None:
        query = """
            INSERT INTO events (
                id, project_id, event_type, topic, producer_agent_id, 
                target_agent_id, payload, correlation_id, causation_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
        """
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                query,
                event.event_id,
                UUID(project_id) if isinstance(project_id, str) else project_id,
                event.event_type.value,
                event.topic,
                event.producer_agent_id,
                event.target_agent_id,
                json.dumps(event.payload),
                event.correlation_id,
                event.causation_id
            )

    async def get_event(self, event_id: str) -> dict | None:
        query = "SELECT * FROM events WHERE id = $1;"
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, UUID(event_id) if isinstance(event_id, str) else event_id)
            return dict(row) if row else None

    async def list_pending_approvals(self, project_id: str) -> list[dict]:
        # an approval is "pending" if no APPROVAL_GRANTED/DENIED event references it via causation_id
        query = """
            SELECT * FROM events e
            WHERE e.project_id = $1 AND e.event_type = 'APPROVAL_REQUEST'
            AND NOT EXISTS (
                SELECT 1 FROM events r
                WHERE r.causation_id = e.id::text
                AND r.event_type IN ('APPROVAL_GRANTED', 'APPROVAL_DENIED')
            )
            ORDER BY e.created_at ASC;
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query, UUID(project_id))
            return [dict(row) for row in rows]

class TaskRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def create_task(
        self, 
        project_id: str, 
        title: str, 
        description: str, 
        owner_agent_id: str = None,
        parent_task_id: str = None,
        priority: int = 3,
        acceptance_criteria: list[str] = None, 
        allowed_paths: list[str] = None,
        blocked_paths: list[str] = None,
        expected_outputs: list[str] = None,   
        required_reviewers: list[str] = None,
        affected_contracts: list[str] = None,
        risk_level: str = "LOW",
        embedding: list[float] = None
    ) -> str:
        query = """
            INSERT INTO tasks (
                project_id, title, description, owner_agent_id, parent_task_id, priority, 
                acceptance_criteria, allowed_paths, blocked_paths, expected_outputs,
                required_reviewers, affected_contracts, risk_level, embedding
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::vector)
            RETURNING id;
        """
        p_uuid = UUID(parent_task_id) if parent_task_id else None
        async with self.db.pool.acquire() as conn:
            task_id = await conn.fetchval(
                query, 
                UUID(project_id) if isinstance(project_id, str) else project_id, 
                title, 
                description, 
                owner_agent_id, 
                p_uuid, 
                priority,
                json.dumps(acceptance_criteria or []), 
                allowed_paths or [],
                blocked_paths or [],
                expected_outputs or [],                
                required_reviewers or [],
                affected_contracts or [],
                risk_level,
                embedding
            )
            return str(task_id)

    async def add_dependency(self, task_id: str, depends_on_task_id: str) -> None:
        query = """
            INSERT INTO task_dependencies (task_id, depends_on_task_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING;
        """
        async with self.db.pool.acquire() as conn:
            await conn.execute(query, UUID(task_id), UUID(depends_on_task_id))
        
    async def get_active_tasks(self, project_id: str) -> list[dict]:
        query = """
            SELECT 
                t.id::text, 
                t.title, 
                t.description, 
                t.status, 
                t.owner_agent_id,
                t.priority,
                t.parent_task_id::text,
                t.acceptance_criteria,
                t.allowed_paths,
                t.blocked_paths,
                t.expected_outputs, 
                t.required_reviewers,
                t.affected_contracts,
                t.risk_level,
                COALESCE(
                    ARRAY_AGG(td.depends_on_task_id::text) FILTER (WHERE td.depends_on_task_id IS NOT NULL), 
                    '{}'::text[]
                ) as dependencies
            FROM tasks t
            LEFT JOIN task_dependencies td ON t.id = td.task_id
            WHERE t.project_id = $1 AND t.status != 'COMPLETED'
            GROUP BY t.id
            ORDER BY t.priority DESC, t.created_at ASC;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query, safe_id)
            return [dict(row) for row in rows]

    async def find_similar_task(self, project_id: str, embedding: list[float]) -> dict | None:
        """Uses vector similarity directly on the tasks table to detect duplicate tasks."""
        if not embedding:
            return None
            
        query = """
            SELECT id::text, title, status, (embedding <=> $2::vector) as distance
            FROM tasks
            WHERE project_id = $1 AND status != 'COMPLETED' AND embedding IS NOT NULL
            ORDER BY distance ASC
            LIMIT 1;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        try:
            async with self.db.pool.acquire() as conn:
                row = await conn.fetchrow(query, safe_id, embedding)
                if row and row['distance'] < 0.15:  
                    return dict(row)
        except Exception as e:
            print(f"⚠️ Task vector similarity search bypassed: {e}")
            
        return None

    async def update_task_affected_contracts(self, task_id: str, affected_contracts: list[str]) -> None:
        """Updates the affected_contracts array column on a task record."""
        if not task_id or not affected_contracts:
            return
        query = "UPDATE tasks SET affected_contracts = $1, updated_at = NOW() WHERE id = $2;"
        try:
            safe_task_uuid = UUID(task_id) if isinstance(task_id, str) else task_id
            async with self.db.pool.acquire() as conn:
                await conn.execute(query, affected_contracts, safe_task_uuid)
        except Exception as e:
            print(f"⚠️ Failed to update task affected contracts: {e}")

    
    async def update_task_status(self, task_id: str, status: str) -> None:
        try:
            safe_task_uuid = UUID(task_id) if isinstance(task_id, str) else task_id
        except ValueError:
            print(f" [WARNING]: Agent provided an invalid task ID for status update: {task_id}")
            return

        query = "UPDATE tasks SET status = $1, updated_at = now() WHERE id = $2"
        async with self.db.pool.acquire() as conn:
            await conn.execute(query, status, safe_task_uuid)

class ArtifactRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def create_artifact(self, project_id: str, artifact_type: str, title: str, uri: str = None, task_id: str = None) -> str:
        query = """
            INSERT INTO artifacts (project_id, task_id, artifact_type, title, uri)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id;
        """
        task_uuid = UUID(task_id) if task_id else None
        async with self.db.pool.acquire() as conn:
            art_id = await conn.fetchval(query, UUID(project_id), task_uuid, artifact_type, title, uri)
            return str(art_id)

class CheckpointRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def save_checkpoint(
        self, 
        project_id: str, 
        agent_id: str, 
        achievement: str, 
        summary: str, 
        task_id: str = None,
        agent_state_snapshot: dict = None,
        artifacts: list[str] = None
    ) -> str:
        query = """
            INSERT INTO checkpoints (
                project_id, agent_id, task_id, achievement, summary, agent_state_snapshot, artifacts
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id;
        """
        task_uuid = UUID(task_id) if task_id else None
        
        # Serialize the state snapshot dictionary to a JSON string for the JSONB column
        state_json = json.dumps(agent_state_snapshot or {})
        
        async with self.db.pool.acquire() as conn:
            cp_id = await conn.fetchval(
                query, 
                UUID(project_id) if isinstance(project_id, str) else project_id, 
                agent_id, 
                task_uuid, 
                achievement, 
                summary,
                state_json,
                artifacts or []
            )
            return str(cp_id)

class ProviderCallRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def log_call(self, project_id: str, purpose: str, provider: str, model: str, cost_usd: float, prompt_hash: str = None, response_hash: str = None) -> str:
        query = """
            INSERT INTO provider_calls (project_id, purpose, provider, model, cost_usd, prompt_hash, response_hash, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'COMPLETED')
            RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            call_id = await conn.fetchval(query, UUID(project_id), purpose, provider, model, cost_usd, prompt_hash, response_hash)
            return str(call_id)

class MemoryRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def search_hybrid_memories(
        self, 
        project_id: str, 
        agent_id: str,
        allowed_scopes: list[str], 
        query_text: str, 
        query_embedding: list[float], 
        limit: int = 5
    ) -> list[dict]:
        """Performs a hybrid search on memory_items using both lexical and vector similarity, with recency and importance scoring."""
        if not query_embedding:
            return []

        query = """
            WITH ranked_memories AS (
                SELECT 
                    mi.id,
                    mi.title,
                    mi.content,
                    mi.scope,
                    mi.memory_type,
                    mi.created_at,
                    COALESCE(mi.importance_score, 1.0) as importance,
                    
                    
                    (1.0 - (me.embedding <=> $4::vector)) as vector_score,
                    
                    ts_rank(
                        to_tsvector('english', mi.title || ' ' || mi.content), 
                        websearch_to_tsquery('english', $3)
                    ) as lexical_score,
                    
                    (1.0 / (1.0 + (EXTRACT(EPOCH FROM (NOW() - mi.created_at)) / 3600.0))) 
                    as recency_score

                FROM memory_items mi
                JOIN memory_embeddings me ON me.memory_item_id = mi.id
                WHERE (mi.project_id = $1 OR mi.scope = 'global_patterns')
                  AND mi.scope = ANY($2::text[])
            )
            SELECT 
                title,
                content,
                scope,
                memory_type,
                vector_score,
                lexical_score,
                recency_score,
                importance,
                (
                    (0.45 * vector_score) + 
                    (0.25 * LEAST(lexical_score, 1.0)) + 
                    (0.20 * recency_score) + 
                    (0.10 * LEAST(importance / 5.0, 1.0))
                ) as final_rank_score
            FROM ranked_memories
            ORDER BY final_rank_score DESC
            LIMIT $5;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query, safe_id, allowed_scopes, query_text, query_embedding, limit)
                return [dict(r) for r in rows]
        except Exception as e:
            print(f"⚠️ Hybrid lexical/vector memory search failed: {e}")
            return []

    async def save_memory_item(
        self, 
        project_id: str | None, 
        scope: str, 
        owner_agent_id: str, 
        memory_type: str, 
        title: str, 
        content: str,
        importance_score: float = 1.0
    ) -> str:
        """
        Saves a memory item to PostgreSQL including an explicit importance_score (1.0 to 5.0).
        """
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        
        query = """
            INSERT INTO memory_items (
                project_id, scope, owner_agent_id, memory_type, title, content, content_hash, importance_score
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
            RETURNING id;
        """
        p_uuid = UUID(project_id) if project_id else None
        async with self.db.pool.acquire() as conn:
            mem_id = await conn.fetchval(
                query, 
                p_uuid, 
                scope, 
                owner_agent_id, 
                memory_type, 
                title, 
                content, 
                content_hash,
                importance_score
            )
            return str(mem_id)

    async def find_similar_failures(self, project_id: str, error_embedding: list[float]) -> list[dict]:
        if not error_embedding:
            return []
        query = """
            SELECT mi.title, mi.content, (me.embedding <=> $2::vector) as distance
            FROM memory_items mi
            JOIN memory_embeddings me ON me.memory_item_id = mi.id
            WHERE mi.project_id = $1 
              AND mi.memory_type = 'execution_failure'
            ORDER BY distance ASC
            LIMIT 2;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query, safe_id, error_embedding)
                return [dict(r) for r in rows if r['distance'] < 0.35]
        except Exception as e:
            print(f"⚠️ Similar failure lookup bypassed: {e}")
            return []

    async def find_affected_contracts(self, project_id: str, change_embedding: list[float]) -> list[str]:
        """Queries contract_memory items using pgvector to identify API or interface contracts affected by a code change."""
        if not change_embedding:
            return []
        query = """
            SELECT mi.title, (me.embedding <=> $2::vector) as distance
            FROM memory_items mi
            JOIN memory_embeddings me ON me.memory_item_id = mi.id
            WHERE mi.project_id = $1 
              AND mi.scope = 'contract_memory'
            ORDER BY distance ASC
            LIMIT 3;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query, safe_id, change_embedding)
                # Return titles of contracts with distance < 0.30
                return [r['title'] for r in rows if r['distance'] < 0.30]
        except Exception as e:
            print(f"⚠️ Contract impact vector search bypassed: {e}")
            return []
    
    async def save_global_lesson(
        self, 
        owner_agent_id: str, 
        title: str, 
        lesson_content: str, 
        lesson_embedding: list[float]
    ) -> str:
        """Saves a reusable cross-project architectural pattern with scope='global_patterns' and project_id=None."""
        mem_id = await self.save_memory_item(
            project_id=None,
            scope="global_patterns",
            owner_agent_id=owner_agent_id,
            memory_type="long_term_lesson",
            title=title,
            content=lesson_content
        )
        if lesson_embedding:
            query_vector = """
                INSERT INTO memory_embeddings (memory_item_id, embedding)
                VALUES ($1, $2::vector);
            """
            async with self.db.pool.acquire() as conn:
                await conn.execute(query_vector, UUID(mem_id), lesson_embedding)
        return mem_id
        
class SummaryRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def save_summary(self, project_id: str, scope: str, owner_id: str, summary: str) -> str:
        query = """
            INSERT INTO summaries (project_id, scope, owner_id, summary)
            VALUES ($1, $2, $3, $4) RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            summary_id = await conn.fetchval(query, UUID(project_id), scope, owner_id, summary)
            return str(summary_id)

class AuditEventRepository:
    """Maintains an append-only transaction history log for security compliance verification."""
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def log_audit_event(self, project_id: str, agent_id: str, action_type: str, policy_decision: str, integrity_hash: str) -> str:
        query = """
            INSERT INTO audit_events (project_id, agent_id, event_type, decision, details)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            audit_id = await conn.fetchval(
                query, UUID(project_id), agent_id, action_type, policy_decision,
                json.dumps({"integrity_hash": integrity_hash})
            )
            return str(audit_id)
        

class DoDRepository:
    """Manages explicit Definition of Done (DoD) verification criteria and historical states."""
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def add_dod_check(self, project_id: str, criterion: str) -> str:
        query = """
            INSERT INTO dod_checks (project_id, criterion)
            VALUES ($1, $2)
            RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            check_id = await conn.fetchval(query, UUID(project_id), criterion)
            return str(check_id)

    async def update_check_status(
        self, check_id: str, status: str, agent_id: str, evidence: str = ""
    ) -> None:
        query = """
            UPDATE dod_checks 
            SET status = $1, verified_by_agent_id = $2, evidence_summary = $3, updated_at = NOW()
            WHERE id = $4;
        """
        async with self.db.pool.acquire() as conn:
            await conn.execute(query, status, agent_id, evidence, UUID(check_id))

    async def get_project_dod_status(self, project_id: str) -> list[dict]:
        query = "SELECT * FROM dod_checks WHERE project_id = $1 ORDER BY created_at ASC;"
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query, UUID(project_id))
            return [dict(row) for row in rows]


class CodebaseMapRepository:
    """Manages semantic code indexing and vector search across project files."""
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def index_file_chunk(
        self, 
        project_id: str, 
        file_path: str, 
        chunk_identifier: str, 
        code_snippet: str, 
        embedding: list[float]
    ) -> str:
        query = """
            INSERT INTO codebase_semantic_map (project_id, file_path, chunk_identifier, code_snippet, embedding)
            VALUES ($1, $2, $3, $4, $5::vector)
            RETURNING id;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        async with self.db.pool.acquire() as conn:
            chunk_id = await conn.fetchval(query, safe_id, file_path, chunk_identifier, code_snippet, embedding)
            return str(chunk_id)

    async def clear_file_index(self, project_id: str, file_path: str) -> None:
        """Removes existing indexed chunks for a file before re-indexing."""
        query = "DELETE FROM codebase_semantic_map WHERE project_id = $1 AND file_path = $2;"
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        async with self.db.pool.acquire() as conn:
            await conn.execute(query, safe_id, file_path)

    async def search_codebase(self, project_id: str, query_embedding: list[float], limit: int = 3) -> list[dict]:
        """Performs pgvector cosine distance search against indexed codebase snippets."""
        if not query_embedding:
            return []
        query = """
            SELECT file_path, chunk_identifier, code_snippet, (embedding <=> $2::vector) as distance
            FROM codebase_semantic_map
            WHERE project_id = $1
            ORDER BY distance ASC
            LIMIT $3;
        """
        safe_id = UUID(project_id) if isinstance(project_id, str) else project_id
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query, safe_id, query_embedding, limit)
                return [dict(r) for r in rows if r['distance'] < 0.40]
        except Exception as e:
            print(f"⚠️ Codebase semantic search bypassed: {e}")
            return []