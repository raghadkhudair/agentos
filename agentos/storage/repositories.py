import json
from uuid import UUID
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
        priority: int = 3
    ) -> str:
        """Creates a high-quality task record supporting hierarchical nesting tree steps."""
        query = """
            INSERT INTO tasks (project_id, title, description, owner_agent_id, parent_task_id, priority)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id;
        """
        p_uuid = UUID(parent_task_id) if parent_task_id else None
        async with self.db.pool.acquire() as conn:
            task_id = await conn.fetchval(query, UUID(project_id), title, description, owner_agent_id, p_uuid, priority)
            return str(task_id)

    async def add_dependency(self, task_id: str, depends_on_task_id: str) -> None:
        """Declares a direct dependency relationship baseline link between two tasks."""
        query = """
            INSERT INTO task_dependencies (task_id, depends_on_task_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING;
        """
        async with self.db.pool.acquire() as conn:
            await conn.execute(query, UUID(task_id), UUID(depends_on_task_id))
        
    async def get_active_tasks(self, project_id: str) -> list[dict]:
        """
        Fetches all uncompleted tasks for context packaging, including 
        relational list metadata of blocking ancestor tasks.
        """
        query = """
            SELECT 
                t.id::text, 
                t.title, 
                t.description, 
                t.status, 
                t.owner_agent_id,
                t.priority,
                t.parent_task_id::text,
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

    async def update_task_status(self, task_id: str, status: str) -> None:
        """Ticks task tracking metrics forward across state updates."""
        query = "UPDATE tasks SET status = $1, updated_at = now() WHERE id = $2"
        async with self.db.pool.acquire() as conn:
            await conn.execute(query, status, UUID(task_id))

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

    async def save_checkpoint(self, project_id: str, agent_id: str, achievement: str, summary: str, task_id: str = None) -> str:
        query = """
            INSERT INTO checkpoints (project_id, agent_id, task_id, achievement, summary)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id;
        """
        task_uuid = UUID(task_id) if task_id else None
        async with self.db.pool.acquire() as conn:
            cp_id = await conn.fetchval(query, UUID(project_id), agent_id, task_uuid, achievement, summary)
            return str(cp_id)

class ProviderCallRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def log_call(self, project_id: str, purpose: str, provider: str, model: str, cost_usd: float) -> str:
        query = """
            INSERT INTO provider_calls (project_id, purpose, provider, model, cost_usd, status)
            VALUES ($1, $2, $3, $4, $5, 'COMPLETED')
            RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            call_id = await conn.fetchval(query, UUID(project_id), purpose, provider, model, cost_usd)
            return str(call_id)