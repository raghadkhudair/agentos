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