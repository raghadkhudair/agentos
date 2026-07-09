from __future__ import annotations

from datetime import datetime, timezone
from pydantic import BaseModel, Field

# We import the database and repository layers we built earlier
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import CheckpointRepository


class Checkpoint(BaseModel):
    checkpoint_id: str
    project_id: str
    agent_id: str
    achievement: str
    summary: str
    task_id: str | None = None  # Adding this so we can link checkpoints to specific tasks
    artifacts: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CheckpointManager:
    """Creates durable progress markers after achievements and saves them to Postgres."""

    def __init__(self, db_manager: DatabaseManager):
        # Store the DB manager and initialize our repository
        self.db = db_manager
        self.repo = CheckpointRepository(self.db)

    async def create(self, checkpoint: Checkpoint) -> Checkpoint:
        # Actually save the checkpoint to the database!
        await self.repo.save_checkpoint(
            project_id=checkpoint.project_id,
            agent_id=checkpoint.agent_id,
            achievement=checkpoint.achievement,
            summary=checkpoint.summary,
            task_id=checkpoint.task_id
        )
        return checkpoint