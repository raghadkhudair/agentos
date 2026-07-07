from __future__ import annotations

from datetime import datetime, timezone
from pydantic import BaseModel, Field


class Checkpoint(BaseModel):
    checkpoint_id: str
    project_id: str
    agent_id: str
    achievement: str
    summary: str
    artifacts: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class CheckpointManager:
    """Creates durable progress markers after achievements."""

    async def create(self, checkpoint: Checkpoint) -> Checkpoint:
        # Persist to PostgreSQL in the production implementation.
        return checkpoint
