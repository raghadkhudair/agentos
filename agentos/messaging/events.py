from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(StrEnum):
    PROJECT_CREATED = "PROJECT_CREATED"
    TEAM_PLAN_CREATED = "TEAM_PLAN_CREATED"
    AGENT_CREATED = "AGENT_CREATED"
    AGENT_TRIGGERED = "AGENT_TRIGGERED"
    ACTION_REQUESTED = "ACTION_REQUESTED"
    ACTION_ALLOWED = "ACTION_ALLOWED"
    ACTION_DENIED = "ACTION_DENIED"
    TASK_CREATED = "TASK_CREATED"
    TASK_CLAIMED = "TASK_CLAIMED"
    TASK_COMPLETED = "TASK_COMPLETED"
    CONTRACT_PUBLISHED = "CONTRACT_PUBLISHED"
    REVIEW_REQUESTED = "REVIEW_REQUESTED"
    TEST_RESULT = "TEST_RESULT"
    CHECKPOINT_CREATED = "CHECKPOINT_CREATED"
    SUMMARY_CREATED = "SUMMARY_CREATED"
    BLOCKER_CREATED = "BLOCKER_CREATED"
    DOD_EVALUATED = "DOD_EVALUATED"
    AGENT_QUARANTINED = "AGENT_QUARANTINED"


class Event(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    project_id: str
    event_type: EventType
    producer_agent_id: str | None = None
    target_agent_id: str | None = None
    topic: str
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
