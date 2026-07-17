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

    TASK_PROPOSAL = "TASK_PROPOSAL"
    TASK_UPDATE = "TASK_UPDATE"
    CONTRACT_CHANGE = "CONTRACT_CHANGE"
    BLOCKER = "BLOCKER"
    REVIEW_REQUEST = "REVIEW_REQUEST"
    REVIEW_RESULT = "REVIEW_RESULT"
    SECURITY_ALERT = "SECURITY_ALERT"
    CHECKPOINT = "CHECKPOINT"
    SUMMARY = "SUMMARY"
    ACTION_REQUEST = "ACTION_REQUEST"
    APPROVAL_REQUEST = "APPROVAL_REQUEST"


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

    def get_target_topic(self) -> str:
        t_type = self.event_type
        p_id = self.project_id

        if t_type in {EventType.TASK_CREATED, EventType.TASK_CLAIMED, EventType.TASK_COMPLETED, EventType.TASK_PROPOSAL, EventType.TASK_UPDATE}:
            return f"project.{p_id}.tasks"
        elif t_type in {EventType.CONTRACT_PUBLISHED, EventType.CONTRACT_CHANGE}:
            return f"project.{p_id}.contracts"
        elif t_type in {EventType.REVIEW_REQUESTED, EventType.REVIEW_REQUEST, EventType.REVIEW_RESULT}:
            return f"project.{p_id}.reviews"
        elif t_type == EventType.TEST_RESULT:
            return f"project.{p_id}.tests"
        elif t_type in {EventType.BLOCKER_CREATED, EventType.BLOCKER}:
            return f"project.{p_id}.blockers"
        elif t_type in {EventType.CHECKPOINT_CREATED, EventType.CHECKPOINT}:
            return f"project.{p_id}.checkpoints"
        elif t_type in {EventType.SUMMARY_CREATED, EventType.SUMMARY}:
            return f"project.{p_id}.summaries"

        if self.producer_agent_id:
            role = self.producer_agent_id.lower()
            if "backend" in role:
                return f"squad.backend.events"
            elif "frontend" in role:
                return f"squad.frontend.events"
            elif "platform" in role or "infra" in role:
                return f"squad.platform.events"
            elif "qa" in role:
                return f"squad.qa.events"

        return f"project.{p_id}.events"