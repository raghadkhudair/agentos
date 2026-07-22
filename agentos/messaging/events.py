from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

_TOPIC = re.compile(
    r"^(project\.[A-Za-z0-9-]+\.(events|tasks|contracts|reviews|tests|blockers|checkpoints|summaries|resources)|"
    r"squad\.[a-z0-9_-]+\.events|agent\.[A-Za-z0-9_-]+\.inbox)$"
)


class EventType(StrEnum):
    PROJECT_CREATED = "PROJECT_CREATED"
    TEAM_PLAN_CREATED = "TEAM_PLAN_CREATED"
    AGENT_CREATED = "AGENT_CREATED"
    AGENT_TRIGGERED = "AGENT_TRIGGERED"
    AGENT_HEARTBEAT = "AGENT_HEARTBEAT"
    AGENT_HEALTH_CHANGED = "AGENT_HEALTH_CHANGED"
    COLLABORATION_UPDATE = "COLLABORATION_UPDATE"
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
    APPROVAL_GRANTED = "APPROVAL_GRANTED"
    APPROVAL_DENIED = "APPROVAL_DENIED"
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
    REPLANNING_TRIGGERED = "REPLANNING_TRIGGERED"
    RESOURCE_PLAN_CREATED = "RESOURCE_PLAN_CREATED"
    RESOURCE_PLAN_UPDATED = "RESOURCE_PLAN_UPDATED"
    RESOURCE_PRESSURE = "RESOURCE_PRESSURE"


class Event(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    project_id: str
    event_type: EventType
    producer_agent_id: str | None = None
    target_agent_id: str | None = None
    topic: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    payload_object_uri: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    schema_version: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("project_id")
    @classmethod
    def _project_is_uuid(cls, value: str) -> str:
        return str(UUID(str(value)))

    @model_validator(mode="after")
    def _resolve_and_validate_topic(self) -> Event:
        expected = self.get_target_topic()
        if self.topic is None:
            self.topic = expected
        if not _TOPIC.fullmatch(self.topic):
            raise ValueError(f"invalid event topic: {self.topic}")
        if self.topic.startswith("project.") and not self.topic.startswith(
            f"project.{self.project_id}."
        ):
            raise ValueError("event topic project does not match event project_id")
        inline_size = len(json.dumps(self.payload, default=str).encode("utf-8"))
        if inline_size > 64 * 1024 and not self.payload_object_uri:
            raise ValueError("event payloads above 64 KiB must be stored in MinIO")
        return self

    def get_target_topic(self) -> str:
        project = self.project_id
        event_type = self.event_type
        if event_type in {
            EventType.TASK_CREATED,
            EventType.TASK_CLAIMED,
            EventType.TASK_COMPLETED,
            EventType.TASK_PROPOSAL,
            EventType.TASK_UPDATE,
        }:
            suffix = "tasks"
        elif event_type in {EventType.CONTRACT_PUBLISHED, EventType.CONTRACT_CHANGE}:
            suffix = "contracts"
        elif event_type in {
            EventType.REVIEW_REQUESTED,
            EventType.REVIEW_REQUEST,
            EventType.REVIEW_RESULT,
        }:
            suffix = "reviews"
        elif event_type is EventType.TEST_RESULT:
            suffix = "tests"
        elif event_type in {EventType.BLOCKER_CREATED, EventType.BLOCKER}:
            suffix = "blockers"
        elif event_type in {EventType.CHECKPOINT_CREATED, EventType.CHECKPOINT}:
            suffix = "checkpoints"
        elif event_type in {EventType.SUMMARY_CREATED, EventType.SUMMARY}:
            suffix = "summaries"
        elif event_type in {
            EventType.RESOURCE_PLAN_CREATED,
            EventType.RESOURCE_PLAN_UPDATED,
            EventType.RESOURCE_PRESSURE,
        }:
            suffix = "resources"
        else:
            suffix = "events"
        return f"project.{project}.{suffix}"


def validate_event(event: Event, claimed_agent_id: str | None = None) -> tuple[bool, str]:
    if claimed_agent_id and event.producer_agent_id != claimed_agent_id:
        return False, "producer identity does not match authenticated caller"
    if event.target_agent_id and event.topic and event.topic.startswith("agent."):
        expected = f"agent.{event.target_agent_id}.inbox"
        if event.topic != expected:
            return False, "direct-message topic does not match target agent"
    return True, ""
