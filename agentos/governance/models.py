from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class RiskLevel(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class PolicyDecision(StrEnum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    ALLOW_WITH_CONSTRAINTS = "ALLOW_WITH_CONSTRAINTS"
    REQUIRE_REVIEW = "REQUIRE_REVIEW"
    REQUIRE_HUMAN_APPROVAL = "REQUIRE_HUMAN_APPROVAL"
    REQUIRE_SANDBOX_ONLY = "REQUIRE_SANDBOX_ONLY"
    REQUIRE_BACKUP_FIRST = "REQUIRE_BACKUP_FIRST"
    REQUIRE_SECURITY_REVIEW = "REQUIRE_SECURITY_REVIEW"
    QUARANTINE_AGENT = "QUARANTINE_AGENT"


class AgentIdentity(BaseModel):
    agent_id: str
    role: str
    project_id: str
    squad: str | None = None
    memory_scopes: list[str] = Field(default_factory=list)
    allowed_actions: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)


class ActionRequest(BaseModel):
    project_id: str
    agent_id: str
    action_type: str
    description: str
    target_paths: list[str] = Field(default_factory=list)
    command: str | None = None
    database_operation: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class GuardrailResult(BaseModel):
    decision: PolicyDecision
    risk_level: RiskLevel
    reasons: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
