from __future__ import annotations

import hashlib
import json
import secrets
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


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
    model_config = ConfigDict(extra="forbid")

    agent_id: str = Field(min_length=3, max_length=128)
    role: str = Field(min_length=3, max_length=128)
    project_id: str
    squad: str | None = None
    memory_scopes: list[str] = Field(default_factory=list)
    allowed_actions: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
    ownership_domains: list[str] = Field(default_factory=list)


class ActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    project_id: str
    agent_id: str
    task_id: str | None = None
    action_type: str = Field(min_length=2, max_length=128)
    description: str = Field(min_length=1, max_length=10_000)
    target_paths: list[str] = Field(default_factory=list)
    command: list[str] | None = None
    database_operation: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    issued_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    nonce: str = Field(default_factory=lambda: secrets.token_urlsafe(24))
    integrity_hash: str | None = None

    @field_validator("target_paths")
    @classmethod
    def _relative_paths_only(cls, paths: list[str]) -> list[str]:
        for path in paths:
            normalized = path.replace("\\", "/")
            if normalized.startswith("/") or ".." in normalized.split("/"):
                raise ValueError("target paths must be safe relative paths")
        return paths

    def _canonical_payload(self) -> bytes:
        data = self.model_dump(exclude={"integrity_hash"}, mode="json")
        return json.dumps(data, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")

    @model_validator(mode="after")
    def _seal_integrity_hash(self) -> ActionRequest:
        if self.issued_at.tzinfo is None or self.issued_at.utcoffset() is None:
            raise ValueError("action issued_at must be timezone-aware")
        if self.action_type in {"write_file", "write_code", "read_file"}:
            payload_path = self.payload.get("file_path")
            if not isinstance(payload_path, str) or self.target_paths != [payload_path]:
                raise ValueError("file action payload path must exactly match target_paths")
        if self.action_type in {"shell_command", "run_command"}:
            payload_command = self.payload.get("command")
            if self.command is None or payload_command != self.command:
                raise ValueError("command action payload must exactly match command")
        if self.action_type == "execute_db_operation":
            if self.database_operation != self.payload.get("query"):
                raise ValueError("database action payload must exactly match database_operation")
        expected = hashlib.sha256(self._canonical_payload()).hexdigest()
        if self.integrity_hash and not secrets.compare_digest(self.integrity_hash, expected):
            raise ValueError("action request integrity hash does not match its payload")
        if not self.integrity_hash:
            object.__setattr__(self, "integrity_hash", expected)
        return self

    def verify_integrity(self) -> bool:
        expected = hashlib.sha256(self._canonical_payload()).hexdigest()
        return bool(self.integrity_hash and secrets.compare_digest(self.integrity_hash, expected))


class GuardrailResult(BaseModel):
    decision: PolicyDecision
    risk_level: RiskLevel
    reasons: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
    approval_request_id: str | None = None
