from __future__ import annotations

import hashlib
from enum import StrEnum
from pathlib import PurePosixPath

from pydantic import BaseModel, Field, field_validator, model_validator

from agentos.config.runtime import AgentResourceAllocation


class AgentRole(StrEnum):
    PM_TECH_LEAD = "pm_tech_lead"
    SOLUTION_ARCHITECT = "solution_architect"
    BACKEND_DEVELOPER = "backend_developer"
    FRONTEND_DEVELOPER = "frontend_developer"
    PLATFORM_ENGINEER = "platform_engineer"
    QA_ENGINEER = "qa_engineer"
    CODE_REVIEWER = "code_reviewer"
    SECURITY_REVIEWER = "security_reviewer"
    INFRASTRUCTURE_AGENT = "infrastructure_agent"


class VerificationType(StrEnum):
    TEST = "test"
    ARTIFACT = "artifact"
    REVIEW = "review"
    COMMAND = "command"
    COMPOSITE = "composite"


class DoDCriterion(BaseModel):
    criterion_id: str
    description: str = Field(min_length=3, max_length=2000)
    verification_type: VerificationType = VerificationType.COMPOSITE
    verification_command: list[str] = Field(default_factory=list)
    required_artifacts: list[str] = Field(default_factory=list)
    required_evidence_types: list[str] = Field(default_factory=lambda: ["test", "review"])

    @field_validator("verification_command")
    @classmethod
    def _command_is_a_token_array(cls, value: list[str]) -> list[str]:
        if any(not token or any(char in token for char in ("\x00", "\n", "\r")) for token in value):
            raise ValueError("verification_command must contain nonempty safe tokens")
        return value

    @field_validator("required_artifacts")
    @classmethod
    def _artifact_patterns_are_relative(cls, value: list[str]) -> list[str]:
        for pattern in value:
            normalized = PurePosixPath(pattern.replace("\\", "/"))
            if normalized.is_absolute() or ".." in normalized.parts or not normalized.parts:
                raise ValueError("required artifacts must be safe relative patterns")
        return value

    @field_validator("required_evidence_types")
    @classmethod
    def _known_evidence_types(cls, value: list[str]) -> list[str]:
        allowed = {"artifact", "test", "command", "review", "security_review", "integration"}
        normalized = list(dict.fromkeys(item.strip().lower() for item in value))
        unknown = set(normalized) - allowed
        if unknown:
            raise ValueError(f"unknown evidence types: {sorted(unknown)}")
        return normalized

    @model_validator(mode="after")
    def _verification_contract_is_executable(self) -> DoDCriterion:
        if self.verification_type == VerificationType.COMMAND and not self.verification_command:
            raise ValueError("command verification requires verification_command")
        return self

    @field_validator("criterion_id")
    @classmethod
    def _safe_id(cls, value: str) -> str:
        normalized = value.strip().lower().replace(" ", "-")
        if not normalized or not all(char.isalnum() or char in "-_" for char in normalized):
            raise ValueError("criterion_id must be a safe identifier")
        return normalized

    @classmethod
    def from_text(cls, text: str, index: int) -> DoDCriterion:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]
        return cls(criterion_id=f"dod-{index}-{digest}", description=text)


class InitialTask(BaseModel):
    title: str = Field(min_length=3, max_length=500)
    description: str = Field(min_length=3, max_length=5000)
    priority: int = Field(default=3, ge=1, le=5)
    risk_level: str = Field(default="LOW", pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    acceptance_criteria: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
    blocked_paths: list[str] = Field(default_factory=list)
    expected_outputs: list[str] = Field(default_factory=list)
    required_reviewers: list[str] = Field(default_factory=list)
    owner_role: AgentRole | None = None
    dod_criteria: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    complexity: str = Field(default="standard", pattern="^(low|standard|high|critical)$")

    @field_validator("allowed_paths", "blocked_paths", "expected_outputs")
    @classmethod
    def _task_paths_are_bounded(cls, value: list[str]) -> list[str]:
        for item in value:
            path = PurePosixPath(item.replace("\\", "/"))
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("task paths and output patterns must be safe and relative")
            if path.parts[0].lower() in {".git", ".env", "secrets", "provider_keys"}:
                raise ValueError(f"globally protected task path: {item}")
        return list(dict.fromkeys(value))

    @field_validator("required_reviewers")
    @classmethod
    def _reviewers_are_known_roles(cls, value: list[str]) -> list[str]:
        known = {AgentRole.CODE_REVIEWER.value, AgentRole.SECURITY_REVIEWER.value}
        normalized = [item.strip().lower() for item in value]
        unknown = set(normalized) - known
        if unknown:
            raise ValueError(f"unknown reviewer roles: {sorted(unknown)}")
        return list(dict.fromkeys(normalized))


class AgentSpec(BaseModel):
    role: AgentRole
    count: int = Field(ge=1, le=50)
    description: str = Field(min_length=3, max_length=2000)
    memory_scopes: list[str] = Field(default_factory=list)
    allowed_action_categories: list[str] = Field(default_factory=list)
    ownership_domains: list[str] = Field(default_factory=list)
    event_subscriptions: list[str] = Field(default_factory=list)
    provider_preferences: list[str] = Field(default_factory=list)
    collaboration_interval_seconds: int = Field(default=30, ge=5, le=600)

    @field_validator("ownership_domains")
    @classmethod
    def _ownership_domains_are_relative(cls, value: list[str]) -> list[str]:
        for item in value:
            path = PurePosixPath(item.replace("\\", "/"))
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("ownership domains must be safe relative paths")
            if path.parts[0].lower() in {".git", ".env", "secrets", "provider_keys"}:
                raise ValueError(f"globally protected ownership domain: {item}")
        return list(dict.fromkeys(value))


class TeamPlan(BaseModel):
    project_name: str = Field(min_length=3, max_length=120)
    user_request: str = Field(min_length=3, max_length=100_000)
    high_level_architecture: str = ""
    dod: list[DoDCriterion] = Field(min_length=1)
    assumptions: list[str] = Field(default_factory=list)
    agents: list[AgentSpec] = Field(min_length=1)
    initial_backlog: list[InitialTask] = Field(default_factory=list)
    max_requested_agents: int = Field(ge=1)

    @field_validator("dod", mode="before")
    @classmethod
    def _normalize_dod(cls, value: object) -> object:
        if isinstance(value, list):
            return [
                DoDCriterion.from_text(item, index).model_dump() if isinstance(item, str) else item
                for index, item in enumerate(value, start=1)
            ]
        return value

    @property
    def total_agents(self) -> int:
        return sum(agent.count for agent in self.agents)

    @model_validator(mode="after")
    def _unique_dod_and_roles(self) -> TeamPlan:
        criterion_ids = [criterion.criterion_id for criterion in self.dod]
        if len(criterion_ids) != len(set(criterion_ids)):
            raise ValueError("DoD criterion IDs must be unique")
        if self.total_agents > self.max_requested_agents:
            raise ValueError("team plan exceeds max_requested_agents")
        known_criteria = set(criterion_ids)
        planned_roles = {agent.role for agent in self.agents}
        task_titles = {task.title for task in self.initial_backlog}
        if len(task_titles) != len(self.initial_backlog):
            raise ValueError("initial backlog task titles must be unique")
        for task in self.initial_backlog:
            if task.owner_role is not None and task.owner_role not in planned_roles:
                raise ValueError(
                    f"task {task.title!r} is assigned to an unplanned role: {task.owner_role}"
                )
            if not task.dod_criteria:
                raise ValueError(f"task {task.title!r} must map to at least one DoD criterion")
            unknown = set(task.dod_criteria) - known_criteria
            if unknown:
                raise ValueError(
                    f"task {task.title!r} references unknown DoD criteria: {sorted(unknown)}"
                )
            missing_dependencies = set(task.depends_on) - task_titles
            if missing_dependencies:
                raise ValueError(
                    f"task {task.title!r} references unknown dependencies: {sorted(missing_dependencies)}"
                )
            if task.title in task.depends_on:
                raise ValueError(f"task {task.title!r} cannot depend on itself")

        dependencies = {task.title: set(task.depends_on) for task in self.initial_backlog}
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(title: str) -> None:
            if title in visited:
                return
            if title in visiting:
                raise ValueError("initial backlog contains a dependency cycle")
            visiting.add(title)
            for dependency in dependencies[title]:
                visit(dependency)
            visiting.remove(title)
            visited.add(title)

        for title in dependencies:
            visit(title)
        return self


class ValidatedTeamPlan(BaseModel):
    original: TeamPlan
    agents: list[AgentSpec]
    total_agents: int
    max_active_agents: int
    max_parallel_code_tasks: int
    reduced: bool
    reduction_reason: str | None = None
    resource_allocations: list[AgentResourceAllocation] = Field(default_factory=list)
