from __future__ import annotations

import fnmatch
import hashlib
import json
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Self

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


class EvidenceType(StrEnum):
    ARTIFACT = "artifact"
    TEST = "test"
    COMMAND = "command"
    REVIEW = "review"
    SECURITY_REVIEW = "security_review"
    INTEGRATION = "integration"


class EvidenceScope(StrEnum):
    CRITERION = "criterion"
    TASK = "task"
    ARTIFACT = "artifact"


class CriterionSource(StrEnum):
    USER = "user"
    SYSTEM = "system"
    INFERRED = "inferred"


class CriterionSeverity(StrEnum):
    ADVISORY = "advisory"
    REQUIRED = "required"
    CRITICAL = "critical"


_DEFAULT_EVIDENCE_SCOPE: dict[EvidenceType, EvidenceScope] = {
    EvidenceType.ARTIFACT: EvidenceScope.ARTIFACT,
    EvidenceType.REVIEW: EvidenceScope.ARTIFACT,
    EvidenceType.SECURITY_REVIEW: EvidenceScope.ARTIFACT,
    EvidenceType.TEST: EvidenceScope.CRITERION,
    EvidenceType.COMMAND: EvidenceScope.CRITERION,
    EvidenceType.INTEGRATION: EvidenceScope.TASK,
}


class DoDCriterion(BaseModel):
    criterion_id: str
    description: str = Field(min_length=3, max_length=2000)
    verification_type: VerificationType = VerificationType.COMPOSITE
    verification_command: list[str] = Field(default_factory=list)
    required_artifacts: list[str] = Field(default_factory=list)
    required_evidence_types: list[EvidenceType] = Field(min_length=1)
    evidence_scopes: dict[EvidenceType, EvidenceScope] = Field(default_factory=dict)
    source: CriterionSource = CriterionSource.SYSTEM
    locked: bool = True
    mandatory: bool = True
    severity: CriterionSeverity = CriterionSeverity.REQUIRED
    affected_contracts: list[str] = Field(default_factory=list)

    @field_validator("verification_command")
    @classmethod
    def _command_is_a_token_array(cls, value: list[str]) -> list[str]:
        if any(not token or any(char in token for char in ("\x00", "\n", "\r")) for token in value):
            raise ValueError("verification_command must contain nonempty safe tokens")
        return value

    @field_validator("required_artifacts", "affected_contracts")
    @classmethod
    def _artifact_patterns_are_relative(cls, value: list[str]) -> list[str]:
        for pattern in value:
            normalized = PurePosixPath(pattern.replace("\\", "/"))
            if normalized.is_absolute() or ".." in normalized.parts or not normalized.parts:
                raise ValueError("artifact and contract patterns must be safe relative patterns")
        return list(dict.fromkeys(value))

    @field_validator("required_evidence_types")
    @classmethod
    def _unique_evidence_types(cls, value: list[EvidenceType]) -> list[EvidenceType]:
        if len(value) != len(set(value)):
            raise ValueError("required evidence types must be unique")
        return value

    @model_validator(mode="after")
    def _verification_contract_is_executable(self) -> Self:
        required = set(self.required_evidence_types)
        deterministic = required & {EvidenceType.TEST, EvidenceType.COMMAND}
        if not deterministic:
            raise ValueError("every criterion requires deterministic test or command evidence")
        if deterministic and not self.verification_command:
            raise ValueError("test or command evidence requires verification_command")
        if len(deterministic) > 1:
            raise ValueError("a criterion must use test or command evidence, not both")
        if EvidenceType.ARTIFACT in required and not self.required_artifacts:
            raise ValueError("artifact evidence requires at least one required artifact pattern")
        if required & {EvidenceType.REVIEW, EvidenceType.SECURITY_REVIEW} and (
            EvidenceType.ARTIFACT not in required
        ):
            raise ValueError("review evidence is artifact-scoped and requires artifact evidence")
        if (
            self.verification_type == VerificationType.COMMAND
            and EvidenceType.COMMAND not in required
        ):
            raise ValueError("command verification must require command evidence")
        if self.verification_type == VerificationType.TEST and EvidenceType.TEST not in required:
            raise ValueError("test verification must require test evidence")
        if (
            self.verification_type == VerificationType.ARTIFACT
            and EvidenceType.ARTIFACT not in required
        ):
            raise ValueError("artifact verification must require artifact evidence")
        if (
            self.verification_type == VerificationType.REVIEW
            and EvidenceType.REVIEW not in required
        ):
            raise ValueError("review verification must require review evidence")
        if self.source == CriterionSource.INFERRED and (self.mandatory or self.locked):
            raise ValueError("inferred criteria must remain advisory and unlocked until approved")
        if self.source in {CriterionSource.USER, CriterionSource.SYSTEM} and not self.locked:
            raise ValueError("user and system criteria must remain locked until governed amendment")
        if self.mandatory and self.severity == CriterionSeverity.ADVISORY:
            raise ValueError("mandatory criteria cannot have advisory severity")
        if not self.mandatory and self.severity != CriterionSeverity.ADVISORY:
            raise ValueError("non-mandatory criteria must have advisory severity")
        if self.mandatory:
            baseline = {EvidenceType.ARTIFACT, EvidenceType.REVIEW, EvidenceType.INTEGRATION}
            missing_baseline = baseline - required
            if missing_baseline:
                raise ValueError(
                    "mandatory criteria require artifact, independent review, and integration "
                    f"evidence: missing {sorted(item.value for item in missing_baseline)}"
                )
        if self.severity == CriterionSeverity.CRITICAL and (
            EvidenceType.SECURITY_REVIEW not in required
        ):
            raise ValueError("critical criteria require independent security review evidence")
        unknown_scopes = set(self.evidence_scopes) - required
        if unknown_scopes:
            raise ValueError(
                f"evidence scopes reference unrequired types: {sorted(unknown_scopes)}"
            )
        self.evidence_scopes = {
            evidence_type: self.evidence_scopes.get(
                evidence_type, _DEFAULT_EVIDENCE_SCOPE[evidence_type]
            )
            for evidence_type in self.required_evidence_types
        }
        return self

    @field_validator("criterion_id")
    @classmethod
    def _safe_id(cls, value: str) -> str:
        normalized = value.strip().lower().replace(" ", "-")
        if not normalized or not all(char.isalnum() or char in "-_" for char in normalized):
            raise ValueError("criterion_id must be a safe identifier")
        return normalized

    @property
    def contract_hash(self) -> str:
        payload = self.model_dump(mode="json", exclude_none=True)
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @classmethod
    def from_text(cls, text: str, index: int) -> DoDCriterion:
        raise ValueError(
            "plain-text DoD criteria are ambiguous; provide an explicit executable evidence contract"
        )


class InitialTask(BaseModel):
    title: str = Field(min_length=3, max_length=500)
    description: str = Field(min_length=3, max_length=5000)
    priority: int = Field(default=3, ge=1, le=5)
    risk_level: str = Field(default="LOW", pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    acceptance_criteria: list[str] = Field(min_length=1)
    allowed_paths: list[str] = Field(min_length=1)
    blocked_paths: list[str] = Field(default_factory=list)
    expected_outputs: list[str] = Field(min_length=1)
    required_reviewers: list[str] = Field(min_length=1)
    owner_role: AgentRole
    dod_criteria: list[str] = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)
    affected_contracts: list[str] = Field(default_factory=list)
    complexity: str = Field(default="standard", pattern="^(low|standard|high|critical)$")

    @field_validator("allowed_paths", "blocked_paths", "expected_outputs", "affected_contracts")
    @classmethod
    def _task_paths_are_bounded(cls, value: list[str]) -> list[str]:
        for item in value:
            path = PurePosixPath(item.replace("\\", "/"))
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise ValueError("task paths and output patterns must be safe and relative")
        return list(dict.fromkeys(value))

    @field_validator("allowed_paths", "expected_outputs", "affected_contracts")
    @classmethod
    def _writable_paths_are_not_globally_protected(cls, value: list[str]) -> list[str]:
        for item in value:
            path = PurePosixPath(item.replace("\\", "/"))
            if path.parts[0].lower() in {".git", ".env", "secrets", "provider_keys"}:
                raise ValueError(f"globally protected task path: {item}")
        return value

    @field_validator("required_reviewers")
    @classmethod
    def _reviewers_are_known_roles(cls, value: list[str]) -> list[str]:
        known = {AgentRole.CODE_REVIEWER.value, AgentRole.SECURITY_REVIEWER.value}
        normalized = [item.strip().lower() for item in value]
        unknown = set(normalized) - known
        if unknown:
            raise ValueError(f"unknown reviewer roles: {sorted(unknown)}")
        return list(dict.fromkeys(normalized))

    @model_validator(mode="after")
    def _review_and_output_contracts_are_complete(self) -> Self:
        if AgentRole.CODE_REVIEWER.value not in self.required_reviewers:
            raise ValueError("every task requires an independent code reviewer")
        if self.risk_level in {"HIGH", "CRITICAL"} and (
            AgentRole.SECURITY_REVIEWER.value not in self.required_reviewers
        ):
            raise ValueError("high and critical risk tasks require a security reviewer")
        for output in self.expected_outputs:
            if not any(_patterns_overlap(output, allowed) for allowed in self.allowed_paths):
                raise ValueError(
                    f"expected output {output!r} is outside the task's allowed path contract"
                )
        return self


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


def _patterns_overlap(left: str, right: str) -> bool:
    """Conservatively decide whether two bounded path patterns can describe one file."""

    a = left.replace("\\", "/").rstrip("/")
    b = right.replace("\\", "/").rstrip("/")
    wildcard = "*?["
    a_prefix = a.split("*", 1)[0].split("?", 1)[0]
    b_prefix = b.split("*", 1)[0].split("?", 1)[0]
    return (
        fnmatch.fnmatch(a, b)
        or fnmatch.fnmatch(b, a)
        or a.startswith(b_prefix)
        or b.startswith(a_prefix)
        or (not any(char in b for char in wildcard) and a.startswith(f"{b}/"))
        or (not any(char in a for char in wildcard) and b.startswith(f"{a}/"))
    )


class TeamPlan(BaseModel):
    project_name: str = Field(min_length=3, max_length=120)
    user_request: str = Field(min_length=3, max_length=100_000)
    high_level_architecture: str = Field(min_length=3)
    dod: list[DoDCriterion] = Field(min_length=1)
    assumptions: list[str] = Field(default_factory=list)
    agents: list[AgentSpec] = Field(min_length=1)
    initial_backlog: list[InitialTask] = Field(min_length=1)
    max_requested_agents: int = Field(ge=1)
    contract_version: int = Field(default=1, ge=1)
    source_revision: str = Field(min_length=7, max_length=64)
    planning_context_hash: str = Field(pattern="^[a-f0-9]{64}$")
    prompt_version: str = Field(min_length=1, max_length=120)

    @field_validator("dod", mode="before")
    @classmethod
    def _reject_ambiguous_dod(cls, value: object) -> object:
        if isinstance(value, list) and any(isinstance(item, str) for item in value):
            raise ValueError("DoD entries must be explicit structured evidence contracts")
        return value

    @property
    def total_agents(self) -> int:
        return sum(agent.count for agent in self.agents)

    @property
    def contract_hash(self) -> str:
        payload = {
            "contract_version": self.contract_version,
            "source_revision": self.source_revision,
            "planning_context_hash": self.planning_context_hash,
            "dod": [criterion.model_dump(mode="json") for criterion in self.dod],
            "backlog": [task.model_dump(mode="json") for task in self.initial_backlog],
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @model_validator(mode="after")
    def _cross_object_contract_is_complete(self) -> Self:
        criterion_ids = [criterion.criterion_id for criterion in self.dod]
        if len(criterion_ids) != len(set(criterion_ids)):
            raise ValueError("DoD criterion IDs must be unique")
        semantic_descriptions = [
            " ".join(criterion.description.lower().split()) for criterion in self.dod
        ]
        if len(semantic_descriptions) != len(set(semantic_descriptions)):
            raise ValueError("DoD criteria must not duplicate the same normalized requirement")
        if self.total_agents > self.max_requested_agents:
            raise ValueError("team plan exceeds max_requested_agents")
        known_criteria = set(criterion_ids)
        mandatory_criteria = {
            criterion.criterion_id for criterion in self.dod if criterion.mandatory
        }
        if not mandatory_criteria:
            raise ValueError("a deliverable plan requires at least one mandatory DoD criterion")
        planned_roles = {agent.role for agent in self.agents}
        task_titles = {task.title for task in self.initial_backlog}
        if len(task_titles) != len(self.initial_backlog):
            raise ValueError("initial backlog task titles must be unique")
        covered_criteria: set[str] = set()
        outputs_by_criterion: dict[str, list[str]] = {item: [] for item in known_criteria}
        contracts_by_criterion: dict[str, set[str]] = {item: set() for item in known_criteria}
        criterion_map = {item.criterion_id: item for item in self.dod}
        for task in self.initial_backlog:
            if task.owner_role not in planned_roles:
                raise ValueError(
                    f"task {task.title!r} is assigned to an unplanned role: {task.owner_role}"
                )
            unknown = set(task.dod_criteria) - known_criteria
            if unknown:
                raise ValueError(
                    f"task {task.title!r} references unknown DoD criteria: {sorted(unknown)}"
                )
            covered_criteria.update(task.dod_criteria)
            for criterion_id in task.dod_criteria:
                outputs_by_criterion[criterion_id].extend(task.expected_outputs)
                contracts_by_criterion[criterion_id].update(task.affected_contracts)
                criterion = criterion_map[criterion_id]
                delivery_baseline = {
                    EvidenceType.ARTIFACT,
                    EvidenceType.REVIEW,
                    EvidenceType.INTEGRATION,
                }
                missing_delivery_evidence = delivery_baseline - set(
                    criterion.required_evidence_types
                )
                if missing_delivery_evidence:
                    raise ValueError(
                        f"task {task.title!r} maps to criterion {criterion_id!r} without "
                        "artifact, review, and integration evidence: missing "
                        f"{sorted(item.value for item in missing_delivery_evidence)}"
                    )
                required_reviewers = {AgentRole.CODE_REVIEWER.value}
                if EvidenceType.SECURITY_REVIEW in criterion.required_evidence_types:
                    required_reviewers.add(AgentRole.SECURITY_REVIEWER.value)
                missing = required_reviewers - set(task.required_reviewers)
                if missing:
                    raise ValueError(
                        f"task {task.title!r} is missing criterion-required reviewers: {sorted(missing)}"
                    )
                if task.risk_level in {"HIGH", "CRITICAL"} and (
                    EvidenceType.SECURITY_REVIEW not in criterion.required_evidence_types
                ):
                    raise ValueError(
                        f"task {task.title!r} risk requires security evidence on criterion "
                        f"{criterion_id!r}"
                    )
            missing_dependencies = set(task.depends_on) - task_titles
            if missing_dependencies:
                raise ValueError(
                    f"task {task.title!r} references unknown dependencies: {sorted(missing_dependencies)}"
                )
            if task.title in task.depends_on:
                raise ValueError(f"task {task.title!r} cannot depend on itself")

        uncovered = mandatory_criteria - covered_criteria
        if uncovered:
            raise ValueError(
                f"mandatory DoD criteria lack implementation tasks: {sorted(uncovered)}"
            )
        for criterion in self.dod:
            if not criterion.mandatory:
                continue
            for pattern in criterion.required_artifacts:
                if not any(
                    _patterns_overlap(pattern, output)
                    for output in outputs_by_criterion[criterion.criterion_id]
                ):
                    raise ValueError(
                        f"criterion {criterion.criterion_id!r} artifact {pattern!r} "
                        "is not covered by a mapped task output"
                    )
            missing_contracts = (
                set(criterion.affected_contracts) - contracts_by_criterion[criterion.criterion_id]
            )
            if missing_contracts:
                raise ValueError(
                    f"criterion {criterion.criterion_id!r} affected contracts are not assigned "
                    f"to mapped tasks: {sorted(missing_contracts)}"
                )

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
