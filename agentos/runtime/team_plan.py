from __future__ import annotations

from enum import StrEnum
from pydantic import BaseModel, Field


class AgentRole(StrEnum):
    PM_TECH_LEAD = "pm_tech_lead"
    SOLUTION_ARCHITECT = "solution_architect"
    BACKEND_DEVELOPER = "backend_developer"
    FRONTEND_DEVELOPER = "frontend_developer"
    PLATFORM_ENGINEER = "platform_engineer"
    INFRA_ENGINEER = "infra_engineer"
    QA_ENGINEER = "qa_engineer"
    CODE_REVIEWER = "code_reviewer"
    SECURITY_REVIEWER = "security_reviewer"


class AgentSpec(BaseModel):
    role: AgentRole
    count: int = Field(ge=1)
    description: str
    memory_scopes: list[str] = Field(default_factory=list)
    allowed_action_categories: list[str] = Field(default_factory=list)
    ownership_domains: list[str] = Field(default_factory=list)
    event_subscriptions: list[str] = Field(default_factory=list) 


class TeamPlan(BaseModel):
    project_name: str
    user_request: str
    dod: list[str]
    assumptions: list[str] = Field(default_factory=list)
    agents: list[AgentSpec]
    max_requested_agents: int

    @property
    def total_agents(self) -> int:
        return sum(agent.count for agent in self.agents)


class ValidatedTeamPlan(BaseModel):
    original: TeamPlan
    agents: list[AgentSpec]
    total_agents: int
    max_active_agents: int
    max_parallel_code_tasks: int
    reduced: bool
    reduction_reason: str | None = None