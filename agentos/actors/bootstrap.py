from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

import ray
import structlog

from agentos.config.loader import team_roles
from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.provider.gateway import ProviderRequest
from agentos.runtime.team_plan import AgentRole, AgentSpec, DoDCriterion, InitialTask, TeamPlan

logger = structlog.get_logger()


@ray.remote(num_cpus=0.2, max_restarts=5, max_task_retries=2)
class BootstrapAgentActor:
    def __init__(self, project_id: str, settings_payload: dict[str, Any], provider_actor_name: str):
        self.project_id = project_id
        self.settings = Settings(**settings_payload)
        self.provider = ray.get_actor(provider_actor_name, namespace="agentos")

    @staticmethod
    def _strip_json_fence(content: str) -> str:
        clean = content.strip()
        if clean.startswith("```"):
            clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", clean, flags=re.IGNORECASE)
        return clean

    @staticmethod
    def _role_template(role: AgentRole, config: dict[str, Any]) -> dict[str, Any]:
        return next(
            (item for item in config.get("roles", []) if item["role"].lower() == role.value),
            {},
        )

    def _hydrate_agent(self, proposal: dict[str, Any], config: dict[str, Any]) -> AgentSpec:
        role = AgentRole(str(proposal.get("role", "PM_TECH_LEAD")).lower())
        template = self._role_template(role, config)
        return AgentSpec(
            role=role,
            count=int(proposal.get("count", 1)),
            description=proposal.get("description") or template.get("description") or role.value,
            memory_scopes=template.get("default_memory_scopes", []),
            allowed_action_categories=template.get("allowed_action_categories", []),
            ownership_domains=proposal.get("ownership_domains") or [role.value],
            event_subscriptions=template.get("default_event_subscriptions", []),
            provider_preferences=template.get("provider_preferences", []),
            collaboration_interval_seconds=self.settings.collaboration_interval_seconds,
        )

    def _fallback(self, user_request: str, max_agents_total: int) -> TeamPlan:
        config = team_roles()
        data = config["fallback_team"]
        agents = [self._hydrate_agent(item, config) for item in data["agents"]]
        return TeamPlan(
            project_name=data["project_name"],
            user_request=user_request,
            high_level_architecture=data["high_level_architecture"],
            dod=[DoDCriterion.model_validate(item) for item in data["dod"]],
            assumptions=data["assumptions"],
            agents=agents,
            initial_backlog=[
                InitialTask.model_validate(item) for item in data.get("initial_backlog", [])
            ],
            max_requested_agents=max_agents_total,
        )

    async def create_team_plan(self, user_request: str, max_agents_total: int) -> dict[str, Any]:
        config = team_roles()
        role_catalog = [
            {
                "role": item["role"],
                "description": item["description"],
                "memory_scopes": item["default_memory_scopes"],
                "allowed_actions": item["allowed_action_categories"],
            }
            for item in config.get("roles", [])
        ]
        system_prompt = f"""You are the bootstrap PM and principal architect for AgentOS.
Create a production delivery plan for the user's software request. Use only roles in this catalog:
{json.dumps(role_catalog, indent=2)}

Mandatory rules:
- Include exactly one PM_TECH_LEAD and one INFRASTRUCTURE_AGENT.
- Include SECURITY_REVIEWER, CODE_REVIEWER, and QA_ENGINEER.
- The total count must be at most {max_agents_total}.
- Every DoD criterion must be independently verifiable and require explicit evidence.
- Every initial task must have bounded allowed_paths, blocked_paths, expected_outputs, and review needs.
- Every initial task must name an owner_role that is present in the planned team.
- Do not include credentials, deployment secrets, or fabricated evidence.

Return one JSON object with this shape:
{{
  "project_name": "safe-name",
  "high_level_architecture": "architecture",
  "dod": [{{
    "criterion_id": "safe-id",
    "description": "measurable outcome",
    "verification_type": "test|artifact|review|command|composite",
    "verification_command": ["executable", "arg"],
    "required_artifacts": ["path"],
    "required_evidence_types": ["artifact", "test", "review"]
  }}],
  "assumptions": ["assumption"],
  "agents": [{{"role": "ROLE_NAME", "count": 1, "description": "assignment", "ownership_domains": ["domain"]}}],
  "initial_backlog": [{{
    "title": "task", "description": "details", "priority": 1,
    "risk_level": "LOW|MEDIUM|HIGH|CRITICAL", "complexity": "low|standard|high|critical",
    "acceptance_criteria": ["criterion"], "allowed_paths": ["path"],
    "blocked_paths": ["path"], "expected_outputs": ["path"],
    "required_reviewers": ["role"], "owner_role": "ROLE_NAME",
    "dod_criteria": ["criterion-id"],
    "depends_on": ["earlier task title"]
  }}]
}}"""
        request = ProviderRequest(
            purpose="bootstrap_team_planning",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_request},
            ],
            budget_key=UUID(self.project_id),
            agent_id="pm_tech_lead-bootstrap",
            agent_role="pm_tech_lead",
            complexity=TaskComplexity.HIGH,
            required_capabilities={"chat", "json"},
        )
        try:
            response = await self.provider.get_completion.remote(
                request.model_dump(mode="json"), response_format={"type": "json_object"}
            )
            data = json.loads(self._strip_json_fence(response["content"]))
            agents = [self._hydrate_agent(item, config) for item in data["agents"]]
            plan = TeamPlan(
                project_name=data["project_name"],
                user_request=user_request,
                high_level_architecture=data["high_level_architecture"],
                dod=[DoDCriterion.model_validate(item) for item in data["dod"]],
                assumptions=list(data.get("assumptions", [])),
                agents=agents,
                initial_backlog=[
                    InitialTask.model_validate(item) for item in data.get("initial_backlog", [])
                ],
                max_requested_agents=max_agents_total,
            )
            return plan.model_dump(mode="json")
        except Exception as error:
            logger.error(
                "bootstrap_plan_failed_using_safe_fallback", error_type=type(error).__name__
            )
            return self._fallback(user_request, max_agents_total).model_dump(mode="json")
