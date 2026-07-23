from __future__ import annotations

import json
import re
from string import Template
from typing import Any
from uuid import UUID

import ray
import structlog

from agentos.config.loader import load_prompt, runtime_tuning, team_roles
from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.provider.gateway import ProviderRequest
from agentos.runtime.team_plan import AgentRole, AgentSpec, InitialTask, TeamPlan

logger = structlog.get_logger()
_PROMPT_VERSION = "bootstrap-dod-v1"


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

    async def create_team_plan(
        self,
        user_request: str,
        max_agents_total: int,
        planning_context: dict[str, Any],
    ) -> dict[str, Any]:
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
        prompt = Template(load_prompt("bootstrap_pm.md")).safe_substitute(
            ROLE_CATALOG=json.dumps(role_catalog, sort_keys=True),
            MAX_AGENTS=str(max_agents_total),
            PLANNING_CONTEXT=json.dumps(planning_context, sort_keys=True),
        )
        attempts = int(runtime_tuning().get("planning", {}).get("max_validation_attempts", 2))
        validation_error = ""
        for attempt in range(1, attempts + 1):
            messages = [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_request},
            ]
            if validation_error:
                messages.append(
                    {
                        "role": "user",
                        "content": (
                            "The previous JSON was rejected by the contract validator. Correct every "
                            f"reported issue and return a complete replacement object: {validation_error}"
                        ),
                    }
                )
            request = ProviderRequest(
                purpose="bootstrap_team_planning",
                messages=messages,
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
                    dod=data["dod"],
                    assumptions=list(data.get("assumptions", [])),
                    agents=agents,
                    initial_backlog=[
                        InitialTask.model_validate(item) for item in data["initial_backlog"]
                    ],
                    max_requested_agents=max_agents_total,
                    contract_version=1,
                    source_revision=data["source_revision"],
                    planning_context_hash=data["planning_context_hash"],
                    prompt_version=data["prompt_version"],
                )
                if plan.source_revision != planning_context["source_revision"]:
                    raise ValueError(
                        "source_revision does not match the immutable planning snapshot"
                    )
                if plan.planning_context_hash != planning_context["planning_context_hash"]:
                    raise ValueError("planning_context_hash does not match the supplied context")
                if plan.prompt_version != _PROMPT_VERSION:
                    raise ValueError(f"prompt_version must be {_PROMPT_VERSION}")
                return plan.model_dump(mode="json")
            except Exception as error:
                validation_error = f"{type(error).__name__}: {error}"[:4000]
                logger.warning(
                    "bootstrap_plan_validation_failed",
                    attempt=attempt,
                    max_attempts=attempts,
                    error=validation_error,
                )
        logger.error("bootstrap_plan_failed_closed", attempts=attempts, error=validation_error)
        raise RuntimeError(f"planning failed after {attempts} bounded attempts: {validation_error}")
