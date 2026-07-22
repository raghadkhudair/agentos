from __future__ import annotations

from typing import Any
from uuid import UUID

import psutil
import ray
import structlog

from agentos.config.runtime import ResourcePlanner, RuntimeConfig
from agentos.config.settings import Settings
from agentos.messaging.events import Event, EventType
from agentos.provider.gateway import ProviderRegistry, ProviderRequest
from agentos.runtime.team_plan import AgentSpec
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import EventRepository, ResourcePlanRepository

logger = structlog.get_logger()


class InfrastructurePlanner:
    """Computes a bounded, role-aware resource and model distribution plan."""

    def __init__(self, settings: Settings, registry: ProviderRegistry | None = None):
        self.settings = settings
        self.registry = registry or ProviderRegistry(settings=settings)
        self.resources = ResourcePlanner(settings)

    def provider_assignments(self, agents: list[tuple[str, str]]) -> dict[str, tuple[str, str]]:
        assignments: dict[str, tuple[str, str]] = {}
        for agent_id, role in agents:
            request = ProviderRequest(
                purpose="infrastructure_planning",
                messages=[{"role": "user", "content": "Select a role-appropriate provider."}],
                budget_key=UUID("00000000-0000-0000-0000-000000000000"),
                agent_id=agent_id,
                agent_role=role,
                complexity=self.resources.ROLE_COMPLEXITY.get(role),
            )
            candidates = self.registry.candidates(request)
            if candidates:
                profile, model = candidates[0]
                assignments[agent_id] = (profile.provider_id, model)
            else:
                assignments[agent_id] = (
                    self.settings.provider_default,
                    "unavailable-until-configured",
                )
        return assignments

    def plan(self, specs: list[AgentSpec]) -> RuntimeConfig:
        agents: list[tuple[str, str]] = []
        for spec in specs:
            for index in range(1, spec.count + 1):
                agents.append((f"{spec.role.value}-{index}", spec.role.value))
        config = self.resources.build_runtime_config(
            agents,
            provider_assignments=self.provider_assignments(agents),
        )
        for allocation in config.allocations:
            profile = self.registry.profiles.get(allocation.provider)
            if profile is not None:
                allocation.model_routes = dict(profile.models)
        return config

    def pressure_snapshot(self) -> dict[str, Any]:
        memory = psutil.virtual_memory()
        cpu_percent = psutil.cpu_percent(interval=None)
        return {
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "memory_available_bytes": memory.available,
            "pressure": "HIGH" if max(cpu_percent, memory.percent) >= 90 else "NORMAL",
        }


@ray.remote(num_cpus=0.2, max_concurrency=4)  # type: ignore[call-overload]
class InfrastructureAgentActor:
    """System actor that plans and records resources alongside the supervisor."""

    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.planner = InfrastructurePlanner(self.settings)
        self.db = PostgresClient(self.settings)
        self.repository = ResourcePlanRepository(self.db)
        self.events = EventRepository(self.db)

    async def determine_resources(
        self, project_id: str, specs: list[dict[str, Any]]
    ) -> dict[str, Any]:
        parsed_specs = [AgentSpec.model_validate(item) for item in specs]
        config = self.planner.plan(parsed_specs)
        payload = config.model_dump(mode="json")
        plan_id = await self.repository.save(project_id, "infrastructure_agent-1", payload)
        event = Event(
            project_id=project_id,
            event_type=EventType.RESOURCE_PLAN_CREATED,
            producer_agent_id="infrastructure_agent-1",
            payload={"resource_plan_id": plan_id, "envelope": payload["envelope"]},
        )
        await self.events.save_event(project_id, event)
        return {"resource_plan_id": plan_id, **payload}

    async def inspect_pressure(self, project_id: str) -> dict[str, Any]:
        snapshot = self.planner.pressure_snapshot()
        if snapshot["pressure"] == "HIGH":
            event = Event(
                project_id=project_id,
                event_type=EventType.RESOURCE_PRESSURE,
                producer_agent_id="infrastructure_agent-1",
                payload=snapshot,
            )
            await self.events.save_event(project_id, event)
        return snapshot
