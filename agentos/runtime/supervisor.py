from __future__ import annotations

from collections.abc import Iterable
import uuid
import ray

from agentos.actors.base import AgentWorkerActor
from agentos.actors.bootstrap import BootstrapAgentActor
from agentos.config.settings import Settings
from agentos.runtime.team_plan import AgentSpec, TeamPlan, ValidatedTeamPlan

# Infrastructure imports
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProjectRepository, EventRepository
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType


class RuntimeSupervisor:
    """Owns project lifecycle and Ray actor supervision."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db_manager = DatabaseManager(settings)
        self.dragonfly = DragonflyBus(settings.dragonfly_url)

    def connect_ray(self) -> None:
        if ray.is_initialized():
            return
        if self.settings.ray_address and self.settings.ray_address.strip():
            ray.init(address=self.settings.ray_address, ignore_reinit_error=True)
        else:
            print("[Supervisor] Initializing local Ray cluster block...")
            # We keep Ray, but we add custom parameters to bypass the Windows dashboard bug!
            ray.init(
                ignore_reinit_error=True, 
                num_cpus=2, 
                namespace="agentos",
                include_dashboard=False,          # Disables the crashing dashboard subsystem
                object_store_memory=250_000_000,   # Prevents shared memory errors on Windows
                _system_config={"gcs_rpc_server_reconnect_timeout_s": 60}
            )

    async def bootstrap_project(self, user_request: str) -> dict:
        print("\n[Supervisor] Waking up Runtime Supervisor...")
        self.connect_ray()
        
        # 1. Connect to our relational database engine
        print("[Supervisor] Connecting to PostgreSQL database...")
        await self.db_manager.connect()
        project_repo = ProjectRepository(self.db_manager)
        event_repo = EventRepository(self.db_manager)

        # 2. Get the initial blueprint plan from the Bootstrap Actor using Ray
        print("[Supervisor] Contacting Bootstrap Agent for team blueprint...")
        bootstrap = BootstrapAgentActor.options(namespace="agentos").remote(project_id=self.settings.project_name)
        raw_plan = await bootstrap.create_team_plan.remote(
            user_request, self.settings.max_agents_total
        )
        plan = TeamPlan.model_validate(raw_plan)
        validated = self.validate_team_plan(plan)

        # 3. Save the project setup permanently to PostgreSQL
        print(f"[Supervisor] Saving project '{self.settings.project_name}' metadata to PostgreSQL...")
        db_project_id = await project_repo.create_project(
            name=self.settings.project_name,
            request=user_request,
            dod=validated.original.dod
        )

        # 4. RESTORED: Spawn all long-running worker loops inside Ray memory
        print(f"[Supervisor] Spawning {validated.total_agents} Ray agent actors into memory...")
        actors = await self.create_agent_actors(validated.agents)

        # 5. Broadcast the initial lifecycle event through Dragonfly and Postgres
        print("[Supervisor] Broadcasting PROJECT_CREATED event to Dragonfly Bus...")
        init_event = Event(
            project_id=db_project_id,
            event_type=EventType.PROJECT_CREATED,
            topic="project.lifecycle",
            payload={"user_request": user_request, "dod": validated.original.dod}
        )
        
        await event_repo.save_event(db_project_id, init_event)
        await self.dragonfly.publish_event("project.lifecycle", init_event)

        # 6. Wake up the PM worker node specifically to start analyzing tasks!
        print("[Supervisor] Activating primary Technical Lead Agent...")
        pm_actor = ray.get_actor("pm_tech_lead-1", namespace="agentos")
        execution_trigger = await pm_actor.handle_event.remote(str(init_event.event_id))

        # Close database connection cleanly
        await self.db_manager.disconnect()

        print("[Supervisor] Project bootstrap sequence complete successfully!\n")
        return {
            "project_id": db_project_id,
            "project_name": self.settings.project_name,
            "team_plan": validated.model_dump(),
            "actors": actors,
            "initial_trigger_result": execution_trigger
        }

    def validate_team_plan(self, plan: TeamPlan) -> ValidatedTeamPlan:
        if plan.total_agents <= self.settings.max_agents_total:
            return ValidatedTeamPlan(
                original=plan,
                agents=plan.agents,
                total_agents=plan.total_agents,
                reduced=False,
            )

        reduced_agents: list[AgentSpec] = []
        remaining = self.settings.max_agents_total
        for spec in plan.agents:
            if remaining <= 0:
                break
            count = min(spec.count, remaining)
            reduced_agents.append(spec.model_copy(update={"count": count}))
            remaining -= count
        return ValidatedTeamPlan(
            original=plan,
            agents=reduced_agents,
            total_agents=sum(agent.count for agent in reduced_agents),
            reduced=True,
            reduction_reason="Bootstrap team exceeded configured max_agents_total.",
        )

    # RESTORED FUNCTION
    async def create_agent_actors(self, specs: Iterable[AgentSpec]) -> list[dict]:
        created: list[dict] = []
        settings_payload = self.settings.model_dump(by_alias=False)
        for spec in specs:
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                
                actor = AgentWorkerActor.options(name=agent_id, namespace="agentos").remote(
                    agent_id=agent_id,
                    role=spec.role.value,
                    project_id=self.settings.project_name,
                    settings=settings_payload,
                )
                started = await actor.start.remote()
                created.append(started)
        return created