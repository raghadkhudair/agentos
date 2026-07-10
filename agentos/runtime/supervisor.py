from __future__ import annotations

import asyncio
from collections.abc import Iterable
import uuid
import ray
import structlog

from agentos.actors.bootstrap import BootstrapAgentActor
from agentos.config.settings import Settings
from agentos.runtime.team_plan import AgentRole, AgentSpec, TeamPlan, ValidatedTeamPlan

# Infrastructure imports
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProjectRepository, EventRepository
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType
from agentos.runtime.trigger_engine import TriggerEngine

logger = structlog.get_logger()


class RuntimeSupervisor:
    """Owns project lifecycle, instantiates Trigger Engines, and manages Ray worker supervision."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db_manager = DatabaseManager(settings)
        self.dragonfly = DragonflyBus(settings.dragonfly_url)
        # Background Trigger Engine Router Component Integration
        self.trigger_engine = TriggerEngine(self.dragonfly)

    def connect_ray(self) -> None:
        if ray.is_initialized():
            return
            
        # FORCE LOCAL BYPASS: Ignore external network bridge lookups on Windows local dev environments
        if self.settings.environment == "local":
            logger.info("forcing_local_standalone_ray_bypass")
            ray.init(
                ignore_reinit_error=True, 
                num_cpus=2, 
                namespace="agentos",
                include_dashboard=False,          
                object_store_memory=250_000_000,   
                _system_config={"gcs_rpc_server_reconnect_timeout_s": 60}
            )
        elif self.settings.ray_address and self.settings.ray_address.strip():
            ray.init(address=self.settings.ray_address, ignore_reinit_error=True)
        else:
            logger.info("initializing_fallback_local_ray_cluster")
            ray.init(ignore_reinit_error=True, num_cpus=2, namespace="agentos")

    async def bootstrap_project(self, user_request: str) -> dict:
        logger.info("runtime_supervisor_waking_up", project_name=self.settings.project_name)
        self.connect_ray()
        
        logger.info("postgresql_connected", database_url=self.settings.database_url)
        await self.db_manager.connect()
        project_repo = ProjectRepository(self.db_manager)
        event_repo = EventRepository(self.db_manager)

        logger.info("contacting_bootstrap_agent_for_team_blueprint")
        bootstrap = BootstrapAgentActor.options(namespace="agentos").remote(project_id=self.settings.project_name)
        raw_plan = await bootstrap.create_team_plan.remote(
            user_request, self.settings.max_agents_total
        )
        plan = TeamPlan.model_validate(raw_plan)
        validated = self.validate_team_plan(plan)

        logger.info("saving_project_metadata_to_postgresql", project=self.settings.project_name)
        db_project_id = await project_repo.create_project(
            name=self.settings.project_name,
            request=user_request,
            dod=validated.original.dod
        )

        logger.info("spawning_ray_agent_actors", total_count=validated.total_agents)
        actors = await self.create_agent_actors(validated.agents)

        unified_stream_key = f"project:{db_project_id}:events"

        # Dynamically register worker actors into our Trigger Engine routing subscription matrix
        first_pm_identity = None
        for spec in validated.agents:
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                if spec.role.value == "pm_tech_lead" and not first_pm_identity:
                    first_pm_identity = agent_id
                
                # Bind events subscriptions down to the active agents
                for e_type in EventType:
                    self.trigger_engine.register_subscription(e_type, agent_id)

        # Kick off the asynchronous Trigger Engine routing task daemon in the background
        asyncio.create_task(self.trigger_engine.start_routing_loop(db_project_id))
        
        # Kick off background watchdog loop daemon cleanly at class instance level
        asyncio.create_task(self.watchdog_loop(db_project_id, validated.original.dod))
        logger.info("background_daemons_activated", active_monitoring_stream=unified_stream_key)

        init_event = Event(
            project_id=db_project_id,
            event_type=EventType.PROJECT_CREATED,
            topic=unified_stream_key,
            payload={"user_request": user_request, "dod": validated.original.dod}
        )
        
        await event_repo.save_event(db_project_id, init_event)
        await self.dragonfly.publish_event(unified_stream_key, init_event)

        target_pm_name = first_pm_identity if first_pm_identity else "pm_tech_lead-1"
        logger.info("activating_primary_technical_lead_agent", target_pm_name=target_pm_name)
        
        pm_actor = ray.get_actor(target_pm_name, namespace="agentos")
        execution_trigger = await pm_actor.process_next_step.remote(str(init_event.event_id))

        await self.db_manager.disconnect()
        logger.info("project_bootstrap_sequence_completed")

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

    async def create_agent_actors(self, specs: Iterable[AgentSpec]) -> list[dict]:
        from agentos.actors.base import AgentWorkerActor

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

    async def watchdog_loop(self, project_id: str, dod: list[str]) -> None:
        """Background daemon loop that executes runtime health evaluations every 30 seconds."""
        from agentos.watchdogs.runtime_watchdogs import DoDWatchdog, StagnationWatchdog, SafetyWatchdog, DeadlockWatchdog
        
        dod_wd = DoDWatchdog(self.db_manager)
        stag_wd = StagnationWatchdog(self.db_manager)
        safety_wd = SafetyWatchdog(self.db_manager)
        deadlock_wd = DeadlockWatchdog(self.db_manager)
        
        while True:
            await asyncio.sleep(30)
            for wd, args in [(dod_wd, (project_id, dod)), (stag_wd, (project_id,)),
                            (safety_wd, (project_id,)), (deadlock_wd, (project_id,))]:
                try:
                    result = await wd.inspect(*args)
                    if result.get("action_required") != "NONE":
                        logger.warning(
                            "watchdog_alert_triggered", 
                            watchdog=wd.__class__.__name__, 
                            action_required=result.get("action_required"),
                            reason=result.get("reason")
                        )
                except Exception as e:
                    logger.error("watchdog_execution_failed", watchdog=wd.__class__.__name__, error=str(e))