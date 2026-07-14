from __future__ import annotations

import asyncio
from collections.abc import Iterable
import uuid
import ray
import structlog

from agentos.actors.bootstrap import BootstrapAgentActor
from agentos.config.settings import Settings
from agentos.runtime.team_plan import AgentSpec, TeamPlan, ValidatedTeamPlan
from agentos.watchdogs.runtime_watchdogs import StagnationWatchdog, SafetyWatchdog, DeadlockWatchdog

# Infrastructure imports
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProjectRepository, EventRepository
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType
from agentos.runtime.trigger_engine import TriggerEngineActor
from agentos.memory.broker import MemoryBrokerActor
from agentos.provider.gateway import ProviderGatewayActor
from agentos.execution.supervisor import ExecutionSupervisorActor
from agentos.checkpoints.manager import CheckpointManagerActor, SummaryManagerActor
from agentos.dod.evaluator import DoDEvaluatorActor

# Dynamic configuration injection
from agentos.config.loader import runtime_tuning
tuning_cfg = runtime_tuning()

logger = structlog.get_logger()


@ray.remote(namespace="agentos")
class RuntimeSupervisorActor:
    """Owns project lifecycle, supervises actor health, and enforces team limits."""

    def __init__(self, settings_payload: dict):
        self.settings = Settings(**settings_payload)
        self.db_manager = DatabaseManager(self.settings)
        self.dragonfly = DragonflyBus(self.settings.dragonfly_url)
        
        # 1. Initialize Ray Actors with remote options and serializable settings payloads
        self.trigger_engine = TriggerEngineActor.options(
            name="trigger_engine",
            namespace="agentos"
        ).remote(self.settings.dragonfly_url)
        
        self.memory_broker = MemoryBrokerActor.options(
            name="memory_broker",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))
        
        self.provider_gateway = ProviderGatewayActor.options(
            name="provider_gateway",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))
        
        self.execution_supervisor = ExecutionSupervisorActor.options(
            name="execution_supervisor",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))
        
        self.checkpoint_manager = CheckpointManagerActor.options(
            name="checkpoint_manager",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))
        
        self.summary_manager = SummaryManagerActor.options(
            name="summary_manager",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))
        
        self.dod_evaluator = DoDEvaluatorActor.options(
            name="dod_evaluator",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))
        
        self._project_complete = asyncio.Event()
        self._actor_registry: dict[str, dict] = {}  
        self.is_running = False

    async def bootstrap_project(self, user_request: str) -> dict:
        """Starts a brand new project lifecycle from a user prompt."""
        self.is_running = True
        logger.info("runtime_supervisor_waking_up", project_name=self.settings.project_name)
        
        await self.db_manager.connect()
        project_repo = ProjectRepository(self.db_manager)
        event_repo = EventRepository(self.db_manager)

        logger.info("contacting_bootstrap_agent_for_team_blueprint")
        bootstrap = BootstrapAgentActor.options(namespace="agentos").remote(project_id=self.settings.project_name)
        
        raw_plan = await bootstrap.create_team_plan.remote(
            user_request, tuning_cfg["agent_limits"]["max_agents_total"]
        )
        plan = TeamPlan.model_validate(raw_plan)
        validated = self.validate_team_plan(plan)
        actual_project_name = plan.project_name

        print("\n" + "="*60)
        print(f"  MULTI-AGENT TEAM BLUEPRINT FOR: {actual_project_name.upper()} ")
        print("="*60)
        
        print(" DEFINITION OF DONE (DoD):")
        for i, item in enumerate(validated.original.dod, 1):
            print(f"    {i}. {item}")
            
        print("\n  PROJECT ASSUMPTIONS:")
        for item in validated.original.assumptions:
            print(f"    - {item}")
            
        print("\n  TEAM ROSTER:")
        for agent_spec in validated.agents:
            print(f"     Role: {agent_spec.role.value:<25} | Workers: {agent_spec.count}")
            print(f"      Domains: {', '.join(agent_spec.ownership_domains)}")
        print("="*60 + "\n")
   
        logger.info("saving_project_metadata_to_postgresql", project=actual_project_name)
        db_project_id = await project_repo.create_project(
            name=actual_project_name,
            request=user_request,
            dod=validated.original.dod
        )

        logger.info("spawning_ray_agent_actors", total_count=validated.total_agents)
        actors = await self.create_agent_actors(validated.agents, db_project_id)

        unified_stream_key = f"project:{db_project_id}:events"

        first_pm_identity = None
        for spec in validated.agents:
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                if spec.role.value == "pm_tech_lead" and not first_pm_identity:
                    first_pm_identity = agent_id
                
                for e_type_str in spec.event_subscriptions:
                    try:
                        # 2. Extract Enum value as a raw string to match TriggerEngineActor expectations
                        e_type = EventType(e_type_str.upper())
                        await self.trigger_engine.register_subscription.remote(e_type.value, agent_id)
                    except ValueError:
                        logger.warning("invalid_event_type_proposed", event_type=e_type_str)

        # 3. Use `.remote()` when invoking async tasks on Ray Actors
        asyncio.create_task(self.trigger_engine.start_routing_loop.remote(db_project_id))
        asyncio.create_task(self.watchdog_loop(db_project_id, validated.original.dod))
        asyncio.create_task(self._supervise_health_loop(db_project_id))
        
        logger.info("background_daemons_activated", active_monitoring_stream=unified_stream_key)

        init_event = Event(
            project_id=db_project_id,
            event_type=EventType.PROJECT_CREATED,
            topic=unified_stream_key,
            payload={"user_request": user_request, "dod": validated.original.dod}
        )
        
        await event_repo.save_event(db_project_id, init_event)
        await self.dragonfly.publish_event(unified_stream_key, init_event)

        target_pm_name = first_pm_identity if first_pm_identity else f"{validated.agents[0].role.value}-1"
        logger.info("activating_primary_technical_lead_agent", target_pm_name=target_pm_name)
        
        pm_actor = ray.get_actor(target_pm_name, namespace="agentos")
        execution_trigger = await pm_actor.process_next_step.remote(str(init_event.event_id))

        logger.info("supervisor_blocking_main_thread_awaiting_agent_completion")
        await self._project_complete.wait()

        await self.shutdown()
        
        return {
            "project_id": db_project_id,
            "project_name": actual_project_name,
            "team_plan": validated.model_dump(),
            "initial_trigger_result": execution_trigger
        }

    async def resume_project(self, project_id: str) -> dict:
        """Resumes a previously stopped project from the database."""
        self.is_running = True
        await self.db_manager.connect()
        logger.info("resuming_project", project_id=project_id)
        return {"status": "resumed", "project_id": project_id}

    def validate_team_plan(self, plan: TeamPlan) -> ValidatedTeamPlan:
        """Enforces limits on maximum total agents."""
        max_allowed = tuning_cfg["agent_limits"]["max_agents_total"]
        if plan.total_agents <= max_allowed:
            return ValidatedTeamPlan(
                original=plan, agents=plan.agents, total_agents=plan.total_agents, reduced=False,
            )

        reduced_agents: list[AgentSpec] = []
        remaining = max_allowed
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
            reduction_reason="Configured max_agents_total constraint enforced.",
        )

    async def create_agent_actors(self, specs: Iterable[AgentSpec], project_id: str) -> list[dict]:
        """Spawns Ray agent actors and registers them for health monitoring."""
        from agentos.actors.base import AgentWorkerActor

        created: list[dict] = []
        settings_payload = self.settings.model_dump(by_alias=False)
        for spec in specs:
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                max_threads = tuning_cfg["agent_limits"]["max_threads_per_agent"]
                
                role_parts = spec.role.value.split("_")
                squad_name = role_parts[0] if role_parts else "engineering"

                spec_payload = {
                    "squad": squad_name,
                    "permissions": ["low_risk", "medium_risk"],
                    "memory_scopes": spec.memory_scopes,
                    "allowed_actions": spec.allowed_action_categories,
                    "ownership_domains": spec.ownership_domains,
                    "event_subscriptions": spec.event_subscriptions,
                    "last_checkpoint_pointer": None  
                }

                actor = AgentWorkerActor.options(
                    name=agent_id, 
                    namespace="agentos",
                    max_concurrency=max_threads+1
                ).remote(
                    agent_id=agent_id,
                    role=spec.role.value,
                    project_id=project_id,
                    settings=settings_payload,
                    spec_payload=spec_payload 
                )
                
                started = await actor.start.remote()
                
                self._actor_registry[agent_id] = {
                    "handle": actor,
                    "spec": spec,
                    "project_id": project_id,
                    "spec_payload": spec_payload
                }
                created.append(started)
        return created

    async def _supervise_health_loop(self, project_id: str) -> None:
        """Background daemon that pings actors and restarts them if they die."""
        from agentos.actors.base import AgentWorkerActor
        
        while self.is_running:
            await asyncio.sleep(15) 
            for agent_id, data in list(self._actor_registry.items()):
                actor = data["handle"]
                try:
                    state = await actor.start.remote() 
                except ray.exceptions.RayActorError:
                    logger.warning("actor_crash_detected_restarting", agent_id=agent_id)

                    settings_payload = self.settings.model_dump(by_alias=False)
                    max_threads = tuning_cfg["agent_limits"]["max_threads_per_agent"]
                    
                    new_actor = AgentWorkerActor.options(
                        name=agent_id, 
                        namespace="agentos",
                        max_concurrency=max_threads+1,
                        lifetime="detached" 
                    ).remote(
                        agent_id=agent_id,
                        role=data["spec"].role.value,
                        project_id=data["project_id"],
                        settings=settings_payload,
                        spec_payload=data["spec_payload"]
                    )
                    await new_actor.start.remote()
                    self._actor_registry[agent_id]["handle"] = new_actor
                    logger.info("actor_successfully_restarted", agent_id=agent_id)

    async def watchdog_loop(self, project_id: str, dod: list[str]) -> None:
        """Monitors system states, delegating DoD validation to the dedicated DoDEvaluatorActor."""
        # 4. Swap local watchdog for the robust, remote Ray DoDEvaluatorActor
        stag_wd = StagnationWatchdog(self.db_manager)
        safety_wd = SafetyWatchdog(self.db_manager)
        deadlock_wd = DeadlockWatchdog(self.db_manager)
        
        while self.is_running:
            await asyncio.sleep(tuning_cfg["watchdog_loop"]["interval_seconds"])
            
            # A. Evaluate the Definition of Done (DoD) remotely on the dedicated Ray Actor
            try:
                evaluation_result = await self.dod_evaluator.evaluate.remote(project_id, dod)
                if evaluation_result.get("satisfied", False):
                    logger.info("[WATCHDOG]: All Definition of Done (DoD) criteria successfully met!")
                    self._project_complete.set()
                    return
            except Exception as e:
                logger.error("watchdog_dod_evaluation_failed", error=str(e))

            # B. Inspect other local behavioral watchdogs
            for wd, args in [
                (stag_wd, (project_id,)),
                (safety_wd, (project_id,)), 
                (deadlock_wd, (project_id,))
            ]:
                try:
                    await wd.inspect(*args)
                except Exception as e:
                    logger.error("watchdog_inspection_failed", watchdog=wd.__class__.__name__, error=str(e))

    async def shutdown(self):
        """Gracefully cleans up resources and shuts down the supervisor."""
        logger.info("initiating_supervisor_shutdown")
        self.is_running = False
        
        # Stop background loops on the Ray Actor trigger engine
        await self.trigger_engine.stop.remote()
        
        for agent_id, data in self._actor_registry.items():
            ray.kill(data["handle"])
            
        await self.db_manager.disconnect()
        logger.info("supervisor_shutdown_complete")