from __future__ import annotations

import asyncio
from collections.abc import Iterable
import uuid
import ray
import structlog

from agentos.actors.bootstrap import BootstrapAgentActor
from agentos.config.settings import Settings
from agentos.runtime.team_plan import AgentRole, AgentSpec, TeamPlan, ValidatedTeamPlan
from agentos.watchdogs.runtime_watchdogs import DoDWatchdog, StagnationWatchdog, SafetyWatchdog, DeadlockWatchdog

from agentos.actors.safety_reviewer import SafetyReviewerAgentActor
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProjectRepository, EventRepository, TaskRepository
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType
from agentos.runtime.trigger_engine import TriggerEngineActor
from agentos.memory.broker import MemoryBrokerActor
from agentos.provider.gateway import ProviderGatewayActor
from agentos.execution.supervisor import ExecutionSupervisorActor
from agentos.checkpoints.manager import CheckpointManagerActor, SummaryManagerActor
from agentos.dod.evaluator import DoDEvaluatorActor
from agentos.actors.reviewer import ReviewerAgentActor

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

        self.code_reviewer = ReviewerAgentActor.options(
            name="code_reviewer",
            namespace="agentos"
        ).remote(self.settings.model_dump(by_alias=False))

        self.safety_reviewer = SafetyReviewerAgentActor.options(
            name="safety_reviewer",
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
        
        await project_repo.update_status(db_project_id, "PLANNING")

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
        
        from agentos.storage.repositories import DoDRepository
        dod_repo = DoDRepository(self.db_manager)
        
        for criterion in validated.original.dod:
            await dod_repo.add_dod_check(db_project_id, criterion)

        await project_repo.update_status(db_project_id, "TEAM_FORMING")
        logger.info("spawning_ray_agent_actors", total_count=validated.total_agents)
        actors = await self.create_agent_actors(validated.agents, db_project_id)

        await project_repo.update_status(db_project_id, "RUNNING")

        for spec in validated.agents:
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                identity_payload = {
                    "agent_id": agent_id,
                    "role": spec.role.value,
                    "project_id": str(db_project_id),
                    "squad": spec.role.value.split("_")[0],
                    "memory_scopes": spec.memory_scopes,
                    "allowed_actions": spec.allowed_action_categories,
                    "allowed_paths": []
                }
                await self.memory_broker.register_agent_identity.remote(identity_payload)

        initial_backlog = raw_plan.get("initial_backlog", [])
        logger.info("seeding_initial_project_task_backlog", count=len(initial_backlog))
       
        task_repo = TaskRepository(self.db_manager)
        for task_data in initial_backlog:
            title = task_data["title"]
            description = task_data["description"]

            task_embedding = None
            try:
                text_to_embed = f"{title}: {description}"
                task_embedding = await self.provider_gateway.get_embedding.remote(
                    text_to_embed, str(db_project_id)
                )
            except Exception as e:
                logger.warning("failed_to_generate_bootstrap_task_embedding", error=str(e))

            await task_repo.create_task(
                project_id=db_project_id,
                title=title,
                description=description,
                priority=int(task_data.get("priority", 3)),
                allowed_paths=task_data.get("allowed_paths", []),
                blocked_paths=task_data.get("blocked_paths", []),
                expected_outputs=task_data.get("expected_outputs", []),
                risk_level=task_data.get("risk_level", "LOW"),
                embedding=task_embedding  
            )
                
                
        unified_stream_key = f"project:{db_project_id}:events"

        first_pm_identity = None
        for spec in validated.agents:
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                if spec.role.value == "pm_tech_lead" and not first_pm_identity:
                    first_pm_identity = agent_id
                
                for e_type_str in spec.event_subscriptions:
                    try:
                        # Extract Enum value as a raw string to match TriggerEngineActor expectations
                        e_type = EventType(e_type_str.upper())
                        await self.trigger_engine.register_subscription.remote(e_type.value, agent_id)
                    except ValueError:
                        logger.warning("invalid_event_type_proposed", event_type=e_type_str)

        for action_cat in spec.allowed_action_categories:
            if action_cat == "implement":
                await self.trigger_engine.register_allowed_producer.remote("TASK_COMPLETED", agent_id)
                await self.trigger_engine.register_allowed_producer.remote("TASK_CREATED", agent_id)
            elif action_cat == "review":
                await self.trigger_engine.register_allowed_producer.remote("REVIEW_REQUEST", agent_id)
                await self.trigger_engine.register_allowed_producer.remote("REVIEW_RESULT", agent_id)

        asyncio.create_task(self.trigger_engine.start_routing_loop.remote(db_project_id))
        asyncio.create_task(self.watchdog_loop(db_project_id, validated.original.dod))
        asyncio.create_task(self._supervise_health_loop(db_project_id))
        
        logger.info("background_daemons_activated", active_monitoring_stream=unified_stream_key)

        init_event = Event(
            project_id=db_project_id,
            event_type=EventType.PROJECT_CREATED,
            topic=f"project.{db_project_id}.events", 
            payload={"user_request": user_request, "dod": validated.original.dod}
        )
        
        target_stream_key = unified_stream_key
        
        await event_repo.save_event(db_project_id, init_event)
        await self.dragonfly.publish_event(target_stream_key, init_event)

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
        """Enforces robust safety rules and strict agent limits derived dynamically from yaml configuration assets."""
        import copy
        from agentos.config.loader import team_roles, runtime_tuning
        
        # Load the configuration data maps dynamically
        roles_cfg = team_roles()
        tuning_cfg = runtime_tuning()
        
        constraints = roles_cfg.get("team_constraints", {})
        role_caps = constraints.get("role_caps", {})
        safety_rules = constraints.get("mandatory_safety_additions", {})
        
        max_total = tuning_cfg["agent_limits"]["max_agents_total"]
        max_active = tuning_cfg["agent_limits"]["max_active_agents"]
        max_parallel_tasks = tuning_cfg.get("agent_limits", {}).get("max_parallel_code_tasks", 2)
        
        reasons = []
        validated_agents: list[AgentSpec] = []
        
        present_role_strings = {spec.role.value.upper() for spec in plan.agents}
        
        for spec in plan.agents:
            spec_copy = spec.model_copy()
            role_enum_str = spec_copy.role.value.upper()
            
            if role_enum_str in role_caps:
                allowed_cap = int(role_caps[role_enum_str])
                if spec_copy.count > allowed_cap:
                    spec_copy.count = allowed_cap
                    reasons.append(f"Capped role {role_enum_str} to configuration file maximum count of {allowed_cap}.")
                    
            validated_agents.append(spec_copy)

        trigger_roles = safety_rules.get("if_roles_present", [])
        inject_roles = safety_rules.get("inject_roles", [])
        
        # Check if any trigger roles (like BACKEND_DEVELOPER) are currently on the proposed roster
        if any(tr.upper() in present_role_strings for tr in trigger_roles):
            for role_to_inject in inject_roles:
                role_to_inject_upper = role_to_inject.upper()
                
                if role_to_inject_upper not in present_role_strings:
                    # Look up standard template mapping defaults defined in roles array block
                    template = next((r for r in roles_cfg.get("roles", []) if r["role"].upper() == role_to_inject_upper), {})
                    
                    validated_agents.append(
                        AgentSpec(
                            role=AgentRole[role_to_inject_upper] if hasattr(AgentRole, role_to_inject_upper) else AgentRole.PM_TECH_LEAD,
                            count=1,
                            description=f"Automatically injected via compliance configurations.",
                            memory_scopes=template.get("default_memory_scopes", ["project"]),
                            allowed_action_categories=template.get("allowed_action_categories", ["implement"]),
                            ownership_domains=[role_to_inject_lower := role_to_inject.lower()],
                            event_subscriptions=template.get("default_event_subscriptions", ["PROJECT_CREATED"])
                        )
                    )
                    reasons.append(f"Injected mandatory safety role compliance asset: {role_to_inject_upper}.")
                    present_role_strings.add(role_to_inject_upper)

        # Enforce Maximum Total Agent limits from tuning yaml
        running_total = sum(a.count for a in validated_agents)
        if running_total > max_total:
            reasons.append(f"Enforced tuning block limit: reduced total team sizing from {running_total} to cap boundary {max_total}.")
            final_agents: list[AgentSpec] = []
            remaining = max_total
            
            for spec in validated_agents:
                if remaining <= 0:
                    break
                count = min(spec.count, remaining)
                final_agents.append(spec.model_copy(update={"count": count}))
                remaining -= count
            validated_agents = final_agents
            running_total = sum(a.count for a in validated_agents)

        reduction_reason = "; ".join(reasons) if reasons else None
        
        return ValidatedTeamPlan(
            original=plan,
            agents=validated_agents,
            total_agents=running_total,
            max_active_agents=max_active,
            max_parallel_code_tasks=max_parallel_tasks,
            reduced=len(reasons) > 0,
            reduction_reason=reduction_reason
        )
    
    async def create_agent_actors(self, specs: Iterable[AgentSpec], project_id: str) -> list[dict]:
        """Spawns Ray agent actors and registers them for health monitoring."""
        from agentos.actors.base import AgentWorkerActor
        event_repo = EventRepository(self.db_manager)

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

    async def _supervise_health_loop(self) -> None:
        """Monitors agent process health and Dragonfly heartbeats."""
        while True:
            try:
                await self._ensure_connected()
                for agent_id in list(self._authenticated_identities.keys()):
                    heartbeat_key = f"agent:{agent_id}:heartbeat"
                    is_alive = await self.redis_client.get(heartbeat_key)

                    if not is_alive:
                        logger.warning("agent_heartbeat_missing_reclaiming_resources", agent_id=agent_id)
                        
                        async with self.db_manager.pool.acquire() as conn:
                            await conn.execute(
                                "UPDATE tasks SET status = 'PENDING' WHERE owner_agent_id = $1 AND status = 'IN_PROGRESS'",
                                agent_id
                            )
                        
                        await self.prune_worktree_resources(agent_id)

            except Exception as e:
                logger.error("health_supervision_loop_error", error=str(e))
                
            await asyncio.sleep(15)

    async def watchdog_loop(self, project_id: str, dod: list[str]) -> None:
        """Monitors system states, delegating DoD validation to the dedicated DoDEvaluatorActor."""
        event_repo = EventRepository(self.db_manager)
        project_repo = ProjectRepository(self.db_manager)
        dod_wd = DoDWatchdog(self.db_manager, self.settings.dragonfly_url)
        stag_wd = StagnationWatchdog(self.db_manager, self.settings.dragonfly_url)
        safety_wd = SafetyWatchdog(self.db_manager, self.settings.dragonfly_url)
        deadlock_wd = DeadlockWatchdog(self.db_manager, self.settings.dragonfly_url)
        
        while self.is_running:
            await asyncio.sleep(tuning_cfg["watchdog_loop"]["interval_seconds"])
            
            try:
                evaluation_result = await self.dod_evaluator.evaluate.remote(project_id, dod)
                if evaluation_result.get("satisfied", False):
                    logger.info("[WATCHDOG]: All Definition of Done (DoD) criteria successfully met!")
                    self._project_complete.set()

                    await project_repo.update_status(project_id, "DOD_SATISFIED")
                    return
                stall_result = await dod_wd.inspect_and_act(
                    project_id,
                    evaluation_result.get("satisfied", False),
                    evaluation_result.get("gaps", [])
                )
                if stall_result.get("action_required") == "TRIGGER_REPLANNING":
                    self._watchdog_cycle_count = getattr(self, "_watchdog_cycle_count", 0) + 1
                    if self._watchdog_cycle_count % 3 == 0:
                        try:
                            project_summary = await self.summary_manager.generate_project_summary.remote(project_id, self.provider_gateway)
                            from agentos.storage.repositories import SummaryRepository
                            summary_repo = SummaryRepository(self.db_manager)
                            await summary_repo.save_summary(project_id, "project", project_id, project_summary)

                            squads = {data["spec_payload"]["squad"] for data in self._actor_registry.values()}
                            for squad_name in squads:
                                squad_summary = await self.summary_manager.generate_squad_summary.remote(project_id, squad_name, self.provider_gateway)
                                await summary_repo.save_summary(project_id, "squad", squad_name, squad_summary)
                        except Exception as e:
                            logger.error("periodic_summary_generation_failed", error=str(e))
                            
            except Exception as e:
                logger.error("watchdog_dod_evaluation_failed", error=str(e))

            for wd, args in [(stag_wd, (project_id,)), (safety_wd, (project_id,)), (deadlock_wd, (project_id,))]:
                try:
                    result = await wd.inspect_and_act(*args)
                    if result.get("action_required") == "QUARANTINE_AGENT" and result.get("agent_id"):
                        from agentos.governance.policy_engine import PolicyEngine
                        policy_engine = PolicyEngine(self.settings)
                        policy_engine.quarantine_agent(result["agent_id"])
                        logger.critical("agent_quarantined_by_watchdog", agent_id=result["agent_id"], reason=result.get("reason"))
                except Exception as e:
                    logger.error("watchdog_inspection_failed", watchdog=wd.__class__.__name__, error=str(e))
                    
    async def shutdown(self):
        """Gracefully cleans up resources and shuts down the supervisor."""
        logger.info("initiating_supervisor_shutdown")
        self.is_running = False
        
        await self.trigger_engine.stop.remote()
        
        for agent_id, data in self._actor_registry.items():
            ray.kill(data["handle"])
            
        await self.db_manager.disconnect()
        logger.info("supervisor_shutdown_complete")

    async def stop_project_by_user(self, project_id: str) -> dict:
        """
        Manually halts project execution and transitions state to terminal STOPPED_BY_USER.
        Section 20 Terminal State Transition.
        """
        logger.info("project_stop_requested_by_user", project_id=project_id)
        
        from agentos.storage.repositories import ProjectRepository
        project_repo = ProjectRepository(self.db_manager)
        
        await project_repo.update_status(project_id, "STOPPED_BY_USER")
        
        await self.shutdown()
        
        return {"status": "SUCCESS", "project_id": project_id, "final_state": "STOPPED_BY_USER"}

    async def mark_failed_by_policy(self, project_id: str, reason: str) -> dict:
        """
        Halts project execution due to safety/policy violation and transitions state to terminal FAILED_BY_POLICY.
        Section 20 Terminal State Transition.
        """
        logger.critical("project_failed_by_policy_violation", project_id=project_id, reason=reason)
        
        from agentos.storage.repositories import ProjectRepository
        project_repo = ProjectRepository(self.db_manager)
        
        await project_repo.update_status(project_id, "FAILED_BY_POLICY")
        
        await self.shutdown()
        
        return {"status": "FAILED", "project_id": project_id, "final_state": "FAILED_BY_POLICY", "reason": reason}
