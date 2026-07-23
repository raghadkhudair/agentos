from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any
from uuid import UUID

import ray
import structlog

from agentos.actors.bootstrap import BootstrapAgentActor
from agentos.actors.infrastructure import InfrastructureAgentActor, InfrastructurePlanner
from agentos.actors.reviewer import ReviewerAgentActor
from agentos.actors.safety_reviewer import SafetyReviewerAgentActor
from agentos.checkpoints.manager import CheckpointManagerActor, SummaryManagerActor
from agentos.config.loader import runtime_tuning, team_roles
from agentos.config.runtime import AgentResourceAllocation, ResourcePlanner, RuntimeConfig
from agentos.config.settings import Settings
from agentos.dod.evaluator import DoDEvaluatorActor
from agentos.execution.supervisor import ExecutionSupervisorActor
from agentos.memory.broker import MemoryBrokerActor
from agentos.messaging.events import Event, EventType
from agentos.messaging.outbox import OutboxDispatcherActor
from agentos.provider.gateway import ProviderGatewayActor, ProviderRegistry
from agentos.runtime.planning_context import build_planning_context
from agentos.runtime.team_plan import AgentRole, AgentSpec, TeamPlan, ValidatedTeamPlan
from agentos.runtime.trigger_engine import TriggerEngineActor
from agentos.storage.clients import (
    DragonflyClient,
    MilvusVectorClient,
    MinioObjectClient,
    MongoDocumentClient,
    PostgresClient,
)
from agentos.storage.repositories import (
    AgentRepository,
    DoDRepository,
    EventRepository,
    ProjectRepository,
    ProjectState,
    TaskRepository,
)
from agentos.watchdogs.runtime_watchdogs import (
    DeadlockWatchdog,
    DoDWatchdog,
    SafetyWatchdog,
    StagnationWatchdog,
)

logger = structlog.get_logger()


@ray.remote(num_cpus=0.2, max_concurrency=16)  # type: ignore[call-overload]
class RuntimeSupervisorActor:
    """Owns lifecycle and policy while independent workers own their local state."""

    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.db = PostgresClient(self.settings)
        self.projects = ProjectRepository(self.db)
        self.events = EventRepository(self.db)
        self.dragonfly = DragonflyClient(self.settings)
        self._actor_registry: dict[str, dict[str, Any]] = {}
        self._service_registry: dict[str, dict[str, Any]] = {}
        self._completion_events: dict[str, asyncio.Event] = {}
        self._background: list[asyncio.Future[Any]] = []
        self.running_projects: set[str] = set()
        self._restart_counts: dict[str, int] = {}
        self.dod_watchdog = DoDWatchdog(self.db)
        self.stagnation_watchdog = StagnationWatchdog(self.db)
        self.deadlock_watchdog = DeadlockWatchdog(self.db)
        self.safety_watchdog = SafetyWatchdog(self.db, self.settings)
        self.tuning = runtime_tuning()

    @staticmethod
    def _suffix(project_id: str) -> str:
        return project_id.replace("-", "")[:12]

    def _service_names(self, project_id: str) -> dict[str, str]:
        suffix = self._suffix(project_id)
        return {
            key: f"{key}-{suffix}"
            for key in (
                "provider",
                "memory",
                "execution",
                "checkpoints",
                "summaries",
                "trigger",
                "reviewer",
                "safety",
                "dod",
                "infrastructure",
                "outbox",
            )
        }

    async def dependency_health(self) -> dict[str, Any]:
        postgres = self.db
        dragonfly = DragonflyClient(self.settings)
        mongodb = MongoDocumentClient(self.settings)
        minio = MinioObjectClient(self.settings)
        milvus = MilvusVectorClient(self.settings)
        initializers = {
            "postgres": postgres.initialize_schema(),
            "mongodb": mongodb.initialize(),
            "minio": minio.initialize(),
            "milvus": milvus.initialize(),
        }
        initialization_results = await asyncio.gather(
            *initializers.values(), return_exceptions=True
        )
        initialization_failures = {
            name: result
            for name, result in zip(initializers, initialization_results, strict=True)
            if isinstance(result, Exception)
        }
        clients = {
            "postgres": postgres.healthcheck(),
            "dragonfly": dragonfly.healthcheck(),
            "mongodb": mongodb.healthcheck(),
            "minio": minio.healthcheck(),
            "milvus": milvus.healthcheck(),
        }
        results = await asyncio.gather(*clients.values(), return_exceptions=True)
        report: dict[str, Any] = {}
        for name, result in zip(clients, results, strict=True):
            failure = initialization_failures.get(name)
            report[name] = (
                {"service": name, "healthy": False, "error": type(failure).__name__}
                if failure is not None
                else (
                    {"service": name, "healthy": False, "error": type(result).__name__}
                    if isinstance(result, Exception)
                    else result
                )
            )
        await dragonfly.close()
        await mongodb.close()
        report["providers"] = {
            provider_id: profile.available()
            for provider_id, profile in ProviderRegistry(settings=self.settings).profiles.items()
        }
        report["healthy"] = all(
            bool(value.get("healthy"))
            for key, value in report.items()
            if key not in {"providers", "healthy"}
        )
        return report

    def _actor(
        self,
        actor_class: Any,
        name: str,
        *args: Any,
        num_cpus: float = 0.05,
        memory: int | None = None,
        max_concurrency: int | None = None,
        runtime_env: dict[str, Any] | None = None,
    ) -> Any:
        try:
            return ray.get_actor(name, namespace="agentos")
        except ValueError:
            options: dict[str, Any] = {
                "name": name,
                "namespace": "agentos",
                "get_if_exists": True,
                "lifetime": "detached",
                "num_cpus": num_cpus,
                "max_restarts": 3,
                "max_task_retries": 2,
            }
            if memory is not None:
                options["memory"] = memory
            if max_concurrency is not None:
                options["max_concurrency"] = max_concurrency
            if runtime_env is not None:
                options["runtime_env"] = runtime_env
            return actor_class.options(
                **options,
            ).remote(*args)

    async def _start_services(self, project_id: str) -> tuple[dict[str, str], dict[str, Any]]:
        names = self._service_names(project_id)
        payload = self.settings.model_dump(mode="python")
        planner = ResourcePlanner(self.settings)
        envelope = planner.build_envelope()
        system_cpu = planner.service_cpu(len(names), envelope)
        system_memory = max(16_777_216, envelope.system_memory_bytes // len(names))
        runtime_env = {"env_vars": planner.thread_environment()}
        handles: dict[str, Any] = {}
        handles["provider"] = self._actor(
            ProviderGatewayActor,
            names["provider"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["memory"] = self._actor(
            MemoryBrokerActor,
            names["memory"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["execution"] = self._actor(
            ExecutionSupervisorActor,
            names["execution"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["checkpoints"] = self._actor(
            CheckpointManagerActor,
            names["checkpoints"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["summaries"] = self._actor(
            SummaryManagerActor,
            names["summaries"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["trigger"] = self._actor(
            TriggerEngineActor,
            names["trigger"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["reviewer"] = self._actor(
            ReviewerAgentActor,
            names["reviewer"],
            payload,
            names["provider"],
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["safety"] = self._actor(
            SafetyReviewerAgentActor,
            names["safety"],
            payload,
            names["provider"],
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["dod"] = self._actor(
            DoDEvaluatorActor,
            names["dod"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["infrastructure"] = self._actor(
            InfrastructureAgentActor,
            names["infrastructure"],
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        handles["outbox"] = self._actor(
            OutboxDispatcherActor,
            names["outbox"],
            project_id,
            payload,
            num_cpus=system_cpu,
            memory=system_memory,
            runtime_env=runtime_env,
        )
        self._service_registry[project_id] = handles
        self._background.append(asyncio.ensure_future(handles["outbox"].run.remote()))
        self._background.append(
            asyncio.ensure_future(handles["trigger"].start_routing_loop.remote(project_id))
        )
        return names, handles

    async def bootstrap_project(
        self, user_request: str, *, wait_for_completion: bool = False
    ) -> dict[str, Any]:
        if self.settings.environment == "production":
            self.settings.validate_production_secrets()
        await self.db.connect()
        await self.db.initialize_schema()
        active_projects = await self.db.fetchval(
            "SELECT count(*) FROM projects WHERE status IN ('TEAM_FORMING','RUNNING','REPLANNING','INTEGRATING','VERIFYING')"
        )
        if int(active_projects or 0) > 0:
            raise RuntimeError(
                "another project already owns the single-host resource envelope; pause or finish it first"
            )
        health = await self.dependency_health()
        if not health["healthy"] and self.settings.dependency_health_fail_closed:
            raise RuntimeError("required storage dependency health check failed")

        project_id = await self.projects.create_project(
            self.settings.project_name,
            user_request,
            [],
        )
        await self.projects.update_status(project_id, "PLANNING")
        try:
            planning_context = build_planning_context(self.settings.source_repository, user_request)
        except Exception as error:
            await self._record_planning_blocker(project_id, error)
            raise
        try:
            names, services = await self._start_services(project_id)
        except Exception as error:
            await self._record_planning_blocker(project_id, error)
            raise
        bootstrap_name = f"bootstrap-{self._suffix(project_id)}"
        bootstrap = self._actor(
            BootstrapAgentActor,
            bootstrap_name,
            project_id,
            self.settings.model_dump(mode="python"),
            names["provider"],
            num_cpus=0.05,
        )
        try:
            try:
                raw_plan = await bootstrap.create_team_plan.remote(
                    user_request, self.settings.max_agents_total, planning_context
                )
                plan = TeamPlan.model_validate(raw_plan)
                validated = self.validate_team_plan(plan)
                runtime_payload = await services["infrastructure"].determine_resources.remote(
                    project_id, [spec.model_dump(mode="json") for spec in validated.agents]
                )
                runtime_config = RuntimeConfig.model_validate(runtime_payload)
                validated.resource_allocations = runtime_config.allocations
                persisted = await self.projects.persist_plan_bundle(
                    project_id,
                    plan,
                    validated.agents,
                    runtime_config,
                    self.settings.safe_snapshot(),
                    planning_context,
                )
            except Exception as error:
                await self._record_planning_blocker(project_id, error)
                await self._stop_project_runtime(project_id)
                raise
        finally:
            ray.kill(bootstrap, no_restart=True)
        resource_plan = {"resource_plan_id": persisted["resource_plan_id"], **runtime_payload}
        await services["infrastructure"].announce_resources.remote(
            project_id, persisted["resource_plan_id"], runtime_payload
        )
        self._completion_events[project_id] = asyncio.Event()
        provider_ready = any(health["providers"].values())
        actors: list[dict[str, Any]] = []
        if provider_ready:
            await self.projects.update_status(project_id, "TEAM_FORMING")
            actors = await self.create_agent_actors(
                validated.agents,
                project_id,
                runtime_config,
                names,
            )
            await self.projects.update_status(project_id, "RUNNING")
            self.running_projects.add(project_id)
            self._background.append(asyncio.ensure_future(self._watchdog_loop(project_id)))
            self._background.append(asyncio.ensure_future(self._health_loop(project_id)))
        else:
            await self.projects.update_status(project_id, "BLOCKED_REQUIRES_INPUT")

        created = Event(
            project_id=project_id,
            event_type=EventType.PROJECT_CREATED,
            producer_agent_id="runtime_supervisor",
            payload={
                "request": user_request,
                "dod": [item.model_dump(mode="json") for item in plan.dod],
                "resource_plan_id": resource_plan["resource_plan_id"],
                "contract_version": plan.contract_version,
                "contract_hash": plan.contract_hash,
                "source_revision": plan.source_revision,
            },
        )
        await self.events.save_event(project_id, created)
        result = {
            "project_id": project_id,
            "project_name": plan.project_name,
            "team_plan": validated.model_dump(mode="json"),
            "resource_plan": resource_plan,
            "agents": actors,
            "dependency_health": health,
            "provider_ready": provider_ready,
            "status": "RUNNING" if provider_ready else "BLOCKED_REQUIRES_INPUT",
        }
        if wait_for_completion and provider_ready:
            await self._completion_events[project_id].wait()
            final_project = await self.projects.get(project_id)
            result["status"] = (
                final_project["status"] if final_project else "BLOCKED_REQUIRES_INPUT"
            )
        return result

    async def plan_project(self, user_request: str) -> dict[str, Any]:
        """Persist and return a validated plan without launching worker agents."""
        if self.settings.environment == "production":
            self.settings.validate_production_secrets()
        await self.db.connect()
        await self.db.initialize_schema()
        project_id = await self.projects.create_project(
            self.settings.project_name, user_request, []
        )
        await self.projects.update_status(project_id, "PLANNING")
        try:
            planning_context = build_planning_context(self.settings.source_repository, user_request)
        except Exception as error:
            await self._record_planning_blocker(project_id, error)
            raise
        suffix = self._suffix(project_id)
        provider_name = f"plan-provider-{suffix}"
        bootstrap_name = f"plan-bootstrap-{suffix}"
        payload = self.settings.model_dump(mode="python")
        provider = self._actor(ProviderGatewayActor, provider_name, payload, num_cpus=0.05)
        bootstrap = self._actor(
            BootstrapAgentActor,
            bootstrap_name,
            project_id,
            payload,
            provider_name,
            num_cpus=0.05,
        )
        try:
            try:
                raw = await bootstrap.create_team_plan.remote(
                    user_request, self.settings.max_agents_total, planning_context
                )
                plan = TeamPlan.model_validate(raw)
                validated = self.validate_team_plan(plan)
                runtime = InfrastructurePlanner(self.settings).plan(validated.agents)
                validated.resource_allocations = runtime.allocations
                persisted = await self.projects.persist_plan_bundle(
                    project_id,
                    plan,
                    validated.agents,
                    runtime,
                    self.settings.safe_snapshot(),
                    planning_context,
                )
                return {
                    "project_id": project_id,
                    "team_plan": validated.model_dump(mode="json"),
                    "runtime_config": runtime.model_dump(mode="json"),
                    "resource_plan_id": persisted["resource_plan_id"],
                    "runtime_snapshot_id": persisted["runtime_snapshot_id"],
                    "contract_hash": plan.contract_hash,
                }
            except Exception as error:
                await self._record_planning_blocker(project_id, error)
                raise
        finally:
            ray.kill(bootstrap, no_restart=True)
            ray.kill(provider, no_restart=True)

    async def _record_planning_blocker(self, project_id: str, error: Exception) -> None:
        await self.projects.update_status(project_id, ProjectState.BLOCKED_REQUIRES_INPUT)
        await self.events.save_event(
            project_id,
            Event(
                project_id=project_id,
                event_type=EventType.BLOCKER_CREATED,
                producer_agent_id="runtime_supervisor",
                payload={
                    "stage": "planning",
                    "error_type": type(error).__name__,
                    "message": str(error)[:4000],
                    "fail_closed": True,
                },
            ),
        )

    def validate_team_plan(self, plan: TeamPlan) -> ValidatedTeamPlan:
        config = team_roles()
        caps = config["team_constraints"].get("role_caps", {})
        mandatory = [AgentRole[item] for item in config["team_constraints"]["mandatory_roles"]]
        templates = {AgentRole[item["role"]]: item for item in config["roles"]}
        by_role: dict[AgentRole, AgentSpec] = {}
        reasons: list[str] = []
        for spec in plan.agents:
            template = templates[spec.role]
            hardened = spec.model_copy(
                update={
                    "memory_scopes": list(template["default_memory_scopes"]),
                    "allowed_action_categories": list(template["allowed_action_categories"]),
                    "event_subscriptions": list(template["default_event_subscriptions"]),
                    "provider_preferences": list(template["provider_preferences"]),
                    "collaboration_interval_seconds": self.settings.collaboration_interval_seconds,
                },
                deep=True,
            )
            existing = by_role.get(spec.role)
            if existing:
                existing.count += hardened.count
                reasons.append(f"merged duplicate role {spec.role.value}")
            else:
                by_role[spec.role] = hardened
        for role in mandatory:
            if role in by_role:
                continue
            template = templates[role]
            by_role[role] = AgentSpec(
                role=role,
                count=1,
                description=template["description"],
                memory_scopes=template["default_memory_scopes"],
                allowed_action_categories=template["allowed_action_categories"],
                ownership_domains=[role.value],
                event_subscriptions=template["default_event_subscriptions"],
                provider_preferences=template["provider_preferences"],
                collaboration_interval_seconds=self.settings.collaboration_interval_seconds,
            )
            reasons.append(f"injected mandatory role {role.value}")
        for role, spec in by_role.items():
            cap = int(caps.get(role.name, spec.count))
            if spec.count > cap:
                spec.count = cap
                reasons.append(f"capped {role.value} at {cap}")

        agents = list(by_role.values())
        total = sum(spec.count for spec in agents)
        if total > self.settings.max_agents_total:
            task_roles = {
                task.owner_role for task in plan.initial_backlog if task.owner_role is not None
            }
            reducible_roles = list(reversed(agents))
            over = total - self.settings.max_agents_total
            for spec in reducible_roles:
                minimum = 1 if spec.role in mandatory or spec.role in task_roles else 0
                reducible = min(over, max(0, spec.count - minimum))
                spec.count -= reducible
                over -= reducible
                if over == 0:
                    break
            agents = [spec for spec in agents if spec.count > 0]
            total = sum(spec.count for spec in agents)
            reasons.append("reduced optional roles to the configured team cap")
        if total > self.settings.max_agents_total:
            raise ValueError(
                "configured max agent count cannot fit mandatory and task-owning roles"
            )
        present_roles = {spec.role for spec in agents}
        missing_task_roles = {
            task.owner_role
            for task in plan.initial_backlog
            if task.owner_role is not None and task.owner_role not in present_roles
        }
        if missing_task_roles:
            raise ValueError(
                f"team reduction removed task-owning roles: {sorted(role.value for role in missing_task_roles)}"
            )
        hardened_payload = plan.model_dump(mode="json")
        hardened_payload["agents"] = [spec.model_dump(mode="json") for spec in agents]
        TeamPlan.model_validate(hardened_payload)
        envelope = ResourcePlanner(self.settings).build_envelope()
        return ValidatedTeamPlan(
            original=plan,
            agents=agents,
            total_agents=total,
            max_active_agents=envelope.max_active_agents,
            max_parallel_code_tasks=self.settings.max_parallel_code_tasks,
            reduced=bool(reasons),
            reduction_reason="; ".join(reasons) or None,
        )

    async def create_agent_actors(
        self,
        specs: Iterable[AgentSpec],
        project_id: str,
        runtime_config: RuntimeConfig,
        service_names: dict[str, str],
    ) -> list[dict[str, Any]]:
        from agentos.actors.base import AgentWorkerActor

        allocations = {item.agent_id: item for item in runtime_config.allocations}
        settings_payload = self.settings.model_dump(mode="python")
        created: list[dict[str, Any]] = []
        for spec in specs:
            if spec.role is AgentRole.INFRASTRUCTURE_AGENT:
                continue
            for index in range(1, spec.count + 1):
                agent_id = f"{spec.role.value}-{index}"
                allocation = allocations[agent_id]
                actor_name = f"{agent_id}-{self._suffix(project_id)}"
                spec_payload = {
                    "squad": spec.role.value.split("_", 1)[0],
                    "memory_scopes": spec.memory_scopes,
                    "allowed_actions": spec.allowed_action_categories,
                    "ownership_domains": spec.ownership_domains,
                    "event_subscriptions": spec.event_subscriptions,
                    "provider_assignment": {
                        "provider": allocation.provider,
                        "model": allocation.model,
                        "model_routes": {
                            key.value: value for key, value in allocation.model_routes.items()
                        },
                    },
                    "resource_allocation": allocation.model_dump(mode="json"),
                    "runtime_limits": {
                        "max_active_agents": runtime_config.envelope.max_active_agents,
                        "max_parallel_code_tasks": min(
                            self.settings.max_parallel_code_tasks,
                            runtime_config.envelope.max_active_agents,
                        ),
                    },
                }
                actor = self._actor(
                    AgentWorkerActor,
                    actor_name,
                    agent_id,
                    spec.role.value,
                    project_id,
                    settings_payload,
                    spec_payload,
                    service_names,
                    num_cpus=allocation.cpu_cores,
                    memory=allocation.memory_bytes,
                    max_concurrency=allocation.max_concurrency,
                    runtime_env={"env_vars": runtime_config.thread_environment},
                )
                started = await actor.start.remote()
                self._actor_registry[f"{project_id}:{agent_id}"] = {
                    "handle": actor,
                    "actor_name": actor_name,
                    "spec": spec,
                }
                created.append(started)
        return created

    async def _watchdog_loop(self, project_id: str) -> None:
        services = self._service_registry[project_id]
        while project_id in self.running_projects:
            await asyncio.sleep(float(self.tuning["watchdog_loop"]["interval_seconds"]))
            try:
                health = await self.dependency_health()
                if not health["healthy"]:
                    await self.projects.update_status(project_id, "BLOCKED_REQUIRES_INPUT")
                    await self._suspend_workers(project_id)
                    self.running_projects.discard(project_id)
                    self._completion_events[project_id].set()
                    return
                project = await self.projects.get(project_id)
                if project and project["status"] == "REPLANNING":
                    runnable = await TaskRepository(self.db).get_runnable_tasks(project_id)
                    if runnable:
                        await self.projects.update_status(project_id, "RUNNING")
                evaluation = await services["dod"].evaluate.remote(project_id)
                await self.db.execute(
                    "UPDATE projects SET evaluation_failure_count=0 WHERE id=$1",
                    UUID(project_id),
                )
                dod_action = await self.handle_dod_evaluation(project_id, evaluation)
                if dod_action.get("finalized") or dod_action.get("action_required") == "BLOCK":
                    return
                stagnation = await self.stagnation_watchdog.inspect_and_act(project_id)
                deadlock = await self.deadlock_watchdog.inspect_and_act(project_id)
                await self.safety_watchdog.inspect_and_act(project_id)
                if (
                    stagnation.get("action_required") == "REPLAN"
                    and dod_action.get("action_required") == "NONE"
                ):
                    await self.dod_watchdog.inspect_and_act(
                        project_id,
                        False,
                        list(evaluation["gaps"]),
                        evaluation_run_id=evaluation["evaluation_run_id"],
                    )
                if deadlock.get("action_required") == "RESOLVE_DEADLOCK":
                    await self.projects.update_status(project_id, "BLOCKED_REQUIRES_INPUT")
                    await self._suspend_workers(project_id)
                    self.running_projects.discard(project_id)
                    self._completion_events[project_id].set()
                    return
                if dod_action.get("action_required") == "TRIGGER_REPLANNING":
                    await self.projects.update_status(project_id, "REPLANNING")
            except Exception as error:
                logger.error(
                    "watchdog_loop_failed", project_id=project_id, error_type=type(error).__name__
                )
                failures = await self.db.fetchval(
                    """
                    UPDATE projects SET evaluation_failure_count=evaluation_failure_count+1
                    WHERE id=$1 RETURNING evaluation_failure_count
                    """,
                    UUID(project_id),
                )
                maximum = int(self.tuning["dod"]["max_replan_attempts"])
                if int(failures or 0) >= maximum:
                    await self.projects.update_status(project_id, "BLOCKED_REQUIRES_INPUT")
                    await self.events.save_event(
                        project_id,
                        Event(
                            project_id=project_id,
                            event_type=EventType.BLOCKER_CREATED,
                            producer_agent_id="runtime_supervisor",
                            payload={
                                "stage": "dod_evaluation",
                                "reason": "bounded_evaluation_failures_exhausted",
                                "attempts": int(failures),
                                "error_type": type(error).__name__,
                            },
                        ),
                    )
                    await self._suspend_workers(project_id)
                    self.running_projects.discard(project_id)
                    self._completion_events[project_id].set()
                    return

    async def handle_dod_evaluation(
        self, project_id: str, evaluation: dict[str, Any]
    ) -> dict[str, Any]:
        """Apply one durable evaluation event; periodic watchdog calls are recovery."""

        if evaluation["satisfied"]:
            finalized = await DoDRepository(self.db).finalize_project(
                project_id, evaluation["evaluation_run_id"]
            )
            if finalized:
                await self._suspend_workers(project_id)
                self.running_projects.discard(project_id)
                completion = self._completion_events.get(project_id)
                if completion:
                    completion.set()
            return {"finalized": finalized, "action_required": "NONE"}
        action = await self.dod_watchdog.inspect_and_act(
            project_id,
            False,
            list(evaluation["gaps"]),
            evaluation_run_id=evaluation["evaluation_run_id"],
        )
        if action.get("action_required") == "BLOCK":
            await self._suspend_workers(project_id)
            self.running_projects.discard(project_id)
            completion = self._completion_events.get(project_id)
            if completion:
                completion.set()
        return action

    async def _health_loop(self, project_id: str) -> None:
        services = self._service_registry[project_id]
        while project_id in self.running_projects:
            await asyncio.sleep(15)
            try:
                pressure = await services["infrastructure"].inspect_pressure.remote(project_id)
                if pressure["pressure"] == "HIGH":
                    await self.dragonfly.redis.set(
                        self.dragonfly.key("project", project_id, "claims_paused"),
                        "resource_pressure",
                        ex=30,
                    )
                    await self.events.save_event(
                        project_id,
                        Event(
                            project_id=project_id,
                            event_type=EventType.RESOURCE_PLAN_UPDATED,
                            producer_agent_id="runtime_supervisor",
                            payload={"action": "throttle_new_work", **pressure},
                        ),
                    )
                else:
                    await self.dragonfly.redis.delete(
                        self.dragonfly.key("project", project_id, "claims_paused")
                    )

                for service_name, method_name in (
                    ("outbox", "run"),
                    ("trigger", "start_routing_loop"),
                ):
                    service = services[service_name]
                    status = await asyncio.wait_for(service.status.remote(), timeout=5)
                    if not status.get("running", False):
                        method = getattr(service, method_name)
                        arguments = () if service_name == "outbox" else (project_id,)
                        self._background.append(asyncio.ensure_future(method.remote(*arguments)))

                exhausted: list[str] = []
                for key, data in list(self._actor_registry.items()):
                    if not key.startswith(f"{project_id}:"):
                        continue
                    try:
                        snapshot = await asyncio.wait_for(
                            data["handle"].snapshot.remote(), timeout=5
                        )
                        if not snapshot.get("running", False):
                            await asyncio.wait_for(data["handle"].start.remote(), timeout=30)
                        self._restart_counts[key] = 0
                    except Exception as actor_error:
                        attempts = self._restart_counts.get(key, 0) + 1
                        self._restart_counts[key] = attempts
                        logger.warning(
                            "worker_recovery_attempt_failed",
                            project_id=project_id,
                            actor=key,
                            attempt=attempts,
                            error_type=type(actor_error).__name__,
                        )
                        if attempts >= 3:
                            exhausted.append(key)
                if exhausted:
                    await self.projects.update_status(project_id, "BLOCKED_REQUIRES_INPUT")
                    await self._suspend_workers(project_id)
                    self.running_projects.discard(project_id)
                    self._completion_events[project_id].set()
                    return
            except Exception as error:
                logger.error(
                    "health_loop_failed", project_id=project_id, error_type=type(error).__name__
                )

    async def _suspend_workers(self, project_id: str) -> None:
        for key, data in list(self._actor_registry.items()):
            if not key.startswith(f"{project_id}:"):
                continue
            try:
                await asyncio.wait_for(data["handle"].stop.remote(), timeout=15)
            except Exception as error:
                logger.warning(
                    "worker_suspend_failed",
                    actor=key,
                    error_type=type(error).__name__,
                )

    async def resume_project(self, project_id: str) -> dict[str, Any]:
        project = await self.projects.get(project_id)
        if project is None:
            raise LookupError("project not found")
        if project["status"] in {state.value for state in ProjectRepository.TERMINAL_STATES}:
            raise ValueError("terminal projects cannot be resumed")
        if project_id in self.running_projects:
            return {"project_id": project_id, "status": "RUNNING", "already_running": True}
        active_projects = await self.db.fetchval(
            """
            SELECT count(*) FROM projects
            WHERE id<>$1 AND status IN (
              'TEAM_FORMING','RUNNING','REPLANNING','INTEGRATING','VERIFYING'
            )
            """,
            UUID(project_id),
        )
        if int(active_projects or 0) > 0:
            raise RuntimeError(
                "another project already owns the single-host resource envelope; pause or finish it first"
            )
        health = await self.dependency_health()
        if not health["healthy"]:
            raise RuntimeError("required storage dependency health check failed")
        if not any(health["providers"].values()):
            await self.projects.update_status(project_id, "BLOCKED_REQUIRES_INPUT")
            raise RuntimeError("no configured AI provider is available")
        names, services = await self._start_services(project_id)
        rows = await AgentRepository(self.db).list_project_agents(project_id)
        snapshot = await self.db.fetchval(
            "SELECT public_config FROM runtime_config_snapshots WHERE project_id=$1 ORDER BY created_at DESC LIMIT 1",
            UUID(project_id),
        )
        if not isinstance(snapshot, dict) or "generated_runtime" not in snapshot:
            raise RuntimeError("persisted generated runtime configuration is missing")
        runtime_config = RuntimeConfig.model_validate(snapshot["generated_runtime"])
        from agentos.actors.base import AgentWorkerActor

        await self.db.execute(
            """
            UPDATE tasks SET status='PENDING',owner_agent_id=NULL,lease_expires_at=NULL
            WHERE project_id=$1 AND status IN ('CLAIMED','IN_PROGRESS','UNDER_REVIEW')
            """,
            UUID(project_id),
        )
        created: list[dict[str, Any]] = []
        settings_payload = self.settings.model_dump(mode="python")
        for row in rows:
            if row["role"] == AgentRole.INFRASTRUCTURE_AGENT.value:
                continue
            permissions = row["permissions"] or {}
            allocation = AgentResourceAllocation.model_validate(row["resource_allocation"])
            spec_payload = {
                "squad": row["squad"],
                "memory_scopes": list(row["memory_scopes"] or []),
                "allowed_actions": list(permissions.get("allowed_actions", [])),
                "ownership_domains": list(permissions.get("ownership_domains", [])),
                "event_subscriptions": list(permissions.get("event_subscriptions", [])),
                "provider_assignment": row["provider_assignment"] or {},
                "resource_allocation": row["resource_allocation"] or {},
                "runtime_limits": {
                    "max_active_agents": runtime_config.envelope.max_active_agents,
                    "max_parallel_code_tasks": min(
                        self.settings.max_parallel_code_tasks,
                        runtime_config.envelope.max_active_agents,
                    ),
                },
            }
            actor_name = f"{row['id']}-{self._suffix(project_id)}"
            actor = self._actor(
                AgentWorkerActor,
                actor_name,
                row["id"],
                row["role"],
                project_id,
                settings_payload,
                spec_payload,
                names,
                num_cpus=allocation.cpu_cores,
                memory=allocation.memory_bytes,
                max_concurrency=allocation.max_concurrency,
                runtime_env={"env_vars": runtime_config.thread_environment},
            )
            created.append(await actor.start.remote())
            self._actor_registry[f"{project_id}:{row['id']}"] = {
                "handle": actor,
                "actor_name": actor_name,
            }
        await self.projects.update_status(project_id, "RUNNING")
        self.running_projects.add(project_id)
        self._completion_events[project_id] = asyncio.Event()
        recovery_evaluation = await services["dod"].evaluate.remote(project_id)
        recovery_action = await self.handle_dod_evaluation(project_id, recovery_evaluation)
        if project_id in self.running_projects:
            self._background.append(asyncio.ensure_future(self._watchdog_loop(project_id)))
            self._background.append(asyncio.ensure_future(self._health_loop(project_id)))
        await self.events.save_event(
            project_id,
            Event(
                project_id=project_id,
                event_type=EventType.AGENT_HEALTH_CHANGED,
                producer_agent_id="runtime_supervisor",
                payload={
                    "message": "Project resumed from durable agent, task, and DoD generations.",
                    "evaluation_run_id": recovery_evaluation["evaluation_run_id"],
                    "recovery_action": recovery_action,
                },
            ),
        )
        resumed = await self.projects.get(project_id)
        return {
            "project_id": project_id,
            "status": resumed["status"] if resumed else "BLOCKED_REQUIRES_INPUT",
            "agents": created,
            "recovery_evaluation": recovery_evaluation,
            "recovery_action": recovery_action,
        }

    async def _stop_project_runtime(self, project_id: str) -> None:
        self.running_projects.discard(project_id)
        for key, data in list(self._actor_registry.items()):
            if key.startswith(f"{project_id}:"):
                try:
                    await data["handle"].stop.remote()
                finally:
                    ray.kill(data["handle"], no_restart=True)
                self._actor_registry.pop(key, None)
        services = self._service_registry.pop(project_id, {})
        for handle in services.values():
            ray.kill(handle, no_restart=True)

    async def pause_project(self, project_id: str) -> dict[str, Any]:
        await self.db.execute(
            """
            UPDATE tasks SET status='PENDING',owner_agent_id=NULL,lease_expires_at=NULL
            WHERE project_id=$1 AND status IN ('CLAIMED','IN_PROGRESS','UNDER_REVIEW')
            """,
            UUID(project_id),
        )
        await self.projects.update_status(project_id, "PAUSED")
        await self._stop_project_runtime(project_id)
        return {"project_id": project_id, "status": "PAUSED"}

    async def evaluate_project(self, project_id: str) -> dict[str, Any]:
        """Run the canonical evaluator on demand and finalize only its exact snapshot."""

        await self.db.connect()
        project = await self.projects.get(project_id)
        if project is None:
            raise LookupError("project not found")
        temporary = False
        if project_id in self._service_registry:
            evaluator = self._service_registry[project_id]["dod"]
        else:
            name = f"manual-dod-{self._suffix(project_id)}"
            evaluator = self._actor(
                DoDEvaluatorActor,
                name,
                self.settings.model_dump(mode="python"),
                num_cpus=0.05,
            )
            temporary = True
        try:
            evaluation = await evaluator.evaluate.remote(project_id)
            finalized = False
            if evaluation["satisfied"]:
                finalized = await DoDRepository(self.db).finalize_project(
                    project_id, evaluation["evaluation_run_id"]
                )
                if finalized and project_id in self.running_projects:
                    await self._suspend_workers(project_id)
                    self.running_projects.discard(project_id)
                    completion = self._completion_events.get(project_id)
                    if completion:
                        completion.set()
            return {**evaluation, "finalized": finalized}
        finally:
            if temporary:
                ray.kill(evaluator, no_restart=True)

    async def amend_dod_contract(
        self,
        project_id: str,
        plan_payload: dict[str, Any],
        reason: str,
        requested_by: str,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        plan = TeamPlan.model_validate(plan_payload)
        self.validate_team_plan(plan)
        if approval_id is None:
            request_id = await self.projects.request_dod_approval(
                project_id,
                "DOD_AMENDMENT",
                {"contract_hash": plan.contract_hash, "reason": reason},
                requested_by,
            )
            return {"status": "PENDING_APPROVAL", "approval_id": request_id}
        result = await self.projects.amend_dod_contract(
            project_id, plan, approval_id, reason, requested_by
        )
        await self.events.save_event(
            project_id,
            Event(
                project_id=project_id,
                event_type=EventType.CONTRACT_CHANGE,
                producer_agent_id="runtime_supervisor",
                payload={**result, "reason": reason, "approval_id": approval_id},
            ),
        )
        return {"status": "AMENDED", **result}

    async def waive_dod_criterion(
        self,
        project_id: str,
        criterion_id: str,
        reason: str,
        requested_by: str,
        approval_id: str | None = None,
    ) -> dict[str, Any]:
        criterion = await self.db.fetchrow(
            """
            SELECT criterion_hash FROM dod_checks
            WHERE project_id=$1 AND criterion_id=$2 AND active
            """,
            UUID(project_id),
            criterion_id,
        )
        if criterion is None:
            raise LookupError("active criterion not found")
        if approval_id is None:
            request_id = await self.projects.request_dod_approval(
                project_id,
                "DOD_WAIVER",
                {
                    "criterion_id": criterion_id,
                    "criterion_hash": criterion["criterion_hash"],
                    "reason": reason,
                },
                requested_by,
            )
            return {"status": "PENDING_APPROVAL", "approval_id": request_id}
        await self.projects.waive_dod_criterion(project_id, criterion_id, approval_id, reason)
        return {
            "status": "WAIVED_BY_HUMAN",
            "project_id": project_id,
            "criterion_id": criterion_id,
            "approval_id": approval_id,
        }

    async def shutdown(self, project_id: str | None = None) -> None:
        projects = [project_id] if project_id else list(self.running_projects)
        for current in projects:
            await self._stop_project_runtime(current)
        await self.db.disconnect()

    async def stop_project_by_user(self, project_id: str) -> dict[str, Any]:
        await self.projects.update_status(project_id, "STOPPED_BY_USER")
        await self.shutdown(project_id)
        return {"project_id": project_id, "status": "STOPPED_BY_USER"}

    async def mark_failed_by_policy(self, project_id: str, reason: str) -> dict[str, Any]:
        await self.projects.update_status(project_id, "FAILED_BY_POLICY")
        await self.shutdown(project_id)
        return {"project_id": project_id, "status": "FAILED_BY_POLICY", "reason": reason}
