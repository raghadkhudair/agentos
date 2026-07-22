from __future__ import annotations

import asyncio
import fnmatch
import json
import re
import time
from typing import Any
from uuid import UUID

import ray
import structlog

from agentos.checkpoints.manager import Checkpoint
from agentos.config.loader import runtime_tuning
from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, AgentIdentity
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType
from agentos.provider.gateway import ProviderRequest
from agentos.storage.clients.mongodb import MongoDocumentClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import (
    AgentRepository,
    DoDRepository,
    EventRepository,
    TaskRepository,
)

logger = structlog.get_logger()


@ray.remote(max_restarts=5, max_task_retries=2)
class AgentWorkerActor:
    """Independent, project-scoped worker that collaborates only through governed services."""

    def __init__(
        self,
        agent_id: str,
        role: str,
        project_id: str,
        settings: dict[str, Any],
        spec_payload: dict[str, Any],
        service_names: dict[str, str],
    ):
        self.agent_id = agent_id
        self.role = role
        self.project_id = project_id
        self.settings = Settings(**settings)
        self.squad = str(spec_payload.get("squad", "engineering"))
        self.memory_scopes = list(spec_payload.get("memory_scopes", []))
        self.allowed_actions = list(spec_payload.get("allowed_actions", []))
        self.ownership_domains = list(spec_payload.get("ownership_domains", []))
        self.event_subscriptions = list(spec_payload.get("event_subscriptions", []))
        self.provider_assignment = dict(spec_payload.get("provider_assignment", {}))
        self.resource_allocation = dict(spec_payload.get("resource_allocation", {}))
        runtime_limits = dict(spec_payload.get("runtime_limits", {}))
        self.max_active_agents = int(
            runtime_limits.get("max_active_agents", self.settings.max_active_agents)
        )
        self.max_parallel_code_tasks = int(
            runtime_limits.get("max_parallel_code_tasks", self.settings.max_parallel_code_tasks)
        )
        self.service_names = service_names
        self.identity = AgentIdentity(
            agent_id=agent_id,
            role=role,
            project_id=project_id,
            squad=self.squad,
            memory_scopes=self.memory_scopes,
            allowed_actions=self.allowed_actions,
            ownership_domains=self.ownership_domains,
        )
        self.db = PostgresClient(self.settings)
        self.mongo = MongoDocumentClient(self.settings)
        self.agents = AgentRepository(self.db)
        self.tasks = TaskRepository(self.db)
        self.dod = DoDRepository(self.db)
        self.events = EventRepository(self.db)
        self.bus = DragonflyBus(self.settings)
        self.status = "STARTING"
        self.current_task_id: str | None = None
        self.running = False
        self.action_counter = 0
        self._processing_lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._background_tasks: list[asyncio.Task[Any]] = []
        self._inbox_consumer_name: str | None = None
        self._inbox_group: str | None = None
        self.inbox_tuning = runtime_tuning()["agent_inbox_loop"]

    _ACQUIRE_SLOT = """
    redis.call('zremrangebyscore', KEYS[1], '-inf', ARGV[1])
    if redis.call('zcard', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
    redis.call('zadd', KEYS[1], ARGV[3], ARGV[4])
    redis.call('expire', KEYS[1], tonumber(ARGV[5]))
    return 1
    """

    async def _acquire_capacity(self, category: str, limit: int, ttl_seconds: int = 900) -> bool:
        now = int(time.time())
        key = self.bus.client.key("capacity", self.project_id, category)
        return bool(
            await self.bus.redis.eval(
                self._ACQUIRE_SLOT,
                1,
                key,
                now,
                limit,
                now + ttl_seconds,
                self.agent_id,
                ttl_seconds,
            )
        )

    async def _release_capacity(self, category: str) -> None:
        await self.bus.redis.zrem(
            self.bus.client.key("capacity", self.project_id, category), self.agent_id
        )

    async def start(self) -> dict[str, Any]:
        async with self._start_lock:
            if self.running:
                return self.snapshot()
            return await self._start_once()

    async def _start_once(self) -> dict[str, Any]:
        self.provider = ray.get_actor(self.service_names["provider"], namespace="agentos")
        self.memory = ray.get_actor(self.service_names["memory"], namespace="agentos")
        self.execution = ray.get_actor(self.service_names["execution"], namespace="agentos")
        self.checkpoints = ray.get_actor(self.service_names["checkpoints"], namespace="agentos")
        self.summaries = ray.get_actor(self.service_names["summaries"], namespace="agentos")
        self.trigger = ray.get_actor(self.service_names["trigger"], namespace="agentos")
        self.reviewer = ray.get_actor(self.service_names["reviewer"], namespace="agentos")
        self.safety = ray.get_actor(self.service_names["safety"], namespace="agentos")
        await self.mongo.initialize()
        await self.agents.register_agent(
            self.agent_id,
            self.project_id,
            self.role,
            self.squad,
            memory_scopes=self.memory_scopes,
            permissions={
                "allowed_actions": self.allowed_actions,
                "ownership_domains": self.ownership_domains,
                "event_subscriptions": self.event_subscriptions,
            },
            provider_assignment=self.provider_assignment,
            resource_allocation=self.resource_allocation,
        )
        await self.memory.register_agent_identity.remote(self.identity.model_dump(mode="json"))
        await self.execution.register_agent_identity.remote(self.identity.model_dump(mode="json"))
        await self.trigger.register_agent.remote(
            self.agent_id,
            self.event_subscriptions,
            [
                EventType.TASK_CREATED.value,
                EventType.TASK_COMPLETED.value,
                EventType.TASK_UPDATE.value,
                EventType.REVIEW_REQUESTED.value,
                EventType.TEST_RESULT.value,
                EventType.CHECKPOINT_CREATED.value,
                EventType.COLLABORATION_UPDATE.value,
                EventType.BLOCKER_CREATED.value,
            ],
            coordinator=self.role == "pm_tech_lead",
        )
        restored = await self.checkpoints.recover_agent_state.remote(self.project_id, self.agent_id)
        if restored:
            state = restored.get("agent_state_snapshot", restored)
            self.current_task_id = state.get("current_task_id")
        local_state = await self.mongo.load_agent_state(
            project_id=self.project_id, agent_id=self.agent_id
        )
        if local_state:
            self.current_task_id = local_state.get("current_task_id", self.current_task_id)
        self._inbox_group = await self.bus.ensure_inbox_group(self.project_id, self.agent_id)
        self._inbox_consumer_name = f"{self.agent_id}-{time.time_ns()}"
        self.status = "IDLE"
        self.running = True
        self._background_tasks = [
            asyncio.create_task(self._inbox_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._collaboration_loop()),
            asyncio.create_task(self._work_poll_loop()),
        ]
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "project_id": self.project_id,
            "squad": self.squad,
            "status": self.status,
            "current_task_id": self.current_task_id,
            "provider_assignment": self.provider_assignment,
            "resource_allocation": self.resource_allocation,
            "running": self.running,
        }

    async def _inbox_loop(self) -> None:
        if self._inbox_consumer_name is None or self._inbox_group is None:
            raise RuntimeError("agent inbox consumer was not initialized")
        consumer_name = self._inbox_consumer_name
        group = self._inbox_group
        batch_size = int(self.inbox_tuning["batch_size"])
        block_milliseconds = int(self.inbox_tuning["stream_block_milliseconds"])
        lease_seconds = int(self.inbox_tuning["processing_lease_seconds"])
        while self.running:
            try:
                messages = await self.bus.reclaim_inbox(
                    self.project_id,
                    self.agent_id,
                    consumer_name,
                    min_idle_milliseconds=lease_seconds * 1000,
                    count=batch_size,
                )
                if not messages:
                    messages = await self.bus.read_inbox(
                        self.project_id,
                        self.agent_id,
                        consumer_name,
                        count=batch_size,
                        block_milliseconds=block_milliseconds,
                    )
                for message_id, fields in messages:
                    await self._handle_inbox_message(
                        message_id,
                        fields,
                        group=group,
                        consumer_name=consumer_name,
                        lease_seconds=lease_seconds,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.error(
                    "agent_inbox_error", agent_id=self.agent_id, error_type=type(error).__name__
                )
                await asyncio.sleep(float(self.inbox_tuning["error_backoff_seconds"]))

    async def _handle_inbox_message(
        self,
        message_id: str,
        fields: dict[str, str],
        *,
        group: str,
        consumer_name: str,
        lease_seconds: int,
    ) -> bool:
        del group
        raw = fields.get("event")
        if not raw:
            return False
        event = Event.model_validate_json(raw)
        if event.project_id != self.project_id:
            raise PermissionError("inbox event belongs to another project")
        event_id = str(event.event_id)
        status = await self.events.event_receipt_status(self.project_id, event_id, self.agent_id)
        if status is None:
            await self.events.record_event_delivery(
                self.project_id, event_id, self.agent_id, message_id
            )
            status = await self.events.event_receipt_status(
                self.project_id, event_id, self.agent_id
            )
        if status == "PROCESSED":
            await self.bus.acknowledge_inbox(self.project_id, self.agent_id, message_id)
            return True
        claimed = await self.events.claim_event_receipt(
            self.project_id,
            event_id,
            self.agent_id,
            consumer_name,
            lease_seconds,
        )
        if not claimed:
            return False
        try:
            result = await self._process_event(event)
            result_status = str(result.get("status", ""))
            if result_status in {"BUSY", "THROTTLED"}:
                await self.events.fail_event_receipt(
                    self.project_id,
                    event_id,
                    self.agent_id,
                    consumer_name,
                    result_status,
                )
                return False
            completed = await self.events.complete_event_receipt(
                self.project_id, event_id, self.agent_id, consumer_name
            )
            if not completed:
                return False
            await self.bus.acknowledge_inbox(self.project_id, self.agent_id, message_id)
            return True
        except Exception as error:
            await self.events.fail_event_receipt(
                self.project_id,
                event_id,
                self.agent_id,
                consumer_name,
                type(error).__name__,
            )
            logger.error(
                "agent_inbox_processing_failed",
                agent_id=self.agent_id,
                event_id=event_id,
                error_type=type(error).__name__,
            )
            return False

    @staticmethod
    def _clean_json(content: str) -> dict[str, Any]:
        clean = content.strip()
        if clean.startswith("```"):
            clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", clean, flags=re.IGNORECASE)
        result = json.loads(clean)
        if not isinstance(result, dict):
            raise ValueError("decision must be a JSON object")
        return result

    async def process_next_step(self, event_id: str) -> dict[str, Any]:
        row = await self.events.get_event(event_id)
        if row:
            event = Event(
                event_id=row["id"],
                project_id=str(row["project_id"]),
                event_type=row["event_type"],
                producer_agent_id=row["producer_agent_id"],
                target_agent_id=row["target_agent_id"],
                topic=row["topic"],
                payload=row["payload"],
                correlation_id=row["correlation_id"],
                causation_id=row["causation_id"],
                created_at=row["created_at"],
            )
        else:
            event = Event(
                project_id=self.project_id,
                event_type=EventType.AGENT_TRIGGERED,
                producer_agent_id="runtime_supervisor",
                target_agent_id=self.agent_id,
                payload={"event_id": event_id},
            )
        return await self._process_event(event)

    async def _process_event(self, event: Event) -> dict[str, Any]:
        if self._processing_lock.locked():
            return {"status": "BUSY"}
        if await self.bus.redis.exists(
            self.bus.client.key("project", self.project_id, "claims_paused")
        ):
            return {"status": "THROTTLED", "reason": "project claims are paused"}
        async with self._processing_lock:
            if not await self._acquire_capacity("active_agents", self.max_active_agents):
                return {"status": "THROTTLED"}
            try:
                return await self._process_event_with_capacity(event)
            finally:
                await self._release_capacity("active_agents")

    async def _process_event_with_capacity(self, event: Event) -> dict[str, Any]:
        task = await self.tasks.claim_next(self.project_id, self.agent_id, self.role)
        if task is None:
            if self.role == "pm_tech_lead" and event.event_type == EventType.REPLANNING_TRIGGERED:
                self.status = "CATCH_UP"
                query_text = str(
                    event.payload.get("message") or json.dumps(event.payload, default=str)
                )
                packet = await self.memory.build_catchup_packet.remote(
                    project_id=self.project_id,
                    agent_id=self.agent_id,
                    trigger_event_id=str(event.event_id),
                    provider_gateway=self.provider,
                    query_text=query_text[:8000],
                )
                created = await self._replan_gaps(list(event.payload.get("gaps", [])), packet)
                self.status = "IDLE"
                return {"status": "REPLANNED", "created_tasks": created}
            self.status = "IDLE"
            return {"status": "IDLE", "reason": "no runnable task"}
        self.status = "CATCH_UP"
        query_text = str(event.payload.get("message") or json.dumps(event.payload, default=str))
        packet = await self.memory.build_catchup_packet.remote(
            project_id=self.project_id,
            agent_id=self.agent_id,
            trigger_event_id=str(event.event_id),
            provider_gateway=self.provider,
            query_text=query_text[:8000],
        )
        task_id = str(task["id"])
        self.current_task_id = task_id
        await self.tasks.update_task_status(task_id, "IN_PROGRESS")
        self.status = "DECIDE_NEXT_ACTION"
        complexity = TaskComplexity(str(task.get("complexity") or "standard"))
        prompt = {
            "identity": self.identity.model_dump(mode="json"),
            "task": json.loads(json.dumps(task, default=str)),
            "context": packet,
            "response_schema": {
                "action_type": self.allowed_actions,
                "description": "string",
                "payload": {
                    "file_path": "relative path for write/read",
                    "content": "complete file content for write_file",
                    "test_command": ["executable", "argument"],
                },
            },
            "rules": [
                "Choose one allowed action only.",
                "Never use shell strings; test_command is a token array.",
                "Use only task allowed_paths and expected_outputs.",
                "Do not claim completion; review, test, evidence, and merge gates decide it.",
            ],
        }
        request = ProviderRequest(
            purpose="decide_next_action",
            messages=[
                {
                    "role": "system",
                    "content": "Act as the assigned software-delivery worker. Return JSON only.",
                },
                {"role": "user", "content": json.dumps(prompt, default=str)},
            ],
            budget_key=UUID(self.project_id),
            agent_id=self.agent_id,
            agent_role=self.role,
            complexity=complexity,
            preferred_provider=self.provider_assignment.get("provider"),
            preferred_model=dict(self.provider_assignment.get("model_routes", {})).get(
                complexity.value, self.provider_assignment.get("model")
            ),
            required_capabilities={"chat", "json"},
        )
        try:
            response = await self.provider.get_completion.remote(
                request.model_dump(mode="json"), response_format={"type": "json_object"}
            )
            decision = self._clean_json(response["content"])
            action_type = str(decision.get("action_type", "wait"))
            description = str(decision.get("description", "No description"))
            payload = decision.get("payload") or {}
            if action_type not in self.allowed_actions:
                raise PermissionError("provider proposed an action outside the role capability")
            result = await self._execute_decision(task, action_type, description, payload)
        except Exception as error:
            await self.tasks.update_task_status(task_id, "PENDING")
            result = {"status": "FAILED", "error": type(error).__name__}
        await self._checkpoint_and_share(task_id, result)
        self.current_task_id = None
        self.status = "IDLE"
        return result

    async def _replan_gaps(self, gaps: list[str], packet: dict[str, Any]) -> list[str]:
        if not gaps:
            return []
        request = ProviderRequest(
            purpose="bootstrap_team_planning",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Create the smallest safe backlog that closes the supplied DoD gaps. Return JSON "
                        '{"tasks":[{"title":str,"description":str,"priority":1-5,'
                        '"risk_level":"LOW|MEDIUM|HIGH|CRITICAL","complexity":'
                        '"low|standard|high|critical","allowed_paths":[str],'
                        '"blocked_paths":[str],"expected_outputs":[str],'
                        '"acceptance_criteria":[str],"owner_role":"ROLE_NAME",'
                        '"dod_criteria":[str]}]}.'
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps({"gaps": gaps, "context": packet}, default=str),
                },
            ],
            budget_key=UUID(self.project_id),
            agent_id=self.agent_id,
            agent_role=self.role,
            complexity=TaskComplexity.HIGH,
            required_capabilities={"chat", "json"},
        )
        response = await self.provider.get_completion.remote(
            request.model_dump(mode="json"), response_format={"type": "json_object"}
        )
        data = self._clean_json(response["content"])
        created: list[str] = []
        for item in data.get("tasks", []):
            criteria = list(item.get("dod_criteria", []))
            if not criteria or not set(criteria).issubset(set(gaps)):
                raise ValueError("replanned task must map only to current DoD gaps")
            task_id = await self.tasks.create_task(
                self.project_id,
                str(item["title"]),
                str(item["description"]),
                owner_role=str(item.get("owner_role") or self.role),
                priority=int(item.get("priority", 3)),
                acceptance_criteria=list(item.get("acceptance_criteria", [])),
                allowed_paths=list(item.get("allowed_paths", [])),
                blocked_paths=list(item.get("blocked_paths", [])),
                expected_outputs=list(item.get("expected_outputs", [])),
                dod_criteria=criteria,
                risk_level=str(item.get("risk_level", "LOW")),
                complexity=str(item.get("complexity", "standard")),
            )
            created.append(task_id)
            await self.events.save_event(
                self.project_id,
                Event(
                    project_id=self.project_id,
                    event_type=EventType.TASK_CREATED,
                    producer_agent_id=self.agent_id,
                    payload={"task_id": task_id, "source": "replanning"},
                ),
            )
        return created

    async def _execute_decision(
        self, task: dict[str, Any], action_type: str, description: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        task_id = str(task["id"])
        if action_type == "wait":
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "WAIT"}
        if action_type == "create_task":
            new_id = await self.tasks.create_task(
                self.project_id,
                str(payload["title"]),
                str(payload["description"]),
                owner_role=str(payload.get("owner_role") or self.role),
                priority=int(payload.get("priority", 3)),
                acceptance_criteria=list(payload.get("acceptance_criteria", [])),
                allowed_paths=list(payload.get("allowed_paths", [])),
                blocked_paths=list(payload.get("blocked_paths", [])),
                expected_outputs=list(payload.get("expected_outputs", [])),
                required_reviewers=list(payload.get("required_reviewers", [])),
                dod_criteria=list(payload.get("dod_criteria", [])),
                risk_level=str(payload.get("risk_level", "LOW")),
                complexity=str(payload.get("complexity", "standard")),
            )
            await self.events.save_event(
                self.project_id,
                Event(
                    project_id=self.project_id,
                    event_type=EventType.TASK_CREATED,
                    producer_agent_id=self.agent_id,
                    payload={"task_id": new_id, "parent_task_id": task_id},
                ),
            )
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "TASK_CREATED", "task_id": new_id}

        action = ActionRequest(
            project_id=self.project_id,
            agent_id=self.agent_id,
            task_id=task_id,
            action_type=action_type,
            description=description,
            target_paths=[str(payload["file_path"])] if payload.get("file_path") else [],
            command=payload.get("command") if isinstance(payload.get("command"), list) else None,
            database_operation=payload.get("query")
            if action_type == "execute_db_operation"
            else None,
            payload=payload,
        )
        code_slot = False
        if action_type in {"write_file", "write_code"}:
            code_slot = await self._acquire_capacity(
                "parallel_code_tasks", self.max_parallel_code_tasks
            )
            if not code_slot:
                await self.tasks.update_task_status(task_id, "PENDING")
                return {"status": "THROTTLED", "reason": "parallel code task limit reached"}
        try:
            execution = await self.execution.request_execution.remote(
                action.model_dump(mode="json")
            )
        finally:
            if code_slot:
                await self._release_capacity("parallel_code_tasks")
        if not execution.get("executed"):
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "BLOCKED", "execution": execution}
        if action_type not in {"write_file", "write_code"}:
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "ACTION_EXECUTED", "execution": execution}

        result = execution["result"]
        criteria = list(task.get("dod_criteria") or [])
        if not criteria:
            await self.tasks.update_task_status(task_id, "FAILED_VERIFICATION")
            return {"status": "FAILED_VERIFICATION", "reason": "task has no DoD criterion mapping"}
        for criterion_id in criteria:
            await self.dod.add_evidence(
                self.project_id,
                criterion_id,
                "artifact",
                self.agent_id,
                summary=f"Artifact {result['path']} stored with checksum {result['checksum_sha256']}",
                passed=True,
                artifact_id=result["artifact_id"],
                checksum_sha256=result["checksum_sha256"],
                metadata={"task_id": task_id},
            )

        expected_outputs = [
            str(item).replace("\\", "/") for item in task.get("expected_outputs") or []
        ]
        actual_outputs = [
            item.replace("\\", "/") for item in await self.tasks.artifact_titles(task_id)
        ]
        missing_outputs = [
            pattern
            for pattern in expected_outputs
            if not any(fnmatch.fnmatch(output, pattern) for output in actual_outputs)
        ]
        review = await self.reviewer.review_code_patch.remote(
            project_id=self.project_id,
            task_id=task_id,
            criterion_ids=criteria,
            artifact_id=result["artifact_id"],
            file_path=result["path"],
            code_content=str(result.get("review_content", "")),
        )
        if not review.get("approved"):
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "FAILED_REVIEW", "review": review}

        security_review: dict[str, Any] | None = None
        security_required = str(task.get("risk_level")) in {
            "HIGH",
            "CRITICAL",
        } or "security_reviewer" in set(task.get("required_reviewers") or [])
        if security_required:
            security_review = await self.safety.review_code_change.remote(
                project_id=self.project_id,
                task_id=task_id,
                criterion_ids=criteria,
                artifact_id=result["artifact_id"],
                file_path=result["path"],
                diff_content=str(result.get("review_content", "")),
                risk_level=str(task.get("risk_level", "HIGH")),
            )
            if not security_review.get("safe"):
                await self.tasks.update_task_status(task_id, "PENDING")
                return {
                    "status": "FAILED_SECURITY_REVIEW",
                    "review": review,
                    "security_review": security_review,
                }

        if missing_outputs:
            await self.tasks.update_task_status(task_id, "PENDING")
            return {
                "status": "PARTIAL_OUTPUT",
                "artifact": result,
                "review": review,
                "security_review": security_review,
                "missing_expected_outputs": missing_outputs,
            }

        configured_checks = {
            str(item["criterion_id"]): item
            for item in await self.dod.get_checks(self.project_id, criteria)
        }
        proposed_command = payload.get("test_command")
        default_command = (
            [str(token) for token in proposed_command]
            if isinstance(proposed_command, list) and proposed_command
            else []
        )
        configured_commands = [
            [str(token) for token in item.get("verification_command", [])]
            for item in configured_checks.values()
            if item.get("verification_command")
        ]
        if not default_command and configured_commands:
            default_command = configured_commands[0]
        criterion_commands = {
            criterion_id: [
                str(token)
                for token in configured_checks.get(criterion_id, {}).get("verification_command", [])
            ]
            or default_command
            for criterion_id in criteria
        }
        commands = list(
            dict.fromkeys(tuple(command) for command in criterion_commands.values() if command)
        )
        if not commands:
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "ARTIFACT_REVIEWED", "reason": "test command required before merge"}
        test_results: dict[tuple[str, ...], dict[str, Any]] = {}
        for command in commands:
            command_list = list(command)
            test_action = ActionRequest(
                project_id=self.project_id,
                agent_id=self.agent_id,
                task_id=task_id,
                action_type="shell_command",
                description=f"Verify task {task_id}",
                command=command_list,
                payload={"command": command_list},
            )
            test_results[command] = await self.execution.request_execution.remote(
                test_action.model_dump(mode="json")
            )
        passed = True
        for criterion_id in criteria:
            test_command = criterion_commands[criterion_id]
            test = test_results.get(tuple(test_command), {})
            criterion_passed = bool(
                test.get("executed") and test.get("result", {}).get("exit_code") == 0
            )
            passed = passed and criterion_passed
            await self.dod.add_evidence(
                self.project_id,
                criterion_id,
                "test",
                self.agent_id,
                summary=(
                    f"Command {json.dumps(test_command)} "
                    f"{'passed' if criterion_passed else 'failed'}"
                ),
                passed=criterion_passed,
                command=json.dumps(test_command),
                exit_code=test.get("result", {}).get("exit_code") if test.get("executed") else None,
                metadata={"task_id": task_id},
            )
        if not passed:
            await self.tasks.update_task_status(task_id, "PENDING")
            return {"status": "FAILED_TEST", "tests": list(test_results.values())}
        merge = await self.execution.merge_and_finalize_branch.remote(self.agent_id, task_id)
        if not merge.get("success"):
            await self.tasks.update_task_status(task_id, "BLOCKED")
            return {"status": "MERGE_BLOCKED", "merge": merge}
        completion = Event(
            project_id=self.project_id,
            event_type=EventType.TASK_COMPLETED,
            producer_agent_id=self.agent_id,
            payload={"task_id": task_id, "artifact_id": result["artifact_id"]},
        )
        await self.events.save_event(self.project_id, completion)
        return {
            "status": "COMPLETED",
            "artifact": result,
            "review": review,
            "security_review": security_review,
            "tests": list(test_results.values()),
            "merge": merge,
        }

    async def _checkpoint_and_share(self, task_id: str, result: dict[str, Any]) -> None:
        checkpoint = Checkpoint(
            project_id=self.project_id,
            agent_id=self.agent_id,
            task_id=task_id,
            achievement=str(result.get("status", "ACTION_PROCESSED")).lower(),
            summary=json.dumps(result, default=str)[:4000],
            agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status},
        )
        await self.checkpoints.create.remote(checkpoint.model_dump(mode="json"))
        await self.mongo.save_agent_state(
            project_id=self.project_id,
            agent_id=self.agent_id,
            state={
                "current_task_id": self.current_task_id,
                "status": self.status,
                "action_counter": self.action_counter,
            },
        )
        try:
            await self.memory.record_memory.remote(
                project_id=self.project_id,
                agent_id=self.agent_id,
                scope="project_memory",
                kind="collaboration_update",
                title=f"Task {task_id} update",
                content=json.dumps(result, default=str),
                importance=3,
                provider_gateway=self.provider,
                metadata={"task_id": task_id},
                promote_long_term=True,
            )
        except Exception as error:
            logger.error(
                "memory_promotion_failed",
                agent_id=self.agent_id,
                error_type=type(error).__name__,
            )
        event = Event(
            project_id=self.project_id,
            event_type=EventType.COLLABORATION_UPDATE,
            producer_agent_id=self.agent_id,
            payload={"task_id": task_id, "status": result.get("status")},
        )
        await self.events.save_event(self.project_id, event)
        self.action_counter += 1
        if self.action_counter % 5 == 0:
            await self.summaries.generate_local_agent_summary.remote(
                self.project_id, self.agent_id, self.provider
            )

    async def _heartbeat_loop(self) -> None:
        while self.running:
            try:
                await self.agents.heartbeat(self.project_id, self.agent_id, self.status)
                if self.current_task_id:
                    await self.tasks.renew_lease(self.current_task_id, self.agent_id)
                key = self.bus.client.key("agent", self.project_id, self.agent_id, "heartbeat")
                await self.bus.redis.set(key, self.status, ex=30)
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "agent_heartbeat_failed",
                    agent_id=self.agent_id,
                    error_type=type(error).__name__,
                )
                await asyncio.sleep(5)

    async def _collaboration_loop(self) -> None:
        interval = self.settings.collaboration_interval_seconds
        while self.running:
            try:
                await asyncio.sleep(interval)
                event = Event(
                    project_id=self.project_id,
                    event_type=EventType.COLLABORATION_UPDATE,
                    producer_agent_id=self.agent_id,
                    payload={"status": self.status, "current_task_id": self.current_task_id},
                )
                await self.events.save_event(self.project_id, event)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "collaboration_update_failed",
                    agent_id=self.agent_id,
                    error_type=type(error).__name__,
                )

    async def _work_poll_loop(self) -> None:
        """Claim durable work even when no new stream event arrives."""

        interval = max(5, min(15, self.settings.collaboration_interval_seconds))
        while self.running:
            try:
                await asyncio.sleep(interval)
                if self.status != "IDLE" or self._processing_lock.locked():
                    continue
                event = Event(
                    project_id=self.project_id,
                    event_type=EventType.AGENT_TRIGGERED,
                    producer_agent_id="runtime_supervisor",
                    target_agent_id=self.agent_id,
                    payload={"reason": "durable_work_poll"},
                )
                await self._process_event(event)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "agent_work_poll_failed",
                    agent_id=self.agent_id,
                    error_type=type(error).__name__,
                )

    async def stop(self) -> None:
        self.running = False
        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        await self.db.disconnect()
        await self.mongo.close()
        await self.bus.close()
