from __future__ import annotations

import json
import re
import asyncio
import uuid
from uuid import uuid4
import ray
import structlog

from agentos.checkpoints.manager import Checkpoint, CheckpointManager
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, AgentIdentity
from agentos.memory.broker import MemoryBroker
from agentos.storage.database import DatabaseManager
from agentos.messaging.events import Event, EventType
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.storage.repositories import (
    MemoryRepository,
    ProviderCallRepository, 
    TaskRepository, 
    AuditEventRepository, 
    ArtifactRepository,
    SummaryRepository
)
from agentos.provider.gateway import  ProviderRequest
from agentos.config.loader import runtime_tuning
cfg = runtime_tuning()["agent_inbox_loop"]

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()


@ray.remote(max_restarts=-1, max_task_retries=3)
class AgentWorkerActor:

    def __init__(self, agent_id: str, role: str, project_id: str, settings: dict, spec_payload: dict):
        self.agent_id = agent_id
        self.role = role
        self.project_id = project_id
        self.settings = Settings(**settings) if settings else Settings()
        
        self.squad = spec_payload.get("squad", "engineering")
        self.permissions = spec_payload.get("permissions", ["low_risk"])
        self.memory_scopes = spec_payload.get("memory_scopes", ["project"])
        self.allowed_actions = spec_payload.get("allowed_actions", ["implement"])
        self.ownership_domains = spec_payload.get("ownership_domains", [])
        self.event_subscriptions = spec_payload.get("event_subscriptions", [])
        self.last_checkpoint_pointer = spec_payload.get("last_checkpoint_pointer", None)
        self.identity = AgentIdentity(
        agent_id=agent_id,
        role=role,
        project_id=project_id,
        squad=self.squad,
        memory_scopes=self.memory_scopes,
        allowed_actions=self.allowed_actions,
        allowed_paths=[],  # no per-agent path allowlist exists yet; tracked per-task instead (see Fix B note below)
    )
        self.db_manager = DatabaseManager(self.settings)
        self.provider = None
        
        self.status = "STARTING"
        self.current_task_id = None
        self.is_running = False
        self.action_counter = 0

    async def start(self) -> dict:
        from agentos.execution.supervisor import ExecutionSupervisor

        await self.db_manager.connect()
        self.memory_broker = ray.get_actor("memory_broker", namespace="agentos")
        self.supervisor = ray.get_actor("execution_supervisor", namespace="agentos")
        self.checkpoints = ray.get_actor("checkpoint_manager", namespace="agentos") 
        self.summary_manager = ray.get_actor("summary_manager", namespace="agentos")
        self.bus = DragonflyBus(self.settings.dragonfly_url)
        self.task_repo = TaskRepository(self.db_manager)
        self.audit_repo = AuditEventRepository(self.db_manager)
        self.artifact_repo = ArtifactRepository(self.db_manager)
        self.summary_repo = SummaryRepository(self.db_manager)
        self.provider = ray.get_actor("provider_gateway", namespace="agentos")
        
        from redis.asyncio import Redis
        self.redis_client = Redis.from_url(self.settings.dragonfly_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        
        # Incase the agent crahsed and stopped to process events, 
        # we will try to restore the last known state from the checkpoint manager
        self.status = "RESTORE_FROM_POSTGRES"
        try:
            restored_state = await self.checkpoints.recover_agent_state.remote(self.project_id, self.agent_id)
            if restored_state:
                snapshot = restored_state.get("agent_state_snapshot", {})
                self.current_task_id = snapshot.get("current_task_id")
                self.last_checkpoint_pointer = restored_state.get("checkpoint_id")
                logger.info("agent_state_restored", agent_id=self.agent_id, checkpoint_id=self.last_checkpoint_pointer)
        except Exception as e:
            logger.error("failed_to_restore_from_postgres", agent_id=self.agent_id, error=str(e))
        
        self.status = "SUBSCRIBE_TO_EVENTS"
        wakeup_channel = f"agent:{self.agent_id}:wakeup"
        await self.pubsub.subscribe(wakeup_channel) 
        
        self.status = "IDLE"
        self.is_running = True
        
        self._inbox_task = asyncio.create_task(self._inbox_listening_loop())
        
        logger.info("agent_started", agent_id=self.agent_id, role=self.role, project_id=self.project_id)
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "project_id": self.project_id,
            "status": self.status,
            "squad": self.squad
        }

    async def _inbox_listening_loop(self) -> None:
        inbox_key = f"agent:{self.agent_id}:inbox"

        while self.is_running:
            try:
                raw_event_data = await self.redis_client.lpop(inbox_key)
                if not raw_event_data:
                    message = await self.pubsub.get_message(
                        ignore_subscribe_messages=True, 
                        timeout=cfg["pubsub_poll_timeout_seconds"]
                    )
                    if message and message["data"] == "NEW_EVENT":
                        continue
                    await asyncio.sleep(cfg["empty_inbox_sleep_seconds"])
                    continue

                event_dict = json.loads(raw_event_data)
                self.status = "TRIGGERED"
                asyncio.create_task(self.process_next_step(event_dict.get("event_id")))
            except Exception as e:
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    logger.info("actor_loop_terminating_stopping_inbox", agent_id=self.agent_id)
                    self.is_running = False
                    break

                logger.error("inbox_loop_error", agent_id=self.agent_id, error=str(e))
                await asyncio.sleep(cfg["error_backoff_sleep_seconds"])

    async def process_next_step(self, event_id: str) -> dict:
        self.status = "CATCH_UP"
        
        packet = await self.memory_broker.build_catchup_packet.remote(
            project_id=self.project_id,
            agent_id=self.agent_id,
            trigger_event_id=event_id,
            agent_allowed_scopes=self.memory_scopes,
            provider_gateway=self.provider
        )

        # return the active tasks that dont depend on any other uncompleted tasks, 
        # so the agent can pick the most critical one to work on
        active_tasks = await self.task_repo.get_active_tasks(self.project_id)
        
        uncompleted_task_ids = {t["id"] for t in active_tasks}
        runnable_tasks = []
        for t in active_tasks:
            dependencies = t.get("dependencies", [])
            unsatisfied = [dep for dep in dependencies if dep in uncompleted_task_ids]
            if not unsatisfied:
                runnable_tasks.append(t)

        self.status = "DECIDE_NEXT_ACTION"
        system_prompt = (
            f"You are {self.agent_id}, a {self.role} in the {self.squad} squad.\n"
            f"Your ownership domains are: {json.dumps(self.ownership_domains)}.\n"
            f"Your allowed action capabilities are: {json.dumps(self.allowed_actions)}.\n"
            f"Here are the ongoing uncompleted tasks for this project that are currently UNBLOCKED and ready to work:\n{json.dumps(runnable_tasks)}\n"
            "Choose the most critical task from the list above that matches your role boundaries.\n"
            "CRITICAL: You must return the exact 'target_task_id' you are working on in your JSON response.\n\n"
            "If there are NO runnable tasks left, but you believe the project is not actually finished, "
            "use action_type 'create_task' to define new work instead of choosing 'wait'.\n\n"
            "SCHEMA LAYOUT:\n"
            "{\n"
            "  \"target_task_id\": \"string-uuid or null (null only when action_type is 'create_task')\",\n"
            "  \"action_type\": \"write_file\" | \"read_file\" | \"shell_command\" | \"create_task\" | \"wait\",\n"
            "  \"description\": \"Objective summary\",\n"
            "  \"payload\": {\"file_path\": \"src/app.py\", \"content\": \"...\"} "
            "or {\"title\": \"New task title\", \"task_description\": \"...\", \"priority\": 1-5} for create_task\n"
            "}"
        )
        
        request = ProviderRequest(
            purpose="decide_next_action",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": f"Context Packet:\n{packet}"}],
            budget_key=self.project_id
        )
        
        response_dict = await self.provider.get_completion.remote(
            request, 
            response_format={"type": "json_object"}
        )
        clean_content = response_dict["content"].strip()
        if clean_content.startswith("```"):
            clean_content = re.sub(r"^```json\s*|^```\s*", "", clean_content, flags=re.MULTILINE)
            clean_content = re.sub(r"\s*```$", "", clean_content, flags=re.MULTILINE).strip()

        try:
            decision = json.loads(clean_content)
            target_task_id = decision.get("target_task_id")
            action_type = decision.get("action_type", "wait")
            description = decision.get("description", "")
            payload = decision.get("payload", {})
        except Exception:
            action_type = "wait"
            target_task_id = None
            description = "Failed to parse choice structural template response."
            payload = {}

        #if the agent action not wait we will try to acquire a lock on the task 
        # to avoid multiple agents working on the same task at the same time.
        if action_type == "create_task":
            self.status = "CREATE_NEW_TASK"
            new_title = payload.get("title", "Untitled follow-up task")
            new_description = payload.get("task_description", description)
            new_priority = int(payload.get("priority", 3))

            new_task_id = await self.task_repo.create_task(
                project_id=self.project_id,
                title=new_title,
                description=new_description,
                priority=new_priority,
            )
            logger.info("new_task_created_by_agent", agent_id=self.agent_id, task_id=new_task_id, title=new_title)

            success_checkpoint_data = Checkpoint(
                checkpoint_id=str(uuid4()),
                project_id=self.project_id,
                agent_id=self.agent_id,
                achievement="task_created",
                summary=f"Created new task '{new_title}' to close a detected DoD gap.",
                agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
            ).model_dump()

            self.status = "CHECKPOINT"
            checkpoint_res = await self.checkpoints.create.remote(success_checkpoint_data)
            self.last_checkpoint_pointer = checkpoint_res["checkpoint_id"]

            self.status = "IDLE"
            return {"status": "SUCCESS", "checkpoint_id": checkpoint_res["checkpoint_id"], "new_task_id": new_task_id}
   
        if action_type != "wait":
            self.status = "REQUEST_LOCKS"
            lock_acquired = False
            lock_key = f"project:{self.project_id}:lock:{target_task_id or 'global'}"
            lease_key = f"agent:{self.agent_id}:processing_lease"
            
            try:
                await self.redis_client.set(lease_key, "ACTIVE", ex=60)
                lock_acquired = await self.redis_client.set(lock_key, self.agent_id, nx=True, ex=120)
                if not lock_acquired:
                    logger.warning("failed_to_acquire_task_lock", agent_id=self.agent_id, target_task_id=target_task_id)
                    self.status = "IDLE"
                    await self.redis_client.delete(lease_key)
                    return {"status": "LOCK_ACQUISITION_FAILED"}
            except Exception as e:
                logger.error("lock_infrastructure_error", agent_id=self.agent_id, error=str(e))
                self.status = "IDLE"
                await self.redis_client.delete(lease_key)
                return {"status": "LOCK_ERROR"}

            self.status = "SUBMIT_ACTION_REQUEST"
            action_req = ActionRequest(
                project_id=self.project_id, agent_id=self.agent_id, action_type=action_type, description=description, payload=payload
            )
            
            self.status = "EXECUTION_SUPERVISOR_RUNS_IF_ALLOWED"
            exec_res = await self.supervisor.request_execution.remote(action_req.model_dump())
            
            if action_type in {"write_file", "write_code"} and exec_res.get("executed"):
                self.status = "PUBLISH_OUTPUT"
                from agentos.actors.reviewer import ReviewerAgentActor
                
                reviewer = ReviewerAgentActor.options(namespace="agentos").remote(settings_payload=self.settings.model_dump())
                review = await reviewer.review_code_patch.remote(payload.get("file_path", ""), payload.get("content", ""))
                
                if not review.get("approved", False):
                    error_checkpoint_data = Checkpoint(
                        checkpoint_id=str(uuid4()),
                        project_id=self.project_id,
                        agent_id=self.agent_id,
                        achievement="review_failed",
                        summary=f"Blocker: Code patch rejected. Vulnerabilities: {review.get('vulnerabilities_found')}",
                        agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
                    ).model_dump() 

                    self.status = "CHECKPOINT"
                    checkpoint_res = await self.checkpoints.create.remote(error_checkpoint_data)
                    
                    self.last_checkpoint_pointer = checkpoint_res["checkpoint_id"]
                    
                    await self.redis_client.delete(lock_key)
                    await self.redis_client.delete(lease_key)
                    
                    self.status = "IDLE"
                    return {"status": "BLOCKED_BY_REVIEW"}

            policy_decision = exec_res.get("guardrail", {}).get("decision", "ALLOW")
            await self.audit_repo.log_audit_event(
                project_id=self.project_id,
                agent_id=self.agent_id,
                action_type=action_type,
                policy_decision=policy_decision,
                integrity_hash=action_req.integrity_hash
            )

            if exec_res.get("executed") and target_task_id:
                self.status = "PUBLISH_OUTPUT"
                file_path = payload.get("file_path", "unknown_file")
                
                if action_type == "write_file":
                    await self.artifact_repo.create_artifact(
                        project_id=self.project_id,
                        task_id=target_task_id,
                        artifact_type="FILE",
                        title=file_path,
                        uri=file_path
                    )
                
                await self.task_repo.update_task_status(target_task_id, "COMPLETED")
                logger.info("task_completed_by_agent", agent_id=self.agent_id, task_id=target_task_id)

                unified_stream_key = f"project:{self.project_id}:events"
                completion_event = Event(
                    project_id=self.project_id,
                    event_type=EventType.TASK_COMPLETED,
                    producer_agent_id=self.agent_id,
                    topic=unified_stream_key,
                    payload={
                        "task_id": target_task_id,
                        "message": f"Task '{target_task_id}' completed by agent {self.agent_id}.",
                        "file_path": file_path
                    }
                )
                try:
                    await self.bus.publish_event(unified_stream_key, completion_event)
                    logger.info("task_completed_event_dispatched_to_stream", agent_id=self.agent_id, task_id=target_task_id)
                except Exception as e:
                    logger.error("failed_to_publish_task_completed_event", agent_id=self.agent_id, error=str(e))
                
                try:
                    from agentos.storage.repositories import MemoryRepository
                    memory_repo = MemoryRepository(self.db_manager)
                    memory_content = f"Task '{target_task_id}' completed by {self.agent_id}: {description}"
                    mem_id = await memory_repo.save_memory_item(
                        project_id=self.project_id,
                        scope="project",
                        owner_agent_id=self.agent_id,
                        memory_type="task_completion",
                        title=file_path if action_type == "write_file" else description[:80],
                        content=memory_content,
                    )
                    embedding_vector = await self.provider.get_embedding.remote(memory_content)
                    async with self.db_manager.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO memory_embeddings (memory_item_id, embedding) VALUES ($1, $2::vector)",
                            uuid.UUID(mem_id), embedding_vector
                        )
                except Exception as e:
                    logger.error("failed_to_write_memory_item", agent_id=self.agent_id, error=str(e))

                merge_result = await self.supervisor.merge_and_finalize_branch.remote(self.agent_id)
                
            await self.redis_client.delete(lock_key)
            await self.redis_client.delete(lease_key)

        success_checkpoint_data = Checkpoint(
            checkpoint_id=str(uuid4()),
            project_id=self.project_id,
            agent_id=self.agent_id,
            achievement="action_processed",
            summary=description,
            agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
        ).model_dump() 
        
        self.status = "CHECKPOINT"
        checkpoint_res = await self.checkpoints.create.remote(success_checkpoint_data)
        self.last_checkpoint_pointer = checkpoint_res["checkpoint_id"]
        
        self.action_counter += 1
        self.status = "SUMMARIZE_IF_NEEDED"
        if self.action_counter % 5 == 0:
            try:
                summary_text = await self.summary_manager.generate_local_agent_summary.remote(
                    self.project_id, self.agent_id, self.provider
                )
                await self.summary_repo.save_summary(self.project_id, "agent_local", self.agent_id, summary_text)
                logger.info("local_agent_summary_generated", agent_id=self.agent_id)
            except Exception as e:
                logger.error("failed_to_generate_periodic_summary", agent_id=self.agent_id, error=str(e))

        self.status = "IDLE"
        return {"status": "SUCCESS", "checkpoint_id": checkpoint_res["checkpoint_id"]}