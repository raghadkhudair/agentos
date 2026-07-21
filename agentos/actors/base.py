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
    SummaryRepository,
    AgentRepository
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
            allowed_paths=[],
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
        self.agent_repo = AgentRepository(self.db_manager)
        await self.agent_repo.register_agent(self.agent_id, self.project_id, self.role, self.squad)
        self.provider = ray.get_actor("provider_gateway", namespace="agentos")
        
        from redis.asyncio import Redis
        self.redis_client = Redis.from_url(self.settings.dragonfly_url, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        
        await self.memory_broker.register_agent_identity.remote(self.identity.model_dump())
        
        self.status = "REGISTRATION"
        try:
            await self.supervisor.register_agent_identity.remote(self.identity.model_dump())
            logger.info("agent_identity_registered_with_supervisor", agent_id=self.agent_id)
        except Exception as e:
            logger.error("identity_registration_failed", agent_id=self.agent_id, error=str(e))

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
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
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
            provider_gateway=self.provider
        )

        active_tasks = await self.task_repo.get_active_tasks(self.project_id)
        
        uncompleted_task_ids = {t["id"] for t in active_tasks}
        runnable_tasks = []
        for t in active_tasks:
            if t.get("status") == "IN_PROGRESS":
                continue
                
            dependencies = t.get("dependencies", [])
            unsatisfied = [dep for dep in dependencies if dep in uncompleted_task_ids]
            if not unsatisfied:
                runnable_tasks.append(t)

        self.status = "DECIDE_NEXT_ACTION"

        failure_hints = []
        if self.provider:
            try:
                from agentos.storage.repositories import MemoryRepository
                memory_repo = MemoryRepository(self.db_manager)
                trigger_text = str(event_id)
                err_vector = await self.provider.get_embedding.remote(trigger_text, self.project_id)
                past_failures = await memory_repo.find_similar_failures(self.project_id, err_vector)
                for pf in past_failures:
                    failure_hints.append(f"⚠️ Past Similar Failure ({pf['title']}): {pf['content']}")
            except Exception as e:
                logger.warning("failure_lookup_bypassed", error=str(e))
                
        failure_context_str = "\n".join(failure_hints) if failure_hints else "No prior matching failures found."

        system_prompt = (
            f"You are {self.agent_id}, a {self.role} in the {self.squad} squad.\n"
            f"Your ownership domains are: {json.dumps(self.ownership_domains)}.\n"
            f"Your allowed action capabilities are: {json.dumps(self.allowed_actions)}.\n"
            f"Historical Failure Lessons:\n{failure_context_str}\n"
            f"Here are the uncompleted tasks for this project that are ready to work:\n{json.dumps(runnable_tasks)}\n"
            "Choose the most critical task from the list above that matches your role boundaries.\n"
            "CRITICAL: You must return the exact 'target_task_id' you are working on in your JSON response.\n\n"
            "If there are NO runnable tasks left matching your capabilities, you must choose action_type 'wait'. "
            "Do not invent new milestones autonomously.\n\n"
            "SCHEMA LAYOUT:\n"
            "{\n"
            "  \"target_task_id\": \"string-uuid or null (null only when action_type is 'create_task')\",\n"
            "  \"action_type\": \"write_file\" | \"read_file\" | \"shell_command\" | \"create_task\" | \"execute_db_operation\" | \"wait\",\n"
            "  \"description\": \"Objective summary\",\n"
            "  \"payload\": {\n"
            "     \"query\": \"CREATE TABLE users (...)\",\n"
            "     \"parameters\": [],\n"
            "     \"file_path\": \"src/app.py\", \n"
            "     \"content\": \"...\",\n"
            "     \"title\": \"New task title (create_task only)\",\n"
            "     \"task_description\": \"Description (create_task only)\",\n"
            "     \"priority\": 1-5,\n"
            "     \"risk_level\": \"LOW\" | \"MEDIUM\" | \"HIGH\" | \"CRITICAL\",\n"
            "     \"acceptance_criteria\": [\"conditions\"],\n"
            "     \"allowed_paths\": [\"paths/\"],\n"
            "     \"blocked_paths\": [\"paths/\"],\n"
            "     \"expected_outputs\": [\"files\"]\n"
            "  }\n"
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
            
            if action_type != "wait" and action_type not in self.identity.allowed_actions:
                logger.warning("hallucinated_or_unauthorized_tool_rejected_locally", agent_id=self.agent_id, action_type=action_type)
                action_type = "wait"
                target_task_id = None
                payload = {}
        except Exception:
            action_type = "wait"
            target_task_id = None
            description = "Failed to parse choice structural template response."
            payload = {}

        if action_type == "create_task":
            self.status = "CREATE_NEW_TASK"
            new_title = payload.get("title", "Untitled follow-up task")
            new_description = payload.get("task_description", description)
            new_priority = int(payload.get("priority", 3))

            task_text_to_embed = f"{new_title}: {new_description}"
            task_embedding = None
            if self.provider:
                try:
                    task_embedding = await self.provider.get_embedding.remote(task_text_to_embed, self.project_id)
                except Exception as e:
                    logger.warning("failed_to_generate_task_embedding", error=str(e))

            if task_embedding:
                similar_task = await self.task_repo.find_similar_task(self.project_id, task_embedding)
                if similar_task:
                    logger.info(
                        "duplicate_task_creation_blocked", 
                        agent_id=self.agent_id, 
                        existing_task_id=similar_task["id"],
                        distance=similar_task["distance"]
                    )
                    self.status = "IDLE"
                    return {"status": "SKIPPED_DUPLICATE", "existing_task_id": similar_task["id"]}

            new_task_id = await self.task_repo.create_task(
                project_id=self.project_id,
                title=new_title,
                description=new_description,
                owner_agent_id=None,
                parent_task_id=self.current_task_id,
                priority=new_priority,
                acceptance_criteria=payload.get("acceptance_criteria", []),
                allowed_paths=payload.get("allowed_paths", []),
                blocked_paths=payload.get("blocked_paths", []),
                expected_outputs=payload.get("expected_outputs", []),
                risk_level=payload.get("risk_level", "LOW"),
                embedding=task_embedding  
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

            # Set database task status to IN_PROGRESS and log claimed state checkpoint
            if target_task_id:
                self.current_task_id = target_task_id
                await self.task_repo.update_task_status(target_task_id, "IN_PROGRESS")
                
                claim_checkpoint = Checkpoint(
                    checkpoint_id=str(uuid4()),
                    project_id=self.project_id,
                    agent_id=self.agent_id,
                    task_id=target_task_id,
                    achievement="task_claimed",
                    summary=f"Claimed task '{target_task_id}' and locked processing boundaries.",
                    agent_state_snapshot={"current_task_id": self.current_task_id, "status": "EXECUTING"}
                ).model_dump()
                await self.checkpoints.create.remote(claim_checkpoint)

            self.status = "SUBMIT_ACTION_REQUEST"
            action_req = ActionRequest(
                project_id=self.project_id, 
                agent_id=self.identity.agent_id, 
                action_type=action_type, 
                description=description, 
                payload=payload
            )
            
            self.status = "EXECUTION_SUPERVISOR_RUNS_IF_ALLOWED"
            exec_res = await self.supervisor.request_execution.remote(action_req.model_dump())
            
            if not exec_res.get("executed"):
                error_msg = exec_res.get("error") or exec_res.get("reason") or "Execution blocked by supervisor guardrails"
                try:
                    from agentos.storage.repositories import MemoryRepository
                    memory_repo = MemoryRepository(self.db_manager)
                    
                    mem_id = await memory_repo.save_memory_item(
                        project_id=self.project_id,
                        scope="execution_memory",
                        owner_agent_id=self.agent_id,
                        memory_type="execution_failure",
                        title=f"Failure during {action_type}",
                        content=f"Action: {action_type} | Description: {description} | Error: {error_msg}"
                    )
                    
                    err_vector = await self.provider.get_embedding.remote(error_msg, self.project_id)
                    async with self.db_manager.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO memory_embeddings (memory_item_id, embedding) VALUES ($1, $2::vector)",
                            uuid.UUID(mem_id), err_vector
                        )
                    logger.info("execution_failure_memory_saved", agent_id=self.agent_id)
                    blocker_cp = Checkpoint(
                        checkpoint_id=str(uuid4()),
                        project_id=self.project_id,
                        agent_id=self.agent_id,
                        task_id=target_task_id,
                        achievement="blocker_opened",
                        summary=f"Execution blocked during {action_type}: {error_msg}",
                        agent_state_snapshot={"current_task_id": self.current_task_id, "status": "BLOCKED"}
                    ).model_dump()
                    await self.checkpoints.create.remote(blocker_cp)
                    if target_task_id:
                        await self.task_repo.update_status(target_task_id, "BLOCKED")
                except Exception as e:
                    logger.error("failed_to_log_execution_failure_memory", error=str(e))

            current_task_status = next((t["status"] for t in active_tasks if str(t["id"]) == str(target_task_id)), None)
            if current_task_status == "BLOCKED" and exec_res.get("executed"):
                try:
                    resolved_cp = Checkpoint(
                        checkpoint_id=str(uuid4()),
                        project_id=self.project_id,
                        agent_id=self.agent_id,
                        task_id=target_task_id,
                        achievement="blocker_resolved",
                        summary=f"Previously blocked task succeeded on action: {action_type}",
                        agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
                    ).model_dump()
                    await self.checkpoints.create.remote(resolved_cp)
                except Exception as e:
                    logger.error("failed_to_save_blocker_resolved_checkpoint", error=str(e))
                    
            if action_type in {"write_file", "write_code"} and exec_res.get("executed"):
                self.status = "PUBLISH_OUTPUT"
                try:
                    patch_cp = Checkpoint(
                        checkpoint_id=str(uuid4()),
                        project_id=self.project_id,
                        agent_id=self.agent_id,
                        task_id=target_task_id,
                        achievement="code_patch_generated",
                        summary=f"Generated code patch for file '{payload.get('file_path', 'code_file')}'",
                        agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
                    ).model_dump()
                    await self.checkpoints.create.remote(patch_cp)
                except Exception as e:
                    logger.error("failed_to_save_code_patch_checkpoint", error=str(e))

                code_reviewer = ray.get_actor("code_reviewer", namespace="agentos")
                review_result = await code_reviewer.review_code_patch.remote(
                    payload.get("file_path", "unknown_file"), payload.get("content", "")
                )
                try:
                    review_cp = Checkpoint(
                        checkpoint_id=str(uuid4()),
                        project_id=self.project_id,
                        agent_id=self.agent_id,
                        task_id=target_task_id,
                        achievement="review_completed",
                        summary=f"Code review result: approved={review_result.get('approved')}",
                        agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
                    ).model_dump()
                    await self.checkpoints.create.remote(review_cp)
                except Exception as e:
                    logger.error("failed_to_save_review_checkpoint", error=str(e))

            if exec_res.get("executed") and target_task_id:
                self.status = "PUBLISH_OUTPUT"
                file_path = payload.get("file_path", "unknown_file")
                file_content = payload.get("content", "")

                affected_contracts = []
                if self.provider and (file_content or description):
                    try:
                        from agentos.storage.repositories import MemoryRepository
                        memory_repo = MemoryRepository(self.db_manager)
                        
                        change_text = f"File: {file_path}\nDescription: {description}\nContent: {file_content[:500]}"
                        change_vector = await self.provider.get_embedding.remote(change_text, self.project_id)
                        
                        affected_contracts = await memory_repo.find_affected_contracts(self.project_id, change_vector)
                        if affected_contracts:
                            await self.task_repo.update_task_affected_contracts(target_task_id, affected_contracts)
                            logger.info("contract_impact_detected", task_id=target_task_id, affected=affected_contracts)
                            contract_cp = Checkpoint(
                                checkpoint_id=str(uuid4()),
                                project_id=self.project_id,
                                agent_id=self.agent_id,
                                task_id=target_task_id,
                                achievement="contract_published",
                                summary=f"Contract impact published for contracts: {', '.join(affected_contracts)}",
                                agent_state_snapshot={"current_task_id": self.current_task_id, "status": self.status}
                            ).model_dump()
                            await self.checkpoints.create.remote(contract_cp)
                    
                    except Exception as e:
                        logger.error("contract_impact_analysis_failed", error=str(e))

                if action_type == "write_file":
                    await self.artifact_repo.create_artifact(
                        project_id=self.project_id,
                        task_id=target_task_id,
                        artifact_type="FILE",
                        title=file_path,
                        uri=file_path
                    )

                if file_content and self.provider:
                        try:
                            from agentos.storage.repositories import CodebaseMapRepository
                            code_repo = CodebaseMapRepository(self.db_manager)
                            
                            await code_repo.clear_file_index(self.project_id, file_path)
                            
                            snippet_to_embed = f"File: {file_path}\nContent:\n{file_content[:1500]}"
                            code_vector = await self.provider.get_embedding.remote(snippet_to_embed, self.project_id)
                            
                            await code_repo.index_file_chunk(
                                project_id=self.project_id,
                                file_path=file_path,
                                chunk_identifier="file_content",
                                code_snippet=file_content[:2000],
                                embedding=code_vector
                            )
                            logger.info("codebase_semantic_map_updated", file_path=file_path)
                        except Exception as e:
                            logger.error("failed_to_index_codebase_semantic_map", file_path=file_path, error=str(e))

                
                await self.task_repo.update_task_status(target_task_id, "COMPLETED")
                self.current_task_id = None
                try:
                    completed_cp = Checkpoint(
                        checkpoint_id=str(uuid4()),
                        project_id=self.project_id,
                        agent_id=self.agent_id,
                        task_id=target_task_id,
                        achievement="task_completed",
                        summary=f"Successfully completed task '{target_task_id}': {description}",
                        agent_state_snapshot={"current_task_id": None, "status": "COMPLETED"}
                    ).model_dump()
                    await self.checkpoints.create.remote(completed_cp)
                except Exception as e:
                    logger.error("failed_to_save_task_completed_checkpoint", error=str(e))

                logger.info("task_completed_by_agent", agent_id=self.agent_id, task_id=target_task_id)

                unified_stream_key = f"project:{self.project_id}:events"
                completion_event = Event(
                    project_id=self.project_id,
                    event_type=EventType.TASK_COMPLETED,
                    producer_agent_id=self.identity.agent_id,
                    topic=unified_stream_key,
                    payload={
                        "task_id": target_task_id,
                        "message": f"Task '{target_task_id}' completed by agent {self.identity.agent_id}.",
                        "file_path": file_path
                    }
                )
                try:
                    from agentos.storage.repositories import MemoryRepository
                    memory_repo = MemoryRepository(self.db_manager)
                    memory_content = f"Task '{target_task_id}' completed by {self.agent_id}: {description}"
                    
                    target_scope = "execution_memory" if action_type in {"write_file", "shell_command"} else "project_memory"

                    mem_id = await memory_repo.save_memory_item(
                        project_id=self.project_id,
                        scope=target_scope,
                        owner_agent_id=self.agent_id,
                        memory_type="task_completion",
                        title=file_path if action_type == "write_file" else description[:80],
                        content=memory_content,
                    )
                    embedding_vector = await self.provider.get_embedding.remote(memory_content, self.project_id)
                    async with self.db_manager.pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO memory_embeddings (memory_item_id, embedding) VALUES ($1, $2::vector)",
                            uuid.UUID(mem_id), embedding_vector
                        )
                    logger.info("execution_memory_saved", agent_id=self.agent_id, scope=target_scope)
                except Exception as e:
                    logger.error("failed_to_write_memory_item", agent_id=self.agent_id, error=str(e))


                merge_result = await self.supervisor.merge_and_finalize_branch.remote(self.agent_id)
                if merge_result.get("success"):
                    try:
                        merge_cp = Checkpoint(
                            checkpoint_id=str(uuid4()),
                            project_id=self.project_id,
                            agent_id=self.agent_id,
                            achievement="merge_completed",
                            summary=f"Branch changes successfully merged to main for agent {self.agent_id}",
                            agent_state_snapshot={"current_task_id": None, "status": "IDLE"}
                        ).model_dump()
                        await self.checkpoints.create.remote(merge_cp)
                    except Exception as e:
                        logger.error("failed_to_save_merge_completed_checkpoint", error=str(e))
                else:
                    logger.warning("branch_merge_failed", agent_id=self.agent_id, error=merge_result.get("error"))

            
            elif not exec_res.get("executed") and target_task_id:
                # Fall back status to PENDING if guardrail filters or runtime execution blocks the call
                await self.task_repo.update_task_status(target_task_id, "PENDING")
                self.current_task_id = None
                    
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

    async def _heartbeat_loop(self) -> None:
        """Periodically updates a short-lived heartbeat key in Dragonfly."""
        heartbeat_key = f"agent:{self.agent_id}:heartbeat"
        while self.is_running:
            try:
                await self.redis_client.set(heartbeat_key, "ALIVE", ex=30)
                await asyncio.sleep(10)
            except Exception as e:
                logger.warning("heartbeat_failed", agent_id=self.agent_id, error=str(e))
                await asyncio.sleep(5)