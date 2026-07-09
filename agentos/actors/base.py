from __future__ import annotations

import json
import re
import asyncio
from uuid import uuid4
import ray

from agentos.checkpoints.manager import Checkpoint, CheckpointManager
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest
from agentos.memory.broker import MemoryBroker
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProviderCallRepository, TaskRepository
from agentos.provider.gateway import ProviderGateway, ProviderRequest



@ray.remote(max_restarts=-1, max_task_retries=3)
class AgentWorkerActor:
    """Long-running Ray actor representing an asynchronous, event-driven specialized developer."""

    def __init__(self, agent_id: str, role: str, project_id: str, settings: dict):
        self.agent_id = agent_id
        self.role = role
        self.project_id = project_id
        self.settings = Settings(**settings) if settings else Settings()
        
        self.db_manager = DatabaseManager(self.settings)
        self.provider = ProviderGateway(self.settings)
        self.checkpoints = CheckpointManager(self.db_manager)
        
        self.status = "STARTING"
        self.current_task_id: str | None = None
        self.is_running = False

    async def start(self) -> dict:
        from agentos.execution.supervisor import ExecutionSupervisor
        """Connects resources and kicks off the background inbox processing engine worker."""
        await self.db_manager.connect()
        self.memory_broker = MemoryBroker(self.db_manager)
        self.task_repo = TaskRepository(self.db_manager)
        self.supervisor = ExecutionSupervisor(self.settings)

        self.provider.db_manager = self.db_manager
        self.provider.call_repo = ProviderCallRepository(self.db_manager)
        
        self.status = "IDLE"
        self.is_running = True
        
        # Spawn the long-running inbox polling loop as an un-awaited background task
        asyncio.create_task(self._inbox_listening_loop())
        
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "project_id": self.project_id,
            "status": self.status,
        }

    async def heartbeat(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "role": self.role,
            "project_id": self.project_id,
            "status": self.status,
            "current_task_id": self.current_task_id,
        }

    async def _inbox_listening_loop(self) -> None:
        """Asynchronously listens to the agent's dedicated wakeup channel and processes items."""
        from redis.asyncio import Redis
        
        redis_client = Redis.from_url(self.settings.dragonfly_url, decode_responses=True)
        pubsub = redis_client.pubsub()
        
        wakeup_channel = f"agent:{self.agent_id}:wakeup"
        inbox_key = f"agent:{self.agent_id}:inbox"
        
        await pubsub.subscribe(wakeup_channel)
        print(f"🤖 [AGENT {self.agent_id}]: Listening for async inbox wakeup triggers...")

        while self.is_running:
            try:
                # 1. Pop items off our persistent Dragonfly inbox queue
                raw_event_data = await redis_client.lpop(inbox_key)
                
                if not raw_event_data:
                    # If empty, block and wait for a live pub/sub wakeup notification signal
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message and message["data"] == "NEW_EVENT":
                        continue
                    await asyncio.sleep(0.1)
                    continue

                event_dict = json.loads(raw_event_data)
                event_id = event_dict.get("event_id")
                
                # 2. Run the decision making and execution handler logic asynchronously
                await self.process_next_step(event_id)

            except Exception as e:
                print(f"Error encountered in Agent {self.agent_id} background inbox loop: {e}")
                await asyncio.sleep(1.0)

    async def process_next_step(self, event_id: str) -> dict:
        from agentos.execution.supervisor import ExecutionSupervisor
        """The main autonomous reasoning loop turn. Builds context, calls LLM, and triggers actions."""
        self.status = "CATCH_UP"
       
        packet = await self.memory_broker.build_catchup_packet(
            project_id=self.project_id,
            agent_id=self.agent_id,
            trigger_event_id=event_id,
            provider_gateway=self.provider  
        )

        self.status = "DECIDE_NEXT_ACTION"
        
        system_prompt = (
            f"You are {self.agent_id}, a {self.role} in an autonomous software delivery team.\n"
            "Based on your recent memories, tasks list, and active project events, decide your next execution step.\n\n"
            "AVAILABLE ACTIONS:\n"
            "- 'write_file': Create or overwrite a target file path. Requires 'file_path' and 'content' keys inside payload.\n"
            "- 'read_file': Inspect a specific file's content. Requires 'file_path' key inside payload.\n"
            "- 'shell_command': Run terminal tests, check lists, or execute script logic. Requires a non-interactive 'command' string payload (e.g. 'python3 hello.py').\n"
            "- 'wait': Enter idle sleep state. Use this ONLY if all active tasks are verified.\n\n"
            "CRITICAL: Respond with a single un-wrapped valid JSON object matching this exact schema layout:\n"
            "{\n"
            "  \"action_type\": \"write_file\" | \"read_file\" | \"shell_command\" | \"wait\",\n"
            "  \"description\": \"An analytical sentence explaining the quality objective matching this checkpoint step\",\n"
            "  \"payload\": {\n"
            "     \"file_path\": \"filename.py\",\n"
            "     \"content\": \"string\",\n"
            "     \"command\": \"python3 filename.py\"\n"
            "  }\n"
            "}"
        )
        
        user_prompt = f"Here is your active database context catch-up packet:\n{packet}\n\nWhat is your next action response?"
        
        request = ProviderRequest(
            purpose="decide_next_action",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            budget_key=self.project_id,
            metadata={"trigger_event_id": event_id}
        )
        
        response = await self.provider.get_completion(request, response_format={"type": "json_object"})
        
        clean_content = response.content.strip()
        if clean_content.startswith("```"):
            clean_content = re.sub(r"^```json\s*|^```\s*", "", clean_content, flags=re.MULTILINE)
            clean_content = re.sub(r"\s*```$", "", clean_content, flags=re.MULTILINE).strip()

        try:
            decision = json.loads(clean_content)
            action_type = decision.get("action_type", "wait")
            description = decision.get("description", "Agent entered idle wait.")
            payload = decision.get("payload", decision)
        except Exception as e:
            action_type = "error"
            description = f"Failed to parse LLM string layout configuration: {e}"
            payload = {"raw_response": response.content}

        # 3. Securely submit action directly through the Execution Supervisor sandbox
        if action_type != "wait" and action_type != "error":
            from agentos.governance.models import ActionRequest as GovActionRequest
            action_req = GovActionRequest(
                project_id=self.project_id,
                agent_id=self.agent_id,
                action_type=action_type,
                description=description,
                payload=payload
            )
            # Perform supervised execution and write logs back into memory
            exec_res = await self.supervisor.request_execution(action_req)
            
            # Auto-update database states relationally based on task success parameters
            # Finds tasks by name parsing heuristics inside our database repository
            if exec_res.get("executed") and exec_res.get("result", {}).get("success"):
                query_tasks = "SELECT id FROM tasks WHERE project_id = $1 AND status != 'COMPLETED'"
                active_tasks = await self.db_manager.pool.fetch(query_tasks, uuid.UUID(self.project_id))
                if active_tasks:
                    # Complete the first item sequentially as an illustrative benchmark
                    await self.task_repo.update_task_status(str(active_tasks[0]["id"]), "COMPLETED")

        # 4. Persistence Audit Checkpointing
        self.status = "CHECKPOINT"
        checkpoint = await self.checkpoints.create(
            Checkpoint(
                checkpoint_id=str(uuid4()),
                project_id=self.project_id,
                agent_id=self.agent_id,
                achievement="milestone_completed" if action_type == "wait" else "action_executed",
                summary=f"Action [{action_type}]: {description}",
            )
        )
        
        self.status = "IDLE"
        return {
            "agent_id": self.agent_id,
            "action_type": action_type,
            "description": description,
            "checkpoint_id": checkpoint.checkpoint_id
        }

    def shutdown(self) -> None:
        """Gracefully signs off the worker loop thread channels."""
        self.is_running = False
        self.status = "OFFLINE"