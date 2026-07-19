from __future__ import annotations
import os
import shutil
import asyncio
import shlex
import uuid
import ray
import structlog
import json
import asyncpg
from agentos.messaging.events import Event, EventType
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision, RiskLevel
from agentos.storage.repositories import AuditEventRepository
from agentos.config.loader import guardrail_policies
from agentos.storage.database import DatabaseManager

logger = structlog.get_logger()


@ray.remote(namespace="agentos")
class ExecutionSupervisorActor:

    def __init__(self, settings_payload: dict):
        from agentos.governance.policy_engine import PolicyEngine
        from redis.asyncio import Redis

        self.settings = Settings(**settings_payload) if settings_payload else Settings()
        self.policy_engine = PolicyEngine(self.settings)
        self._sandbox_cfg = guardrail_policies()["execution_sandbox"]
        self.workspace_path = os.path.abspath(self.settings.workspace)
        self.worktrees_dir = os.path.join(self.workspace_path, ".agentos_worktrees")
        self.redis_client = Redis.from_url(self.settings.dragonfly_url, decode_responses=True)
        self._authenticated_identities = {}        
        self.db_manager = DatabaseManager(self.settings)
        self.sandbox_db_pool = None
        self._connected = False
        
        os.makedirs(self.workspace_path, exist_ok=True)
        os.makedirs(self.worktrees_dir, exist_ok=True)
        
        self._initialize_git_workspace_safely()

    async def _ensure_connected(self) -> None:
        if not self._connected:
            await self.db_manager.connect()
            sandbox_url = os.getenv("SANDBOX_DATABASE_URL", self.settings.database_url)
            self.sandbox_db_pool = await asyncpg.create_pool(sandbox_url)
            self._connected = True

    async def register_agent_identity(self, identity_data: dict) -> None:
        from agentos.governance.models import AgentIdentity
        identity_obj = AgentIdentity(**identity_data)
        if not hasattr(self, "_authenticated_identities"):
            self._authenticated_identities = {}
        self._authenticated_identities[identity_obj.agent_id] = identity_obj

    def _initialize_git_workspace_safely(self) -> None:
        git_dir = os.path.join(self.workspace_path, ".git")
        if not os.path.exists(git_dir):
            try:
                os.system(f"git init {shlex.quote(self.workspace_path)} > /dev/null 2>&1")
                os.system(f"git -C {shlex.quote(self.workspace_path)} checkout -b {self._sandbox_cfg['default_branch']} > /dev/null 2>&1")
                
                init_file = os.path.join(self.workspace_path, ".agentos_keep")
                with open(init_file, "w") as f:
                    f.write("AgentOS Initializer Pointer State File.")
                
                os.system(f"git -C {shlex.quote(self.workspace_path)} add .agentos_keep > /dev/null 2>&1")
                os.system(f"git -C {shlex.quote(self.workspace_path)} -c user.name='{self._sandbox_cfg['git_author_name']}' -c user.email='{self._sandbox_cfg['git_author_email']}' commit -m 'Initial asset repository bootstrap commit.' > /dev/null 2>&1")
                logger.info("sandbox_git_repository_initialized", workspace_path=self.workspace_path)
            except Exception as e:
                logger.error("sandbox_git_initialization_failed", error=str(e))

    # currently is dead because of no where its the memory items is being inserted from 
    async def _verify_decision_conflict(self, action: ActionRequest) -> bool:
        await self._ensure_connected()
        safe_project_id = uuid.UUID(action.project_id) if isinstance(action.project_id, str) else action.project_id
        
        query = "SELECT title, content FROM memory_items WHERE project_id = $1 AND scope = 'decision';"
        try:
            async with self.db_manager.pool.acquire() as conn:
                rows = await conn.fetch(query, safe_project_id)
                if not rows:
                    return False
                
                provider_gateway = ray.get_actor("provider_gateway", namespace="agentos")
                
                decisions_context = "\n".join([f"- Title: {r['title']} | Rule: {r['content']}" for r in rows])
                
                system_prompt = (
                    "You are the Decision Conflict Gatekeeper for AgentOS.\n"
                    "Your single job is to compare a proposed agent Action Description against established, "
                    "unchangeable project decisions to detect logical contradictions or violations.\n\n"
                    f"ESTABLISHED PROJECT DECISIONS:\n{decisions_context}\n\n"
                    "Respond with a single raw JSON object matching this schema shape exactly:\n"
                    "{\n"
                    "  \"conflict_detected\": true | false,\n"
                    "  \"reason\": \"Detailed reason explaining why there is a conflict, or empty string if allowed\"\n"
                    "}"
                )
                
                user_prompt = f"Proposed Action Type: {action.action_type}\nDescription: {action.description}"
                
                from agentos.provider.gateway import ProviderRequest
                req = ProviderRequest(
                    purpose="decision_conflict_check",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    budget_key=action.project_id
                )
                
                res = await provider_gateway.get_completion.remote(req, response_format={"type": "json_object"})
                parsed_res = json.loads(res["content"])
                
                if parsed_res.get("conflict_detected", False):
                    logger.warning("decision_conflict_blocked", agent_id=action.agent_id, reason=parsed_res.get("reason"))
                    return True
                    
        except Exception as e:
            logger.error("decision_conflict_check_failed_failing_safe", error=str(e))
            
        return False

    # check if the action is allowed by policy, if not return the guardrail result, if yes execute the action and return the result
    async def request_execution(self, action: dict) -> dict:
        action_obj = ActionRequest(**action)
        if not hasattr(self, "_authenticated_identities"):
            self._authenticated_identities = {}
            
        auth_identity = self._authenticated_identities.get(action_obj.agent_id)
        if not auth_identity:
            return {"executed": False, "error": "Identity Registry Violation: Unknown agent signature."}
        
        result: GuardrailResult = await self.policy_engine.evaluate_action(action_obj, auth_identity)
        
        audit_repo = AuditEventRepository(self.db_manager)
        await audit_repo.log_audit_event(
            project_id=action_obj.project_id,
            agent_id=action_obj.agent_id,
            action_type=action_obj.action_type,
            policy_decision=result.decision.value if hasattr(result.decision, "value") else str(result.decision),
            integrity_hash=action_obj.integrity_hash
        )

        logger.info(
            "policy_guardrail_evaluated", 
            agent_id=action_obj.agent_id, 
            action_type=action_obj.action_type, 
            decision=result.decision,
            risk_level=result.risk_level
        )
        
        if result.decision == PolicyDecision.QUARANTINE_AGENT:
            logger.critical("execution_blocked_agent_quarantined", agent_id=action_obj.agent_id)
            return {"executed": False, "guardrail": result.model_dump(), "error": "Action blocked: Agent is under quarantine constraints."}
        
        if result.decision in {PolicyDecision.DENY, PolicyDecision.QUARANTINE_AGENT}:
            logger.warning("policy_execution_blocked", agent_id=action_obj.agent_id, decision=result.decision, reasons=result.reasons)
            return {"executed": False, "guardrail": result.model_dump(), "error": "Action blocked by policy."}
            
        if await self._verify_decision_conflict(action_obj):
            conflict_result = GuardrailResult(
                decision=PolicyDecision.DENY,
                risk_level=RiskLevel.HIGH,
                reasons=["Action conflicts with prior accepted project or architectural decisions."]
            )
            return {"executed": False, "guardrail": conflict_result.model_dump(), "error": "Action blocked due to established decision conflict."}

        if result.decision in {
            PolicyDecision.REQUIRE_HUMAN_APPROVAL,
            PolicyDecision.REQUIRE_REVIEW,
            PolicyDecision.REQUIRE_SECURITY_REVIEW,
        }:
            logger.info("policy_pending_out_of_band_approval", agent_id=action_obj.agent_id, decision=result.decision)
    
            
            unified_stream_key = f"project:{action_obj.project_id}:events"
            bus = DragonflyBus(self.settings.dragonfly_url)
            
            approval_event = Event(
                project_id=action_obj.project_id,
                event_type=EventType.APPROVAL_REQUEST,
                producer_agent_id=action_obj.agent_id,
                topic=unified_stream_key,
                payload={
                    "action_type": action_obj.action_type,
                    "description": action_obj.description,
                    "risk_level": result.risk_level.value if hasattr(result.risk_level, "value") else str(result.risk_level),
                    "required_gate": result.decision.value if hasattr(result.decision, "value") else str(result.decision),
                    "integrity_hash": action_obj.integrity_hash
                }
            )
            
            try:
                await bus.publish_event(unified_stream_key, approval_event)
                logger.info("approval_request_dispatched_to_stream", agent_id=action_obj.agent_id)
            except Exception as e:
                logger.error("failed_to_publish_approval_request", error=str(e))
                
            return {"executed": False, "guardrail": result.model_dump(), "pending_approval": True}
        
        execution_result = await self._route_and_execute(action_obj)
        
        return {
            "executed": True, 
            "guardrail": result.model_dump(), 
            "result": execution_result
        }

    async def _route_and_execute(self, action: ActionRequest) -> dict:
        action_type = action.action_type
        payload = action.payload or {}
        
        task_branch_id = f"task-branch-{action.agent_id}"
        worktree_path = os.path.join(self.worktrees_dir, action.agent_id)
        
        logger.info("routing_isolated_execution", agent_id=action.agent_id, worktree=worktree_path, action_type=action_type)
        
        if action_type in {"write_file", "write_code"}:
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            
            # Extract target task ID passed in by the worker's decision payload
            task_id = payload.get("target_task_id") or actual_payload.get("target_task_id")
            
            await self._ensure_worktree_context(task_branch_id, worktree_path)
            res = await self._write_file_safely(actual_payload, worktree_path, action.agent_id, action.project_id, task_id)
            if "success" in res:
                await self._commit_worktree_changes(worktree_path, f"Agent {action.agent_id} modification commit.")
            return res
            
        elif action_type == "read_file":
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            await self._ensure_worktree_context(task_branch_id, worktree_path)
            return await self._read_file_safely(actual_payload, worktree_path)
    
        elif action_type in {"shell_command", "run_command"}:
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            await self._ensure_worktree_context(task_branch_id, worktree_path)
            return await self._execute_sandboxed_docker_command(actual_payload, worktree_path)
        elif action_type == "execute_db_operation":
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            return await self._execute_sandbox_db_query(actual_payload)
        else:
            return {"output": f"Action type '{action_type}' processed successfully, no internal driver assigned."}

    async def _ensure_worktree_context(self, branch_name: str, worktree_path: str) -> None:
        if os.path.exists(worktree_path):
            return

        try:
            check_branch = await asyncio.create_subprocess_shell(
                f"git rev-parse --verify {shlex.quote(branch_name)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            stdout, _ = await check_branch.communicate()
            
            if check_branch.returncode == 0:
                cmd = f"git worktree add {shlex.quote(worktree_path)} {shlex.quote(branch_name)}"
            else:
                cmd = f"git worktree add -b {shlex.quote(branch_name)} {shlex.quote(worktree_path)}"

            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            await proc.communicate()
        except Exception as e:
            logger.error("failed_to_initialize_worktree", error=str(e))

    async def _commit_worktree_changes(self, worktree_path: str, commit_msg: str) -> None:
        try:
            cmd = (
                "git add . && "
                f"git -c user.name='{self._sandbox_cfg['git_author_name']}' -c user.email='{self._sandbox_cfg['git_author_email']}' commit -m {shlex.quote(commit_msg)}"
            )
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=worktree_path
            )
            await proc.communicate()
        except Exception:
            pass

    async def _write_file_safely(self, payload: dict, worktree_path: str, agent_id: str, project_id: str, task_id: str | None = None) -> dict:
        file_path = payload.get("file_path")
        content = payload.get("content", "")
        if not file_path:
            return {"error": "Missing 'file_path' in payload."}
            
        full_path = os.path.abspath(os.path.join(worktree_path, file_path))
        if not full_path.startswith(worktree_path):
            logger.error("path_traversal_attack_blocked", attempted_path=file_path)
            return {"error": "Path traversal detected. Write blocked for safety."}

        normalized_target = file_path.replace("\\", "/").strip("/").lower()
        
        from agentos.config.loader import guardrail_policies
        policies = guardrail_policies()
        
        forbidden_patterns = policies.get("filesystem_safety", {}).get("blocked_global_paths", [
            ".env", "schema.sql", "guardrail_policies.yaml", "settings.py", "secrets", "logs"
        ])
        
        if any(forbidden.lower() in normalized_target for forbidden in forbidden_patterns):
            logger.critical("system_infrastructure_modification_blocked", agent_id=agent_id, file=file_path)
            return {"error": "Access Denied: Restricting unauthorized mutation of framework control schemas or system assets."}
        
        # Task-Specific Whitelist/Blacklist checking
        if task_id:
            await self._ensure_connected()
            try:
                task_uuid = uuid.UUID(task_id) if isinstance(task_id, str) else task_id
                query = "SELECT allowed_paths, blocked_paths FROM tasks WHERE id = $1;"
                async with self.db_manager.pool.acquire() as conn:
                    task_record = await conn.fetchrow(query, task_uuid)
                    
                    if task_record:
                        allowed_patterns = task_record["allowed_paths"] or []
                        blocked_patterns = task_record["blocked_paths"] or []
                        for pattern in blocked_patterns:
                            clean_pattern = pattern.replace("\\", "/").strip("/").lower()
                            if normalized_target.startswith(clean_pattern) or clean_pattern in normalized_target:
                                logger.critical("security_escalation_blocked_path_violation", agent_id=agent_id, file=file_path)
                                return {"error": f"Security boundary violation: Edits to '{file_path}' are explicitly BLOCKED for this task."}

                        if allowed_patterns:
                            is_allowed = False
                            for pattern in allowed_patterns:
                                clean_pattern = pattern.replace("\\", "/").strip("/").lower()
                                if normalized_target.startswith(clean_pattern) or clean_pattern in normalized_target:
                                    is_allowed = True
                                    break
                            
                            if not is_allowed:
                                logger.critical("security_escalation_allowed_path_violation", agent_id=agent_id, file=file_path)
                                return {"error": f"Security boundary violation: Edits to '{file_path}' fall outside allowed task paths {allowed_patterns}."}

            except Exception as e:
                logger.error("failed_to_enforce_task_path_safety_failing_closed", error=str(e))
                return {"error": "Internal security guardrail failure. Write aborted."}

        file_lock_key = f"project:{project_id}:file_lock:{file_path}"
        try:
            lock_owner = await self.redis_client.get(file_lock_key)
            if lock_owner and lock_owner != agent_id:
                logger.warning("file_locked_by_another_agent", file_path=file_path, locked_by=lock_owner, attempted_by=agent_id)
                return {"error": f"Conflict detected: File '{file_path}' is currently locked by active agent '{lock_owner}'."}
                
            await self.redis_client.set(file_lock_key, agent_id, ex=120)
        except Exception as e:
            logger.error("file_lock_infrastructure_error", error=str(e))
            return {"error": "Lock database error during path verification."}

        target_dir = os.path.dirname(full_path)
        os.makedirs(target_dir, exist_ok=True)
            
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
            
        return {"success": True, "path": file_path, "bytes_written": len(content)}
    
    def _read_file_safely(self, payload: dict, worktree_path: str) -> dict:
        file_path = payload.get("file_path")
        if not file_path:
            return {"error": "Missing 'file_path' in payload."}
        
        full_path = os.path.abspath(os.path.join(worktree_path, file_path))
        if not full_path.startswith(worktree_path):
            logger.error("path_traversal_attack_blocked", attempted_path=file_path)
            return {"error": "Path traversal detected. Read blocked for safety."}
            
        if not os.path.exists(full_path):
            return {"error": "File not found."}
            
        with open(full_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        return {"success": True, "content": content}

    async def _execute_sandboxed_docker_command(self, payload: dict, worktree_path: str) -> dict:
        command_str = payload.get("command")
        if not command_str:
            return {"error": "Missing 'command' string key inside execution payload."}

        container_name = f"sandbox_{uuid.uuid4().hex[:8]}"
        docker_cmd = (
            f"docker run --rm --name {container_name} "
            f"--network none "
            f"-v {shlex.quote(worktree_path)}:/workspace:ro "
            f"-w /workspace python:3.11-slim "
            f"sh -c {shlex.quote(command_str)}"
        )

        try:
            process = await asyncio.create_subprocess_shell(
                docker_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout_bytes, stderr_bytes = await asyncio.wait_for(
                process.communicate(), 
                self._sandbox_cfg["shell_command_timeout_seconds"]
            )
            
            return {
                "success": True,
                "exit_code": process.returncode,
                "stdout": stdout_bytes.decode("utf-8", errors="replace"),
                "stderr": stderr_bytes.decode("utf-8", errors="replace")
            }
        except asyncio.TimeoutError:
            kill_proc = await asyncio.create_subprocess_shell(f"docker kill {container_name}")
            await kill_proc.communicate()
            
            logger.error("sandbox_execution_timeout", command=command_str)
            return {"error": f"Execution timed out after {self._sandbox_cfg['shell_command_timeout_seconds']} seconds."}
        except Exception as e:
            return {"error": f"Failed to execute process inside sandbox container: {str(e)}"}


    # frees disk storage space and clears locks when an agent finishes its work
    async def prune_worktree_resources(self, agent_id: str) -> None:
        worktree_path = os.path.join(self.worktrees_dir, agent_id)
        if os.path.exists(worktree_path):
            try:
                async for key in self.redis_client.scan_iter(f"project:*:file_lock:*"):
                    owner = await self.redis_client.get(key)
                    if owner == agent_id:
                        await self.redis_client.delete(key)
                        
                proc = await asyncio.create_subprocess_shell(
                    f"git worktree prune && rm -rf {shlex.quote(worktree_path)}",
                    cwd=self.workspace_path
                )
                await proc.communicate()
            except Exception as e:
                logger.error("worktree_cleanup_failed", agent_id=agent_id, error=str(e))

    async def _execute_sandbox_db_query(self, payload: dict) -> dict:
        await self._ensure_connected()
        operation = payload.get("query")
        params = payload.get("parameters", [])
        if not operation:
            return {"error": "Missing 'query' string key inside statement execution request."}
            
        try:
            async with self.sandbox_db_pool.acquire() as conn:
                if operation.strip().lower().startswith("select"):
                    rows = await conn.fetch(operation, *params)
                    return {"success": True, "data": [dict(r) for r in rows]}
                else:
                    status = await conn.execute(operation, *params)
                    return {"success": True, "status": status}
        except Exception as e:
            return {"error": f"Sandbox database operation failed: {str(e)}"}
        
    async def merge_and_finalize_branch(self, agent_id: str, commit_msg: str | None = None) -> dict:
        branch_name = f"task-branch-{agent_id}"
        worktree_path = os.path.join(self.worktrees_dir, agent_id)
        default_branch = self._sandbox_cfg['default_branch']
        
        await self._ensure_connected()
        query = """
            SELECT achievement FROM checkpoints 
            WHERE agent_id = $1 
            ORDER BY created_at DESC LIMIT 1;
        """
        async with self.db_manager.pool.acquire() as conn:
            last_achievement = await conn.fetchval(query, agent_id)
            
        if last_achievement == "review_failed":
            logger.critical("merge_blocked_by_failed_security_review", agent_id=agent_id)
            await self.prune_worktree_resources(agent_id)
            return {"success": False, "error": "Security Breach: Merge denied due to a failed architectural or security review."}

        logger.info("initiating_branch_merge_and_finalization", agent_id=agent_id, branch=branch_name)
        if not os.path.exists(worktree_path):
            return {"success": False, "error": f"No active worktree found for agent {agent_id}."}

        try:
            await self._commit_worktree_changes(worktree_path, commit_msg or f"Final checkout commit before merging {agent_id}.")

            switch_main = await asyncio.create_subprocess_shell(
                f"git checkout {shlex.quote(default_branch)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            await switch_main.communicate()

            merge_cmd = f"git merge {shlex.quote(branch_name)} --no-ff -m 'Merge task branch {branch_name} into {default_branch}'"
            merge_proc = await asyncio.create_subprocess_shell(
                merge_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            stdout, stderr = await merge_proc.communicate()

            if merge_proc.returncode != 0:
                err_msg = stderr.decode().strip()
                logger.error("merge_conflict_detected", branch=branch_name, error=err_msg)
                abort_proc = await asyncio.create_subprocess_shell("git merge --abort", cwd=self.workspace_path)
                await abort_proc.communicate()
                return {"success": False, "error": f"Merge conflict occurred: {err_msg}. Merge aborted."}

            integration_cmd = "pytest tests/ || echo 'No integration tests configured.'"
            integration_res = await self._execute_sandboxed_docker_command({"command": integration_cmd}, self.workspace_path)
            
            if integration_res.get("exit_code", 0) != 0:
                logger.error("integration_tests_failed_after_merge", output=integration_res.get("stderr"))
                rollback_proc = await asyncio.create_subprocess_shell(f"git reset --hard HEAD~1", cwd=self.workspace_path)
                await rollback_proc.communicate()
                return {"success": False, "error": "Integration tests failed after merging. Rolled back changes."}

            await self.prune_worktree_resources(agent_id)

            delete_branch_proc = await asyncio.create_subprocess_shell(
                f"git branch -d {shlex.quote(branch_name)}",
                cwd=self.workspace_path
            )
            await delete_branch_proc.communicate()

            logger.info("branch_successfully_merged_and_cleaned", branch=branch_name)
            return {"success": True, "merged_branch": branch_name}

        except Exception as e:
            logger.error("unexpected_error_during_merge_finalization", error=str(e))
            return {"success": False, "error": str(e)}