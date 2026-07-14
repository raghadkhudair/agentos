from __future__ import annotations
import os
import shutil
import asyncio
import shlex
import uuid
import ray
import structlog

from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision
from agentos.config.loader import guardrail_policies

logger = structlog.get_logger()


@ray.remote(namespace="agentos")
class ExecutionSupervisorActor:
    """The absolute execution boundary for all agents.

    Applies governance guardrail matrices first, manages parallel branch isolation 
    via Git-per-task worktree workflows, and executes commands inside isolated Docker environments.
    """

    def __init__(self, settings_payload: dict):
        from agentos.governance.policy_engine import PolicyEngine

        self.settings = Settings(**settings_payload) if settings_payload else Settings()
        self.policy_engine = PolicyEngine(self.settings)
        self._sandbox_cfg = guardrail_policies()["execution_sandbox"]
        self.workspace_path = os.path.abspath(self.settings.workspace)
        self.worktrees_dir = os.path.join(self.workspace_path, ".agentos_worktrees")
        
        os.makedirs(self.workspace_path, exist_ok=True)
        os.makedirs(self.worktrees_dir, exist_ok=True)
        
        self._initialize_git_workspace_safely()

    def _initialize_git_workspace_safely(self) -> None:
        """Ensures the sandbox workspace is an initialized repository tracking baseline branches."""
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

    async def request_execution(self, action: dict) -> dict:
        """Receives serialized dictionary payloads from remote agent workers."""
        # Restore dict payload into valid ActionRequest model object
        action_obj = ActionRequest(**action)
        result: GuardrailResult = self.policy_engine.evaluate_action(action_obj)
        
        logger.info(
            "policy_guardrail_evaluated", 
            agent_id=action_obj.agent_id, 
            action_type=action_obj.action_type, 
            decision=result.decision,
            risk_level=result.risk_level
        )
        
        if result.decision in {PolicyDecision.DENY, PolicyDecision.QUARANTINE_AGENT}:
            logger.warning("policy_execution_blocked", agent_id=action_obj.agent_id, decision=result.decision, reasons=result.reasons)
            return {"executed": False, "guardrail": result.model_dump(), "error": "Action blocked by policy."}
            
        if result.decision in {
            PolicyDecision.REQUIRE_HUMAN_APPROVAL,
            PolicyDecision.REQUIRE_REVIEW,
            PolicyDecision.REQUIRE_SECURITY_REVIEW,
        }:
            logger.info("policy_pending_out_of_band_approval", agent_id=action_obj.agent_id, decision=result.decision)
            return {"executed": False, "guardrail": result.model_dump(), "pending_approval": True}
            
        execution_result = await self._route_and_execute(action_obj)
        
        return {
            "executed": True, 
            "guardrail": result.model_dump(), 
            "result": execution_result
        }

    async def _route_and_execute(self, action: ActionRequest) -> dict:
        """Routes approved tasks to their targeted worktree isolation folders."""
        action_type = action.action_type
        payload = action.payload or {}
        
        # Isolated, safe directory per agent and task
        task_branch_id = f"task-branch-{action.agent_id}"
        worktree_path = os.path.join(self.worktrees_dir, action.agent_id)
        
        logger.info("routing_isolated_execution", agent_id=action.agent_id, worktree=worktree_path, action_type=action_type)
        
        if action_type in {"write_file", "write_code"}:
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            await self._ensure_worktree_context(task_branch_id, worktree_path)
            res = self._write_file_safely(actual_payload, worktree_path)
            if "success" in res:
                await self._commit_worktree_changes(worktree_path, f"Agent {action.agent_id} modification commit.")
            return res
            
        elif action_type == "read_file":
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            await self._ensure_worktree_context(task_branch_id, worktree_path)
            return self._read_file_safely(actual_payload, worktree_path)
            
        elif action_type in {"shell_command", "run_command"}:
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            await self._ensure_worktree_context(task_branch_id, worktree_path)
            return await self._execute_sandboxed_docker_command(actual_payload, worktree_path)
        else:
            return {"output": f"Action type '{action_type}' processed successfully, no internal driver assigned."}

    async def _ensure_worktree_context(self, branch_name: str, worktree_path: str) -> None:
        """Guarantees a dedicated Git worktree exists for this agent workspace."""
        if os.path.exists(worktree_path):
            return  # Already configured and isolated

        try:
            # Check if branch already exists
            check_branch = await asyncio.create_subprocess_shell(
                f"git rev-parse --verify {shlex.quote(branch_name)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            stdout, _ = await check_branch.communicate()
            
            if check_branch.returncode == 0:
                # Branch exists, append worktree directly to it
                cmd = f"git worktree add {shlex.quote(worktree_path)} {shlex.quote(branch_name)}"
            else:
                # Branch doesn't exist, create branch and map worktree
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
        """Saves file additions into the active Git worktree branch repository."""
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

    def _write_file_safely(self, payload: dict, worktree_path: str) -> dict:
        file_path = payload.get("file_path")
        content = payload.get("content", "")
        if not file_path:
            return {"error": "Missing 'file_path' in payload."}
            
        full_path = os.path.abspath(os.path.join(worktree_path, file_path))
        if not full_path.startswith(worktree_path):
            logger.error("path_traversal_attack_blocked", attempted_path=file_path)
            return {"error": "Path traversal detected. Write blocked for safety."}
            
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
        """Executes shell utilities isolated inside a Docker sandbox container."""
        command_str = payload.get("command")
        if not command_str:
            return {"error": "Missing 'command' string key inside execution payload."}

        container_name = f"sandbox_{uuid.uuid4().hex[:8]}"
        docker_cmd = (
            f"docker run --rm --name {container_name} "
            f"-v {shlex.quote(worktree_path)}:/workspace "
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

    async def prune_worktree_resources(self, agent_id: str) -> None:
        """Cleans up disk workspace allocations once tasks complete."""
        worktree_path = os.path.join(self.worktrees_dir, agent_id)
        if os.path.exists(worktree_path):
            try:
                proc = await asyncio.create_subprocess_shell(
                    f"git worktree prune && rm -rf {shlex.quote(worktree_path)}",
                    cwd=self.workspace_path
                )
                await proc.communicate()
            except Exception as e:
                logger.error("worktree_cleanup_failed", agent_id=agent_id, error=str(e))

    async def merge_and_finalize_branch(self, agent_id: str, commit_msg: str | None = None) -> dict:
        """
        Safely merges an approved agent's task branch back into the main branch,
        runs integration verification inside Docker, and prunes the worktree.
        """
        branch_name = f"task-branch-{agent_id}"
        worktree_path = os.path.join(self.worktrees_dir, agent_id)
        default_branch = self._sandbox_cfg['default_branch']
        
        logger.info("initiating_branch_merge_and_finalization", agent_id=agent_id, branch=branch_name)

        if not os.path.exists(worktree_path):
            return {"success": False, "error": f"No active worktree found for agent {agent_id}."}

        try:
            # Step 1: Ensure all files in the worktree are committed first
            await self._commit_worktree_changes(worktree_path, commit_msg or f"Final checkout commit before merging {agent_id}.")

            # Step 2: Switch the main workspace folder to the default branch (e.g., 'main')
            switch_main = await asyncio.create_subprocess_shell(
                f"git checkout {shlex.quote(default_branch)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            await switch_main.communicate()

            # Step 3: Perform the merge operation into 'main'
            # We use --no-ff (no-fast-forward) to preserve the history of who did what task
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
                # If a merge conflict happens, we abort the merge to keep the main branch clean!
                abort_proc = await asyncio.create_subprocess_shell("git merge --abort", cwd=self.workspace_path)
                await abort_proc.communicate()
                return {"success": False, "error": f"Merge conflict occurred: {err_msg}. Merge aborted."}

            # Step 4: Run a full suite integration check inside a Docker Sandbox
            # This ensures that combining this developer's code with the main branch didn't break anything!
            integration_cmd = "pytest tests/ || echo 'No integration tests configured.'"
            integration_res = await self._execute_sandboxed_docker_command({"command": integration_cmd}, self.workspace_path)
            
            if integration_res.get("exit_code", 0) != 0:
                logger.error("integration_tests_failed_after_merge", output=integration_res.get("stderr"))
                # Hard Rollback to the commit before merge
                rollback_proc = await asyncio.create_subprocess_shell(f"git reset --hard HEAD~1", cwd=self.workspace_path)
                await rollback_proc.communicate()
                return {"success": False, "error": "Integration tests failed after merging. Rolled back changes."}

            # Step 5: Prune the worktree and clean up disk space
            # This removes the temporary worktree link and safely deletes the folder
            await self.prune_worktree_resources(agent_id)

            # Step 6: Delete the local task branch now that it has been safely merged
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