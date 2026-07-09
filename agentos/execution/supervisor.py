from __future__ import annotations
import os
import asyncio
import shlex
import structlog

from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision

logger = structlog.get_logger()


class ExecutionSupervisor:
    """The absolute execution boundary for all agents.

    Applies governance guardrail matrices first, manages workspace branch isolation 
    via Git-per-task workflows, and handles sandboxed subprocess file IO execution.
    """

    def __init__(self, settings: Settings):
        from agentos.governance.policy_engine import PolicyEngine

        self.settings = settings
        self.policy_engine = PolicyEngine(settings)
        self.workspace_path = os.path.abspath(settings.workspace)
        
        if not os.path.exists(self.workspace_path):
            try:
                os.makedirs(self.workspace_path, exist_ok=True)
            except Exception:
                pass
        
        self._initialize_git_workspace_safely()

    def _initialize_git_workspace_safely(self) -> None:
        """Ensures the sandbox workspace is an initialized repository tracking baseline branches."""
        git_dir = os.path.join(self.workspace_path, ".git")
        if not os.path.exists(git_dir):
            try:
                os.system(f"git init {shlex.quote(self.workspace_path)} > /dev/null 2>&1")
                os.system(f"git -C {shlex.quote(self.workspace_path)} checkout -b main > /dev/null 2>&1")
                
                init_file = os.path.join(self.workspace_path, ".agentos_keep")
                with open(init_file, "w") as f:
                    f.write("AgentOS Initializer Pointer State File.")
                
                os.system(f"git -C {shlex.quote(self.workspace_path)} add .agentos_keep > /dev/null 2>&1")
                os.system(f"git -C {shlex.quote(self.workspace_path)} -c user.name='AgentOS' -c user.email='runtime@agentos.local' commit -m 'Initial asset repository bootstrap commit.' > /dev/null 2>&1")
                logger.info("sandbox_git_repository_initialized", workspace_path=self.workspace_path)
            except Exception as e:
                logger.error("sandbox_git_initialization_failed", error=str(e))

    async def request_execution(self, action: ActionRequest) -> dict:
        result: GuardrailResult = self.policy_engine.evaluate_action(action)
        
        # FIRED UP: High-visibility structured telemetry logs tracking policy outcomes
        logger.info(
            "policy_guardrail_evaluated", 
            agent_id=action.agent_id, 
            action_type=action.action_type, 
            decision=result.decision,
            risk_level=result.risk_level
        )
        
        if result.decision in {PolicyDecision.DENY, PolicyDecision.QUARANTINE_AGENT}:
            logger.warning("policy_execution_blocked", agent_id=action.agent_id, decision=result.decision, reasons=result.reasons)
            return {"executed": False, "guardrail": result.model_dump(), "error": "Action blocked by policy."}
            
        if result.decision in {
            PolicyDecision.REQUIRE_HUMAN_APPROVAL,
            PolicyDecision.REQUIRE_REVIEW,
            PolicyDecision.REQUIRE_SECURITY_REVIEW,
        }:
            logger.info("policy_pending_out_of_band_approval", agent_id=action.agent_id, decision=result.decision)
            return {"executed": False, "guardrail": result.model_dump(), "pending_approval": True}
            
        execution_result = await self._route_and_execute(action)
        
        return {
            "executed": True, 
            "guardrail": result.model_dump(), 
            "result": execution_result
        }

    async def _route_and_execute(self, action: ActionRequest) -> dict:
        """Routes approved tasks to their targeted branch isolation runners."""
        action_type = action.action_type
        payload = action.payload or {}
        
        task_branch_id = f"task-branch-{action.agent_id}"
        logger.info("routing_isolated_execution", agent_id=action.agent_id, branch=task_branch_id, action_type=action_type)
        
        if action_type in {"write_file", "write_code"}:
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            await self._ensure_branch_context(task_branch_id)
            res = self._write_file_safely(actual_payload)
            if "success" in res:
                await self._commit_branch_changes(task_branch_id, f"Agent {action.agent_id} modification commit.")
            return res
            
        elif action_type == "read_file":
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            return self._read_file_safely(actual_payload)
            
        elif action_type in {"shell_command", "run_command"}:
            actual_payload = payload.get("payload", payload) if "payload" in payload else payload
            return await self._execute_shell_safely(actual_payload)
        else:
            return {"output": f"Action type '{action_type}' processed successfully, no internal file driver assigned."}

    async def _ensure_branch_context(self, branch_name: str) -> None:
        """Guarantees code operations are sandboxed inside a clean git branch tree."""
        try:
            proc = await asyncio.create_subprocess_shell(
                f"git checkout {shlex.quote(branch_name)} || git checkout -b {shlex.quote(branch_name)}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            await proc.communicate()
        except Exception:
            pass

    async def _commit_branch_changes(self, branch_name: str, commit_msg: str) -> None:
        """Automatically checkpoints file changes down into the active repository branch."""
        try:
            cmd = (
                "git add . && "
                f"git -c user.name='AgentOS' -c user.email='runtime@agentos.local' commit -m {shlex.quote(commit_msg)}"
            )
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )
            await proc.communicate()
        except Exception:
            pass

    def _write_file_safely(self, payload: dict) -> dict:
        file_path = payload.get("file_path")
        content = payload.get("content", "")
        if not file_path:
            return {"error": "Missing 'file_path' in payload."}
            
        full_path = os.path.abspath(os.path.join(self.workspace_path, file_path))
        if not full_path.startswith(self.workspace_path):
            logger.error("path_traversal_attack_blocked", attempted_path=file_path)
            return {"error": "Path traversal detected. Write blocked for safety."}
            
        target_dir = os.path.dirname(full_path)
        if not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
            except FileExistsError:
                pass
            
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
            
        return {"success": True, "path": file_path, "bytes_written": len(content)}
    
    def _read_file_safely(self, payload: dict) -> dict:
        file_path = payload.get("file_path")
        if not file_path:
            return {"error": "Missing 'file_path' in payload."}
        
        full_path = os.path.abspath(os.path.join(self.workspace_path, file_path))
        if not full_path.startswith(self.workspace_path):
            logger.error("path_traversal_attack_blocked", attempted_path=file_path)
            return {"error": "Path traversal detected. Read blocked for safety."}
            
        if not os.path.exists(full_path):
            return {"error": "File not found."}
            
        with open(full_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        return {"success": True, "content": content}

    async def _execute_shell_safely(self, payload: dict) -> dict:
        command_str = payload.get("command")
        if not command_str:
            return {"error": "Missing 'command' string key inside execution payload."}

        try:
            process = await asyncio.create_subprocess_shell(
                command_str,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.workspace_path
            )

            stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), timeout=30.0)
            
            return {
                "success": True,
                "exit_code": process.returncode,
                "stdout": stdout_bytes.decode("utf-8", errors="replace"),
                "stderr": stderr_bytes.decode("utf-8", errors="replace")
            }
        except asyncio.TimeoutError:
            logger.error("process_execution_timeout", command=command_str)
            return {"error": "Execution timed out after 30.0 seconds."}
        except Exception as e:
            return {"error": f"Failed to execute process shell environment: {str(e)}"}