from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import json
import math
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import asyncpg
import docker
import ray
import structlog

from agentos.config.loader import runtime_tuning
from agentos.config.runtime import ResourcePlanner
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, AgentIdentity, PolicyDecision
from agentos.governance.policy_engine import PolicyEngine
from agentos.messaging.events import Event, EventType
from agentos.storage.clients.dragonfly import DragonflyClient
from agentos.storage.clients.minio import MinioObjectClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import (
    ArtifactRepository,
    AuditEventRepository,
    DoDRepository,
    EventRepository,
    TaskRepository,
)

logger = structlog.get_logger()


class ExecutionService:
    """Controlled file, Git, test-container, and sandbox-database execution."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.db = PostgresClient(settings)
        self.dragonfly = DragonflyClient(settings)
        self.policy = PolicyEngine(settings, self.dragonfly)
        self.minio = MinioObjectClient(settings)
        self.tasks = TaskRepository(self.db)
        self.artifacts = ArtifactRepository(self.db)
        self.audit = AuditEventRepository(self.db)
        self.events = EventRepository(self.db)
        self.dod = DoDRepository(self.db)
        self.identities: dict[str, AgentIdentity] = {}
        self.tuning = runtime_tuning()["execution"]
        self.workspace = settings.workspace.resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        self._sandbox_pool: asyncpg.Pool | None = None
        self._repository_lock = asyncio.Lock()
        self._worktree_locks: dict[str, asyncio.Lock] = {}

    async def register_identity(self, identity: AgentIdentity) -> None:
        row = await self.db.fetchrow(
            "SELECT role,permissions,memory_scopes FROM agents WHERE project_id=$1 AND id=$2",
            UUID(identity.project_id),
            identity.agent_id,
        )
        if row is None or row["role"] != identity.role:
            raise PermissionError("execution identity is not registered for this project and role")
        permissions = row["permissions"] or {}
        allowed_actions = set(permissions.get("allowed_actions", []))
        allowed_paths = set(permissions.get("ownership_domains", []))
        if not set(identity.allowed_actions).issubset(allowed_actions):
            raise PermissionError("execution identity requests actions outside its persisted role")
        if not set(identity.allowed_paths).issubset(allowed_paths):
            raise PermissionError(
                "execution identity requests paths outside its persisted ownership"
            )
        existing = self.identities.get(identity.agent_id)
        if existing is not None and existing != identity:
            raise PermissionError("execution identity is already registered with different claims")
        self.identities[identity.agent_id] = identity

    def _project_repository(self, project_id: str) -> Path:
        UUID(project_id)
        return (self.workspace / project_id / "repository").resolve()

    def _worktree_path(self, project_id: str, task_id: str) -> Path:
        UUID(task_id)
        return (self.workspace / project_id / "worktrees" / task_id).resolve()

    async def _run_host(
        self, *args: str, cwd: Path, timeout_seconds: float = 30
    ) -> tuple[int, str, str]:
        process = await asyncio.create_subprocess_exec(
            *args,
            cwd=str(cwd),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout_seconds)
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise TimeoutError(f"host command timed out: {args[0]}") from error
        return (
            process.returncode or 0,
            stdout.decode(errors="replace"),
            stderr.decode(errors="replace"),
        )

    async def _ensure_repository(self, project_id: str) -> Path:
        repository = self._project_repository(project_id)
        async with self._repository_lock:
            repository.parent.mkdir(parents=True, exist_ok=True)
            if not (repository / ".git").is_dir():
                if repository.exists() and any(repository.iterdir()):
                    raise RuntimeError("managed repository exists but is not a Git repository")
                if self.settings.source_repository is not None:
                    source = self.settings.source_repository
                    code, output, _ = await self._run_host(
                        "git", "rev-parse", "--is-inside-work-tree", cwd=source
                    )
                    if code or output.strip() != "true":
                        raise RuntimeError("AGENTOS_SOURCE_REPOSITORY is not a Git worktree")
                    if repository.exists():
                        repository.rmdir()
                    code, _, error = await self._run_host(
                        "git",
                        "clone",
                        "--no-hardlinks",
                        "--",
                        str(source),
                        str(repository),
                        cwd=repository.parent,
                        timeout_seconds=120,
                    )
                    if code:
                        raise RuntimeError(f"git clone failed: {error[:500]}")
                    await self._verify_repository_state(project_id, repository)
                    return repository
                repository.mkdir(parents=True, exist_ok=True)
                code, _, error = await self._run_host("git", "init", "-b", "main", cwd=repository)
                if code:
                    raise RuntimeError(f"git init failed: {error[:500]}")
                keep = repository / ".agentos_keep"
                keep.write_text("AgentOS managed repository.\n", encoding="utf-8")
                await self._run_host("git", "add", ".agentos_keep", cwd=repository)
                code, _, error = await self._run_host(
                    "git",
                    "-c",
                    "user.name=AgentOS",
                    "-c",
                    "user.email=runtime@agentos.local",
                    "commit",
                    "-m",
                    "Initialize AgentOS workspace",
                    cwd=repository,
                )
                if code:
                    raise RuntimeError(f"initial commit failed: {error[:500]}")
            await self._verify_repository_state(project_id, repository)
        return repository

    async def _verify_repository_state(self, project_id: str, repository: Path) -> str:
        project = await self.db.fetchrow(
            "SELECT source_revision,integration_head FROM projects WHERE id=$1", UUID(project_id)
        )
        if project is None:
            raise LookupError(f"project not found: {project_id}")
        code, output, error = await self._run_host("git", "rev-parse", "HEAD", cwd=repository)
        if code:
            raise RuntimeError(f"managed repository HEAD is unavailable: {error[:500]}")
        head = output.strip()
        expected = project["integration_head"] or project["source_revision"]
        if expected and expected != "EMPTY_WORKSPACE" and head != expected:
            pending = await self.db.fetchrow(
                """
                SELECT * FROM integration_attempts WHERE project_id=$1 AND status='PREPARED'
                ORDER BY created_at DESC LIMIT 1
                """,
                UUID(project_id),
            )
            valid_pending_merge = False
            if pending:
                code, parents, _ = await self._run_host(
                    "git", "show", "-s", "--format=%P", head, cwd=repository
                )
                parent_set = set(parents.split()) if code == 0 else set()
                valid_pending_merge = {
                    pending["pre_head"],
                    pending["branch_head"],
                }.issubset(parent_set)
            if not valid_pending_merge:
                raise RuntimeError(
                    f"managed repository drift detected: expected {expected}, observed {head}"
                )
        return head

    async def _ensure_worktree(self, project_id: str, task_id: str) -> Path:
        repository = await self._ensure_repository(project_id)
        worktree = self._worktree_path(project_id, task_id)
        lock = self._worktree_locks.setdefault(task_id, asyncio.Lock())
        async with lock:
            if worktree.is_dir():
                return worktree
            worktree.parent.mkdir(parents=True, exist_ok=True)
            branch = f"agentos/task-{task_id}"
            code, _, _ = await self._run_host(
                "git", "show-ref", "--verify", f"refs/heads/{branch}", cwd=repository
            )
            if code:
                command = ["git", "worktree", "add", "-b", branch, str(worktree), "HEAD"]
            else:
                command = ["git", "worktree", "add", str(worktree), branch]
            code, _, error = await self._run_host(*command, cwd=repository)
            if code:
                raise RuntimeError(f"git worktree creation failed: {error[:1000]}")
        return worktree

    @staticmethod
    def _resolve_relative(root: Path, relative: str) -> Path:
        normalized = PurePosixPath(relative.replace("\\", "/"))
        if normalized.is_absolute() or ".." in normalized.parts:
            raise PermissionError("unsafe relative path")
        target = (root / Path(*normalized.parts)).resolve()
        try:
            target.relative_to(root.resolve())
        except ValueError as error:
            raise PermissionError("path escapes assigned worktree") from error
        return target

    @staticmethod
    def _matches_boundary(relative: str, boundaries: list[str]) -> bool:
        normalized = PurePosixPath(relative.replace("\\", "/")).as_posix().strip("/")
        for boundary in boundaries:
            prefix = PurePosixPath(boundary.replace("\\", "/")).as_posix().strip("/")
            if (
                normalized == prefix
                or normalized.startswith(f"{prefix}/")
                or fnmatch.fnmatch(normalized, prefix)
            ):
                return True
        return False

    async def _owned_task(self, project_id: str, task_id: str, agent_id: str) -> dict[str, Any]:
        row = await self.db.fetchrow(
            """
            SELECT t.*,p.status AS project_status FROM tasks t
            JOIN projects p ON p.id=t.project_id WHERE t.id=$1
            """,
            UUID(task_id),
        )
        task = dict(row) if row else None
        if task is None:
            raise LookupError("task does not exist")
        if str(task["project_id"]) != project_id:
            raise PermissionError("task belongs to another project")
        if task.get("owner_agent_id") != agent_id:
            raise PermissionError("task is not actively leased to this agent")
        if task["project_status"] in {"DOD_SATISFIED", "FAILED_BY_POLICY", "STOPPED_BY_USER"}:
            raise ValueError("execution is immutable after project finalization")
        return task

    async def _task_path_check(
        self, project_id: str, task_id: str, agent_id: str, relative: str
    ) -> dict[str, Any]:
        task = await self._owned_task(project_id, task_id, agent_id)
        if task["blocked_paths"] and self._matches_boundary(relative, task["blocked_paths"]):
            raise PermissionError("path is blocked by task ownership")
        if task["allowed_paths"] and not self._matches_boundary(relative, task["allowed_paths"]):
            raise PermissionError("path is outside task allowlist")
        return task

    async def _commit(self, worktree: Path, message: str, paths: list[str]) -> str:
        approved = {PurePosixPath(item.replace("\\", "/")).as_posix() for item in paths}
        changed: set[str] = set()
        for command in (
            ("git", "diff", "--name-only", "--"),
            ("git", "diff", "--cached", "--name-only", "--"),
            ("git", "ls-files", "--others", "--exclude-standard", "--"),
        ):
            code, output, error = await self._run_host(*command, cwd=worktree)
            if code:
                raise RuntimeError(f"git change inspection failed: {error[:1000]}")
            changed.update(
                line.strip().replace("\\", "/") for line in output.splitlines() if line.strip()
            )
        unexpected = changed - approved
        if unexpected:
            raise PermissionError(
                f"worktree contains changes outside the approved action: {sorted(unexpected)}"
            )
        if not changed:
            raise ValueError("write produced no material repository change")
        await self._run_host("git", "add", "--", *sorted(approved), cwd=worktree)
        code, _, error = await self._run_host(
            "git",
            "-c",
            "user.name=AgentOS",
            "-c",
            "user.email=runtime@agentos.local",
            "commit",
            "-m",
            message[:200],
            cwd=worktree,
        )
        if code:
            raise RuntimeError(f"git commit failed: {error[:1000]}")
        _, commit, _ = await self._run_host("git", "rev-parse", "HEAD", cwd=worktree)
        return commit.strip()

    async def _write_file(self, action: ActionRequest) -> dict[str, Any]:
        if not action.task_id:
            raise ValueError("write_file requires task_id")
        relative = str(action.payload.get("file_path", ""))
        if not relative:
            raise ValueError("write_file requires payload.file_path")
        content = action.payload.get("content")
        if not isinstance(content, str):
            raise TypeError("write_file payload.content must be text")
        await self._task_path_check(action.project_id, action.task_id, action.agent_id, relative)
        worktree = await self._ensure_worktree(action.project_id, action.task_id)
        target = self._resolve_relative(worktree, relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_name = tempfile.mkstemp(prefix=".agentos-", dir=str(target.parent))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temp_name, target)
        finally:
            await asyncio.to_thread(Path(temp_name).unlink, missing_ok=True)
        commit = await self._commit(
            worktree,
            f"Task {action.task_id}: {action.description}",
            [normalized_relative := relative.replace("\\", "/")],
        )
        data = content.encode("utf-8")
        object_name = f"{action.project_id}/tasks/{action.task_id}/{normalized_relative}"
        object_metadata = await self.minio.put_bytes(
            bucket=self.settings.minio_artifacts_bucket,
            object_name=object_name,
            data=data,
            content_type="text/plain; charset=utf-8",
            metadata={"git-commit": commit, "agent-id": action.agent_id},
        )
        artifact_id = await self.artifacts.create_artifact(
            action.project_id,
            "FILE",
            relative,
            object_uri=object_metadata.uri,
            object_version_id=object_metadata.version_id,
            checksum_sha256=object_metadata.sha256,
            content_length=object_metadata.size,
            content_type="text/plain; charset=utf-8",
            task_id=action.task_id,
            summary=action.description,
            metadata={"git_commit": commit, "agent_id": action.agent_id},
        )
        await self.tasks.update_task_status(action.task_id, "UNDER_REVIEW")
        _, review_content, _ = await self._run_host(
            "git",
            "show",
            "--format=",
            "--no-ext-diff",
            "HEAD",
            "--",
            normalized_relative,
            cwd=worktree,
        )
        return {
            "path": relative,
            "git_commit": commit,
            "artifact_id": artifact_id,
            "object_uri": object_metadata.uri,
            "checksum_sha256": object_metadata.sha256,
            "review_content": review_content[:200_000],
        }

    async def _read_file(self, action: ActionRequest) -> dict[str, Any]:
        if not action.task_id:
            raise ValueError("read_file requires task_id")
        relative = str(action.payload.get("file_path", ""))
        await self._task_path_check(action.project_id, action.task_id, action.agent_id, relative)
        worktree = await self._ensure_worktree(action.project_id, action.task_id)
        target = self._resolve_relative(worktree, relative)
        if not target.is_file():
            raise FileNotFoundError(relative)
        if target.stat().st_size > 2_000_000:
            raise ValueError("read_file limit is 2 MB")
        return {"path": relative, "content": target.read_text(encoding="utf-8")}

    def _validate_sandbox_command(self, command: list[str]) -> None:
        if not command or command[0] not in set(self.tuning["allowed_executables"]):
            raise PermissionError("sandbox executable is not allowlisted")
        if any("\x00" in token or "\n" in token or "\r" in token for token in command):
            raise PermissionError("sandbox command contains control characters")

    async def _run_sandbox_at(
        self,
        project_id: str,
        location: Path,
        command: list[str],
        *,
        image: str | None = None,
    ) -> dict[str, Any]:
        self._validate_sandbox_command(command)
        selected_image = image or self.settings.sandbox_image
        if selected_image not in set(self.tuning["allowed_sandbox_images"]):
            raise PermissionError("sandbox image is not allowlisted")
        location = await asyncio.to_thread(location.resolve)
        location.relative_to(self.workspace)
        sandbox_contract = {
            "image": selected_image,
            "command": command,
            "network_disabled": True,
            "read_only": True,
            "cpu": self.settings.sandbox_cpu_limit,
            "memory": self.settings.sandbox_memory_bytes,
            "pids": self.settings.sandbox_pids_limit,
        }
        sandbox_digest = hashlib.sha256(
            json.dumps(sandbox_contract, sort_keys=True).encode("utf-8")
        ).hexdigest()

        def run_container() -> tuple[int, str]:
            client = (
                docker.DockerClient(base_url=self.settings.docker_host)
                if self.settings.docker_host
                else docker.from_env()
            )
            if self.settings.sandbox_workspace_volume:
                volumes = {
                    self.settings.sandbox_workspace_volume: {"bind": "/workspace", "mode": "ro"}
                }
                relative = location.relative_to(self.workspace).as_posix()
                working_dir = f"/workspace/{relative}"
            else:
                volumes = {str(location): {"bind": "/workspace", "mode": "ro"}}
                working_dir = "/workspace"
            container = None
            try:
                container = client.containers.run(
                    image=selected_image,
                    command=command,
                    working_dir=working_dir,
                    user="65532:65532",
                    detach=True,
                    network_disabled=True,
                    read_only=True,
                    tmpfs={"/tmp": "rw,noexec,nosuid,size=256m"},  # noqa: S108
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges:true"],
                    pids_limit=self.settings.sandbox_pids_limit,
                    mem_limit=self.settings.sandbox_memory_bytes,
                    nano_cpus=int(self.settings.sandbox_cpu_limit * 1_000_000_000),
                    volumes=volumes,
                    environment={
                        "PYTHONDONTWRITEBYTECODE": "1",
                        "HOME": "/tmp",  # noqa: S108 - container-private tmpfs
                        **{
                            key: str(
                                min(
                                    self.settings.max_threads_per_agent,
                                    max(1, math.floor(self.settings.sandbox_cpu_limit)),
                                )
                            )
                            for key in ResourcePlanner.THREAD_ENV_KEYS
                        },
                    },
                )
                result = container.wait(timeout=float(self.tuning["command_timeout_seconds"]))
                output = container.logs(stdout=True, stderr=True).decode(errors="replace")
                return (
                    int(result.get("StatusCode", 1)),
                    output[: int(self.tuning["max_output_characters"])],
                )
            finally:
                if container is not None:
                    container.remove(force=True)
                client.close()

        exit_code, output = await asyncio.to_thread(run_container)
        return {
            "exit_code": exit_code,
            "output": output,
            "sandbox_digest": sandbox_digest,
            "sandbox_image": selected_image,
            "project_id": project_id,
        }

    async def _sandbox_command(self, action: ActionRequest) -> dict[str, Any]:
        if not action.task_id:
            raise ValueError("shell_command requires task_id")
        command = action.command or action.payload.get("command")
        if not isinstance(command, list) or not all(isinstance(token, str) for token in command):
            raise TypeError("sandbox command must be a token array, never a shell string")
        self._validate_sandbox_command(command)
        await self._owned_task(action.project_id, action.task_id, action.agent_id)
        worktree = await self._ensure_worktree(action.project_id, action.task_id)
        image = str(action.payload.get("image", self.settings.sandbox_image))
        result = await self._run_sandbox_at(action.project_id, worktree, command, image=image)
        code, commit, error = await self._run_host("git", "rev-parse", "HEAD", cwd=worktree)
        if code:
            raise RuntimeError(f"task worktree HEAD is unavailable: {error[:500]}")
        return {**result, "git_commit": commit.strip()}

    async def _sandbox_database(self, action: ActionRequest) -> dict[str, Any]:
        if not action.task_id:
            raise ValueError("execute_db_operation requires task_id")
        await self._owned_task(action.project_id, action.task_id, action.agent_id)
        if self.settings.sandbox_database_url is None:
            raise RuntimeError("SANDBOX_DATABASE_URL is required for database execution")
        dsn = self.settings.sandbox_database_url.get_secret_value()
        if dsn == self.settings.postgres_dsn:
            raise RuntimeError(
                "sandbox database must be physically separate from the control database"
            )
        sql = action.database_operation or action.payload.get("query")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("database operation is empty")
        first = re.match(r"^\s*([A-Za-z]+)", sql)
        if not first or first.group(1).upper() not in {
            "SELECT",
            "INSERT",
            "UPDATE",
            "CREATE",
            "ALTER",
        }:
            raise PermissionError("database statement class is not permitted")
        if ";" in sql.rstrip().rstrip(";"):
            raise PermissionError("multiple database statements are forbidden")
        if self._sandbox_pool is None:
            self._sandbox_pool = await asyncpg.create_pool(
                dsn=dsn, min_size=1, max_size=2, command_timeout=30
            )
        parameters = action.payload.get("parameters", [])
        async with self._sandbox_pool.acquire() as connection:
            async with connection.transaction():
                result = await connection.execute(sql, *parameters)
        return {"status": result}

    async def request_execution(self, action_data: dict[str, Any]) -> dict[str, Any]:
        action = ActionRequest.model_validate(action_data)
        now = datetime.now(UTC)
        if action.issued_at < now - timedelta(minutes=5) or action.issued_at > now + timedelta(
            minutes=1
        ):
            return {"executed": False, "error": "expired or future-dated action request"}
        replay_key = self.dragonfly.key(
            "action_nonce", action.project_id, action.agent_id, action.nonce
        )
        if not await self.dragonfly.redis.set(replay_key, "1", ex=86_400, nx=True):
            return {"executed": False, "error": "replayed action request"}
        identity = self.identities.get(action.agent_id)
        guardrail = await self.policy.evaluate_action(action, identity)
        await self.audit.log_audit_event(
            action.project_id,
            action.agent_id,
            action.action_type,
            guardrail.decision.value,
            action.integrity_hash or "",
            risk_level=guardrail.risk_level.value,
        )
        evidence_gate_decisions = {
            PolicyDecision.REQUIRE_REVIEW,
            PolicyDecision.REQUIRE_SECURITY_REVIEW,
            PolicyDecision.REQUIRE_BACKUP_FIRST,
        }
        if guardrail.decision in evidence_gate_decisions:
            return {
                "executed": False,
                "required_gate": guardrail.decision.value,
                "guardrail": guardrail.model_dump(mode="json"),
                "reason": "required independent evidence has not been supplied",
            }
        if guardrail.decision is PolicyDecision.REQUIRE_HUMAN_APPROVAL:
            row = await self.db.fetchrow(
                """
                INSERT INTO approval_requests(
                  project_id,action_integrity_hash,requested_by_agent_id,required_gate,
                  request_payload,expires_at
                ) VALUES($1,$2,$3,$4,$5::jsonb,now()+interval '24 hours')
                ON CONFLICT(action_integrity_hash) DO UPDATE SET request_payload=EXCLUDED.request_payload
                RETURNING id
                """,
                UUID(action.project_id),
                action.integrity_hash,
                action.agent_id,
                guardrail.decision.value,
                action.model_dump_json(),
            )
            assert row is not None
            return {
                "executed": False,
                "pending_approval": True,
                "approval_request_id": str(row["id"]),
                "guardrail": guardrail.model_dump(mode="json"),
            }
        allowed = {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
            PolicyDecision.REQUIRE_SANDBOX_ONLY,
        }
        if guardrail.decision not in allowed:
            return {"executed": False, "guardrail": guardrail.model_dump(mode="json")}
        return await self._dispatch(action, guardrail.model_dump(mode="json"))

    async def _dispatch(
        self, action: ActionRequest, guardrail_payload: dict[str, Any]
    ) -> dict[str, Any]:
        try:
            if action.action_type in {"write_file", "write_code"}:
                result = await self._write_file(action)
            elif action.action_type == "read_file":
                result = await self._read_file(action)
            elif action.action_type in {"shell_command", "run_command"}:
                result = await self._sandbox_command(action)
            elif action.action_type == "execute_db_operation":
                result = await self._sandbox_database(action)
            else:
                raise ValueError(f"no execution driver for {action.action_type}")
            event = Event(
                project_id=action.project_id,
                event_type=EventType.ACTION_ALLOWED,
                producer_agent_id="runtime_supervisor",
                target_agent_id=action.agent_id,
                payload={
                    "action_type": action.action_type,
                    "task_id": action.task_id,
                    "result": result,
                },
            )
            await self.events.save_event(action.project_id, event)
            return {"executed": True, "guardrail": guardrail_payload, "result": result}
        except Exception as error:
            logger.error("controlled_execution_failed", error_type=type(error).__name__)
            return {
                "executed": False,
                "guardrail": guardrail_payload,
                "error": type(error).__name__,
            }

    async def execute_approved_action(
        self, approval_request_id: str, action_data: dict[str, Any]
    ) -> dict[str, Any]:
        action = ActionRequest.model_validate(action_data)
        async with self.db.transaction() as connection:
            row = await connection.fetchrow(
                """
                SELECT * FROM approval_requests
                WHERE id=$1 AND project_id=$2 AND status='APPROVED' AND expires_at>now()
                FOR UPDATE
                """,
                UUID(approval_request_id),
                UUID(action.project_id),
            )
            if row is None or row["action_integrity_hash"] != action.integrity_hash:
                return {"executed": False, "error": "valid approved request not found"}
            await connection.execute(
                """
                UPDATE approval_requests
                SET status='EXPIRED',expires_at=now(),
                    decision_reason=concat(decision_reason,' [consumed by execution]')
                WHERE id=$1
                """,
                UUID(approval_request_id),
            )
        identity = self.identities.get(action.agent_id)
        guardrail = await self.policy.evaluate_action(action, identity)
        if guardrail.decision in {PolicyDecision.DENY, PolicyDecision.QUARANTINE_AGENT}:
            return {"executed": False, "guardrail": guardrail.model_dump(mode="json")}
        return await self._dispatch(action, guardrail.model_dump(mode="json"))

    async def _validate_merge_evidence(
        self, project_id: str, task: dict[str, Any]
    ) -> tuple[bool, str]:
        task_id = str(task["id"])
        criteria = list(task.get("dod_criteria") or [])
        if not criteria:
            return False, "task has no mapped DoD criteria"
        checks = {
            str(row["criterion_id"]): dict(row)
            for row in await self.db.fetch(
                """
                SELECT * FROM dod_checks WHERE project_id=$1 AND active AND criterion_id=ANY($2::text[])
                  AND contract_version=$3
                """,
                UUID(project_id),
                criteria,
                task["dod_contract_version"],
            )
        }
        if set(checks) != set(criteria):
            return False, "task references a missing or stale DoD contract"
        required_reviewers = {"code_reviewer"}
        if any(
            "security_review" in (check["required_evidence_types"] or [])
            for check in checks.values()
        ):
            required_reviewers.add("security_reviewer")
        if not required_reviewers.issubset(set(task.get("required_reviewers") or [])):
            return False, "task reviewer gates do not cover its criterion evidence contract"
        artifact_rows = await self.db.fetch(
            """
            SELECT id,object_uri,checksum_sha256,content_length
            FROM artifacts WHERE project_id=$1 AND task_id=$2 ORDER BY created_at
            """,
            UUID(project_id),
            UUID(task_id),
        )
        if not artifact_rows:
            return False, "task has no persisted artifacts"
        evidence_rows = await self.db.fetch(
            """
            SELECT DISTINCT ON (e.criterion_id,e.evidence_type,e.artifact_id)
              e.*
            FROM dod_evidence e
            WHERE e.project_id=$1 AND e.task_id=$2
              AND e.contract_version=$3
            ORDER BY e.criterion_id,e.evidence_type,e.artifact_id,e.created_at DESC
            """,
            UUID(project_id),
            UUID(task_id),
            task["dod_contract_version"],
        )
        by_key = {
            (
                str(row["criterion_id"]),
                str(row["evidence_type"]),
                str(row["artifact_id"]) if row["artifact_id"] else None,
            ): row
            for row in evidence_rows
        }
        worktree = await self._ensure_worktree(project_id, task_id)
        branch_head_code, branch_head, branch_error = await self._run_host(
            "git", "rev-parse", "HEAD", cwd=worktree
        )
        if branch_head_code:
            return False, f"task branch HEAD is unavailable: {branch_error[:500]}"
        branch_head = branch_head.strip()
        for artifact in artifact_rows:
            artifact_id = str(artifact["id"])
            parsed = urlparse(artifact["object_uri"])
            if parsed.scheme != "minio":
                return False, f"artifact {artifact_id} is not stored in MinIO"
            version = parse_qs(parsed.query).get("versionId", [None])[0]
            metadata = await self.minio.stat(
                bucket=parsed.netloc,
                object_name=parsed.path.lstrip("/"),
                version_id=version,
            )
            if (
                not metadata.sha256
                or metadata.sha256 != artifact["checksum_sha256"]
                or metadata.size != artifact["content_length"]
            ):
                return False, f"artifact {artifact_id} integrity check failed"
            for criterion_id in criteria:
                check = checks[criterion_id]
                scopes = dict(check["evidence_scopes"] or {})
                artifact_types = {
                    evidence_type
                    for evidence_type in check["required_evidence_types"] or []
                    if scopes.get(evidence_type) == "artifact"
                }
                for evidence_type in sorted(artifact_types):
                    row = by_key.get((criterion_id, evidence_type, artifact_id))
                    if (
                        row is None
                        or not row["passed"]
                        or row["run_status"] != "OK"
                        or row["criterion_hash"] != check["criterion_hash"]
                    ):
                        return (
                            False,
                            f"artifact {artifact_id} lacks passing {evidence_type} "
                            f"for criterion {criterion_id}",
                        )
                    if evidence_type in {"review", "security_review"}:
                        expected_role = (
                            "security_reviewer"
                            if evidence_type == "security_review"
                            else "code_reviewer"
                        )
                        if (
                            row["source_agent_id"] == task.get("owner_agent_id")
                            or row["source_role"] != expected_role
                        ):
                            return False, f"artifact {artifact_id} review is not independent"

        for criterion_id in criteria:
            check = checks[criterion_id]
            required = set(check["required_evidence_types"] or [])
            deterministic = next(iter(required & {"test", "command"}), None)
            if deterministic is None:
                return False, f"criterion {criterion_id} has no deterministic evidence contract"
            candidates = [
                row
                for row in evidence_rows
                if row["criterion_id"] == criterion_id
                and row["evidence_type"] == deterministic
                and row["artifact_id"] is None
            ]
            row = max(candidates, key=lambda item: item["created_at"]) if candidates else None
            if (
                row is None
                or not row["passed"]
                or row["run_status"] != "OK"
                or row["exit_code"] != 0
                or row["subject_commit"] != branch_head
                or row["criterion_hash"] != check["criterion_hash"]
            ):
                return (
                    False,
                    f"criterion {criterion_id} lacks fresh passing {deterministic} evidence",
                )
            try:
                executed = json.loads(row["command"] or "[]")
            except json.JSONDecodeError:
                executed = []
            if executed != list(check["verification_command"] or []):
                return False, f"criterion {criterion_id} command differs from its contract"

        expected_outputs = [
            str(item).replace("\\", "/") for item in task.get("expected_outputs") or []
        ]
        actual_outputs = [
            item.replace("\\", "/") for item in await self.tasks.artifact_titles(task_id)
        ]
        for pattern in expected_outputs:
            if not any(fnmatch.fnmatch(output, pattern) for output in actual_outputs):
                return False, f"required output is missing: {pattern}"
        return True, ""

    async def merge_task(self, project_id: str, task_id: str, agent_id: str) -> dict[str, Any]:
        task = await self._owned_task(project_id, task_id, agent_id)
        if task["status"] == "COMPLETED":
            repository = await self._ensure_repository(project_id)
            branch = f"agentos/task-{task_id}"
            code, _, _ = await self._run_host(
                "git", "merge-base", "--is-ancestor", branch, "HEAD", cwd=repository
            )
            return {
                "success": code == 0,
                "already_completed": code == 0,
                "reason": "completed task branch is not integrated" if code else "",
            }
        if task["status"] != "UNDER_REVIEW":
            return {"success": False, "reason": "task is not ready for integration"}
        evidence_valid, reason = await self._validate_merge_evidence(project_id, task)
        if not evidence_valid:
            return {"success": False, "reason": reason}
        repository = await self._ensure_repository(project_id)
        branch = f"agentos/task-{task_id}"
        async with self.dragonfly.lock(f"merge:{project_id}", ttl_seconds=120) as acquired:
            if not acquired:
                return {"success": False, "reason": "merge queue is busy"}
            _, pre_head, _ = await self._run_host("git", "rev-parse", "HEAD", cwd=repository)
            _, branch_head, _ = await self._run_host("git", "rev-parse", branch, cwd=repository)
            pre_head = pre_head.strip()
            branch_head = branch_head.strip()
            repository_head = pre_head
            attempt = await self.db.fetchrow(
                """
                SELECT * FROM integration_attempts
                WHERE project_id=$1 AND task_id=$2 AND status IN ('PREPARED','COMMITTED')
                ORDER BY created_at DESC LIMIT 1
                """,
                UUID(project_id),
                UUID(task_id),
            )
            ancestor_code, _, _ = await self._run_host(
                "git", "merge-base", "--is-ancestor", branch, "HEAD", cwd=repository
            )
            already_integrated = ancestor_code == 0
            merge_in_progress = False
            attempt_already_committed = bool(attempt and attempt["status"] == "COMMITTED")
            if attempt:
                attempt_id = attempt["id"]
                pre_head = str(attempt["pre_head"])
                branch_head = str(attempt["branch_head"])
                if attempt_already_committed:
                    if not already_integrated or str(attempt["result_head"]) != repository_head:
                        return {
                            "success": False,
                            "reason": "committed integration attempt does not match repository state",
                        }
                elif not already_integrated:
                    merge_head_code, merge_head, _ = await self._run_host(
                        "git", "rev-parse", "-q", "--verify", "MERGE_HEAD", cwd=repository
                    )
                    if merge_head_code == 0:
                        if merge_head.strip() != branch_head or repository_head != pre_head:
                            return {
                                "success": False,
                                "reason": "prepared merge state does not match its durable attempt",
                            }
                        merge_in_progress = True
                    elif repository_head != pre_head:
                        return {
                            "success": False,
                            "reason": "prepared integration does not match repository state",
                        }
            else:
                if already_integrated:
                    return {
                        "success": False,
                        "reason": "task branch was integrated outside the governed merge attempt",
                    }
                attempt_id = await self.db.fetchval(
                    """
                    INSERT INTO integration_attempts(project_id,task_id,pre_head,branch_head)
                    VALUES($1,$2,$3,$4) RETURNING id
                    """,
                    UUID(project_id),
                    UUID(task_id),
                    pre_head,
                    branch_head,
                )
            output = ""
            if not already_integrated:
                if merge_in_progress:
                    output = "Recovered durable prospective merge state."
                else:
                    status_code, dirty, _ = await self._run_host(
                        "git", "status", "--porcelain", cwd=repository
                    )
                    if status_code or dirty.strip():
                        await self.db.execute(
                            """
                            UPDATE integration_attempts SET status='ABORTED',
                              failure_reason='managed repository was not clean' WHERE id=$1
                            """,
                            attempt_id,
                        )
                        return {"success": False, "reason": "managed repository is not clean"}
                    code, output, error = await self._run_host(
                        "git",
                        "-c",
                        "user.name=AgentOS",
                        "-c",
                        "user.email=runtime@agentos.local",
                        "merge",
                        "--no-ff",
                        "--no-commit",
                        branch,
                        cwd=repository,
                        timeout_seconds=120,
                    )
                    if code:
                        await self._run_host("git", "merge", "--abort", cwd=repository)
                        await self.db.execute(
                            """
                            UPDATE integration_attempts SET status='ABORTED',failure_reason=$2
                            WHERE id=$1
                            """,
                            attempt_id,
                            f"merge conflict: {error[-1800:]}",
                        )
                        return {
                            "success": False,
                            "reason": "merge conflict",
                            "details": error[-2000:],
                        }

            eligible_checks = [
                dict(row)
                for row in await self.db.fetch(
                    """
                    SELECT c.* FROM dod_checks c
                    WHERE c.project_id=$1 AND c.active AND c.contract_version=$2
                      AND (c.mandatory OR c.criterion_id=ANY($3::text[]))
                      AND NOT EXISTS(
                        SELECT 1 FROM tasks t
                        WHERE t.project_id=c.project_id AND c.criterion_id=ANY(t.dod_criteria)
                          AND t.id<>$4 AND t.status<>'COMPLETED'
                      )
                    ORDER BY c.criterion_id
                    """,
                    UUID(project_id),
                    task["dod_contract_version"],
                    list(task["dod_criteria"]),
                    UUID(task_id),
                )
            ]
            command_results: dict[tuple[str, ...], dict[str, Any]] = {}
            for check in eligible_checks:
                command = tuple(str(token) for token in check["verification_command"] or [])
                if command and command not in command_results:
                    command_results[command] = await self._run_sandbox_at(
                        project_id, repository, list(command)
                    )
            failed_commands = [
                {"command": list(command), **result}
                for command, result in command_results.items()
                if result["exit_code"] != 0
            ]
            if failed_commands:
                if not already_integrated:
                    await self._run_host("git", "merge", "--abort", cwd=repository)
                await self.db.execute(
                    """
                    UPDATE integration_attempts SET status='ABORTED',failure_reason=$2 WHERE id=$1
                    """,
                    attempt_id,
                    json.dumps(failed_commands, default=str)[:2000],
                )
                return {
                    "success": False,
                    "reason": "prospective integrated HEAD failed its DoD verification commands",
                    "failed_commands": failed_commands,
                }
            if not already_integrated:
                code, _, error = await self._run_host(
                    "git",
                    "-c",
                    "user.name=AgentOS",
                    "-c",
                    "user.email=runtime@agentos.local",
                    "commit",
                    "-m",
                    f"Integrate task {task_id}",
                    cwd=repository,
                )
                if code:
                    await self._run_host("git", "merge", "--abort", cwd=repository)
                    await self.db.execute(
                        "UPDATE integration_attempts SET status='ABORTED',failure_reason=$2 WHERE id=$1",
                        attempt_id,
                        f"integration commit failed: {error[-1800:]}",
                    )
                    return {"success": False, "reason": "integration commit failed"}
            _, integrated_commit, _ = await self._run_host(
                "git", "rev-parse", "HEAD", cwd=repository
            )
            integrated_commit = integrated_commit.strip()
            if not attempt_already_committed:
                async with self.db.transaction() as connection:
                    project = await connection.fetchrow(
                        "SELECT integration_head,source_revision FROM projects WHERE id=$1 FOR UPDATE",
                        UUID(project_id),
                    )
                    expected_pre_head = project["integration_head"] or project["source_revision"]
                    if (
                        expected_pre_head not in {pre_head, "EMPTY_WORKSPACE"}
                        and project["integration_head"] != integrated_commit
                    ):
                        raise RuntimeError("project integration fence changed during merge")
                    await connection.execute(
                        """
                        UPDATE projects SET integration_head=$2,
                          evidence_generation=evidence_generation+1,
                          evaluation_requested_generation=evidence_generation+1 WHERE id=$1
                        """,
                        UUID(project_id),
                        integrated_commit,
                    )
                    await connection.execute(
                        """
                        UPDATE integration_attempts SET status='COMMITTED',result_head=$2,
                          failure_reason=NULL,updated_at=now() WHERE id=$1
                        """,
                        attempt_id,
                        integrated_commit,
                    )

        for check in eligible_checks:
            required = set(check["required_evidence_types"] or [])
            evidence_type = "command" if "command" in required else "test"
            command = tuple(str(token) for token in check["verification_command"] or [])
            result = command_results[command]
            await self.dod.add_evidence(
                project_id,
                str(check["criterion_id"]),
                evidence_type,
                "integration_supervisor",
                summary=(
                    f"Integrated HEAD {integrated_commit} passed {json.dumps(list(command))} "
                    f"in sandbox {result['sandbox_digest']}"
                ),
                passed=True,
                command=json.dumps(list(command)),
                exit_code=0,
                source_role="integration_supervisor",
                subject_commit=integrated_commit,
                integration_commit=integrated_commit,
                sandbox_digest=result["sandbox_digest"],
                watched_paths=list(check["required_artifacts"] or []),
                affected_contracts=list(check["affected_contracts"] or []),
                metadata={"output_tail": str(result["output"])[-2000:]},
            )
        checks_by_id = {
            str(row["criterion_id"]): dict(row)
            for row in await self.db.fetch(
                "SELECT * FROM dod_checks WHERE project_id=$1 AND active AND criterion_id=ANY($2::text[])",
                UUID(project_id),
                list(task["dod_criteria"]),
            )
        }
        for criterion_id in task["dod_criteria"]:
            check = checks_by_id[criterion_id]
            if "integration" not in (check["required_evidence_types"] or []):
                continue
            await self.dod.add_evidence(
                project_id,
                criterion_id,
                "integration",
                "integration_supervisor",
                summary=f"Task branch {branch_head} integrated at {integrated_commit}",
                passed=True,
                task_id=task_id,
                source_role="integration_supervisor",
                subject_commit=branch_head,
                integration_commit=integrated_commit,
                watched_paths=list(task["allowed_paths"] or []),
                affected_contracts=list(task["affected_contracts"] or []),
            )
        await self.tasks.update_task_status(task_id, "COMPLETED")
        await self.db.execute(
            "UPDATE projects SET replan_attempts=0,next_replan_at=NULL WHERE id=$1",
            UUID(project_id),
        )
        return {
            "success": True,
            "output": output[-2000:],
            "integrated_commit": integrated_commit,
            "validated_criteria": [str(check["criterion_id"]) for check in eligible_checks],
        }


@ray.remote(num_cpus=0.2, max_concurrency=16)  # type: ignore[call-overload]
class ExecutionSupervisorActor:
    def __init__(self, settings_payload: dict[str, Any]):
        self.service = ExecutionService(Settings(**settings_payload))

    async def register_agent_identity(self, identity_data: dict[str, Any]) -> None:
        await self.service.register_identity(AgentIdentity.model_validate(identity_data))

    async def request_execution(self, action: dict[str, Any]) -> dict[str, Any]:
        return await self.service.request_execution(action)

    async def execute_approved_action(
        self, approval_request_id: str, action: dict[str, Any]
    ) -> dict[str, Any]:
        return await self.service.execute_approved_action(approval_request_id, action)

    async def merge_and_finalize_branch(self, agent_id: str, task_id: str) -> dict[str, Any]:
        identity = self.service.identities.get(agent_id)
        if identity is None:
            return {"success": False, "reason": "unknown identity"}
        return await self.service.merge_task(identity.project_id, task_id, agent_id)


ExecutionSupervisor = ExecutionService

__all__ = ["ExecutionService", "ExecutionSupervisor", "ExecutionSupervisorActor"]
