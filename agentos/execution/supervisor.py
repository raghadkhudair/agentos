from __future__ import annotations

import asyncio
import fnmatch
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
        return repository

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
        task = await self.tasks.get(task_id)
        if task is None:
            raise LookupError("task does not exist")
        if str(task["project_id"]) != project_id:
            raise PermissionError("task belongs to another project")
        if task.get("owner_agent_id") != agent_id:
            raise PermissionError("task is not actively leased to this agent")
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
        if code and "nothing to commit" not in error.lower():
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
        if image not in set(self.tuning["allowed_sandbox_images"]):
            raise PermissionError("sandbox image is not allowlisted")

        def run_container() -> tuple[int, str]:
            client = (
                docker.DockerClient(base_url=self.settings.docker_host)
                if self.settings.docker_host
                else docker.from_env()
            )
            if self.settings.sandbox_workspace_volume:
                volumes = {
                    self.settings.sandbox_workspace_volume: {"bind": "/workspace", "mode": "rw"}
                }
                working_dir = f"/workspace/{action.project_id}/worktrees/{action.task_id}"
            else:
                volumes = {str(worktree): {"bind": "/workspace", "mode": "rw"}}
                working_dir = "/workspace"
            container = None
            try:
                container = client.containers.run(
                    image=image,
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
                    volumes={
                        source: {**options, "mode": "ro"} for source, options in volumes.items()
                    },
                    environment={
                        "PYTHONDONTWRITEBYTECODE": "1",
                        "HOME": "/tmp",  # noqa: S108 - container-private tmpfs, non-root sandbox
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
                return int(result.get("StatusCode", 1)), output[
                    : int(self.tuning["max_output_characters"])
                ]
            finally:
                if container is not None:
                    container.remove(force=True)
                client.close()

        exit_code, output = await asyncio.to_thread(run_container)
        evidence = action.payload.get("evidence")
        if isinstance(evidence, dict) and evidence.get("criterion_id"):
            await self.dod.add_evidence(
                action.project_id,
                str(evidence["criterion_id"]),
                "test",
                action.agent_id,
                summary=f"Sandbox command exited {exit_code}",
                passed=exit_code == 0,
                command=json.dumps(command),
                exit_code=exit_code,
                metadata={"task_id": action.task_id, "output_tail": output[-2000:]},
            )
        return {"exit_code": exit_code, "output": output}

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
        evidence = await self.db.fetch(
            """
            SELECT DISTINCT ON (e.criterion_id,e.evidence_type)
              e.criterion_id,e.evidence_type,e.passed,e.exit_code,e.source_agent_id,
              e.artifact_id,a.object_uri,a.checksum_sha256,a.content_length
            FROM dod_evidence e
            LEFT JOIN artifacts a
              ON a.id=e.artifact_id AND a.project_id=e.project_id AND a.task_id=$3
            WHERE e.project_id=$1 AND e.metadata->>'task_id'=$2
            ORDER BY e.criterion_id,e.evidence_type,e.created_at DESC
            """,
            UUID(project_id),
            task_id,
            UUID(task_id),
        )
        by_key = {(str(row["criterion_id"]), str(row["evidence_type"])): row for row in evidence}
        security_required = str(task.get("risk_level")) in {
            "HIGH",
            "CRITICAL",
        } or "security_reviewer" in set(task.get("required_reviewers") or [])
        supported_reviewers = {"code_reviewer", "security_reviewer"}
        unknown_reviewers = set(task.get("required_reviewers") or []) - supported_reviewers
        if unknown_reviewers:
            return False, f"unsupported required reviewers: {sorted(unknown_reviewers)}"

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
        artifact_ids = [row["id"] for row in artifact_rows]
        artifact_evidence = await self.db.fetch(
            """
            SELECT DISTINCT ON (e.criterion_id,e.evidence_type,e.artifact_id)
              e.criterion_id,e.evidence_type,e.artifact_id,e.passed,e.source_agent_id,
              reviewer.role AS reviewer_role
            FROM dod_evidence e
            LEFT JOIN agents reviewer
              ON reviewer.project_id=e.project_id AND reviewer.id=e.source_agent_id
            WHERE e.project_id=$1 AND e.metadata->>'task_id'=$2
              AND e.artifact_id=ANY($3::uuid[])
              AND e.evidence_type IN ('artifact','review','security_review')
            ORDER BY e.criterion_id,e.evidence_type,e.artifact_id,e.created_at DESC
            """,
            UUID(project_id),
            task_id,
            artifact_ids,
        )
        artifact_evidence_by_key = {
            (
                str(row["criterion_id"]),
                str(row["evidence_type"]),
                str(row["artifact_id"]),
            ): row
            for row in artifact_evidence
        }
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
                artifact_types = {"artifact", "review"}
                if security_required:
                    artifact_types.add("security_review")
                for evidence_type in artifact_types:
                    row = artifact_evidence_by_key.get((criterion_id, evidence_type, artifact_id))
                    if row is None or not row["passed"]:
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
                            or row["reviewer_role"] != expected_role
                        ):
                            return False, f"artifact {artifact_id} review is not independent"

        for criterion_id in criteria:
            required_types = {"artifact", "test", "review"}
            if security_required:
                required_types.add("security_review")
            for evidence_type in required_types:
                row = by_key.get((criterion_id, evidence_type))
                if row is None or not row["passed"]:
                    return False, f"criterion {criterion_id} lacks passing {evidence_type} evidence"
                if evidence_type == "test" and row["exit_code"] != 0:
                    return False, f"criterion {criterion_id} has nonzero test evidence"
                if evidence_type in {"review", "security_review"}:
                    if row["source_agent_id"] == task.get("owner_agent_id"):
                        return False, f"criterion {criterion_id} review is not independent"
                    expected_role = (
                        "security_reviewer"
                        if evidence_type == "security_review"
                        else "code_reviewer"
                    )
                    role = await self.db.fetchval(
                        "SELECT role FROM agents WHERE project_id=$1 AND id=$2",
                        UUID(project_id),
                        row["source_agent_id"],
                    )
                    if role != expected_role:
                        return False, f"criterion {criterion_id} reviewer role is invalid"
                if evidence_type == "artifact":
                    if not row["artifact_id"] or not row["object_uri"]:
                        return False, f"criterion {criterion_id} artifact is not project/task bound"
                    parsed = urlparse(row["object_uri"])
                    if parsed.scheme != "minio":
                        return False, f"criterion {criterion_id} artifact is not stored in MinIO"
                    version = parse_qs(parsed.query).get("versionId", [None])[0]
                    metadata = await self.minio.stat(
                        bucket=parsed.netloc,
                        object_name=parsed.path.lstrip("/"),
                        version_id=version,
                    )
                    if (
                        not metadata.sha256
                        or metadata.sha256 != row["checksum_sha256"]
                        or metadata.size != row["content_length"]
                    ):
                        return False, f"criterion {criterion_id} artifact integrity check failed"

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
            code, output, error = await self._run_host(
                "git",
                "merge",
                "--no-ff",
                branch,
                "-m",
                f"Integrate task {task_id}",
                cwd=repository,
                timeout_seconds=120,
            )
            if code:
                await self._run_host("git", "merge", "--abort", cwd=repository)
                return {"success": False, "reason": "merge conflict", "details": error[-2000:]}
        _, integrated_commit, _ = await self._run_host("git", "rev-parse", "HEAD", cwd=repository)
        for criterion_id in task["dod_criteria"]:
            await self.dod.add_evidence(
                project_id,
                criterion_id,
                "integration",
                "runtime_supervisor",
                summary=f"Task branch integrated at {integrated_commit.strip()}",
                passed=True,
                metadata={"task_id": task_id, "commit": integrated_commit.strip()},
            )
        await self.tasks.update_task_status(task_id, "COMPLETED")
        return {
            "success": True,
            "output": output[-2000:],
            "integrated_commit": integrated_commit.strip(),
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
