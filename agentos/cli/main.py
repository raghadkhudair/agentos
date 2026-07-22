from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Annotated, Any, cast
from uuid import UUID

import ray
import typer
from rich.console import Console
from rich.table import Table

from agentos.config.runtime import ResourcePlanner
from agentos.config.settings import Settings, load_settings
from agentos.governance.models import ActionRequest, AgentIdentity
from agentos.governance.policy_engine import PolicyEngine
from agentos.runtime.supervisor import RuntimeSupervisorActor
from agentos.storage.clients import (
    DragonflyClient,
    MilvusVectorClient,
    MinioObjectClient,
    MongoDocumentClient,
    PostgresClient,
)

app = typer.Typer(no_args_is_help=True, help="AgentOS production CLI")
console = Console()


async def _await_ref(reference: Any) -> Any:
    return await reference


def _ensure_ray(settings: Settings) -> None:
    if ray.is_initialized():
        return
    planner = ResourcePlanner(settings)
    envelope = planner.build_envelope()
    for key, value in planner.thread_environment().items():
        os.environ[key] = value
    kwargs: dict[str, Any] = {
        "address": settings.ray_address,
        "namespace": "agentos",
        "ignore_reinit_error": True,
        "include_dashboard": False,
        "log_to_driver": True,
    }
    if not settings.ray_address:
        kwargs.update(
            num_cpus=envelope.allocated_cpu_cores,
            object_store_memory=envelope.object_store_memory_bytes,
        )
    ray.init(**kwargs)


def _supervisor(settings: Settings) -> Any:
    _ensure_ray(settings)
    planner = ResourcePlanner(settings)
    envelope = planner.build_envelope()
    return RuntimeSupervisorActor.options(  # type: ignore[attr-defined]
        name="runtime-supervisor",
        namespace="agentos",
        get_if_exists=True,
        lifetime="detached",
        num_cpus=planner.supervisor_cpu(envelope),
        memory=max(16_777_216, envelope.system_memory_bytes // 5),
        max_restarts=3,
        max_task_retries=2,
        runtime_env={"env_vars": planner.thread_environment()},
    ).remote(settings.model_dump(mode="python"))


@app.command()
def init(project_name: str = typer.Argument(..., help="Local project workspace name.")) -> None:
    """Initialize schema, all storage clients, and a local workspace."""
    settings = load_settings()
    if settings.environment == "production":
        settings.validate_production_secrets()
    root = (settings.workspace / project_name).resolve()
    for directory in ("repository", "worktrees", "exports"):
        (root / directory).mkdir(parents=True, exist_ok=True)

    async def initialize() -> dict[str, Any]:
        postgres = PostgresClient(settings)
        mongo = MongoDocumentClient(settings)
        minio = MinioObjectClient(settings)
        milvus = MilvusVectorClient(settings)
        dragonfly = DragonflyClient(settings)
        await postgres.connect()
        await postgres.initialize_schema()
        await mongo.initialize()
        await minio.initialize()
        await milvus.initialize()
        health = {
            "postgres": await postgres.healthcheck(),
            "mongodb": await mongo.healthcheck(),
            "minio": await minio.healthcheck(),
            "milvus": await milvus.healthcheck(),
            "dragonfly": await dragonfly.healthcheck(),
        }
        await postgres.disconnect()
        await mongo.close()
        await dragonfly.close()
        return health

    health = asyncio.run(initialize())
    manifest = root / "agentos-project.json"
    manifest.write_text(
        json.dumps(
            {"project_name": project_name, "workspace": str(root), "health": health}, indent=2
        ),
        encoding="utf-8",
    )
    console.print(
        f"[green]Initialized all production storage clients and workspace:[/green] {root}"
    )


@app.command()
def plan(request: str = typer.Argument(..., help="Software-delivery request.")) -> None:
    """Generate and persist a validated team/resource plan without launching workers."""
    settings = load_settings()
    result = asyncio.run(_await_ref(_supervisor(settings).plan_project.remote(request)))
    console.print_json(data=result)


@app.command()
def run(
    request: str = typer.Argument(..., help="Software-delivery request."),
    detach: bool = typer.Option(
        False, "--detach", help="Return after startup instead of waiting for DoD."
    ),
) -> None:
    """Launch the governed runtime and, by default, remain DoD-bound."""
    settings = load_settings()
    result = asyncio.run(
        _await_ref(
            _supervisor(settings).bootstrap_project.remote(request, wait_for_completion=not detach)
        )
    )
    console.print_json(data=result)


@app.command("runtime-config")
def runtime_config(
    agents: Annotated[
        list[str] | None,
        typer.Option("--agent", help="Role name; may be repeated."),
    ] = None,
    output: Annotated[
        Path | None,
        typer.Option("--output", help="Optional JSON output path."),
    ] = None,
) -> None:
    """Generate a bounded runtime configuration from current host resources."""
    settings = load_settings()
    selected_agents = agents or ["pm_tech_lead", "infrastructure_agent"]
    role_counts: dict[str, int] = {}
    identities: list[tuple[str, str]] = []
    for role in selected_agents:
        role_counts[role] = role_counts.get(role, 0) + 1
        identities.append((f"{role}-{role_counts[role]}", role))
    config = ResourcePlanner(settings).build_runtime_config(identities)
    payload = config.model_dump(mode="json")
    if output:
        target = output.expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        console.print(f"[green]Wrote runtime configuration:[/green] {target}")
    console.print_json(data=payload)


@app.command()
def doctor() -> None:
    """Run live health checks for every required database and provider configuration."""
    settings = load_settings()
    result = asyncio.run(_await_ref(_supervisor(settings).dependency_health.remote()))
    console.print_json(data=result)
    if not result["healthy"]:
        raise typer.Exit(1)


@app.command()
def status(project_id: str | None = typer.Argument(None, help="Optional project UUID.")) -> None:
    """Show safe configuration and durable project state."""
    settings = load_settings()

    async def read_status() -> list[dict[str, Any]]:
        db = PostgresClient(settings)
        await db.connect()
        if project_id:
            rows = await db.fetch("SELECT * FROM projects WHERE id=$1", UUID(project_id))
        else:
            rows = await db.fetch("SELECT * FROM projects ORDER BY created_at DESC LIMIT 10")
        await db.disconnect()
        return [json.loads(json.dumps(dict(row), default=str)) for row in rows]

    table = Table(title="AgentOS runtime limits")
    table.add_column("Setting")
    table.add_column("Value")
    envelope = ResourcePlanner(settings).build_envelope()
    table.add_row("Environment", settings.environment)
    table.add_row("Workspace", str(settings.workspace))
    table.add_row(
        "Allocated / detected CPUs",
        f"{envelope.allocated_cpu_cores} / {envelope.detected_cpu_cores}",
    )
    table.add_row("Reserved CPUs", str(envelope.reserved_cpu_cores))
    table.add_row("Max active agents", str(envelope.max_active_agents))
    table.add_row(
        "Destructive actions",
        "allowed with approval" if settings.allow_destructive_actions else "denied",
    )
    console.print(table)
    console.print_json(data={"projects": asyncio.run(read_status())})


@app.command()
def logs(
    project_id: str = typer.Argument(..., help="Project UUID."),
    limit: int = typer.Option(100, min=1, max=1000),
) -> None:
    """Read durable structured project events."""
    settings = load_settings()

    async def read() -> list[dict[str, Any]]:
        db = PostgresClient(settings)
        rows = await db.fetch(
            "SELECT * FROM events WHERE project_id=$1 ORDER BY created_at DESC LIMIT $2",
            UUID(project_id),
            limit,
        )
        await db.disconnect()
        return [json.loads(json.dumps(dict(row), default=str)) for row in rows]

    console.print_json(data={"events": asyncio.run(read())})


@app.command()
def inspect(project_id: str = typer.Argument(..., help="Project UUID.")) -> None:
    """Inspect tasks, agents, DoD, evidence, and active resource plan."""
    settings = load_settings()

    async def read() -> dict[str, Any]:
        db = PostgresClient(settings)
        pid = UUID(project_id)
        data = {
            "project": await db.fetchrow("SELECT * FROM projects WHERE id=$1", pid),
            "agents": await db.fetch("SELECT * FROM agents WHERE project_id=$1 ORDER BY id", pid),
            "tasks": await db.fetch(
                "SELECT * FROM tasks WHERE project_id=$1 ORDER BY created_at", pid
            ),
            "dod": await db.fetch(
                "SELECT * FROM dod_checks WHERE project_id=$1 ORDER BY created_at", pid
            ),
            "evidence": await db.fetch(
                "SELECT * FROM dod_evidence WHERE project_id=$1 ORDER BY created_at", pid
            ),
            "resource_plan": await db.fetchrow(
                "SELECT * FROM resource_plans WHERE project_id=$1 AND active", pid
            ),
        }
        await db.disconnect()
        return cast(dict[str, Any], json.loads(json.dumps(data, default=str)))

    console.print_json(data=asyncio.run(read()))


@app.command()
def pause(project_id: str = typer.Argument(..., help="Project UUID.")) -> None:
    settings = load_settings()
    result = asyncio.run(_await_ref(_supervisor(settings).pause_project.remote(project_id)))
    console.print_json(data=result)


@app.command()
def resume(project_id: str = typer.Argument(..., help="Project UUID.")) -> None:
    settings = load_settings()
    result = asyncio.run(_await_ref(_supervisor(settings).resume_project.remote(project_id)))
    console.print_json(data=result)


@app.command()
def approve(
    approval_id: str = typer.Argument(..., help="Approval request UUID."),
    approver: str = typer.Option(..., "--approver", help="Human approver identity."),
    reason: str = typer.Option(..., "--reason", help="Auditable approval reason."),
) -> None:
    settings = load_settings()

    async def decide() -> dict[str, Any]:
        db = PostgresClient(settings)
        async with db.transaction() as connection:
            row = await connection.fetchrow(
                "SELECT * FROM approval_requests WHERE id=$1 FOR UPDATE", UUID(approval_id)
            )
            if (
                row is None
                or row["status"] != "PENDING"
                or row["expires_at"].timestamp() <= __import__("time").time()
            ):
                raise ValueError("approval request is missing, expired, or already decided")
            await connection.execute(
                "UPDATE approval_requests SET status='APPROVED',decided_by=$2,decision_reason=$3,decided_at=now() WHERE id=$1",
                UUID(approval_id),
                approver,
                reason,
            )
        suffix = str(row["project_id"]).replace("-", "")[:12]
        execution = ray.get_actor(f"execution-{suffix}", namespace="agentos")
        action = row["request_payload"]
        result = await execution.execute_approved_action.remote(approval_id, action)
        return cast(dict[str, Any], result)

    _ensure_ray(settings)
    console.print_json(data=asyncio.run(decide()))


@app.command()
def reject(
    approval_id: str = typer.Argument(..., help="Approval request UUID."),
    approver: str = typer.Option(..., "--approver"),
    reason: str = typer.Option(..., "--reason"),
) -> None:
    settings = load_settings()

    async def decide() -> dict[str, Any]:
        db = PostgresClient(settings)
        result = await db.execute(
            """
            UPDATE approval_requests SET status='REJECTED',decided_by=$2,decision_reason=$3,decided_at=now()
            WHERE id=$1 AND status='PENDING'
            """,
            UUID(approval_id),
            approver,
            reason,
        )
        await db.disconnect()
        if result != "UPDATE 1":
            raise ValueError("approval request is missing or already decided")
        return {"approval_id": approval_id, "status": "REJECTED"}

    console.print_json(data=asyncio.run(decide()))


@app.command("guardrail-check")
def guardrail_check(
    action: str = typer.Argument(..., help="Action description to evaluate."),
) -> None:
    settings = load_settings()

    async def check() -> dict[str, Any]:
        identity = AgentIdentity(
            agent_id="cli-inspector",
            role="security_reviewer",
            project_id="00000000-0000-0000-0000-000000000000",
            allowed_actions=["read_file"],
        )
        request = ActionRequest(
            project_id=identity.project_id,
            agent_id=identity.agent_id,
            action_type="read_file",
            description=action,
        )
        result = await PolicyEngine(settings).evaluate_action(request, identity)
        return result.model_dump(mode="json")

    console.print_json(data=asyncio.run(check()))


if __name__ == "__main__":
    app()
