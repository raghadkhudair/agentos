from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from agentos.config.settings import load_settings
from agentos.runtime.supervisor import RuntimeSupervisor
from agentos.storage.database import DatabaseManager

app = typer.Typer(no_args_is_help=True, help="AgentOS Local CLI")
console = Console()


@app.command()
def init(project_name: str = typer.Argument(..., help="Local project name.")) -> None:
    """Create a local project workspace skeleton and initialize the database."""
    
    
    settings = load_settings()
    root = Path(settings.workspace) / project_name
    
    # 1. Create the local workspace folders
    for subdir in ["source", "artifacts", "logs", ".agentos", "summaries", "checkpoints"]:
        (root / subdir).mkdir(parents=True, exist_ok=True)
        
    project_file = root / ".agentos" / "project.json"
    project_file.write_text(json.dumps({"project_name": project_name, "status": "INITIALIZED"}, indent=2))
    console.print(f"Initialized project workspace: {root}")

    # 2. Initialize the PostgreSQL database schema
    async def setup_db():
        db = DatabaseManager(settings)
        await db.connect()
        await db.initialize_schema()
        await db.disconnect()
        
    try:
        asyncio.run(setup_db())
        console.print("[bold green]Database schema initialized successfully with pgvector support.[/bold green]")
    except Exception as e:
        console.print(f"[bold red]Failed to initialize database: {e}[/bold red]")
        console.print("Make sure your Docker compose stack (postgres) is running!")

@app.command()
def plan(request: str = typer.Argument(..., help="Project request, for example: Build an ecommerce website.")) -> None:
    """Create a first deterministic bootstrap plan preview."""
    settings = load_settings()
    supervisor = RuntimeSupervisor(settings)
    result = asyncio.run(supervisor.bootstrap_project(request))
    console.print_json(data=result["team_plan"])


@app.command()
def run(request: str = typer.Argument(..., help="Project request to execute until DoD is satisfied.")) -> None:
    """Start the runtime supervisor and create the first Ray agent team."""
    settings = load_settings()
    supervisor = RuntimeSupervisor(settings)
    result = asyncio.run(supervisor.bootstrap_project(request))
    console.print("Runtime started. Starter scaffold creates and starts agent actors only.")
    console.print_json(data=result)


@app.command()
def status() -> None:
    """Print local runtime configuration status."""
    settings = load_settings()
    table = Table(title="AgentOS Local Status")
    table.add_column("Setting")
    table.add_column("Value")
    table.add_row("Project", settings.project_name)
    table.add_row("Workspace", settings.workspace)
    table.add_row("Ray address", str(settings.ray_address))
    table.add_row("Database", settings.database_url)
    table.add_row("Dragonfly", settings.dragonfly_url)
    table.add_row("Max agents", str(settings.max_agents_total))
    table.add_row("Max active agents", str(settings.max_active_agents))
    table.add_row("Destructive actions allowed", str(settings.allow_destructive_actions))
    console.print(table)


@app.command("guardrail-check")
def guardrail_check(action: str = typer.Argument(..., help="Action description to evaluate.")) -> None:
    """Evaluate an example action against deterministic guardrails."""
    from agentos.governance.models import ActionRequest
    from agentos.governance.policy_engine import PolicyEngine

    settings = load_settings()
    engine = PolicyEngine(settings)
    result = engine.evaluate_action(
        ActionRequest(
            project_id=settings.project_name,
            agent_id="manual-check",
            action_type="manual",
            description=action,
        )
    )
    console.print_json(data=result.model_dump())
