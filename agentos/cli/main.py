from __future__ import annotations

import asyncio
import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table
import ray
from agentos.dod.evaluator import DoDEvaluator

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
    
    for subdir in ["source", "artifacts", "logs", ".agentos", "summaries", "checkpoints"]:
        (root / subdir).mkdir(parents=True, exist_ok=True)
        
    project_file = root / ".agentos" / "project.json"
    project_file.write_text(json.dumps({"project_name": project_name, "status": "INITIALIZED"}, indent=2))
    console.print(f"Initialized project workspace: {root}")

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

@app.command("test-agent")
def test_agent() -> None:
    """End-to-End test: Runs advanced multi-step software engineering task graphs and quality checks."""
    import uuid
    import asyncio
    
    from agentos.actors.base import AgentWorkerActor
    from agentos.execution.supervisor import ExecutionSupervisor
    from agentos.governance.models import ActionRequest
    from agentos.storage.database import DatabaseManager
    from agentos.storage.repositories import TaskRepository
    
    settings = load_settings()
    console.print("[bold yellow]Initializing Advanced Multi-File Software Delivery Test Loop...[/bold yellow]")
    
    async def run_test():
        # 1. Setup Database & Repositories
        db = DatabaseManager(settings)
        await db.connect()
        task_repo = TaskRepository(db)
        evaluator = DoDEvaluator(db)
        
        project_id = str(uuid.uuid4())
        unique_project_name = f"SecureCalcAPI-{project_id[:8]}"
        
        # High-Quality Quality Contract (DoD Checkboxes)
        project_dod = [
            "calculator.py", 
            "test_calculator.py", 
            "verify code output standard"
        ]
        
        # Register project parameters
        await db.pool.execute(
            "INSERT INTO projects (id, name, dod) VALUES ($1, $2, $3)", 
            uuid.UUID(project_id), unique_project_name, json.dumps(project_dod)
        )
        
        console.print(f"[bold green]Created Advanced Project Entry:[/bold green] {unique_project_name}")
        console.print(f"[bold blue]Target Quality Contract (DoD):[/bold blue] {project_dod}")
        
        # 2. Dynamic Task Graph Generation (Sequential Tree Topology)
        console.print("[cyan]Seeding engineering sub-tasks tree checklist into PostgreSQL database...[/cyan]")
        
        task_1_id = await task_repo.create_task(
            project_id=project_id,
            title="Implement Core Logic",
            description="Create calculator.py containing add, subtract, multiply, and safe divide functions.",
            priority=3
        )
        
        task_2_id = await task_repo.create_task(
            project_id=project_id,
            title="Implement Verification Suite",
            description="Create test_calculator.py containing assertion statements verifying calculation accuracy.",
            priority=4,
            parent_task_id=task_1_id
        )
        
        await task_repo.add_dependency(task_id=task_2_id, depends_on_task_id=task_1_id)
        
        # Create starting system trigger event with rigorous instructions
        fake_event_id = str(uuid.uuid4())
        await db.pool.execute(
            "INSERT INTO events (id, project_id, event_type, topic, payload) VALUES ($1, $2, $3, $4, $5)",
            uuid.UUID(fake_event_id), uuid.UUID(project_id), "UserRequest", "New Task Execution", 
            json.dumps({
                "message": (
                    f"Build a production-ready, error-safe math processing module for project {unique_project_name}.\n"
                    "STEP 1: Write 'calculator.py' with add, subtract, multiply, and divide logic.\n"
                    "STEP 2: Write 'test_calculator.py' which imports 'calculator' and runs assert tests.\n"
                    "STEP 3: CRITICAL MANDATE - Run 'python3 test_calculator.py' using a 'shell_command' to verify the code output standard."
                )
            })
        )
        
        if hasattr(AgentWorkerActor, "__ray_metadata__"):
            metadata = AgentWorkerActor.__ray_metadata__
            underlying_class = getattr(metadata, "modified_class", getattr(metadata, "class_target", None))
        else:
            underlying_class = AgentWorkerActor

        console.print("[cyan]Spawning Developer Agent worker persona...[/cyan]")
        agent = underlying_class(
            agent_id="dev-test-1", 
            role="Senior Python Engineer", 
            project_id=project_id, 
            settings=settings.model_dump()
        )
        
        await agent.start()
        supervisor = ExecutionSupervisor(settings)
        
        # 4. Multi-Step Inline Quality Gating Execution Loop
        max_iterations = 4
        current_step = 1
        
        while current_step <= max_iterations:
            console.print(f"\n[bold magenta]=== AUTONOMOUS LOOP ITERATION {current_step}/{max_iterations} ===[/bold magenta]")
            console.print(f"[cyan]Evaluating active database task dependencies...[/cyan]")
            
            result = await agent.handle_event(fake_event_id)
            
            proposed_action_dict = result.get("proposed_action", {})
            action_type = proposed_action_dict.get("action_type", "wait")
            description = proposed_action_dict.get("description", "")
            
            console.print(f"\n[green]Agent Decision: {action_type.upper()}[/green] - [dim]{description}[/dim]")
            
            if action_type == "wait":
                console.print("[bold green]Agent entered IDLE state successfully.[/bold green]")
                break
                
            console.print("[cyan]Routing action to Execution Supervisor sandbox...[/cyan]")
            action_request = ActionRequest(**proposed_action_dict)
            execution_result = await supervisor.request_execution(action_request)
            
            console.print("[bold green]Execution Output Logs Returned to Memory Base:[/bold green]")
            console.print_json(data=execution_result)
            
            # Update status maps dynamically based on which file the agent target writes
            if action_type in {"write_file", "write_code"} and execution_result.get("executed"):
                file_written = proposed_action_dict.get("payload", {}).get("file_path", "")
                if "test" in file_written:
                    await task_repo.update_task_status(task_2_id, "COMPLETED")
                else:
                    await task_repo.update_task_status(task_1_id, "COMPLETED")
            
            # --- 🔍 VISIBILITY CHECKPOINT: THE WATCHDOG DECISION MOMENT ---
            console.print("\n[bold yellow]🔍 [SUPERVISOR QUALITY MONITOR]: Running real-time evaluation of checkpoint database states...[/bold yellow]")
            dod_report = await evaluator.evaluate(project_id, project_dod)
            
            # Display current completion standings live on every turn
            console.print(f"[dim]Current Status -> Satisfied: {dod_report.satisfied} | Remaining Gaps: {dod_report.gaps}[/dim]")
            
            if dod_report.satisfied:
                console.print("\n[bold green]🎯 REAL-TIME DOD GATING BREAK: All contract milestones met perfectly! Shutting down loop early to save resources.[/bold green]")
                console.print_json(data=dod_report.model_dump())
                break
            
            fake_event_id = str(uuid.uuid4())
            await db.pool.execute(
                "INSERT INTO events (id, project_id, event_type, topic, payload) VALUES ($1, $2, $3, $4, $5)",
                uuid.UUID(fake_event_id), uuid.UUID(project_id), "ExecutionFeedback", "Task Progress Tracking",
                json.dumps({"last_action": action_type, "execution_success": execution_result.get("executed", False), "runtime_response": execution_result.get("result", {})})
            )
            
            current_step += 1
            await asyncio.sleep(1.0)

        # 5. Final Dynamic Closure Watchdog Report Validation Gating
        console.print("\n[bold cyan]=== RUNTIME CLOSURE GATING: FINAL STATUS EVALUATION ===[/bold cyan]")
        final_report = await evaluator.evaluate(project_id, project_dod)
        
        if final_report.satisfied:
            console.print("\n[bold green]🎉 SUCCESS: All multi-file test criteria satisfied successfully! Gating loop closed cleanly.[/bold green]")
        else:
            console.print_json(data=final_report.model_dump())
            console.print("\n[bold red]⚠️ WATCHDOG ALERT TRIGGERED: Project incomplete after maximum loops.[/bold red]")

        await db.disconnect()

    asyncio.run(run_test())
    console.print("[bold yellow]Advanced Configuration Test Complete.[/bold yellow]")