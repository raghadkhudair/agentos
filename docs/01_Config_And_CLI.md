# 01 — Configuration & the Command Line Interface (CLI)

Files covered:
- `agentos/config/settings.py`
- `agentos/config/loader.py`
- `agentos/cli/main.py`

These are the files that run **first** — before any AI agent, any database
connection, any Ray actor exists. Read this file with `00_Python_Fundamentals_Glossary.md`
open in another tab; I'll reference sections like "(§3)" for async/await.

---

## `agentos/config/settings.py`

This file's whole job: read configuration (URLs, limits, feature flags) from
environment variables, with sensible fallback defaults, and package it into
one object every other part of the program can use.

```python
from __future__ import annotations
import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
```
- Line 1: compatibility import (§Glossary 7).
- Line 2: `os` is Python's standard library for talking to the operating
  system — here it's used to find the user's home directory.
- Line 3: `Field` from Pydantic lets us set defaults + aliases (§Glossary 5).
- Line 4: `BaseSettings` is the special Pydantic base class built specifically
  for reading configuration from environment variables (§Glossary 5).
  `SettingsConfigDict` configures *how* it reads them.

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True)
```
- We define a class `Settings` that inherits all the "read from environment"
  superpowers of `BaseSettings`.
- `model_config` is special Pydantic configuration (not a regular field):
  - `env_file=".env"` — also read from a local `.env` file, not just real
    environment variables (handy for local development).
  - `extra="ignore"` — if the `.env` file has variables this class doesn't
    know about, don't error, just ignore them.
  - `populate_by_name=True` — allows creating a `Settings` object either by
    its Python attribute name or by its alias.

```python
    project_name: str = Field(default="agentos-project", alias="AGENTOS_PROJECT_NAME")
```
- This declares a field called `project_name`, of type `str`.
- `alias="AGENTOS_PROJECT_NAME"` tells Pydantic: "when reading from the
  environment or `.env` file, look for a variable literally named
  `AGENTOS_PROJECT_NAME`, and store its value here."
- `default="agentos-project"` — if that environment variable isn't set at
  all, use this fallback value.

```python
    workspace: str = Field(
        default_factory=lambda: os.path.abspath(os.path.join(os.path.expanduser("~"), ".agentos_sandbox")),
        alias="AGENTOS_WORKSPACE"
    )
```
- `default_factory` (instead of `default`) is used when the default value
  needs to be *computed*, not just a fixed literal. Here it's a `lambda`
  (an anonymous, one-line function — §Glossary bonus concept) that computes:
  "the user's home folder, plus `.agentos_sandbox`" — e.g. on Linux/Mac,
  something like `/home/yourname/.agentos_sandbox`. This is the local
  folder AgentOS uses to store generated project files.

```python
    environment: str = Field(default="local", alias="AGENTOS_ENV")
    log_level: str = Field(default="INFO", alias="AGENTOS_LOG_LEVEL")
    ray_address: str | None = Field(default=None, alias="RAY_ADDRESS")
```
- `environment` and `log_level` are simple string settings.
- `ray_address` can be a string *or* `None` (§Glossary 2) — this is a
  **leftover field**. Earlier versions of the code used it to decide whether
  to connect to a separate Ray cluster container. As of the latest commits,
  `runtime/supervisor.py` no longer reads this value at all (it always
  starts a local embedded Ray instance) — so this field is now unused
  dead code. Harmless, but worth knowing it doesn't do anything currently.

```python
    database_url: str = Field(
        default="postgresql://agentos:agentos@localhost:5432/agentos", alias="DATABASE_URL"
    )
    dragonfly_url: str = Field(default="redis://dragonfly:6379/0", alias="DRAGONFLY_URL")
```
- Standard connection-string settings for Postgres and Dragonfly/Redis.
  Format is `protocol://username:password@host:port/database_number`.
  Inside Docker Compose, `host` is the *service name* (`postgres`,
  `dragonfly`) — Docker's internal DNS resolves those names to the right
  container automatically. That's why these look like URLs pointing to
  machines named "postgres" and "dragonfly" rather than IP addresses.

```python
    provider_default_model: str = Field(
        default="gemini/gemini-2.5-pro", alias="AGENTOS_PROVIDER_DEFAULT_MODEL"
    )
    provider_fallback_model: str = Field(
        default="gemini/gemini-2.5-flash", alias="AGENTOS_PROVIDER_FALLBACK_MODEL"
    )
```
- Which LLM model to use by default, and which to fall back to if the
  primary one fails. Note: in practice, `provider/gateway.py` currently
  overrides these with values from `runtime_tuning.yaml` instead (covered
  in the Provider file) — so changing these two lines alone won't actually
  change which model is used anymore.

```python
    daily_budget_usd: float = Field(default=100.0, alias="AGENTOS_DAILY_BUDGET_USD")
    monthly_budget_usd: float = Field(default=1000.0, alias="AGENTOS_MONTHLY_BUDGET_USD")

    require_review: bool = Field(default=True, alias="AGENTOS_REQUIRE_REVIEW")
    require_tests: bool = Field(default=True, alias="AGENTOS_REQUIRE_TESTS")
    require_human_approval_for_critical: bool = Field(
        default=True, alias="AGENTOS_REQUIRE_HUMAN_APPROVAL_FOR_CRITICAL"
    )
    allow_destructive_actions: bool = Field(default=False, alias="AGENTOS_ALLOW_DESTRUCTIVE_ACTIONS")
```
- Spending caps (used by `provider/gateway.py` to stop calling the LLM if
  you've spent too much).
- Safety toggles (`bool` = `True`/`False`) — these feed into the guardrail
  system in `governance/policy_engine.py`. `allow_destructive_actions`
  defaulting to `False` is an important safety default: it means
  potentially dangerous shell commands are blocked unless you explicitly
  turn this on.

```python
def load_settings() -> Settings:
    return Settings()
```
- A tiny "factory function." Calling `Settings()` with no arguments triggers
  Pydantic to automatically read every field from the environment/`.env`
  file, using the aliases and defaults above. Every file in the codebase
  that needs configuration calls `load_settings()` rather than constructing
  `Settings()` directly — this is a common pattern so there's one obvious
  place to look if you want to know "how do I get my config?"

---

## `agentos/config/loader.py`

This is a newer file (added in the latest commits) that loads three YAML
config files: `actor_team.yml`, `guardrail_policies.yaml`, `runtime_tuning.yaml`.
Unlike `settings.py` (environment variables), these are meant to hold
**structured, nested configuration** — lists of patterns, tuning knobs,
role definitions — that would be awkward to express as flat environment
variables.

```python
import os
from functools import lru_cache
import yaml

_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
```
- `__file__` is a built-in variable Python gives every module: the path to
  the current `.py` file on disk. `os.path.dirname(os.path.abspath(__file__))`
  turns that into "the absolute path of the folder this file lives in" —
  i.e. `agentos/config/`. This is how the loader knows where to look for
  the YAML files, regardless of what folder you ran the program *from*.
- `yaml` is a third-party library for reading `.yaml`/`.yml` files into
  Python dictionaries.

```python
@lru_cache(maxsize=None)
def load_config(filename: str) -> dict:
    path = os.path.join(_CONFIG_DIR, filename)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
```
- `@lru_cache(maxsize=None)` is a decorator (§Glossary 4) that **caches**
  the result of this function. The first time you call `load_config("runtime_tuning.yaml")`,
  it actually reads the file. Every call after that with the *same argument*
  instantly returns the cached result instead of re-reading the file from
  disk. `maxsize=None` means "cache unlimited distinct results" (fine here
  since there are only 3 possible filenames).
- `with open(...) as f:` — a context manager (§Glossary 10): opens the file,
  and guarantees it's closed afterward even if something goes wrong.
- `yaml.safe_load(f)` parses the YAML text into a Python dictionary.
  `safe_load` (vs. plain `load`) refuses to execute arbitrary Python code
  that could theoretically be embedded in a malicious YAML file — a
  security best practice.
- `or {}` — if the file is empty, `yaml.safe_load` returns `None`; this
  swaps that for an empty dictionary so callers don't have to handle `None`.

```python
def team_roles() -> dict:
    return load_config("actor_team.yml")

def guardrail_policies() -> dict:
    return load_config("guardrail_policies.yaml")

def runtime_tuning() -> dict:
    return load_config("runtime_tuning.yaml")
```
- Three thin convenience wrappers, one per config file, so the rest of the
  codebase can write `runtime_tuning()["ray"]["num_cpus"]` instead of
  remembering exact filenames everywhere.

---

## `agentos/cli/main.py`

This is the actual program you run from your terminal: `agentos <command> ...`.
It's built with **Typer**, a library that turns regular Python functions
into command-line commands automatically, based on their type hints.

```python
import typer
from rich.console import Console
from rich.table import Table
```
- `typer` builds the CLI itself.
- `rich` is a library for pretty terminal output — colored text, tables,
  formatted JSON (that's where all the `[bold green]...[/bold green]`
  markup you saw in your logs comes from).

```python
app = typer.Typer(no_args_is_help=True, help="AgentOS Local CLI")
console = Console()
```
- `app` is the Typer application object. Every function decorated with
  `@app.command()` below becomes a subcommand (`agentos init`, `agentos run`, etc).
  `no_args_is_help=True` means running `agentos` with no arguments just
  prints the help text instead of erroring.
- `console` is the shared Rich console object used for all pretty-printing.

### `agentos init <project_name>`

```python
@app.command()
def init(project_name: str = typer.Argument(..., help="Local project name.")) -> None:
```
- `typer.Argument(...)` marks `project_name` as a **required** positional
  command-line argument (the `...` — Python's `Ellipsis` — is Typer's way
  of saying "no default, this is mandatory"). So you'd run:
  `agentos init my-cool-project`.

```python
    settings = load_settings()
    root = Path(settings.workspace) / project_name
    for subdir in ["source", "artifacts", "logs", ".agentos", "summaries", "checkpoints"]:
        (root / subdir).mkdir(parents=True, exist_ok=True)
```
- Loads config, then builds a folder path: `<workspace>/<project_name>`.
  `Path` (from Python's standard `pathlib`) lets you build file paths with
  the `/` operator instead of manually gluing strings together with slashes
  — it also automatically uses the right slash direction for your OS.
- Loops through 6 subfolder names and creates each one.
  `parents=True` means "create any missing parent folders too."
  `exist_ok=True` means "don't error if the folder already exists."

```python
    project_file = root / ".agentos" / "project.json"
    project_file.write_text(json.dumps({"project_name": project_name, "status": "INITIALIZED"}, indent=2))
    console.print(f"Initialized project workspace: {root}")
```
- Writes a small JSON file recording that this project workspace exists.
  `json.dumps(...)` converts a Python dictionary into a JSON-formatted
  string; `indent=2` makes it human-readable (pretty-printed).

```python
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
```
- Defines a small nested async function (§Glossary 3) that connects to
  Postgres, runs the schema-creation SQL, then disconnects.
- `asyncio.run(setup_db())` is how we call async code from this otherwise
  synchronous `init` function — it starts an event loop, runs `setup_db()`
  to completion, then returns.
- Wrapped in `try/except` (§Glossary 11) so that if Postgres isn't running
  yet, you get a friendly error message instead of a scary crash.

### `agentos plan <request>` and `agentos run <request>`

```python
@app.command()
def plan(request: str = typer.Argument(...)) -> None:
    settings = load_settings()
    supervisor = RuntimeSupervisor(settings)
    result = asyncio.run(supervisor.bootstrap_project(request))
    console.print_json(data=result["team_plan"])
```
- Creates a `RuntimeSupervisor` (the orchestrator class — full detail in
  the Runtime file) and runs its `bootstrap_project()` coroutine to
  completion, then prints just the `"team_plan"` part of the result as
  formatted JSON.

```python
@app.command()
def run(request: str = typer.Argument(...)) -> None:
    settings = load_settings()
    supervisor = RuntimeSupervisor(settings)
    result = asyncio.run(supervisor.bootstrap_project(request))
    console.print("Runtime started. Starter scaffold creates and starts agent actors only.")
    console.print_json(data=result)
```
- Nearly identical to `plan`, but prints the *entire* result dict, not just
  the team plan. Both `plan` and `run` actually call the exact same
  `bootstrap_project()` method underneath — as of the latest code,
  `bootstrap_project()` doesn't return until the whole project is complete
  (it blocks on an internal "are we done yet?" signal — see the Runtime
  file for exactly how). So despite the comment saying "creates and starts
  agent actors only," in the current version `run` (and even `plan`, which
  probably wasn't intended) will actually block until the DoD watchdog
  reports the project finished.

### `agentos status`

```python
@app.command()
def status() -> None:
    settings = load_settings()
    table = Table(title="AgentOS Local Status")
    table.add_column("Setting")
    table.add_column("Value")
    table.add_row("Project", settings.project_name)
    ...
    console.print(table)
```
- Builds a Rich `Table` object, adds two columns, then adds one row per
  setting you want to display, and prints it. Purely a diagnostic command —
  doesn't touch the database, Ray, or the LLM at all. Good first command to
  run to sanity-check your `.env` file is being read correctly.

### `agentos guardrail-check <action>`

```python
@app.command("guardrail-check")
def guardrail_check(action: str = typer.Argument(...)) -> None:
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
```
- `@app.command("guardrail-check")` — passing an explicit string names the
  CLI command `guardrail-check` (Typer would otherwise turn the Python
  function name `guardrail_check` into `guardrail-check` automatically by
  replacing underscores with dashes, but here it's done explicitly).
- Notice the imports are **inside** the function, not at the top of the
  file. This is a deliberate pattern used throughout this codebase — it
  delays importing (and therefore initializing) the governance module until
  you actually run this specific command, keeping startup fast for the
  other commands that don't need it. You'll see this "lazy import" pattern
  repeated in several other files.
- Builds a fake `ActionRequest` describing whatever text you passed on the
  command line, runs it through `PolicyEngine.evaluate_action()` (full
  detail in the Governance file), and prints the verdict (ALLOW/DENY/etc.)
  as JSON. Handy for testing guardrail rules without spinning up any agents.

### `agentos test-agent`

This is the longest command — a **self-contained integration test** that
proves the core "agent does work, gets reviewed, checks DoD" loop functions,
without needing Ray's trigger/event routing running. It:

1. Connects to the database directly.
2. Creates a fake project with 3 DoD (Definition of Done) items:
   `["calculator.py", "test_calculator.py", "verify code output standard"]`.
3. Seeds two dependent tasks: "write calculator.py" then "write
   test_calculator.py" (the second depends on the first via
   `task_repo.add_dependency(...)`).
4. Inserts one fake "UserRequest" event describing exactly what to build,
   in plain English, as if it came from the trigger engine.
5. There's a slightly unusual block here:
   ```python
   if hasattr(AgentWorkerActor, "__ray_metadata__"):
       metadata = AgentWorkerActor.__ray_metadata__
       underlying_class = getattr(metadata, "modified_class", getattr(metadata, "class_target", None))
   else:
       underlying_class = AgentWorkerActor
   ```
   Recall `AgentWorkerActor` is decorated with `@ray.remote` (§Glossary 4,
   12). That decorator actually *replaces* the class with a special Ray
   wrapper object — so `AgentWorkerActor(...)` no longer creates a plain
   Python object directly, it creates a Ray actor handle. This block is
   reaching *through* that Ray wrapper to grab the **original**, undecorated
   Python class underneath (`__ray_metadata__` is where Ray stashes it), so
   this test can create a plain, local, non-Ray instance of the agent —
   letting the test call its methods directly and see print statements/errors
   immediately, without any of the complexity of actors running in separate
   processes. It's a debugging convenience, not something you'll need to
   write yourself.
6. Calls `agent.start()` then loops up to 4 times calling
   `agent.process_next_step(fake_event_id)`, feeding the result into the
   `ExecutionSupervisor` (full detail in the Governance/Execution file),
   checking DoD after each step via `DoDEvaluator`, and breaking early if
   everything's satisfied.
7. Prints a final success/failure summary.

This command is genuinely useful for you: if something's broken in the
"agent decides an action → guardrail checks it → action executes → DoD
re-evaluated" pipeline, running `docker compose exec agentos agentos test-agent`
will surface the problem much faster than running the full multi-agent `run`
command, because it skips Ray actor creation, the trigger engine, and the
watchdog loop entirely.

---

## What's next

Next file: **`02_Actors.md`** — this is where the actual "AI agent" classes
live (`base.py`, `bootstrap.py`, `reviewer.py`), including the full
explanation of how `@ray.remote` actors work, the inbox/wakeup loop, and how
an agent decides what to do next.
