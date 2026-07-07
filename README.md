# AgentOS Local

AgentOS Local is a local-first, Python-based autonomous software delivery platform. It is designed to run a dedicated team of IT and development agents that work in parallel, communicate through guarded events, use scoped memory, execute through controlled supervisors, and continue operating until the project Definition of Done is satisfied.

This repository is a starter scaffold. It establishes the architecture, package structure, runtime boundaries, guardrail model, Docker deployment base, PostgreSQL/pgvector schema, Dragonfly coordination layer, Ray actor model, and CLI entry points.

## Vision

AgentOS Local is not a general assistant, chatbot, or generic automation tool. It is a specialized software engineering platform. A user should be able to provide a request such as:

```text
Build an ecommerce website.
```

The platform should then:

1. Start a bootstrap PM/Tech Lead agent.
2. Let that agent define the project DoD, assumptions, team roles, memory scopes, ownership boundaries, and initial plan.
3. Validate the requested team against configured limits.
4. Start Ray actor workers for the approved agents.
5. Let agents communicate through guarded events.
6. Let agents decide their next best action from events, memory, and DoD gaps.
7. Execute work only through guarded supervisors.
8. Checkpoint and summarize progress after achievements.
9. Continuously evaluate DoD completion.
10. Continue until all mandatory DoD items are verified with evidence.

## Current scope

This starter version provides:

- Python project skeleton.
- Typer CLI.
- Ray actor bootstrap.
- Bootstrap PM/Tech Lead actor.
- Generic Agent Worker actor.
- Runtime supervisor.
- Governance and policy engine skeleton.
- Provider gateway placeholder.
- Memory broker placeholder.
- Checkpoint manager placeholder.
- DoD evaluator placeholder.
- Execution supervisor placeholder.
- PostgreSQL schema with pgvector support.
- Dragonfly event bus helper.
- Dockerfile and Docker Compose.
- Architecture and business documentation.

It does not yet implement the full autonomous coding loop. The next implementation phase should connect provider calls, persistent repositories, Git workspaces, Docker sandbox execution, event routing, task management, semantic memory retrieval, and DoD verification.

## Core architecture

```text
CLI
 ↓
Runtime Supervisor Actor
 ↓
Bootstrap PM/Tech Lead Agent
 ↓
Validated Dynamic Team Plan
 ↓
Ray Agent Worker Actors
 ↓
Agent Governance Layer
 ↓
Guarded Communication Bus
 ↓
Scoped Memory Broker
 ↓
Guarded Action Requests
 ↓
Execution Supervisor
 ↓
Docker Sandbox / Git Workspace / Test Database
 ↓
Checkpoints + Summaries + Audit Logs
 ↓
DoD Evaluator
 ↓
Continue until DoD is satisfied
```

## Technology stack

| Layer | Technology |
|---|---|
| Language | Python 3.11+ |
| CLI | Typer + Rich |
| Actor runtime | Ray actors |
| Durable storage | PostgreSQL |
| Semantic memory | pgvector inside PostgreSQL |
| Hot coordination | Dragonfly, Redis-compatible |
| External AI provider | Provider Gateway, planned LiteLLM adapter |
| Execution isolation | Docker |
| Source control integration | Git workspaces and branches, planned |
| Logging | Structured logs, planned |
| Guardrails | Runtime policy engine, communication/action/memory guardrails |

## Repository structure

```text
agentos_local/
├── agentos/
│   ├── actors/              # Ray actor definitions
│   ├── checkpoints/         # Achievement checkpoints
│   ├── cli/                 # Typer CLI
│   ├── config/              # Settings and environment loading
│   ├── dod/                 # Definition of Done evaluation
│   ├── execution/           # Guarded execution supervisor
│   ├── governance/          # Policy engine and guardrail models
│   ├── memory/              # Scoped memory broker
│   ├── messaging/           # Event schemas and Dragonfly helper
│   ├── provider/            # External AI provider gateway
│   ├── runtime/             # Runtime supervisor and team plan models
│   ├── storage/             # PostgreSQL schema
│   └── watchdogs/           # DoD, stagnation, and safety watchdogs
├── prompts/                 # Future agent prompt templates
├── examples/                # Example project requests
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── arch_plan.md
├── goal.md
└── README.md
```

## Runtime concepts

### 1. Bootstrap agent

The first agent is always the Bootstrap PM/Tech Lead agent. It creates the initial project plan and proposes the team composition. The runtime validates the plan before creating any additional agent actors.

### 2. Ray actors

Each agent worker is a Ray actor. Agents are long-running, stateful workers that can be restarted and restored from durable state. Actor memory is runtime state only. PostgreSQL is the source of truth.

### 3. Event-driven execution

The intended model is not a fixed loop. Agents are triggered by events, catch up from scoped memory, decide the next best action, submit an action request, publish artifacts, checkpoint, summarize, and return to idle.

### 4. Guarded execution

Agents do not directly run shell commands, delete files, modify databases, or call external providers. They propose actions. Supervisors and policy engines decide whether those actions are allowed.

### 5. Run-to-DoD

The platform is DoD-bound, not time-bound. It should run while the DoD is incomplete, and stop only when mandatory DoD items are verified with evidence, or when a safety/approval boundary blocks progress.

## Installation

### Prerequisites

- Docker
- Docker Compose
- Python 3.11+
- Git

### Local Python setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e .
```

### Environment setup

```bash
cp .env.example .env
```

Edit `.env` and add any required external provider keys. Do not commit `.env`.

## Running with Docker Compose

Start the supporting services:

```bash
docker compose up -d postgres dragonfly ray-head
```

Build the application image:

```bash
docker compose build agentos
```

Check status:

```bash
docker compose run --rm agentos status
```

Run a bootstrap plan:

```bash
docker compose run --rm agentos plan "Build an ecommerce website"
```

Start a starter runtime run:

```bash
docker compose run --rm agentos run "Build an ecommerce website"
```

## Running locally without Docker for the Python process

Start Postgres, Dragonfly, and Ray through Docker Compose:

```bash
docker compose up -d postgres dragonfly ray-head
```

Then run:

```bash
agentos status
agentos plan "Build an ecommerce website"
agentos run "Build an ecommerce website"
```

For local Ray without the Ray container, unset `RAY_ADDRESS` and the supervisor will initialize an embedded local Ray runtime.

## CLI commands

### Initialize a project workspace

```bash
agentos init ecommerce-demo
```

Creates:

```text
workspace/ecommerce-demo/
├── source/
├── artifacts/
├── logs/
├── summaries/
├── checkpoints/
└── .agentos/project.json
```

### Preview the bootstrap plan

```bash
agentos plan "Build an ecommerce website"
```

This starts the bootstrap actor, creates a deterministic starter team plan, validates it against configured max agents, and creates starter Ray actors.

### Start the runtime

```bash
agentos run "Build an ecommerce website"
```

The starter scaffold creates the agent team. Future iterations should continue from here into event routing, task creation, Git branches, sandboxed execution, review, tests, and DoD evaluation.

### Inspect configuration status

```bash
agentos status
```

### Test guardrail classification

```bash
agentos guardrail-check "drop database ecommerce"
```

Expected behavior: the policy engine classifies this as critical and denies it unless destructive actions are explicitly allowed and approved.

## Safety model

AgentOS Local follows a zero-trust agent runtime model.

Rules:

- Agents may reason freely.
- Agents may communicate through guarded event envelopes.
- Agents may propose actions.
- Agents may not execute sensitive actions directly.
- The runtime decides what is allowed.
- Destructive actions are denied or escalated.
- Memory access is scoped.
- Provider calls go through the gateway.
- Audit logs are append-only in the target design.

Examples of blocked or approval-required actions:

- Drop database.
- Drop table.
- Truncate persistent data.
- Delete checkpoints.
- Delete audit logs.
- Disable guardrails.
- Disable tests silently.
- Modify provider keys.
- Access secrets without policy.
- Self-approve critical actions.

## Memory model

AgentOS Local uses two memory categories:

### Short-term memory

Backed by Dragonfly. Intended for:

- active events
- locks
- leases
- heartbeats
- hot context
- active work state
- budget counters

### Long-term memory

Backed by PostgreSQL. Intended for:

- events
- checkpoints
- summaries
- decisions
- tasks
- artifacts
- DoD evidence
- provider call audit records
- memory items
- embeddings through pgvector

### Vector index

The vector index is a semantic recall layer, not the source of truth. It should store summaries and retrieval handles, not raw secrets, raw logs, or full repository dumps.

Use pgvector for:

- catch-up packet retrieval
- duplicate task detection
- similar failure lookup
- semantic code map
- contract impact analysis
- DoD gap similarity
- long-term lessons learned

## Development roadmap

### Phase 1: Foundation

- Finish PostgreSQL repository layer.
- Add database migrations.
- Persist projects, agents, events, checkpoints, and summaries.
- Connect Dragonfly streams to Trigger Engine.
- Add structured logging.

### Phase 2: Provider integration

- Implement LiteLLM adapter.
- Add budget tracking.
- Add redaction policy.
- Add provider call audit records.
- Add output safety checks.

### Phase 3: Memory retrieval

- Implement memory ACLs.
- Add embeddings creation pipeline.
- Add pgvector retrieval.
- Add catch-up packet generation.
- Add memory promotion from raw events to validated long-term memories.

### Phase 4: Task and event runtime

- Implement event router.
- Implement trigger engine.
- Implement task model and dependency handling.
- Add ownership locks.
- Add agent subscriptions.

### Phase 5: Execution layer

- Add Git branch/worktree manager.
- Add Docker sandbox runner.
- Add patch application workflow.
- Add test/lint/build runners.
- Enforce allowed paths and command policies.

### Phase 6: Review and DoD loop

- Add reviewer agent flows.
- Add QA agent flows.
- Add DoD evidence model.
- Add DoD watchdog.
- Add stagnation and deadlock detection.
- Implement run-until-DoD runtime behavior.

## Development standards

- Keep agents domain-restricted to IT and software development.
- Every action must be logged.
- Every meaningful achievement must create a checkpoint.
- Every completed checkpoint should create or update a summary.
- Do not allow raw agent execution of shell/database/file operations.
- Use PostgreSQL as truth and Dragonfly as coordination/cache.
- Use pgvector only for semantic recall.
- Use deterministic guardrails for known-dangerous actions.
- Use reviewer/safety agents for ambiguous engineering risk.
- Require evidence for every DoD item.

## License

Add your preferred license before public release.
