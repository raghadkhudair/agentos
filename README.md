# AgentOS Local

AgentOS Local is a production-oriented, local-first autonomous software-delivery runtime. A bootstrap PM creates an evidence-bound plan, an infrastructure agent computes a bounded resource envelope, independent Ray actors execute role-owned tasks, and the supervisor continues until every mandatory Definition of Done (DoD) criterion has current passing evidence.

This repository contains the implemented runtime, not a starter scaffold.

## What is implemented

- Independent, restartable Ray worker actors with private runtime state, independent database/bus clients, role-specific permissions, inboxes, task leases, heartbeats, checkpoints, and frequent collaboration events.
- Supervisor and infrastructure-agent separation. The infrastructure agent detects CPU and memory, preserves host headroom, assigns per-agent CPU/memory/concurrency/provider/model capacity, and persists the generated runtime configuration.
- PostgreSQL as the durable system of record; DragonflyDB for hot coordination; MongoDB for expiring mid-term memory; MinIO for versioned objects; Milvus for semantic indexes and references.
- Real client classes for every data system under `agentos/storage/clients/`, used by runtime services rather than merely declared as dependencies.
- Provider-neutral LiteLLM gateway for OpenAI, Anthropic/Claude, Gemini, DeepSeek, Moonshot/Kimi, Alibaba/Qwen, Z.AI/GLM, MiniMax, and Ollama.
- Complexity- and role-aware model selection, provider fallback, concurrency caps, budgets, credential redaction, egress-host validation, audit records, and circuit breakers.
- Git worktree isolation, atomic file writes, versioned MinIO artifacts, sandboxed commands, a physically separate sandbox database, independent code/security review, and test-before-merge enforcement.
- Transactional PostgreSQL event outbox, Dragonfly Streams delivery, per-agent consumer groups, durable receipt/lease state, stale-message reclaim, scoped shared memory, and catch-up packets.
- Versioned, source-revision-grounded DoD contracts with stable criterion IDs/hashes, provenance and locks, explicit evidence cardinality, governed amendment/waiver, typed gaps, and append-only task/artifact/Git-bound evidence.
- Prospective integrated-tree verification, renewable merge locking, durable replayable integration attempts, conservative path/contract freshness invalidation, and an atomic contract/HEAD/evidence-generation finalization fence. Text similarity cannot mark work complete.
- Pause, resume, approval, rejection, status, logs, inspection, dependency health, runtime configuration, and guardrail CLI surfaces.

## Architecture

```text
CLI
  -> Runtime Supervisor (lifecycle and policy)
      -> Bootstrap PM (immutable source snapshot -> versioned DoD -> backlog/team)
      -> Infrastructure Agent (resource and provider allocation)
      -> Independent Ray Workers (role-owned task execution)
      -> Provider Gateway (all model calls, budgets, failover)
      -> Memory Broker (Dragonfly + MongoDB + PostgreSQL + MinIO + Milvus)
      -> Execution Supervisor (policy + Git + Docker sandbox + test DB)
      -> Per-criterion Reviewer / Security Reviewer / QA evidence
      -> Snapshot-fenced DoD Evaluator and bounded repair watchdogs
```

The supervisor coordinates; it does not share mutable in-process worker state. PostgreSQL and the durable outbox are the cross-process truth. Dragonfly is disposable coordination state, MongoDB is TTL-bound working memory, MinIO stores large/versioned payloads, and Milvus stores semantic vectors plus references. Milvus is not an authority for completion.

## Required services

| Service | Responsibility | Client |
|---|---|---|
| PostgreSQL 16 | Projects, plans, agents, tasks, events/outbox, evidence, audit, checkpoints, summaries, memory metadata, runtime snapshots | `PostgresClient` / `asyncpg` |
| DragonflyDB | Streams, inboxes, locks, leases, budgets, circuit breakers, hot state | `DragonflyClient` / Redis protocol |
| MongoDB 8 | Mid-term working memory and recoverable agent state with TTL indexes | `MongoDocumentClient` / `AsyncMongoClient` |
| MinIO | Versioned artifacts and large memory bodies | `MinioObjectClient` / MinIO SDK |
| Milvus 2.6 | Strong-consistency semantic lookup over scoped references | `MilvusVectorClient` / `MilvusClient` |

## Quick start

Prerequisites: Docker with Compose, Git, and enough memory for Milvus. The default container ceilings are 4 CPUs and 6 GiB, while the runtime still reserves host CPU/memory headroom.

```bash
cp .env.example .env
```

Replace every `CHANGE_ME` value, configure at least one AI provider, and never commit `.env`. Set `AGENTOS_SOURCE_REPOSITORY` to an existing Git worktree visible to the runtime process when a run must begin from real source; the execution service clones that source into its managed repository before creating task worktrees. Under Compose, seed the repository inside the `/workspaces` volume or add an explicit read-only bind mount and use its container path.

Planning captures only Git-tracked paths plus bounded relevant manifests/docs and binds the contract to the clean repository HEAD. A dirty, bare, unreadable, or oversized source snapshot blocks planning instead of producing an ungrounded DoD. Then:

```bash
docker compose config --quiet
docker compose up -d postgres sandbox-postgres dragonfly mongodb minio etcd milvus docker-proxy
docker compose run --rm agentos doctor
docker compose run --rm agentos init my-project
docker compose run --rm agentos runtime-config
docker compose run --rm agentos run "Build the requested production software"
```

`doctor` verifies all five required stores. A missing provider does not make storage unhealthy, but `run` persists the plan and enters `BLOCKED_REQUIRES_INPUT` instead of inventing credentials or pretending workers ran.

The Compose stack binds developer-facing service ports to nonstandard localhost defaults (`55432`, `56380`, `57017`, `59000`, `59001`) to avoid common local collisions. Internal service traffic stays on an isolated Docker network.

## Direct production policy

AgentOS has one delivery path. There is no canary/staging implementation that can be mistaken for completion. Each task is developed on an isolated Git worktree and reaches the integration branch only after artifact checks, independent review, sandbox verification, and evidence gates pass. Deployment to an external production environment is intentionally outside this runtime; AgentOS produces production-grade source and delivery evidence, not unapproved infrastructure promotion.

Production configuration fails closed when credentials are missing, default/placeholder passwords remain, the sandbox database equals the control database, or an external MinIO/Milvus connection is configured without required TLS/authentication.

## Provider configuration

Configure one or more:

| Provider | Credential |
|---|---|
| OpenAI | `OPENAI_API_KEY` |
| Anthropic / Claude | `ANTHROPIC_API_KEY` |
| Google Gemini | `GEMINI_API_KEY` |
| DeepSeek | `DEEPSEEK_API_KEY` |
| Moonshot / Kimi | `MOONSHOT_API_KEY` |
| Alibaba / Qwen | `DASHSCOPE_API_KEY` |
| Z.AI / GLM | `ZAI_API_KEY` |
| MiniMax | `MINIMAX_API_KEY` |
| Ollama | `OLLAMA_API_BASE` |

Default model routes and role preferences live in `agentos/config/providers.yaml`. Override any tier with variables such as `OPENAI_MODEL_LOW`, `OPENAI_MODEL_STANDARD`, `OPENAI_MODEL_HIGH`, and `OPENAI_MODEL_CRITICAL`. Every model identifier includes the provider prefix required by LiteLLM.

Provider calls never happen directly from workers. Before egress, the gateway persists an append-only call intent, validates availability/capabilities and the egress destination, selects by task complexity, redacts credentials, and reserves budget atomically. It then applies timeouts/concurrency limits, records hashes and usage against the intent, and fails over only to eligible configured providers.

## Resource controls

Important environment variables:

- `AGENTOS_CPU_USAGE_FRACTION`, `AGENTOS_RESERVED_CPU_CORES`, `AGENTOS_MAX_CPU_CORES`
- `AGENTOS_MEMORY_USAGE_FRACTION`, `AGENTOS_RESERVED_MEMORY_BYTES`, `AGENTOS_MAX_MEMORY_BYTES`
- `AGENTOS_WORKER_CPU`, `AGENTOS_WORKER_MEMORY_BYTES`, `AGENTOS_MAX_ACTIVE_AGENTS`
- `AGENTOS_MAX_PARALLEL_CODE_TASKS`, `AGENTOS_MAX_THREADS_PER_AGENT`
- `AGENTOS_CONTAINER_CPUS`, `AGENTOS_CONTAINER_MEMORY`, `AGENTOS_SHM_SIZE`

The planner always leaves at least one detected core unallocated on multi-core hosts. Ray resources are admission-control quantities, so the runtime also sets BLAS/OpenMP thread ceilings, enforces concurrency semaphores, and relies on container CPU/memory/PID limits for hard execution boundaries.

## CLI

```text
agentos init PROJECT_NAME
agentos plan REQUEST
agentos run REQUEST [--detach]
agentos runtime-config [--agent ROLE ...] [--output FILE]
agentos doctor
agentos status [PROJECT_ID]
agentos logs PROJECT_ID [--limit N]
agentos inspect PROJECT_ID
agentos re-evaluate PROJECT_ID
agentos amend-dod PROJECT_ID --contract TEAM_PLAN.json --reason TEXT --requested-by NAME [--approval-id UUID]
agentos waive-dod PROJECT_ID CRITERION_ID --reason TEXT --requested-by NAME [--approval-id UUID]
agentos pause PROJECT_ID
agentos resume PROJECT_ID
agentos approve APPROVAL_ID --approver NAME
agentos reject APPROVAL_ID --approver NAME --reason TEXT
agentos guardrail-check TEXT
```

`run` waits for evidence-backed completion by default. `--detach` uses named detached Ray actors; status is persisted in PostgreSQL. `status PROJECT_ID` presents the active contract, mapped tasks, retry state, latest evaluation/gaps, current evidence revisions, and amendment/waiver decisions. `resume` rechecks dependencies/providers and immediately reconciles the latest durable DoD generation before its periodic recovery loops begin.

## Development and verification

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pip install -e .
ruff format --check agentos
ruff check agentos
mypy agentos
pytest -q -m "not integration"
python -m compileall -q agentos
```

Run the real storage round-trip against Compose:

```bash
RUN_AGENTOS_INTEGRATION=1 pytest -q -m integration
```

The integration suite initializes and round-trips PostgreSQL, DragonflyDB, MongoDB, MinIO, and Milvus through the production clients. It also proves native JSON/outbox behavior, cross-project and evidence-authority rejection, atomic plan rollback, append-only evidence, exact-gap graph-validated/idempotent replanning, durable evaluation recovery, finalization race fencing, terminal write barriers, the lossless PostgreSQL-MinIO-MongoDB memory saga, and restricted Docker sandboxing. Its live delivery test executes the canonical Git/MinIO artifact, command evidence, independent reviewer identity, prospective merge, integrated-HEAD evaluator, and atomic `DOD_SATISFIED` path end to end. The runtime Docker image excludes development tooling; build the test target with `docker build --target test -t agentos-local:test .`.

## Operational cautions

- This is a single-host local runtime, not a multi-tenant hosted control plane.
- Store secrets only in environment/secret injection. The gateway redacts prompt material but cannot make a leaked repository secret safe.
- Back up PostgreSQL, MongoDB, MinIO, and Milvus volumes according to your recovery objectives.
- Changing `AGENTOS_EMBEDDING_DIMENSION` requires a new compatible Milvus collection.
- Legacy pre-polyglot schemas are rejected; AgentOS will not perform a destructive in-place migration.
- No provider credential is bundled. Provider availability and billing remain operator responsibilities.

See [arch_plan.md](arch_plan.md) for the implementation contract, [goal.md](goal.md) for acceptance criteria, and `docs/` for subsystem details.
