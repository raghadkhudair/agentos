# Configuration and CLI

## Configuration sources

AgentOS uses three layers:

1. Environment-backed typed `Settings` in `agentos/config/settings.py`.
2. Versioned YAML policies in `agentos/config/`.
3. A generated `RuntimeConfig`, persisted per project after host detection and infrastructure planning.

`loader.py` accepts `${NAME}` and `${NAME:-default}` substitutions only, contains config reads to the package directory, and caches parsed results. YAML, `schema.sql`, and the single authoritative versioned bootstrap/worker Markdown prompts under `agentos/config/prompts/` are declared as wheel package data. Root-level shadow prompts are forbidden by contract tests.

## Environment groups

### Core and resources

`AGENTOS_ENV`, project/workspace/log level, optional `AGENTOS_SOURCE_REPOSITORY` Git path visible to the runtime process, CPU/memory fractions, reserved cores/bytes, absolute limits, Ray address/object-store memory, per-worker CPU/memory, active/total-agent limits, parallel-code limit, per-agent threads, and collaboration interval.

The generated envelope always leaves at least one detected CPU unallocated on multi-core machines.

### Data services

- `DATABASE_URL` plus PostgreSQL pool/timeouts and `POSTGRES_CONNECTION_BUDGET`
- `DRAGONFLY_URL`
- `MONGODB_URL`, database, and mid-term TTL
- `MINIO_ENDPOINT`, access/secret, TLS, region, buckets
- `MILVUS_URI`, database, token, prefix
- embedding model and dimension
- physically separate `SANDBOX_DATABASE_URL`

### Providers

Credentials: `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `DEEPSEEK_API_KEY`, `MOONSHOT_API_KEY`, `DASHSCOPE_API_KEY`, `ZAI_API_KEY`, `MINIMAX_API_KEY`; Ollama uses `OLLAMA_API_BASE`.

Each provider has an optional `*_API_BASE` and `*_MODEL_LOW|STANDARD|HIGH|CRITICAL` override. `AGENTOS_PROVIDER_REGISTRY_PATH` may point to a bounded validated YAML registry. Custom remote base URLs are allowlisted and HTTPS-only in production.

### Governance and execution

Review/test requirements, destructive-action policy, provider budgets/concurrency/timeouts, Docker host, sandbox image/volume/CPU/memory/PID limit, and dependency-health fail-closed behavior.

## Production validation

`validate_production_secrets()` rejects:

- `CHANGE_ME`/example/default database or object-store credentials;
- unauthenticated PostgreSQL, Dragonfly, or MongoDB URLs;
- missing sandbox database or reuse of the control database;
- external non-TLS MinIO;
- external Milvus without TLS and token;
- invalid pool/resource/budget/concurrency relationships;
- a PostgreSQL connection envelope that exceeds `POSTGRES_CONNECTION_BUDGET`;
- a missing/non-Git `AGENTOS_SOURCE_REPOSITORY` when configured.

`safe_snapshot()` redacts Pydantic secret fields before runtime configuration is stored. The planning boundary separately rejects an empty request, requests above 100,000 UTF-8 bytes, unsafe tracked paths, and source snapshots that cannot be cleanly revision-bound. Tracked symlinks are listed but never followed into provider context.

## YAML responsibilities

- `providers.yaml`: nine providers, models, capabilities, egress, role/purpose routing, circuits.
- `actor_team.yml`: role catalog, permissions, explicit PM DoD/replanning subscriptions, caps, and mandatory roles. It contains no substitute delivery plan.
- `runtime_tuning.yaml`: bounded planning repair, DoD replan/backoff/recovery, per-criterion review concurrency/cache, inbox, collaboration, memory, provider, Ray, watchdog, and execution allowlists.
- `guardrail_policies.yaml`: destructive patterns, gates, action risk groups, protected paths, sanitization.

## CLI commands

### Initialize

```bash
agentos init PROJECT_NAME
```

Creates the local workspace, initializes PostgreSQL schema, MongoDB indexes, MinIO versioned buckets, Milvus collection/index, and verifies all five storage clients. It writes a project manifest only after initialization succeeds.

### Plan

```bash
agentos plan "REQUEST"
```

Captures the clean bounded source context without following tracked symlinks, performs bounded fail-closed plan validation, and persists the versioned DoD, backlog/dependencies, resource plan, planning/runtime snapshot, and planned agents in one transaction. It does not launch delivery workers. An invalid provider response or unsafe/oversized source context leaves a visible planning blocker and no partial contract.

### Run

```bash
agentos run "REQUEST"
agentos run "REQUEST" --detach
```

Without `--detach`, wait for terminal completion/blockage. With it, named detached Ray actors continue and state is inspected separately. If no provider is configured, the request is persisted and visibly blocked.

### Generate resource configuration

```bash
agentos runtime-config --agent backend_developer --agent qa_engineer --output runtime.json
```

Prints detected/allocated/reserved resources, thread environment, and allocations for each repeated `--agent ROLE` without starting a project. With no `--agent`, it generates PM and infrastructure allocations.

### Diagnose

```bash
agentos doctor
```

Starts or attaches to a bounded Ray runtime and performs live client health checks for PostgreSQL, Dragonfly, MongoDB, MinIO, and Milvus. Provider entries report configuration availability separately.

### Inspect and operate

```bash
agentos status
agentos status UUID
agentos logs UUID --limit 100
agentos inspect UUID
agentos re-evaluate UUID
agentos pause UUID
agentos resume UUID
```

`status UUID` includes active criteria/provenance/locks/scopes, mapped tasks, current contract/HEAD/retry generation, latest evaluation and typed gaps, latest evidence revisions plus command/sandbox/checksum provenance, and amendment/waiver decisions. `inspect` retains the complete raw history. `re-evaluate` runs only the canonical snapshot-fenced evaluator; an inconclusive snapshot is rechecked rather than treated as reusable terminal truth. Resume rechecks dependencies/providers, reclaims expired leases, and immediately reconciles the latest durable evaluation generation before periodic recovery.

### Human gates

```bash
agentos approve APPROVAL_UUID --approver OPERATOR
agentos reject APPROVAL_UUID --approver OPERATOR --reason "reason"
agentos amend-dod UUID --contract NEXT_TEAM_PLAN.json --reason "reason" --requested-by OPERATOR
agentos amend-dod UUID --contract NEXT_TEAM_PLAN.json --reason "reason" --requested-by OPERATOR --approval-id APPROVAL_UUID
agentos waive-dod UUID CRITERION_ID --reason "reason" --requested-by OPERATOR
agentos waive-dod UUID CRITERION_ID --reason "reason" --requested-by OPERATOR --approval-id APPROVAL_UUID
```

The first amendment/waiver call creates an exact contract-hash or criterion-hash-and-reason-bound approval; the second applies only that approved decision. Amendments increment the contract version, retain append-only contract history, invalidate old evidence by version/hash, and atomically replace the active work graph. Waivers change only one active criterion and remain visible in evaluation. Approval/rejection updates only a still-pending, unexpired record and preserves approver identity. Approved execution must match the stored action integrity hash and project.

### Policy inspection

```bash
agentos guardrail-check "description"
```

Classifies a sealed example action through the real policy engine; it does not execute it.

## Compose usage

```bash
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d postgres sandbox-postgres dragonfly mongodb minio etcd milvus docker-proxy
docker compose --env-file .env run --rm agentos doctor
```

Compose uses internal DNS URLs. Host ports in `.env.example` exist for operator tools and use nonstandard localhost defaults to avoid local Postgres/Redis/Mongo/MinIO conflicts.
