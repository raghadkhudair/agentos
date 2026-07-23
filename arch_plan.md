# AgentOS Local Architecture and Implementation Plan

Status: implemented architecture, reviewed against the live repository and fresh Compose storage stack on 2026-07-23.

This document is the technical contract for the current code. The original scaffold assumptions (notably pgvector and partially wired actors) have been superseded by the polyglot storage, independent-agent, provider-routing, and resource-governance implementation below.

## 1. Architectural objective

AgentOS accepts a software-delivery request and operates a bounded engineering organization until a strict, evidence-backed DoD is satisfied. Autonomy belongs to workers' reasoning and task choice; authority remains in deterministic supervisors, storage constraints, identities, policy, reviews, and evidence gates.

Core invariants:

1. PostgreSQL is the durable authority.
2. Workers are independent actors, not threads sharing an orchestration object.
3. All cross-agent facts use durable memory/events; all hot coordination is recoverable.
4. Workers never call providers or host execution directly.
5. Resource allocations cannot exceed a generated host envelope.
6. Agent claims cannot complete a project; only current evidence can.
7. Production configuration and required dependencies fail closed.

## 2. System topology

```text
Operator / CLI
    |
    v
RuntimeSupervisorActor (detached, named)
    |-- BootstrapAgentActor
    |-- InfrastructureAgentActor
    |-- ProviderGatewayActor
    |-- MemoryBrokerActor
    |-- ExecutionSupervisorActor
    |-- ReviewerAgentActor
    |-- SafetyReviewerAgentActor
    |-- CheckpointManagerActor / SummaryManagerActor
    |-- TriggerEngineActor / OutboxDispatcherActor
    |-- DoDEvaluatorActor
    `-- independent AgentWorkerActor instances

Durable plane: PostgreSQL ---- MinIO ---- Milvus ---- MongoDB
Hot plane: DragonflyDB
Execution plane: Git worktrees + Docker socket proxy + sandbox PostgreSQL
```

System-service actors are per project, named with the project suffix, and detached so CLI driver exit does not define their lifecycle. Worker names are deterministic and persisted. The supervisor owns handles and lifecycle policy, while the actor's durable state is recoverable from storage.

## 3. Project lifecycle

### 3.1 Bootstrap

1. Validate production secrets when `AGENTOS_ENV=production`.
2. Connect PostgreSQL and apply the idempotent v4 schema under an advisory lock.
3. Health-check PostgreSQL, Dragonfly, MongoDB, MinIO, and Milvus.
4. Create a project in `PLANNING` state.
5. Capture a clean, bounded, Git-tracked planning context with source revision, relevant docs/tree, and canonical hash; block if it cannot be trusted.
6. Start per-project service actors and reliable outbox routing, then ask the bootstrap PM for a structured versioned contract through the provider gateway.
7. Retry invalid provider JSON/contracts only within the configured bound; never activate a generic fallback plan.
8. Validate criterion IDs/hashes/provenance/locks/evidence scopes, mandatory coverage, artifact/output and affected-contract mapping, reviewer/security unions, role caps, task ownership/dependencies, and bounded paths. Re-run the full cross-object validator after roster reduction.
9. Ask the infrastructure agent for the exact resource/model allocation.
10. In one PostgreSQL transaction persist the contract version, active criteria, backlog/dependencies, resource plan, safe runtime/planning snapshot, and planned agents. Any late failure rolls back the whole bundle.
11. If no provider is configured, leave the plan durable and set `BLOCKED_REQUIRES_INPUT`; never synthesize credentials or fabricated work.
12. Otherwise create independent workers, set `RUNNING`, and start health/watchdog loops.

### 3.2 Work cycle

Each worker:

1. Loads its own MongoDB state and memory/event cursors.
2. Emits heartbeats and collaboration summaries at the configured interval.
3. Consumes its own Dragonfly consumer-group inbox, claims a durable receipt lease, and acknowledges only after durable processing state is recorded.
4. Builds a catch-up packet through the memory broker.
5. Claims one dependency-ready `PENDING` task matching its role under a PostgreSQL row lock and expiring lease.
6. Selects task complexity and asks the provider gateway for one structured next action.
7. Seals the `ActionRequest` and submits it to execution.
8. For writes, records versioned artifact evidence, obtains fresh isolated review per criterion (plus security review where required), checks aggregate expected outputs, runs each canonical criterion command in the task sandbox, records authenticated revision-bound evidence, and requests merge.
9. Creates a checkpoint, updates mid-term/long-term memory, and broadcasts durable progress.
10. Returns to idle; no worker is a perpetual busy loop when no work is available.

### 3.3 Completion and replanning

Each evaluation run snapshots the active contract version/hash, integrated HEAD, evidence cutoff, and evidence generation. The evaluator applies each declared evidence scope at criterion, mapped-task, or artifact cardinality; checks authenticated producer/task/artifact/commit provenance, exact commands, independent reviews, MinIO version/length/checksum, integration ancestry, repository HEAD, and conservative watched-path/affected-contract freshness; and persists every typed reason as `MISSING`, `FAILED`, `INCONCLUSIVE`, `STALE`, or `SATISFIED`.

Task/evidence/integration changes advance one durable generation. Evaluation requests are serialized/coalesced per project, code-triggered after integration, and periodically reconciled for recovery. `DOD_SATISFIED` is one atomic compare-and-set against the exact satisfied contract/HEAD/generation; terminal writers then fail closed.

If there is no executing or dependency-runnable work but typed gaps remain, the DoD watchdog emits one evaluation-correlated `REPLANNING_TRIGGERED` event. The PM submits a typed `InitialTask` graph covering exactly the durable run's unsatisfied criteria; current roles, reviewers, paths/outputs, required artifacts/contracts, dependency existence/cycles, and security requirements are validated in one transaction. One deterministic immutable task batch is bound to that evaluation generation: exact redelivery is a no-op and a changed duplicate is rejected. Attempts use exponential backoff and end in a visible blocker. Ordinary replanning cannot mutate the DoD; amendments and waivers require exact hash/reason-bound human approval.

## 4. Agent independence and collaboration

### 4.1 Independence boundaries

Every `AgentWorkerActor` receives immutable identity/configuration plus names of service actors. It constructs its own:

- `PostgresClient` and repositories;
- `DragonflyEventBus` and inbox consumer;
- `MongoDocumentClient` runtime state access;
- task capacity lease and heartbeat loop;
- local cursor, status, current task, and collaboration timer.

Workers do not share Python repositories, database pools, message consumers, task state, or provider clients. They cannot mutate another worker's state. The only intentional shared services are governed actors whose APIs enforce memory/provider/execution policy.

### 4.2 Communication model

Producers insert the canonical event and outbox row in PostgreSQL. The dispatcher reserves only the project's due rows, publishes to Dragonfly Streams, and marks delivery. The trigger engine reads the project stream, applies subscriptions, and writes to a bounded per-agent inbox. Consumer groups, acknowledgement, leases, and retry delays prevent one worker from consuming another worker's message.

Frequent communication is driven by:

- event-triggered task/review/test/blocker messages;
- periodic heartbeat and agent-health events;
- periodic collaboration summaries (default 30 seconds);
- checkpoint and memory-promotion events after meaningful actions;
- infrastructure/resource-pressure messages;
- PM replanning messages.

The event log remains queryable after Dragonfly loss because the durable event and outbox records are authoritative.

### 4.3 Memory scopes

Supported scopes include shared project, squad, contract, decision, execution, infrastructure, security, and private-agent memory. Reads are filtered by project and requested allowed scopes. Private memory additionally requires matching `agent_id`.

## 5. Resource architecture

### 5.1 Resource envelope

`ResourcePlanner.build_envelope()` detects logical CPUs and physical memory and intersects:

- configured usage fraction;
- explicitly reserved CPU cores and bytes;
- configured maximum CPU/memory;
- at least one unallocated CPU on multi-core hosts;
- Ray object-store memory;
- per-worker CPU/memory demand;
- maximum active-agent count.

The Pydantic model rejects an envelope that consumes every multi-core CPU or whose allocated plus reserved memory exceeds detected memory.

### 5.2 Infrastructure agent

The infrastructure actor combines the envelope with the validated agent roster and provider registry. It emits a `RuntimeConfig` containing:

- generation time/environment;
- detected, allocated, and reserved CPU/memory;
- object-store memory and active-agent limit;
- per-agent role, CPU, memory, concurrency, provider, model, and complexity;
- thread environment variables.

The supervisor persists the resource plan and safe runtime snapshot before worker launch. The infrastructure agent is registered in the agent inventory as a first-class system role alongside the supervisor.

### 5.3 Enforcement layers

- Ray `num_cpus` limits actor admission.
- Active-agent and parallel-code semaphores use Dragonfly counters/leases.
- Provider calls use a process semaphore.
- OpenMP, OpenBLAS, MKL, NumExpr, and VecLib thread variables are bounded.
- Compose sets application CPU, memory, and shared-memory ceilings.
- Sandbox containers set nano-CPU, memory, PID, tmpfs, capability, network, and filesystem limits.

Ray CPU resources are scheduling quantities, so the container/thread layers are required to provide real host protection.

## 6. Storage architecture

### 6.1 PostgreSQL: control-plane truth

Schema version 4 contains:

- `schema_migrations`
- `projects`, `agents`, `tasks`, `task_dependencies`
- `events`, `event_outbox`, `event_receipts`
- `artifacts`
- `checkpoints`, `summaries`
- `memory_items`
- `provider_call_intents`, `provider_calls`
- `audit_events`, `approval_requests`
- `dod_contract_versions`, active `dod_checks`, append-only `dod_evidence`
- `dod_evaluation_runs`, append-only `dod_evaluation_items`, `integration_attempts`
- `resource_plans`, `runtime_config_snapshots`

Important mechanics:

- foreign keys plus table-specific project-isolation triggers prevent orphan/cross-project references;
- row locks and `SKIP LOCKED` make task/outbox claims concurrent-safe;
- task leases are renewable and reclaimable;
- append-only triggers protect provider/audit/contract/evidence/evaluation-item rows;
- SQL evidence guards authenticate producer roles and enforce criterion version, evidence type, task/artifact ownership, self-review, result, checksum, and integration requirements;
- one running evaluation per project plus generation/HEAD fences coalesce work and prevent stale completion;
- audit events carry previous/current hashes;
- updated-at triggers are database-side;
- indexes follow project/status/time/task access paths;
- schema initialization holds an advisory lock;
- legacy schemas are detected and rejected rather than destructively altered.

`PostgresClient` owns pool bounds, command/statement timeouts, UTC sessions, transactions, and schema installation.

### 6.2 DragonflyDB: hot coordination

Dragonfly stores streams, inboxes, locks, budget counters, capacity counters, circuit state, quarantine sets, and ephemeral agent coordination. Keys are namespaced. Lock release uses compare-and-delete Lua. Budget reservation is atomic Lua across daily/monthly counters.

No project completion fact exists only in Dragonfly.

### 6.3 MongoDB: mid-term memory

`MongoDocumentClient` uses PyMongo's `AsyncMongoClient`. It creates:

- TTL index on `expires_at`;
- project/scope/time and project/agent/time indexes;
- unique project/agent runtime-state index.

Mid-term memories expire by policy (default seven days). Recent retrieval filters project, allowed scopes, and private ownership.

### 6.4 MinIO: object bodies

Buckets for artifacts and memory are created idempotently and versioning is enabled. Object names are normalized and traversal-safe. Writes return bucket/name, ETag, version ID, length, and SHA-256. Large memory bodies are moved to MinIO while PostgreSQL stores the durable reference and checksum.

### 6.5 Milvus: semantic references

Milvus collection fields are `id`, vector, project, agent, scope, kind, content reference, importance, and timestamp. The collection uses an AUTOINDEX/COSINE vector index and strong consistency for read-after-write recall. Every search expression includes project and allowed scopes, with an additional agent predicate for private memory.

Milvus stores no completion authority. If embedding creation/search fails, lexical/relational memory still works and the failure is logged. Embedding dimension is validated on every upsert/search.

## 7. Provider architecture

### 7.1 Registry

`providers.yaml` defines nine profiles, credential/base URL environment variables, allowed hosts, local/remote behavior, capabilities, and four complexity models. Routing defines default order, role preferences, purpose complexity, and circuit-breaker policy.

Current profiles: OpenAI, Anthropic, Gemini, DeepSeek, Moonshot, Alibaba/DashScope, Z.AI, MiniMax, and Ollama. Model IDs are overridable through environment variables so model lifecycle changes do not require code changes.

### 7.2 Selection

`ProviderRegistry.candidates()` computes:

1. explicit request preference;
2. role preference;
3. global fallback order;
4. configured credential/local base availability;
5. required capability subset;
6. complexity-tier model.

Duplicates are removed while preserving order. A model prefix mismatch is a configuration error.

### 7.3 Gateway controls

The gateway:

- accepts typed chat requests only;
- redacts common API keys, bearer tokens, passwords, and private keys;
- validates optional custom base URLs against the profile allowlist and requires HTTPS remotely in production;
- reserves daily/monthly budget before the call and settles actual cost;
- limits global concurrency and per-request attempts;
- skips open circuits and applies bounded exponential jitter;
- validates nonempty responses and JSON-object output when requested;
- logs provider/model, prompt/response hashes, redaction count, usage, latency, status, and error type;
- returns content only after the audit write succeeds.

Embedding calls use the same redaction, budget, timeout, dimension validation, and audit boundary.

## 8. Governance and execution

### 8.1 Identity and integrity

`ActionRequest` includes project, agent, task, action type, description, paths, command/database operation, payload, timestamp, nonce, and an integrity hash computed from canonical content. `PolicyEngine` rejects hash tampering, unknown identities, identity/project mismatch, disallowed role action, path traversal, and paths outside task/identity boundaries.

### 8.2 Decisions

Policy decisions are:

- allow;
- allow with constraints;
- sandbox only;
- require review/security review/backup/human approval;
- deny;
- quarantine.

Destructive patterns are denied by default. Repeated violations increase a TTL-bound counter and quarantine the agent after the configured threshold. Approval records are project-, hash-, gate-, expiry-, and human-identity-bound.

### 8.3 Git and object flow

When `AGENTOS_SOURCE_REPOSITORY` is set, the execution service first validates and locally clones that Git worktree into the isolated managed repository; otherwise it initializes an empty managed repository. Each task uses branch `agentos/task-{task_id}` in a dedicated worktree. Writes are atomic (`fsync` plus replace), committed with a controlled Git identity, uploaded to versioned MinIO, and recorded as artifacts with checksums and commit metadata.

Task ownership has allowed and blocked path lists. Every produced artifact is reviewed before a partial-output decision, and expected-output globs must all match recorded artifacts before verification/merge. A task without mapped DoD criteria or a verification command cannot merge.

### 8.4 Sandbox

Commands must be token arrays and start with an allowlisted executable. They run through a restricted Docker socket proxy in an allowlisted image with:

- `network_disabled=true`;
- read-only root filesystem;
- `/tmp` as size-limited `noexec,nosuid` tmpfs;
- all capabilities dropped and no-new-privileges;
- bounded CPU, memory, PIDs, threads, duration, and captured output;
- only the assigned worktree volume mounted.

Database operations use `SANDBOX_DATABASE_URL`, which production validation requires to differ from the control database. Only one allowlisted parameterized statement class is accepted per action.

### 8.5 Review and merge

The code and safety reviewers load the current criterion text/hash, task acceptance criteria, exact artifact/checksum, diff, and affected contracts. Each criterion gets an isolated structured provider decision. Calls are concurrency-bounded and successful results are coalesced only for the exact criterion hash, subject commit, artifact checksum, content hash, and review prompt kind; inconclusive results are never cached. Deterministic findings remain fail-closed, and every appended evidence row retains the exact cache/provenance metadata.

Merge evaluates the active criterion evidence policy rather than a hard-coded evidence trio. It acquires a renewable Dragonfly lock, persists a `PREPARED` integration attempt, creates a no-commit prospective merge, and runs every newly eligible unique criterion command in the read-only/network-disabled sandbox against that tree. Failure aborts without a commit. Success creates the non-fast-forward integration commit, atomically fences PostgreSQL to that HEAD, records integrated-HEAD command and per-task integration evidence, and completes the task. `PREPARED` merge, post-commit/pre-database, and `COMMITTED` pre-evidence crash states are replayable without an ungoverned merge path.

Review/test failure returns the task to `PENDING` for repair. Merge conflict moves it to visible `BLOCKED`; it is never silently resolved.

## 9. Messaging reliability

Events are Pydantic-validated envelopes with UUID project ID, producer, optional target, version, UTC time, payload, correlation/causation, priority, and replay flag. Event-type-specific payload validators reject malformed critical events.

PostgreSQL insertion and outbox creation form the durable publication boundary. Dispatcher rows have attempt count, next attempt, reservation time, last error, and delivered time. Reservations expire, so a crashed dispatcher does not permanently own work. Dragonfly stream trimming is bounded by configuration.

Each worker has its own consumer group. PostgreSQL `event_receipts` atomically claim an event/agent pair with an expiring lease; already processed events are idempotent no-ops, and failed handlers remain reclaimable through `XAUTOCLAIM`. Dragonfly acknowledgement happens only after the receipt is durably `PROCESSED`. Error paths record retry state, log type, and apply bounded backoff rather than swallowing failures.

## 10. Checkpoints, summaries, and memory promotion

A checkpoint records project, agent, task, achievement, summary, artifacts, and state metadata. Summaries are versioned by scope and subject. Checkpoints demonstrate progress but are not DoD evidence by themselves.

Long-term memory writes first persist the complete scrubbed body and hash in PostgreSQL. Large bodies begin as `PENDING_OBJECT`, upload to a versioned MinIO object, verify size/SHA-256, then atomically retain a preview plus URI/version and become `READY`; failure is visibly `OBJECT_FAILED`. MongoDB receives the full TTL-bound working copy only after durable object completion. Important memories request an embedding and Milvus upsert, whose failure cannot erase durable memory. Reads hydrate MinIO by exact version and revalidate length/hash before use. Catch-up packets combine recent durable events, MongoDB scope-filtered memory, PostgreSQL long-term records, and semantic references within a configured prompt-character budget.

## 11. Health, watchdogs, and recovery

- Dependency health verifies all five required stores and reports configured provider availability.
- Heartbeat loop renews agent/task leases and records health.
- Expired task leases return to `PENDING`.
- DoD watchdog distinguishes executing, runnable, and blocked work; it triggers typed bounded replanning or final evaluation.
- Stagnation watchdog detects repeated checkpoints and stale work.
- Deadlock watchdog detects cycles in task dependencies.
- Safety watchdog correlates denied/quarantine audit outcomes.
- Actor health loop identifies missed heartbeats and restarts/reassigns within configured limits.
- Evaluator failures are counted and bounded; exhaustion persists a blocker and suspends workers rather than logging forever.
- DoD evaluation is event-triggered after successful integration, generation-coalesced in PostgreSQL, and periodically polled only for recovery.

Pause stops claims while retaining durable state. Resume health-checks stores/providers, reclaims expired leases, restarts missing services/workers, immediately evaluates the latest durable contract/HEAD/generation, and only then resumes periodic health/recovery loops. It does not depend on a prior process's in-memory completion event.

## 12. Configuration model

Configuration has three layers:

1. typed environment settings in `settings.py` for deployments/secrets/ceilings;
2. versioned YAML for providers, roles, governance, execution, memory, and loop tuning;
3. generated, persisted runtime configuration for actual detected capacity and per-agent allocation.

YAML expansion supports `${NAME}` and `${NAME:-default}` without evaluating arbitrary shell syntax. The safe loader accepts only bounded `.yml`/`.yaml` files, contains normal reads to the package config directory unless an explicit provider-registry path is configured, and caches parsed results. `safe_snapshot()` redacts secret fields before persistence.

Production validation covers placeholder credentials, authentication, TLS for external stores, separate sandbox database, resource ranges, pool bounds, budget/concurrency bounds, and path/config consistency.

## 13. Deployment architecture

Compose services:

- PostgreSQL control database;
- physically separate tmpfs sandbox PostgreSQL;
- DragonflyDB with explicit two-thread limit;
- authenticated MongoDB;
- versioned MinIO;
- etcd and standalone Milvus;
- restricted Docker socket proxy;
- AgentOS runtime connected to internal and egress networks.

Persistent services have named volumes, health checks, no-new-privileges, CPU/memory ceilings, and non-default localhost bindings where exposed. The internal network is isolated. The AgentOS image runs as UID/GID 10001, has a minimal production target without development tools, and receives secrets through environment injection. `.dockerignore` excludes local secrets and build/runtime artifacts.

This is a single-host production-oriented deployment. High availability, multi-node Milvus/MinIO/PostgreSQL, secret-manager integration, backups, and disaster-recovery automation are deployment responsibilities, not silently simulated by this repository.

## 14. Direct delivery semantics

There is no canary/staging completion mode. Worktree isolation is an engineering safety boundary, not a lower-quality deployment tier. The one integration path requires the same artifact/review/test/evidence gates for every task.

External live deployment is not an execution driver. Adding one requires a new explicit action type, credential/target policy, independent approval, backup/rollback evidence, and provider-neutral audit behavior.

## 15. Verification strategy

Static gates:

```text
ruff format --check agentos
ruff check agentos
mypy agentos
python -m compileall -q agentos
```

Unit/contract gates cover settings, resource envelopes, all provider profiles, plan role/criterion/output/reviewer contracts, planning-context immutability, packaged prompt uniqueness/version, golden verdict/freshness cases, exact-snapshot review coalescing, task transitions, watchdog bounds, event validation, policy tamper/cross-project/path checks, Git worktrees, schema shape, Compose service/resource contracts, and packaged config/schema data.

Live integration on a fresh Compose project initializes and round-trips:

- PostgreSQL schema/health/query;
- Dragonfly health/set/get;
- MongoDB indexes/write/scoped read;
- MinIO bucket/versioned put/get;
- Milvus collection/upsert/strong-consistency filtered search.
- native PostgreSQL JSON codecs plus transactional event/outbox insertion;
- project-isolation trigger rejection for a cross-project artifact;
- atomic initial plan rollback after a late persistence failure;
- authenticated evidence authority, stale-hash/self-review/forged-integration rejection, and append-only enforcement;
- one-running evaluation coalescing, generation/HEAD finalization races, and terminal task/evidence write barriers;
- the lossless PostgreSQL-MinIO-MongoDB memory write/hydration path;
- a network-disabled, read-only-root, non-root Docker sandbox command.

`agentos init` proves packaged schema/config plus client initialization. `agentos doctor` proves the runtime actor and all required storage clients from the built image.

## 16. Traceability to requested outcomes

| Requested outcome | Architecture owner |
|---|---|
| Totally independent agents | Ray workers + individual client/state/inbox/lease boundaries |
| Frequent communication | outbox, streams, inboxes, collaboration/heartbeat/checkpoint events |
| Shared long/mid-term memories | PostgreSQL/MinIO/Milvus and MongoDB through Memory Broker |
| Five requested databases/clients | `storage/clients` plus live integration |
| Do not consume all cores | ResourcePlanner + infrastructure actor + Ray/thread/container limits |
| Infrastructure agent alongside supervisor | mandatory role and service actor with persisted allocation |
| Nine AI providers | ProviderRegistry/YAML/LiteLLM gateway |
| Different workers/models by complexity | role/purpose/complexity routing and per-agent allocation |
| Enhanced config/runtime/models | typed settings, versioned YAML, generated RuntimeConfig |
| Versioned source-grounded DoD | planning context, `TeamPlan`, contract versions/hashes, atomic bundle |
| Current inspectable evidence | authenticated append-only provenance, per-scope evaluator, path/contract invalidation |
| Race-safe completion and repair | integrated-tree commands, evaluation snapshot fence, typed idempotent replanning |
| Production safe, no canary/staging | one fail-closed review/test/evidence integration path |
| Updated documentation | README, goal, this plan, and subsystem documents |

## 17. Remaining operator responsibilities

Implementation completeness does not eliminate operational inputs. Before real use, the operator must:

- supply real non-placeholder credentials and at least one provider/Ollama endpoint;
- select provider models available to that account/region and override YAML defaults if needed;
- size host memory and Compose ceilings for the planned team;
- configure backup/restore and retention policies;
- monitor provider cost/rate limits and storage capacity;
- define any authorized external deployment integration separately;
- perform organization-specific threat modeling and compliance review.

The runtime reports these missing inputs as blocked configuration, not as implementation success.
