# AgentOS Local: Product Goal and Acceptance Contract

## Goal

Given a software or IT delivery request, AgentOS must form a bounded specialist team, execute work through controlled production-grade tooling, collaborate through durable scoped state, and continue until every mandatory Definition of Done criterion has current independently verifiable evidence.

The system is an autonomous software-delivery runtime, not a chatbot and not a collection of agents sharing one implicit conversation.

## Required product behavior

1. A bootstrap PM/Tech Lead converts the request into measurable DoD criteria, a dependency-aware backlog, bounded file ownership, required outputs, and a role plan.
2. The plan always contains PM, infrastructure, QA, code-review, and security-review responsibility. Role caps and total-agent caps are enforced before actor creation.
3. An infrastructure agent detects available capacity, preserves host headroom, and distributes CPU, memory, concurrency, provider, and model allocations among agents.
4. Every working agent is an independent Ray actor with its own actor state, database client, event-bus client, memory cursor, heartbeat, inbox, permissions, and task lease.
5. Agents communicate frequently through typed events, a durable outbox, per-agent consumer groups, and durable idempotent receipt leases. They share long-term and mid-term memory through explicit scopes, never through shared mutable Python objects.
6. Workers can use different providers/models. Selection responds to role, purpose, capability, and `low`, `standard`, `high`, or `critical` task complexity.
7. All provider calls pass through one gateway with a durable pre-egress intent, redaction, egress policy, concurrency, budget, retry/fallback, circuit-breaker, and linked append-only audit enforcement.
8. All writes and commands pass through governance and execution supervisors. An optional real local Git source is cloned into an isolated managed repository, source work occurs in task-specific Git worktrees, commands run in a bounded network-disabled Docker sandbox, and database actions use a physically separate sandbox database.
9. Code cannot merge without required outputs, checksummed artifacts, independent review, passing tests/commands, and recorded DoD evidence.
10. Completion is controlled by the evaluator, not by an agent statement. The evaluator uses the latest evidence for each criterion/evidence type, validates object existence/checksum, and rejects missing or failed evidence.
11. Empty queues with incomplete DoD trigger replanning. Stagnation, deadlocks, expired leases, unhealthy agents, unsafe behavior, and service failures trigger bounded recovery or a visible blocked state.
12. The operator can initialize, plan, run, detach, inspect, pause, resume, approve, reject, and diagnose the system from the CLI.

## Data-system contract

The implementation must use all of these systems through dedicated production clients:

| System | Authority and purpose |
|---|---|
| PostgreSQL | Durable source of truth for projects, plans, agents, tasks, event log/outbox, artifacts, DoD/evidence, approvals, provider calls, audit chain, checkpoints, summaries, resource plans, runtime snapshots, and long-term memory metadata |
| DragonflyDB | Disposable hot coordination: streams, per-agent inboxes, locks, leases, heartbeats, counters, budgets, circuit breakers, and capacity semaphores |
| MongoDB | Expiring mid-term working memory and recoverable agent runtime state, with project/scope/agent access filters and TTL indexes |
| MinIO | Versioned object bodies for artifacts and large memory payloads, including SHA-256 metadata |
| Milvus | Semantic vector lookup over scoped metadata and durable content references; never the source of truth and never sufficient completion evidence |

The runtime must degrade safely: loss of semantic embedding must not erase durable memory; loss of required durable services must block work instead of silently switching to an in-memory substitute.

## Provider contract

Supported provider families:

- OpenAI
- Anthropic / Claude
- Google Gemini
- DeepSeek
- Moonshot AI / Kimi
- Alibaba AI / Qwen through DashScope
- Z.AI / GLM
- MiniMax
- Ollama

Provider support means: a registered profile, credential/base-URL discovery, allowed egress hosts, capability declaration, four complexity-tier model routes, role preferences, and use through the common gateway. It does not mean bundling credentials or claiming a provider call passed without operator credentials.

The provider registry is configuration-driven. Current default IDs are operational defaults, not hard-coded business logic, and may be overridden without source changes.

## Resource and independence contract

Agent independence is structural:

- no worker invokes another worker's methods to perform its task;
- no worker receives another worker's mutable state;
- task ownership is acquired transactionally by role and lease;
- each worker consumes its own inbox and maintains its own cursor/state;
- shared facts are written to PostgreSQL/MongoDB/MinIO/Milvus and announced via events;
- collaboration is periodic and achievement-triggered;
- detached named actors can survive the initiating driver and are explicitly supervised.

Resource safety is layered:

- the envelope leaves at least one detected CPU core unused when more than one exists;
- configured fractions, reserved cores/memory, and absolute maxima are all respected;
- the infrastructure agent allocates per-agent CPU/memory/concurrency within the envelope;
- Ray admission control prevents oversubscription at actor scheduling time;
- thread environment variables prevent numerical libraries from multiplying threads;
- application semaphores limit active agents, code tasks, and provider calls;
- Compose and sandbox containers impose CPU, memory, PID, shared-memory, capability, filesystem, and network boundaries.

If the requested independent team cannot safely fit, planning fails closed instead of silently oversubscribing the host.

## Safety contract

- Production credentials must be non-placeholder and injected externally.
- Requests are sealed with an integrity hash and authenticated against a project-bound `AgentIdentity`.
- Paths are both task-owned and traversal-safe. Git metadata, environment secrets, provider-key areas, audit stores, and Docker sockets are globally protected.
- Shell input is a token array; no shell-string execution is accepted.
- Executables and sandbox images are allowlisted.
- Sandboxes have no network, no Linux capabilities, no privilege escalation, bounded PIDs/CPU/memory, a read-only root filesystem, and a bounded temporary filesystem.
- Destructive action patterns are denied by default. Approval records are project-bound, expiring, integrity-bound, and human-attributed.
- Provider prompts redact recognized credentials and provider responses are schema-validated when JSON is required.
- Audit and provider-call tables are append-only through database triggers and audit rows form a hash chain.
- Storage/client failures and watchdog errors are visible; no silent success path exists.

## Completion contract

A project reaches `DOD_SATISFIED` only when all mandatory criteria pass. For each criterion, the evaluator requires its configured evidence types and artifacts. Evidence may include:

- a MinIO artifact whose object exists and whose SHA-256 matches PostgreSQL metadata;
- a sandbox command with a zero exit code;
- an independent code/security review with a passing decision;
- a required path/output produced by the owning task;
- task completion after successful Git integration.

When retries occur, the latest evidence for a criterion/type/task supersedes earlier attempts. A stale failed test therefore does not permanently poison a repaired task, and a stale passing test cannot override a newer failure.

## Production delivery stance

AgentOS implements one gated delivery path. It does not use canary or staging branches as substitutes for correctness. Task work is isolated until it passes production-quality review/test/evidence gates, then it merges into the project integration branch.

AgentOS does not autonomously deploy to an external live environment. External production promotion requires a separately authorized deployment integration because credentials, traffic, rollback, regulatory, and blast-radius choices are outside the local delivery request.

## Acceptance matrix

| Requirement | Implemented proof surface |
|---|---|
| Independent agents | Named Ray workers, per-agent clients/state/inboxes, role task claims, leases and heartbeats |
| Frequent communication | Collaboration/work timers, typed events, PostgreSQL outbox/receipts, Dragonfly streams/per-agent consumer groups |
| Long/mid-term memories | Lossless PostgreSQL + versioned MinIO + Milvus long-term pipeline; MongoDB TTL working memory |
| Milvus/Postgres/Dragonfly/MinIO/MongoDB clients | `agentos/storage/clients/` and live integration test |
| Do not use all cores | `ResourcePlanner`, generated `ResourceEnvelope`, thread limits, Ray and Compose ceilings |
| Infrastructure agent | `InfrastructureAgentActor` plus persisted resource plan/runtime snapshot |
| All requested AI providers | `providers.yaml`, `ProviderRegistry`, gateway routing and registry tests |
| Complexity/model switching | `TaskComplexity`, purpose map, role preference order, per-agent assignments and fallback |
| Enhanced configuration/models | Typed `Settings`, YAML schema versions, safe expansion, validation, generated runtime config |
| Production-safe direct path | fail-closed secrets/dependencies, sandbox, review/test/DoD gates, no canary/staging completion path |
| Documentation | README, architecture plan, goal contract, and subsystem documents aligned to live code |

## Explicit non-goals

- Hosted SaaS or enterprise multi-tenancy
- A web dashboard or public API
- Fabricated provider credentials or offline provider-success simulation
- Unreviewed live-environment deployment
- General personal-assistant/non-IT workflows
- Treating semantic similarity, checkpoints, or agent confidence as proof of delivery

## Final success condition

The product goal is satisfied when the runtime can safely turn a software-delivery request into a persisted, resource-bounded, provider-routed independent agent team; execute through real storage, messaging, memory, Git, and sandbox clients; and report completion only from independently verified evidence.
