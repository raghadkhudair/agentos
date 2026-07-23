# Storage, Checkpoints, DoD, and Watchdogs

## Client boundaries

Production clients are under `agentos/storage/clients/`:

- `PostgresClient`: bounded asyncpg pool, UTC/statement timeouts, transactions, schema lock/preflight.
- `DragonflyClient`: namespaced Redis protocol, health, JSON values, token-safe compare-and-delete locks with owner-checked automatic lease renewal.
- `MongoDocumentClient`: `AsyncMongoClient`, indexes, TTL memory, agent state.
- `MinioObjectClient`: async facade, bucket versioning, safe names, checksums, put/get/stat.
- `MilvusVectorClient`: typed schema/index, dimension validation, scoped strong-consistency search.

These classes are instantiated by the supervisor, messaging, memory, execution, CLI, and actor runtime. The live integration test exercises each one.

## PostgreSQL schema

`schema.sql` is idempotent and versioned. The schema initialization transaction holds an advisory lock so multiple processes cannot race installation. If an older incompatible `projects` shape is present, initialization raises a migration message instead of dropping or rewriting existing data.

### Concurrency-sensitive tables

- `tasks`: owner role/agent, strict status transitions/lease, contract version, complexity/risk, path/output/criterion/affected-contract arrays.
- `task_dependencies`: directed dependency graph.
- `event_outbox`: delivery reservation, attempts, next-attempt and error.
- `event_receipts`: per-agent processing state, retry count, error, and expiring lease.
- `approval_requests`: gate/hash/expiry/human decision.
- `dod_contract_versions`: append-only normalized contract/hash/source/context/prompt/amendment/approval history.
- `dod_checks`: authoritative active criterion projection with version/hash/provenance/lock/mandatory/severity/evidence scopes/waiver.
- `dod_evidence`: append-only authenticated criterion/task/artifact/Git/command/sandbox/dependency/generation provenance.
- `dod_evaluation_runs` and `dod_evaluation_items`: immutable snapshot header and per-criterion verdict/reasons/evidence IDs.
- `integration_attempts`: replayable `PREPARED`, `COMMITTED`, or `ABORTED` merge state.

### Integrity-sensitive tables

- `audit_events`: append-only chained hashes.
- `provider_call_intents` and `provider_calls`: append-only pre-egress intent plus linked provider accounting.
- `artifacts`: object URI/version/checksum/length/content type/Git metadata.
- `runtime_config_snapshots`: safe environment plus generated resource allocation.

The evidence contract is enforced twice: `DoDRepository.add_evidence()` validates the caller before insertion, and `agentos_validate_dod_evidence` independently rejects SQL inserts with an inactive/stale criterion, unauthorized type, unauthenticated/mismatched role, missing or cross-task artifact, self-review, inconsistent checksum/exit state, or forged integration identity. Contract versions, evidence, and evaluation items reject update/delete.

Table-specific project-isolation triggers reject cross-project task parents/owners, dependencies, artifact/checkpoint references, summary sources, DoD artifacts, outbox events, and memory source references.

## Checkpoints

A checkpoint records a meaningful agent achievement, task, summary, artifact IDs, and state metadata. It is persisted after an action cycle and announced through events/memory. Checkpoints support restart context and stagnation detection.

A checkpoint is not automatically completion evidence. This prevents a worker from satisfying DoD by writing a confident summary.

## Summaries

Summaries are scope/subject/version records derived from durable history. Versioning makes replacement explicit and retains traceability. They reduce catch-up size but do not replace underlying events/evidence.

## DoD checks

Every `DoDCriterion` has:

- stable safe ID, normalized description, version-derived hash, and affected contracts;
- source (`user`, `system`, `inferred`), lock, mandatory flag, and severity;
- exactly one deterministic `test` or `command` type with a required token-array command;
- artifact patterns and explicit required evidence types;
- an evidence scope per type: criterion-global, every mapped task, or every task artifact.

Mandatory delivered criteria require artifact, independent review, and task integration evidence; critical criteria require security review. User/system criteria remain locked; inferred criteria must be unlocked advisory candidates until governed promotion. The cross-validator rejects duplicate semantics, no mandatory gate, uncovered criteria/artifacts/contracts, missing reviewer/security unions, unknown owners/dependencies, unbounded tasks, and output/path mismatch. The validated initial contract/backlog/resource/runtime bundle is one transaction.

Contract amendment is explicit and version-incrementing. An exact hash/reason-bound approved request preserves the old contract row, replaces only the active criterion projection and current work graph, and advances the evidence generation. Ordinary replanning cannot edit criteria. A waiver is an approved criterion-hash/reason-bound state consumed by evaluation and invalidated by amendment.

## Evidence evaluation

`start_evaluation()` locks the project and snapshots contract version/hash, integration HEAD, evidence generation, and timestamp cutoff. A partial unique index permits one `RUNNING` evaluation per project; exact terminal snapshots are reused, concurrent callers coalesce, newer generations mark old runs stale, and abandoned runs recover visibly.

For each active criterion, the evaluator retrieves only evidence within that snapshot and applies the declared cardinality. It verifies:

1. mandatory task coverage and current-version task completion;
2. every criterion-, task-, or artifact-scoped evidence instance exists and passes;
3. required artifact patterns map to durable task artifacts;
4. artifact MinIO object/version, length, and SHA-256 match PostgreSQL;
5. command tokens match the contract and the passing final command ran on the exact integrated HEAD;
6. review/security evidence is artifact-bound and from the authenticated independent role;
7. task integration commits remain ancestors of the current HEAD;
8. artifact/review subject commits remain fresh under conservative path/directory/glob and cross-task affected-contract invalidation;
9. the managed repository HEAD equals the fenced integration HEAD.

Every observed reason is persisted with criterion, evidence type/scope, task/artifact, retryability, and suggested owner. Precedence is `INCONCLUSIVE`, `STALE`, deterministic `FAILED`, `MISSING`, then `SATISFIED`. Object-store/provider/revision uncertainty never passes. `persist_evaluation()` requires exactly one correctly hashed item per active criterion and a summary consistent with mandatory items.

Finalization locks the project and evaluation run, compares contract version/hash, HEAD, and generation, and rechecks active mandatory statuses/current-version tasks in the same short transaction. Any intervening write forces reevaluation. `DOD_SATISFIED` is terminal and later task/artifact/evidence/contract mutation is rejected.

## Watchdogs

### DoD watchdog

The strict evaluator is triggered after integration and polled periodically for recovery. The watchdog separately counts executing, dependency-runnable, and other nonterminal work. Only no executing/runnable work with mandatory gaps emits one evaluation-correlated typed replan request. The repository compares requested criteria with the run's durable unsatisfied items, validates artifact/contract coverage and an acyclic dependency graph, and binds one immutable task batch to the evaluation generation. Exact duplicate runs/tasks/events coalesce; conflicting duplicates fail closed. Attempts use exponential backoff; exhaustion persists a blocker and suspends work.

### Stagnation watchdog

Queries a bounded recent checkpoint history, detects repeated summaries/stale activity, and requests stream freeze or replanning. SQL limits are parameterized/configured and exceptions are surfaced.

### Deadlock watchdog

Loads active dependencies and performs cycle detection. A cycle produces a visible resolution requirement rather than leaving workers idle forever.

### Safety watchdog

Counts denied/quarantine audit decisions, correlates unsafe activity, and recommends/maintains quarantine at the configured threshold.

### Agent health and lease recovery

The supervisor compares persisted heartbeat time with the health interval, marks unhealthy actors, renews healthy task leases, restarts within bounded limits, and returns expired work to `PENDING`.

## Backup and recovery boundaries

For meaningful recovery, back up PostgreSQL, MongoDB, MinIO, and Milvus volumes consistently. Dragonfly persistence improves restart speed but is not the only copy of authoritative state. The sandbox database is disposable by design.

AgentOS does not claim high availability from this single-host Compose topology. Production HA requires managed/clustered services, external secret management, replicated audit export, tested restores, and environment-specific runbooks.

## Validation commands

```bash
ruff format --check agentos
ruff check agentos
mypy agentos
pytest -q -m "not integration"
python -m compileall -q agentos
docker compose --profile test up --build --abort-on-container-exit --exit-code-from integration-test integration-test
docker compose --env-file .env config --quiet
agentos doctor
```

The focused DoD suite includes model/cross-object rejection, fail-closed planning source checks, packaged-prompt uniqueness, golden verdict/freshness data, exact-snapshot review caching, state-machine/watchdog contracts, atomic plan rollback, SQL evidence authority, append-only mutation rejection, evaluation coalescing/staleness, finalization generation races, and terminal write barriers. The full Compose integration additionally exercises every storage client and restricted execution sandbox.
