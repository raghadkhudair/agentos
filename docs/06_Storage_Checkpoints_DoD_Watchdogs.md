# Storage, Checkpoints, DoD, and Watchdogs

## Client boundaries

Production clients are under `agentos/storage/clients/`:

- `PostgresClient`: bounded asyncpg pool, UTC/statement timeouts, transactions, schema lock/preflight.
- `DragonflyClient`: namespaced Redis protocol, health, JSON values, compare-and-delete locks.
- `MongoDocumentClient`: `AsyncMongoClient`, indexes, TTL memory, agent state.
- `MinioObjectClient`: async facade, bucket versioning, safe names, checksums, put/get/stat.
- `MilvusVectorClient`: typed schema/index, dimension validation, scoped strong-consistency search.

These classes are instantiated by the supervisor, messaging, memory, execution, CLI, and actor runtime. The live integration test exercises each one.

## PostgreSQL schema

`schema.sql` is idempotent and versioned. The schema initialization transaction holds an advisory lock so multiple processes cannot race installation. If an older incompatible `projects` shape is present, initialization raises a migration message instead of dropping or rewriting existing data.

### Concurrency-sensitive tables

- `tasks`: owner role/agent, status, lease, complexity/risk, path/output/DoD arrays.
- `task_dependencies`: directed dependency graph.
- `event_outbox`: delivery reservation, attempts, next-attempt and error.
- `event_receipts`: per-agent processing state, retry count, error, and expiring lease.
- `approval_requests`: gate/hash/expiry/human decision.
- `dod_evidence`: criterion/type/producer/task/artifact/command/pass metadata.

### Integrity-sensitive tables

- `audit_events`: append-only chained hashes.
- `provider_call_intents` and `provider_calls`: append-only pre-egress intent plus linked provider accounting.
- `artifacts`: object URI/version/checksum/length/content type/Git metadata.
- `runtime_config_snapshots`: safe environment plus generated resource allocation.

Table-specific project-isolation triggers reject cross-project task parents/owners, dependencies, artifact/checkpoint references, summary sources, DoD artifacts, outbox events, and memory source references.

## Checkpoints

A checkpoint records a meaningful agent achievement, task, summary, artifact IDs, and state metadata. It is persisted after an action cycle and announced through events/memory. Checkpoints support restart context and stagnation detection.

A checkpoint is not automatically completion evidence. This prevents a worker from satisfying DoD by writing a confident summary.

## Summaries

Summaries are scope/subject/version records derived from durable history. Versioning makes replacement explicit and retains traceability. They reduce catch-up size but do not replace underlying events/evidence.

## DoD checks

Every `DoDCriterion` has:

- stable criterion ID and description;
- mandatory flag;
- verification type;
- optional token-array verification command;
- required artifact names;
- required evidence types.

The bootstrap validator rejects duplicate IDs, criteria with no required evidence, invalid commands, and tasks that reference nonexistent criteria.

## Evidence evaluation

The evaluator retrieves latest evidence per criterion, type, and task. For each mandatory criterion it verifies:

1. every required evidence type exists and passes;
2. required artifact records exist;
3. artifact MinIO objects/versions exist;
4. object length and SHA-256 match recorded metadata;
5. command evidence has zero exit code;
6. review evidence comes from independent reviewer flows;
7. no newer failed attempt supersedes a prior pass.

Only then is the criterion satisfied. All mandatory criteria must be satisfied before project completion.

## Watchdogs

### DoD watchdog

Runs the strict evaluator. An empty runnable queue with gaps emits PM replanning. A satisfied evaluation permits completion.

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
