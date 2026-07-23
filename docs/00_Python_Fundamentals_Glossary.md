# AgentOS Concepts and Python Glossary

This glossary describes terms as they are used by the implemented runtime.

## Async and `await`

Database, provider, actor, and messaging operations are I/O-bound. An `async def` function returns an awaitable; `await` lets the event loop run other work while the operation is waiting. Blocking SDKs such as MinIO and parts of PyMilvus are isolated with `asyncio.to_thread()`.

## Ray actor

A Ray actor is a stateful Python process managed by Ray. AgentOS workers and system services are actors. Named detached actors are discoverable after the initiating CLI driver exits. Actor state is still not durable truth; PostgreSQL/MongoDB/checkpoints are used for recovery.

## Pydantic model

Pydantic validates structured input at boundaries. `Settings`, `TeamPlan`, `Event`, `ActionRequest`, `RuntimeConfig`, provider requests/responses, and DoD models reject malformed or inconsistent data before it reaches side effects.

## Repository

A repository class encapsulates SQL for one domain, such as tasks or evidence. The repositories use parameterized queries (`$1`, `$2`) and UUID conversion, which prevents SQL text injection and centralizes concurrency rules.

## Transaction

A transaction commits several durable changes together or rolls all of them back. AgentOS uses transactions for task claims, event/outbox publication, audit chains, and other consistency-sensitive flows.

## Lease

A lease is temporary ownership with an expiry. Task and processing leases prevent two workers from doing the same work while allowing recovery if an actor dies. Heartbeats renew valid leases; watchdog recovery returns expired work to `PENDING`.

## Outbox pattern

The canonical event and an `event_outbox` record are stored in PostgreSQL. A dispatcher later publishes it to Dragonfly and marks it delivered. This prevents a database commit followed by a process crash from losing the message.

## Consumer group and inbox

Dragonfly Streams consumer groups distribute messages with explicit acknowledgements. The trigger engine routes project events to per-agent inbox streams. Each independent worker consumes only its inbox.

## Definition of Done (DoD)

A DoD criterion is one versioned, hashed, provenance/lock/severity-labeled delivery requirement with exactly one executable test/command contract, required artifacts/evidence, and criterion/task/artifact evidence cardinality. Completion means one fenced evaluation proves all mandatory active criteria at the current integrated HEAD and evidence generation, not merely that tasks say `COMPLETED`.

## Artifact and evidence

An artifact is a produced file/object with a full Git commit, one exact MinIO URI `versionId`, matching version column, size, and SHA-256. Artifact rows are append-only. Append-only evidence links the exact criterion version/hash to an authenticated producer, task, artifact, subject/integration commit, canonical command/sandbox digest, and watched paths/contracts. Review evidence also requires a checksum and length for the exact committed diff. The latest matching attempt at the criterion's declared scope is considered only if it is fresh for the fenced HEAD.

## Memory tiers

- Hot coordination: Dragonfly, disposable and short-lived.
- Mid-term memory: MongoDB documents with TTL and scoped access.
- Long-term memory: PostgreSQL metadata/inline content plus MinIO large bodies.
- Semantic recall: Milvus vectors pointing to durable content references.

Milvus is an index, not truth. Semantic similarity is never DoD proof.

## PostgreSQL

PostgreSQL is the durable control-plane authority. It stores state transitions, task ownership, the event log/outbox, approvals, audit history, provider calls, resource snapshots, and delivery evidence.

## DragonflyDB

DragonflyDB implements the Redis protocol and backs streams, locks, counters, budgets, circuits, heartbeats, and ephemeral capacity. Losing it may interrupt coordination, but must not make completed durable work disappear.

## MongoDB

MongoDB stores flexible working-memory documents and actor state. `AsyncMongoClient` is used directly. TTL indexes expire mid-term memories; project/scope/agent filters enforce access.

## MinIO

MinIO is the S3-compatible object store for versioned artifacts and large memory bodies. AgentOS records version IDs and SHA-256 values so the DoD evaluator can verify object integrity.

## Milvus

Milvus is the vector database. AgentOS creates a typed collection and AUTOINDEX/COSINE index, validates embedding dimensions, and applies project/scope/agent filters with strong-consistency search.

## LiteLLM and provider profile

LiteLLM normalizes provider APIs. A profile defines provider prefix, credential/base environment variables, allowed hosts, capabilities, and models for four complexity tiers. Only the provider gateway invokes LiteLLM.

## Resource envelope

A `ResourceEnvelope` is the safe maximum computed from detected hardware plus fractions, reservations, and absolute limits. `RuntimeConfig` assigns bounded pieces of that envelope to agents. Compose/sandbox limits provide enforcement beyond Ray scheduling quantities.

## Integrity hash

An `ActionRequest` hashes canonical action fields and a nonce. The policy engine recomputes the hash before authorization. An approval is valid only for the exact project and hash it reviewed.

## Hash-chain audit

Each audit event includes the previous hash and its own derived hash. PostgreSQL append-only triggers reject updates/deletes. This provides tamper-evident ordering; it is not a substitute for external log replication.

## Git worktree

A worktree is a separate directory attached to a Git branch. Every AgentOS task receives a distinct worktree and branch, preventing independent writers from modifying the same working directory. Review/test evidence is required before merge.

## Docker sandbox

The sandbox executes an allowlisted token-array command in an allowlisted image with no network, a read-only root, dropped capabilities, no-new-privileges, bounded CPU/memory/PIDs/threads, and only the task worktree mounted.

## Fail closed

Fail closed means missing credentials, unhealthy required stores, invalid plans, failed or uncertain reviews/tests, unsafe paths/globs/symlinks, unverifiable revisions, or unknown actions block progress. AgentOS does not replace these failures with an in-memory mock or a success claim.
