# Actors and Team Independence

## Actor classes

### Runtime supervisor

`RuntimeSupervisorActor` owns lifecycle and policy: dependency checks, project states, plan validation, service/worker creation, health loops, watchdogs, pause/resume, recovery, and final completion. It does not perform worker tasks.

### Bootstrap PM

`BootstrapAgentActor` receives the immutable source revision/tree/docs/context hash, requests the one structured versioned production contract through the provider gateway, hydrates permissions/scopes from the role catalog, and validates the result through `TeamPlan`. Before egress, planning rejects empty or greater-than-100,000-byte requests, unsafe tracked paths, dirty/unbound revisions, and document symlink traversal. Invalid JSON or contract output receives only a bounded full-object repair attempt. Exhaustion raises, persists a planning blocker, stops the project runtime, and launches no workers; there is no fallback roster or generic DoD.

### Infrastructure agent

`InfrastructureAgentActor` is mandatory. It invokes `InfrastructurePlanner` to detect resources, pick eligible provider/model assignments, calculate per-agent CPU/memory/concurrency, persist `resource_plans`, and announce the update. It operates alongside—not inside—the supervisor.

### Worker

`AgentWorkerActor` is generic, but its identity makes it role-specific. It owns its own clients, inbox, memory cursor, task claim, capacity slot, provider preference, heartbeat, and collaboration state. Workers are named, detached, restartable actors.

### Independent reviewers

`ReviewerAgentActor` and `SafetyReviewerAgentActor` load each current criterion description/hash, the task acceptance contract, exact task-bound artifact/checksum, committed diff, and affected contracts. Artifact metadata binds review to that diff's SHA-256 and character length; mismatched supplied content is rejected, while a diff above 100,000 characters is `INCONCLUSIVE` and cannot call or pass review. Strict structured verdicts reject coercions such as string booleans. Calls are concurrency-bounded; only an exact successful criterion-hash/commit/artifact/content snapshot can be reused, inconclusive results are retried, and cancellation of one coalesced waiter cannot cancel shared provider work. The author cannot self-review, and PostgreSQL authenticates the declared reviewer role.

### Governed system actors

Provider, memory, execution, checkpoint, summary, trigger, outbox, and DoD actors expose narrow governed APIs. The DoD evaluator serializes one run per project and emits a durable `DOD_EVALUATED` snapshot event; the runtime supervisor applies that event immediately while its periodic loop remains recovery. A worker can request an action but cannot bypass enforcement.

## Mandatory roles

The team plan must include exactly/at least the configured PM, infrastructure, QA, code-review, and security-review roles. Optional solution architecture, backend, frontend, and platform roles are created based on the request. Role caps and maximum total count are enforced before Ray actor creation.

## Identity

Every worker receives an `AgentIdentity` containing project, agent, role, allowed actions, and allowed paths. Execution registers that identity and authenticates each action against it. A valid action from one project cannot be replayed in another.

## Task ownership

Initial/replanned tasks name `owner_role`. Creation reconstructs the complete `InitialTask` contract, validates its role/identity, paths/globs, outputs, criteria/contracts, reviewers/security requirements, and exact dependency-title set, and rejects a new replan batch after its evaluation generation becomes stale. Claims use one PostgreSQL transaction with dependency checks, role filtering, priority ordering, `FOR UPDATE SKIP LOCKED`, owner assignment, and a lease. This provides independence without duplicate execution.

Each task also defines:

- `allowed_paths` and `blocked_paths`;
- expected-output globs;
- acceptance criteria and mapped DoD IDs;
- required reviewers;
- risk and complexity;
- explicit dependencies.
- the active DoD contract version and affected path/contract boundaries.

## Worker lifecycle

1. Initialize independent clients and restore MongoDB state.
2. Start inbox and heartbeat/collaboration loops.
3. Claim one per-agent event receipt, handle it with scoped catch-up context, persist the processed state, and only then acknowledge Dragonfly.
4. Claim a role-compatible runnable task.
5. Ask the provider gateway for exactly one allowed action.
6. Submit a sealed action to execution.
7. For code, produce authenticated artifact evidence, perform per-criterion review/security review, expected-output and canonical command gates, validate the declared evidence policy, and request the durable prospective-merge path.
8. After successful integration, trigger the canonical evaluator and supervisor handler. If that handoff fails, retain task success and let the persisted generation plus periodic reconciliation recover it.
9. Checkpoint, update memory, emit progress, release capacity, and return to idle.

Errors return retryable work to `PENDING` or a visible blocked/verification state. Exceptions are logged by type; the worker does not report completion from its own exception path.

## Frequent communication

Workers communicate on every material event and at `AGENTOS_COLLABORATION_INTERVAL_SECONDS` (default 30). A separate periodic work poll lets runnable tasks progress even when no new event arrives. Both timers are independent for every actor. Collaboration summaries contain status/task information, not raw secrets or shared Python objects.

## State recovery

Each worker stores its own actor working state through its own MongoDB client, while PostgreSQL retains task/event/checkpoint/contract/evidence truth. On restart, a worker restores state and catches up from durable events/memory. Expired task and event-receipt claims are reclaimed. Resume immediately evaluates the stored contract/HEAD/generation before recovery polling. Named detached actors are rediscovered with `get_if_exists`; duplicate creation is avoided.

## Resource assignment

Each worker actor is created with its persisted Ray CPU quantity. Memory and max concurrency are included in its allocation and enforced through active-agent/code-task/thread/container limits. If the validated team cannot safely fit the host envelope, actor creation does not proceed.
