# Actors and Team Independence

## Actor classes

### Runtime supervisor

`RuntimeSupervisorActor` owns lifecycle and policy: dependency checks, project states, plan validation, service/worker creation, health loops, watchdogs, pause/resume, recovery, and final completion. It does not perform worker tasks.

### Bootstrap PM

`BootstrapAgentActor` requests a structured production plan through the provider gateway. It hydrates permissions/scopes from the role catalog and validates the result through `TeamPlan`. Provider/JSON failure activates a deterministic mandatory safety roster, never a fabricated delivery plan.

### Infrastructure agent

`InfrastructureAgentActor` is mandatory. It invokes `InfrastructurePlanner` to detect resources, pick eligible provider/model assignments, calculate per-agent CPU/memory/concurrency, persist `resource_plans`, and announce the update. It operates alongside—not inside—the supervisor.

### Worker

`AgentWorkerActor` is generic, but its identity makes it role-specific. It owns its own clients, inbox, memory cursor, task claim, capacity slot, provider preference, heartbeat, and collaboration state. Workers are named, detached, restartable actors.

### Independent reviewers

`ReviewerAgentActor` and `SafetyReviewerAgentActor` use separate prompts/provider calls and write review evidence. The author cannot self-mark review evidence.

### Governed system actors

Provider, memory, execution, checkpoint, summary, trigger, outbox, and DoD actors expose narrow governed APIs. A worker can request an action but cannot bypass their enforcement.

## Mandatory roles

The team plan must include exactly/at least the configured PM, infrastructure, QA, code-review, and security-review roles. Optional solution architecture, backend, frontend, and platform roles are created based on the request. Role caps and maximum total count are enforced before Ray actor creation.

## Identity

Every worker receives an `AgentIdentity` containing project, agent, role, allowed actions, and allowed paths. Execution registers that identity and authenticates each action against it. A valid action from one project cannot be replayed in another.

## Task ownership

Initial/replanned tasks name `owner_role`. Claims use one PostgreSQL transaction with dependency checks, role filtering, priority ordering, `FOR UPDATE SKIP LOCKED`, owner assignment, and a lease. This provides independence without duplicate execution.

Each task also defines:

- `allowed_paths` and `blocked_paths`;
- expected-output globs;
- acceptance criteria and mapped DoD IDs;
- required reviewers;
- risk and complexity;
- explicit dependencies.

## Worker lifecycle

1. Initialize independent clients and restore MongoDB state.
2. Start inbox and heartbeat/collaboration loops.
3. Claim one per-agent event receipt, handle it with scoped catch-up context, persist the processed state, and only then acknowledge Dragonfly.
4. Claim a role-compatible runnable task.
5. Ask the provider gateway for exactly one allowed action.
6. Submit a sealed action to execution.
7. For code, perform artifact, expected-output, review, command/test, evidence, and merge gates.
8. Checkpoint, update memory, emit progress, release capacity, and return to idle.

Errors return retryable work to `PENDING` or a visible blocked/verification state. Exceptions are logged by type; the worker does not report completion from its own exception path.

## Frequent communication

Workers communicate on every material event and at `AGENTOS_COLLABORATION_INTERVAL_SECONDS` (default 30). A separate periodic work poll lets runnable tasks progress even when no new event arrives. Both timers are independent for every actor. Collaboration summaries contain status/task information, not raw secrets or shared Python objects.

## State recovery

Each worker stores its own actor working state through its own MongoDB client, while PostgreSQL retains task/event/checkpoint truth. On restart, a worker restores state and catches up from durable events/memory. Expired task and event-receipt claims are reclaimed. Named detached actors are rediscovered with `get_if_exists`; duplicate creation is avoided.

## Resource assignment

Each worker actor is created with its persisted Ray CPU quantity. Memory and max concurrency are included in its allocation and enforced through active-agent/code-task/thread/container limits. If the validated team cannot safely fit the host envelope, actor creation does not proceed.
