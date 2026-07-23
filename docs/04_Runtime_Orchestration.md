# Runtime Orchestration and Resource Distribution

## Supervisor responsibilities

`RuntimeSupervisorActor` owns project lifecycle, not task implementation. Its registries track per-project service and worker handles; PostgreSQL tracks authoritative state.

Named services per project: provider, memory, execution, checkpoint, summary, trigger, reviewer, safety reviewer, DoD evaluator, infrastructure, and outbox dispatcher.

## Ray initialization

The CLI attaches to `RAY_ADDRESS` when configured or starts a local instance. Local startup uses the generated resource envelope's allocated CPU and object-store memory, disables the dashboard by default, and applies thread ceilings before Ray workers spawn.

The runtime supervisor and project actors use deterministic names, namespace `agentos`, `get_if_exists`, and detached lifetime. Resume therefore finds existing actors instead of duplicating them.

## Planning flow

```text
health -> project row -> clean bounded source snapshot -> service actors
       -> bounded bootstrap contract repair -> deterministic cross-validation
       -> infrastructure resource plan -> one atomic contract/backlog/resource/
          runtime-context/planned-agent transaction -> provider readiness
       -> workers or BLOCKED_REQUIRES_INPUT
```

Planning bounds the request to 100,000 UTF-8 bytes, permits only safe relative Git-tracked paths, and never follows tracked documentation symlinks. Plan validation then enforces stable unique DoD IDs and normalized semantics, criterion hashes/provenance/locks/severity, at least one mandatory criterion, exactly one executable test/command type, artifact/review/integration baselines for delivered work, explicit evidence scopes, conservative glob/path/contract coverage, reviewer/security unions, nonempty unique acceptance/criteria/dependencies, role caps, maximum total agents, unique task titles, valid dependencies, and bounded ownership paths. The complete `TeamPlan` is revalidated after roster hardening/reduction and before persistence.

## Resource planner algorithm

CPU allocation is the minimum of:

- floor(detected CPUs times configured fraction);
- detected CPUs minus reserved cores;
- configured maximum;
- detected CPUs minus one on a multi-core host.

Memory allocation is the minimum of:

- detected memory times configured fraction;
- detected memory minus reserved bytes;
- configured maximum.

Object-store memory is reserved before agent memory. CPU and memory slot counts constrain `max_active_agents`. The planner rejects per-agent memory below the safe minimum.

## Infrastructure plan

For each validated agent, the infrastructure agent chooses:

- CPU quantity;
- memory bytes;
- maximum concurrency;
- role-derived complexity;
- eligible provider and model from preferences/availability.

The sum of allocations must fit the envelope. The plan is persisted in `resource_plans`; a safe settings snapshot plus generated runtime config is stored in `runtime_config_snapshots`. This is the exact plan used for actor creation and later diagnosis.

## Scheduling

Workers claim tasks by role, dependency readiness, priority, and creation order. Row locking plus `SKIP LOCKED` provides parallelism without duplicate ownership. Active-agent and code-task capacity slots prevent a large roster from all entering expensive work simultaneously.

System actor CPU shares are small and bounded as a fraction of the envelope. Worker CPU comes from its allocation rather than a hard-coded default.

## Project states

Important states:

- `PLANNING`
- `TEAM_FORMING`
- `RUNNING`
- `PAUSED`
- `BLOCKED_REQUIRES_INPUT`
- `DOD_SATISFIED`
- `FAILED_BY_POLICY`
- `STOPPED_BY_USER`

Transitions are persisted. A CLI process exit does not imply completion.

## Watchdog loop

Task/artifact/evidence/integration writes advance a durable evaluation generation. A successful integration code-triggers the canonical evaluator and supervisor handler. The supervisor also periodically reconciles:

- evidence-backed DoD and empty-queue gaps;
- stagnation/repeated checkpoint behavior;
- task dependency cycles;
- unsafe audit volume/quarantine signals.

Only one evaluation run per project may be `RUNNING`; exact satisfied/unsatisfied snapshots are reused, inconclusive snapshots are re-evaluated, duplicates coalesce, changed snapshots supersede older runs, and abandoned same-evaluator runs recover. Retryable operational gaps take the bounded `VERIFYING`/evaluation-retry path instead of creating code tasks. PM replanning is reserved for repairable delivery gaps, must match that durable run's exact gaps and current generation, and produces one immutable, graph-validated task batch; exact delivery coalesces while changed contracts or dependency graphs fail closed. Other actions include stream freeze/quarantine, deadlock visibility, lease recovery, and snapshot-fenced completion. Replan/evaluator failures have bounded attempts/backoff and end in a durable visible blocker.

## Health loop

The health loop checks agent heartbeats, renews/reclaims leases, restarts allowed actors, and records agent/project health. It also verifies that outbox and trigger actor background loops are actually running and restarts those loops after actor recovery. Restart counts are bounded. Missing storage or provider capacity leads to blocked state instead of degraded fake execution.

## Pause and resume

Pause stops new task progress and persists state. Resume:

1. health-checks all required stores;
2. verifies at least one eligible provider;
3. reclaims expired task leases;
4. rediscovers/recreates services and missing workers from the stored plan/config;
5. immediately evaluates the current contract version/hash, integrated HEAD, and evidence generation and applies completion/repair/blocking;
6. restarts watchdog/health loops only if the project remains active;
7. returns the durable resulting state.

## Terminal completion

The waiting CLI observes the durable project state plus an in-process convenience event. The supervisor sets `DOD_SATISFIED` only by `DoDRepository.finalize_project`, which locks and compares the exact satisfied evaluation's contract version/hash, integrated HEAD, and evidence generation and checks mandatory active criteria/current-version tasks. Concurrent change returns `false` and schedules a newer generation. On success workers are suspended and task/artifact/evidence/contract writers reject terminal mutation. Provider assertions, task count alone, an idle actor, or a stale evaluation cannot terminate the project successfully.
