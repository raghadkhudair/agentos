# Supervisor Agent and Definition of Done Enhancement Review

## Executive conclusion

RAAD already has the stronger delivery-safety foundation. Its current path combines a typed team plan, PostgreSQL-backed DoD criteria and evidence, sandboxed verification commands, checksummed/versioned artifacts, independent reviewer identities, per-task merge gates, and a deterministic evaluator. QuantumByte's current requirements harness is not a replacement for that path: it is primarily a read-only, model-based source audit, and its own roadmap says world-grounded evidence and a real readiness gate are still future work.

The valuable QuantumByte contribution is its **requirements lifecycle logic**:

- reconcile a stable product-truth artifact through a code-triggered stage;
- preserve stable requirement IDs and user-locked requirements;
- invalidate verdicts when a requirement or relevant source revision changes;
- audit one requirement at a time in a fresh, read-only context;
- distinguish `INCONCLUSIVE` from a genuine failure;
- record the Git SHA and files read by each audit;
- re-run only the requirements affected by a later diff;
- coalesce repeated audit requests without allowing verifier and builder access to overlap.

Those patterns should be integrated into RAAD's existing
`BootstrapAgentActor -> TeamPlan -> DoDRepository -> AgentWorkerActor/ExecutionSupervisorActor -> DoDEvaluatorActor -> RuntimeSupervisorActor`
flow. They should not become another harness, API, evidence store, or completion pipeline.

The highest-priority correction is to make every DoD criterion and every piece of evidence **versioned, task-bound, and Git-revision-bound**, then make the final `DOD_SATISFIED` transition consume one atomically fenced evaluation snapshot. Without that, RAAD can validate strong evidence that was true before later integrated changes and can race new evidence or tasks during finalization.

## Review scope and evidence

### Revisions reviewed

- RAAD: local clean checkout at `37fcf2273a6e958a34f381ab507e7e5cd87be8ef` on 2026-07-23.
- QuantumByte: `main` at [`6eaa965a018332dd4407a13466d2a9973a448fc5`](https://github.com/QuantumByteOSS/quantumbyte/tree/6eaa965a018332dd4407a13466d2a9973a448fc5), committed 2026-07-15.

The comparison used executable source, schema, prompts, tests, and architecture documentation. It did not infer implemented behavior from roadmap text when no corresponding code was present.

### Validation limits during the original comparison

These limits describe the investigation that produced this report, before the implementation work recorded in the completion appendix below.

- `python -m compileall -q agentos` passed for RAAD.
- The checkout does not contain a virtual environment and the active Python installation lacks `pydantic`, `pytest`, `ruff`, and `mypy`. Therefore the documented unit/static suites could not be executed in this investigation.
- No external runtime was started. QuantumByte requires PostgreSQL, Redis, GitHub credentials, and an Anthropic/Claude SDK environment; RAAD's live path requires its storage and sandbox services. Findings below are source-backed, not claims of live end-to-end execution.

## Existing architecture mapping

### RAAD's current DoD path

```text
user request
  -> BootstrapAgentActor.create_team_plan()       # proposes DoD + backlog + roles
  -> TeamPlan / DoDCriterion Pydantic validation  # shape and graph checks
  -> RuntimeSupervisorActor.validate_team_plan()  # roster/resource hardening
  -> projects.dod + dod_checks + tasks            # persisted contract and work
  -> AgentWorkerActor                             # writes artifact, requests reviews/tests
  -> ExecutionSupervisorActor                     # validates evidence and merges task
  -> DoDEvaluatorActor                            # evaluates latest evidence
  -> DoDWatchdog / PM replanning                  # closes gaps
  -> RuntimeSupervisorActor                       # sets DOD_SATISFIED
```

The responsibility called “Supervisor Agent” is therefore distributed rather than owned by one class:

- **DoD authoring:** `agentos/actors/bootstrap.py:73-156`.
- **contract models:** `agentos/runtime/team_plan.py:24-219`.
- **plan orchestration and persistence:** `agentos/runtime/supervisor.py:287-400` and `402-468`.
- **team-plan hardening:** `agentos/runtime/supervisor.py:564-652`.
- **evidence production:** `agentos/actors/base.py:592-740`, `agentos/actors/reviewer.py:40-115`, and `agentos/actors/safety_reviewer.py:38-115`.
- **pre-merge enforcement:** `agentos/execution/supervisor.py:609-825`.
- **criterion evaluation:** `agentos/storage/repositories.py:925-1073` and `agentos/dod/evaluator.py:37-274`.
- **replanning/finalization:** `agentos/watchdogs/runtime_watchdogs.py:44-69` and `agentos/runtime/supervisor.py:718-786`.

This separation is sound and should remain. The enhancement should clarify and strengthen the contracts between these existing stages, not collapse them into another agent or service.

### QuantumByte's comparable path

```text
chat turn
  -> builder edits project
  -> code-triggered product-overview reconcile
  -> commit/push
  -> background requirements harness
       -> derive/reconcile requirements when product truth changed
       -> preserve user-locked requirements and stable IDs
       -> select stale/affected requirements by SHA + watched paths
       -> fresh read-only structured audit per requirement
       -> persist SUCCESS / FAIL / INCONCLUSIVE + evidence
  -> client computes a severity-based ship label
```

The relevant implementation is concentrated in:

- [`apps/worker/main.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/main.py#L164-L230), which code-triggers reconciliation of `_doc/product-overview.md` after each substantive turn;
- [`apps/worker/harness.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/harness.py#L1-L23), which documents the derive, diff-scope, audit, and persistence stages;
- [`apps/worker/harness.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/harness.py#L98-L161), which separates business-language derivation from one-requirement read-only verification;
- [`apps/worker/harness.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/harness.py#L226-L288), which protects locked requirements, preserves IDs, and invalidates a verdict when requirement content changes;
- [`apps/worker/harness.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/harness.py#L301-L321), which decides whether a verdict is stale from its SHA and watched paths;
- [`apps/worker/harness.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/harness.py#L327-L500), which performs bounded per-requirement audits and records `INCONCLUSIVE` separately;
- [`apps/worker/harness_scheduler.py`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/worker/harness_scheduler.py#L1-L87), which preempts stale audits and coalesces follow-up runs;
- [`apps/web/prisma/schema.prisma`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/web/prisma/schema.prisma#L148-L202), which stores requirement ownership, severity, result state, source SHA, watched paths, and staleness;
- [`apps/web/src/components/blueprint/harness-slide.tsx`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/apps/web/src/components/blueprint/harness-slide.tsx#L69-L113), which computes the current UI-only ship label.

## RAAD strengths that must be preserved

QuantumByte should influence the DoD contract lifecycle, not weaken RAAD's existing control plane.

1. **Completion is evidence-controlled, not model-controlled.** `DoDEvaluatorActor` does not accept a worker's prose as completion (`agentos/dod/evaluator.py:37-45`). Keep this invariant.
2. **Artifacts are durable and integrity-checked.** MinIO URI, exact version, length, and SHA-256 are revalidated (`agentos/dod/evaluator.py:47-92`; `agentos/execution/supervisor.py:677-693`). QuantumByte's source-only audit does not offer this.
3. **Commands execute in a governed sandbox.** The execution supervisor applies allowlists and resource/network/filesystem constraints before evidence exists. Do not replace command evidence with a model verdict.
4. **Review identity is checked.** RAAD checks author/reviewer separation and reviewer role (`agentos/dod/evaluator.py:161-199`; `agentos/execution/supervisor.py:706-742`). Retain and strengthen it.
5. **Task integration is explicit evidence.** A task is completed only after a guarded non-fast-forward merge, after which integration evidence is recorded (`agentos/execution/supervisor.py:773-825`).
6. **PostgreSQL is authoritative.** DoD facts do not depend on model memory, checkpoints, or the event bus.
7. **There is one delivery path.** Review, test, evidence, and merge occur in the existing worker/execution/evaluator path. Every recommendation below uses that same path.

## QuantumByte patterns worth adopting

### 1. A maintained source-of-truth contract before backlog derivation

QuantumByte does not let every builder turn independently reinterpret the product. A code-owned trigger reconciles the product overview, and requirement derivation consumes that maintained truth. RAAD currently sends only the raw user request and role catalog to a single planning call (`agentos/actors/bootstrap.py:73-150`). It does not inspect the configured brownfield source before defining completion, even though `AGENTOS_SOURCE_REPOSITORY` may point to an existing repository.

**Adopt:** inside the existing bootstrap stage, assemble a deterministic, read-only planning context from the user request, configured source revision, repository manifest/tree, relevant project docs, and declared constraints. Persist its hash with the DoD. Then derive the DoD before deriving tasks. `CodebaseMapRepository` already exists (`agentos/storage/repositories.py:1120-1167`) but is unused; it can support this without adding another service or API.

**Do not copy:** a mutable prose file as a second source of truth. PostgreSQL should remain authoritative; a generated contract artifact may be stored for audit/diff, but it must have the same version/hash as `dod_checks`.

### 2. Stable criterion identity, revisioning, and human locks

QuantumByte retains stable requirement IDs, protects `locked` rows from regeneration, and deletes an old verdict when requirement text or metadata changes. RAAD criterion IDs are stable only within the first plan. `dod_checks` has no contract version/hash, source provenance, lock, or amendment history, while `projects.dod` duplicates the same contract as JSON.

**Adopt:** version each DoD contract and hash each normalized criterion. Add explicit ownership/provenance (`user`, `inferred`, `system`), `locked`, `mandatory`, and `severity` fields. Replanning may create tasks for a gap but must not silently rewrite a locked criterion. A criterion amendment should use the existing governance/approval boundary and invalidate evidence from older criterion hashes.

**Important adaptation:** QuantumByte's `hidden` requirements are useful as labeled candidates, but inferred requirements must not silently become mandatory. Promote them through the existing approval mechanism or keep them advisory until explicitly accepted.

### 3. One isolated audit per criterion

QuantumByte gives each requirement a fresh read-only SDK call without builder conversation history. RAAD's `ReviewerAgentActor.review_code_patch()` receives a list of criterion IDs but not their descriptions or acceptance criteria, performs one generic patch review, then writes the same approval against every criterion (`agentos/actors/reviewer.py:40-115`). That review proves general patch quality; it does not prove each criterion.

**Adopt:** extend the existing reviewer stage so it loads each `DoDCriterion` description, task acceptance criteria, exact artifact/diff, and affected contracts, then produces a structured per-criterion result in an isolated provider call. Keep the reviewer read-only, use the existing `ProviderGatewayActor`, and record the result in `dod_evidence`. A semantic review should augment artifact/test/integration evidence and must never be sufficient by itself.

### 4. `INCONCLUSIVE` and `STALE` as first-class evaluation states

QuantumByte correctly separates verifier/tool failure from a real negative verdict. RAAD currently reduces all gaps to a criterion ID and generally maps operational uncertainty to `FAILED_VERIFICATION`.

**Adopt:** represent at least `MISSING`, `FAILED`, `INCONCLUSIVE`, `STALE`, and `SATISFIED` in evaluation output. The persisted criterion status can remain a controlled enum, but the evaluation result needs a reason code, retryability, and all observed reasons. Only `SATISFIED` may complete; `INCONCLUSIVE` and `STALE` should cause bounded retry or visible input/operations blocking, not automatic failure and not success.

### 5. Git-SHA and dependency-scoped freshness

QuantumByte persists `lastVerifiedSha` and the paths a verifier read, then rechecks only affected requirements. RAAD records an integration commit only in integration-evidence metadata. Tests, reviews, and criteria are not uniformly bound to a criterion revision, artifact version, worktree commit, or final integration HEAD.

**Adopt:** every current evidence row should identify:

- DoD contract version and normalized criterion hash;
- task ID as a real foreign key, not JSON-only metadata;
- subject artifact ID/version when applicable;
- subject worktree commit;
- integration/base commit against which the result was produced;
- verification command digest and sandbox configuration digest;
- affected/watched paths or contracts;
- evaluator run ID and created-at cutoff.

Use RAAD's existing task `affected_contracts`, artifact Git metadata, and integration commit rather than introducing QuantumByte's exact watched-path representation. Invalidation should support glob/path overlap and contract dependency propagation; exact set intersection is insufficient.

### 6. Deterministic trigger and bounded coalescing

QuantumByte runs reconciliation and verification because code schedules them, not because the builder remembers to do so. Its scheduler cancels an audit that is about to become stale and runs the final audit after the write settles.

**Adopt:** keep RAAD's existing watchdog/evaluator, but add a durable evaluation generation or requested-at marker. Coalesce duplicate evaluation requests, serialize them per project with the existing Dragonfly/PostgreSQL coordination, and guarantee that a request for the newest integrated HEAD remains pending if an older run is cancelled. This is an enhancement to the evaluator schedule, not a new background harness.

**Do not copy:** a fixed-TTL, non-renewed lock or degraded-open duplicate execution. Completion evaluation must fail closed.

### 7. Deterministic preconditions before model evaluation

QuantumByte deterministically fails all requirements when no app code exists, instead of asking a model to hallucinate from planning docs.

**Adopt:** add cheap deterministic preflight checks to the existing evaluator: criterion coverage, mapped runnable tasks, required artifact mapping, valid command/evidence compatibility, repository/head existence, integration ancestry, and evidence revision freshness. Run semantic review only after those preconditions pass.

### 8. Operator-visible contract curation and rerun

QuantumByte exposes requirement edit/lock state, stale/inconclusive verdicts, and a manual recheck. RAAD's CLI status returns raw task/DoD/evidence rows (`agentos/cli/main.py:243-262`) but does not show a coherent evaluation snapshot or a governed criterion-amendment flow.

**Adopt:** enhance the existing CLI/status surface to show contract version, provenance, lock state, criterion coverage, latest evaluation run, evidence revision, complete gap reasons, and replan attempt. Reuse the current approval and CLI architecture; do not create a second requirements API.

## Current weaknesses and exact enhancement targets

| ID | Priority | Current weakness and evidence | Required enhancement target |
|---|---|---|---|
| G1 | P0 | Planning is one provider call with an all-exception generic fallback (`agentos/actors/bootstrap.py:73-156`; `agentos/config/actor_team.yml:76-105`). The fallback has generic DoD and an empty backlog, so it is safe structurally but not a faithful completion contract for the request. | `BootstrapAgentActor.create_team_plan`; use bounded structured repair attempts, expose planning failure as blocked input/provider state, and never start delivery with a generic substitute DoD. |
| G2 | P0 | DoD validation is incomplete. `required_evidence_types=[]` is accepted; only `COMMAND` requires a command; task acceptance criteria, allowed paths, and expected outputs may be empty (`agentos/runtime/team_plan.py:32-123`). Every task must map to a criterion, but every criterion need not be covered by a task (`agentos/runtime/team_plan.py:170-219`). | `DoDCriterion`, `InitialTask`, and `TeamPlan` validators; enforce nonempty/compatible evidence, executable command contracts, criterion coverage, artifact/output mapping, reviewer/security requirements, and explicit external/human-only criteria. |
| G3 | P0 | The supervisor's plan validation hardens roles/resources but does not validate DoD semantics or revalidate task/evidence compatibility (`agentos/runtime/supervisor.py:564-652`). | `RuntimeSupervisorActor.validate_team_plan`; add a deterministic cross-object contract validation pass after role reduction and before any persistence/worker launch. |
| G4 | P0 | `projects.dod`, `dod_checks`, tasks, dependencies, and agent/resource rows are written in separate calls, leaving duplicate truth and partial-bootstrap risk (`agentos/runtime/supervisor.py:327-358`, `402-464`). | `RuntimeSupervisorActor.bootstrap_project/plan_project`, `ProjectRepository`, `DoDRepository`, and `TaskRepository`; persist one versioned plan/DoD/backlog transaction and designate `dod_checks` plus contract version as authoritative. |
| G5 | P0 | Criterion execution semantics diverge. The model allows `command`, `security_review`, and `integration`, but workers record configured verification commands as `test`; merge validation hard-codes `artifact + test + review` instead of using each criterion's `required_evidence_types` (`agentos/runtime/team_plan.py:56-70`; `agentos/actors/base.py:666-733`; `agentos/execution/supervisor.py:718-760`). | `AgentWorkerActor._execute_decision`, `ExecutionService._validate_merge_evidence`, and `DoDEvaluatorActor`; define one canonical evidence policy used by plan validation, production, merge, and final evaluation. |
| G6 | P0 | Security evidence can be required by a criterion but never scheduled if its mapped task is low risk and omits `security_reviewer`; conversely merge may require security evidence even when the criterion does not (`agentos/actors/base.py:633-654`; `agentos/execution/supervisor.py:632-721`). | Cross-contract validator plus worker/execution stages; derive task reviewer gates from the union of task risk and mapped criterion contracts. Reject an impossible plan before execution. |
| G7 | P0 | A generic patch review is copied to every mapped criterion without giving the reviewer the criterion text (`agentos/actors/reviewer.py:40-115`). | `ReviewerAgentActor.review_code_patch`; evaluate each criterion separately against description, acceptance criteria, diff/artifact, and affected contracts. Preserve separate reviewer identity and fail closed on provider errors. |
| G8 | P0 | Evidence has no first-class task FK, criterion version/hash, subject commit, evaluation run, or staleness state. `task_id` is optional JSON metadata and `source_agent_id` is not a relational identity (`agentos/storage/schema.sql:338-352`). | `agentos/storage/schema.sql`, `DoDRepository.add_evidence`, artifact/reviewer/test/integration writers; add relational provenance and revision fields, producer constraints, indexes, and immutable evidence protections. |
| G9 | P0 | Verification runs on the task worktree before merge, while no final command is proven against the fully integrated HEAD. A later task can invalidate an earlier test/review without making its evidence stale (`agentos/actors/base.py:697-737`; `agentos/execution/supervisor.py:796-820`). | Existing execution/evaluator path; run required integration/final commands on the current integration commit, or conservatively invalidate and rerun affected criteria after every merge. Store the exact HEAD on every result. |
| G10 | P0 | Finalization is a time-of-check/time-of-use race. Evaluation and status persistence occur before a separate supervisor transaction; that transaction locks only the project row and counts current criterion/task statuses. New evidence or work can arrive between evaluation and `DOD_SATISFIED` (`agentos/dod/evaluator.py:228-274`; `agentos/runtime/supervisor.py:734-760`). | `DoDEvaluatorActor.evaluate`, `RuntimeSupervisorActor._watchdog_loop`, and schema/repositories; create an evaluation-run snapshot with evidence cutoff + integration HEAD and atomically compare/fence those values when transitioning the project. Task/evidence writers must respect the terminal/finalizing fence. |
| G11 | P1 | `evaluate_and_persist` aggregates evidence types at criterion level. It does not require every mapped task to carry every configured evidence type; `_validate_mapped_tasks` checks only completed + integration (`agentos/storage/repositories.py:992-1039`; `agentos/dod/evaluator.py:201-226`). | `DoDRepository.evaluate_and_persist` and evaluator queries; evaluate required evidence at the correct cardinality: criterion-global, per-task, and per-artifact as declared by the contract. |
| G12 | P1 | Only one gap reason survives per criterion because `failure_map` is a simple dictionary; the PM receives criterion IDs rather than actionable missing evidence/tasks/commands (`agentos/dod/evaluator.py:231-259`; `agentos/actors/base.py:458-520`). | `DoDEvaluation` models and replanning payload; return all typed reasons with responsible task/artifact/evidence type, retryability, and suggested owner role. |
| G13 | P1 | Replanned and dynamically created tasks bypass `InitialTask`/`TeamPlan` cross-validation. Replanning validates only that criterion IDs are a subset of current gaps, omits required reviewers/dependencies, and has no idempotency key (`agentos/actors/base.py:458-555`). | `AgentWorkerActor._replan_gaps/_execute_decision` and `TaskRepository.create_task`; construct and validate typed task proposals, verify owner roles and path/output/reviewer contracts, support dependencies, and use deterministic external keys per gap generation. |
| G14 | P1 | The DoD watchdog counts all nonterminal tasks as active rather than runnable. A permanently `BLOCKED` or `FAILED_VERIFICATION` task can suppress empty-queue replanning (`agentos/watchdogs/runtime_watchdogs.py:51-69` versus claimability in `agentos/storage/repositories.py:448-484`). | `DoDWatchdog.inspect_and_act` and `TaskRepository`; query runnable/leased work separately from blocked/unclaimable work, then emit a typed recovery action. Add bounded attempts/backoff and a terminal visible blocker. |
| G15 | P1 | DoD state semantics drift across schema and docs. Schema permits `WAIVED_BY_HUMAN`, but the evaluator accepts only `SATISFIED`; docs describe a `mandatory` flag that does not exist (`agentos/storage/schema.sql:318-335`; `docs/06_Storage_Checkpoints_DoD_Watchdogs.md:47-72`). | Contract model, schema, evaluator, and docs; either implement governed waiver/mandatory semantics end to end or remove the unsupported state and claims. A waiver must never be an unscoped status edit. |
| G16 | P1 | `DoDRepository.add_dod_check()` updates only `description` on conflict, leaving old verification commands, artifacts, and evidence types attached to a changed criterion (`agentos/storage/repositories.py:929-953`). | `DoDRepository`; replace mutable upsert semantics with explicit version/amend operations and evidence invalidation. |
| G17 | P1 | `dod_evidence` is not protected by the append-only trigger used for audit/provider rows, and evidence-type/producer/task compatibility is not enforced in the repository or database (`agentos/storage/schema.sql:338-352`, `515-524`; `agentos/storage/repositories.py:955-990`). | Schema plus `DoDRepository.add_evidence`; make evidence append-only, validate producer authority and required fields by evidence type, and bind artifact/task/project consistently. |
| G18 | P1 | The PM relies on fallback coordinator routing for `REPLANNING_TRIGGERED`; its configured subscriptions omit both replanning and DoD evaluation (`agentos/config/actor_team.yml:13-18`; `agentos/runtime/trigger_engine.py:172-180`). | `actor_team.yml` and trigger contract tests; subscribe explicitly and retain fallback only as resilience behavior. |
| G19 | P2 | DoD evaluation is a fixed 30-second full poll with no durable generation/cutoff and logs exceptions indefinitely (`agentos/runtime/supervisor.py:718-786`). | Existing supervisor/watchdog/evaluator; trigger coalesced evaluation on evidence/task/integration events, retain periodic reconciliation as recovery, and block visibly after bounded repeated evaluator failure. |
| G20 | P0/P1 | Test coverage does not directly exercise `DoDEvaluatorActor`, `DoDRepository.evaluate_and_persist`, DoD watchdog behavior, evidence freshness, replanning validation, or the atomic terminal transition. The model test covers only one invalid plan relationship (`agentos/tests/test_core_models.py:91-113`); the schema test checks table presence (`agentos/tests/test_deployment_contracts.py:14-27`). | Add focused unit/SQL integration/concurrency tests under `agentos/tests/` before changing production behavior. See the validation matrix below. |
| G21 | P2 | The maintained prompt files are not loaded by the bootstrap/worker code; the executable prompts are duplicated inline (`prompts/bootstrap_pm.md`, `prompts/agent_worker.md`, `agentos/actors/bootstrap.py:84-120`, `agentos/actors/base.py:400-428`). | `agentos/config/loader.py` and existing actors; make one packaged prompt source authoritative and test its availability/version. Do not add a second prompt path. |

## Recommended integration design

The following stays within the current architecture and uses one delivery pipeline.

### Stage 1: Build a versioned DoD contract in the existing bootstrap flow

1. `RuntimeSupervisorActor.bootstrap_project()` establishes the source repository and captures its current HEAD before planning.
2. `BootstrapAgentActor.create_team_plan()` receives a bounded planning packet: user request, source HEAD, manifest/tree, relevant docs, constraints, and any previously locked criteria.
3. The actor produces a typed DoD contract first, then the backlog/roster against that exact contract in the same sequential planning operation.
4. `DoDCriterion` validators and a new cross-object validation method reject empty evidence, impossible evidence/reviewer combinations, unmapped criteria, unbounded tasks, duplicate semantics, and non-executable verification.
5. The supervisor persists project plan, DoD contract version, criterion hashes, backlog, dependencies, and resource plan in a transaction. Planning failure becomes `BLOCKED_REQUIRES_INPUT` with a reason; it never activates the generic fallback DoD.

This is a refactor of `create_team_plan` and `validate_team_plan`, not another planner.

### Stage 2: Produce evidence against the criterion and subject revision

1. The existing worker writes artifacts exactly as it does now.
2. Artifact evidence includes task, criterion version/hash, artifact/version/checksum, and worktree commit.
3. `ReviewerAgentActor` performs a fresh per-criterion read-only audit of the exact patch/artifact and records structured review evidence. The existing safety reviewer does the same when task risk or criterion contract requires it.
4. The worker runs the canonical criterion command and records the correct evidence type (`test` or `command`) plus command/sandbox digest and subject commit.
5. `ExecutionService._validate_merge_evidence()` evaluates the criterion's configured evidence policy rather than a hard-coded substitute.
6. Merge records integration evidence at the new integration HEAD and marks earlier affected evidence stale where appropriate.

No evidence is accepted merely because a model said `SUCCESS`.

### Stage 3: Evaluate one coherent snapshot

1. `DoDEvaluatorActor.evaluate()` opens an evaluation run with `project_id`, DoD contract version, integration HEAD, and evidence cutoff.
2. It evaluates deterministic preconditions, criterion-global requirements, per-task requirements, per-artifact requirements, command results, reviewer authority, object integrity, and freshness.
3. It returns every reason per criterion with `SATISFIED`, `FAILED`, `MISSING`, `STALE`, or `INCONCLUSIVE` classification.
4. It persists the run and criterion projections in one transaction.
5. The supervisor transitions to `DOD_SATISFIED` only if the project is still on the same DoD version, HEAD, and evidence generation and no task/evidence writer has advanced the project. Otherwise it schedules evaluation of the newer generation.

### Stage 4: Repair through the existing PM and watchdog

1. The DoD watchdog distinguishes runnable work, in-flight work, retryable evaluator uncertainty, and blocked/unclaimable work.
2. `REPLANNING_TRIGGERED` includes typed gaps, not only IDs.
3. The PM returns typed `InitialTask` proposals mapped only to current gaps and the current DoD version.
4. Cross-validation and idempotency occur before persistence.
5. Repeated identical replans are bounded. Exhaustion changes the project to a visible blocked state with the unresolved evidence contract.

### Stage 5: Expose the contract without adding an API family

Enhance `agentos status` (and any existing status output consumer) to present:

- DoD version/hash/source/lock/mandatory/severity;
- mapped tasks and required evidence cardinality;
- latest evaluation run and integration HEAD;
- evidence status and subject revision;
- all typed gap reasons;
- replan/evaluation retry state;
- any governed amendment or waiver.

## Prioritized implementation roadmap

### P0 — correctness before optimization

1. **Canonical contract validation and fail-closed planning** — G1-G6.
   - Owners: `bootstrap.py`, `team_plan.py`, `runtime/supervisor.py`, `config/actor_team.yml`.
   - Exit: an impossible, generic, uncovered, or evidence-empty DoD cannot start workers.
2. **Revision- and task-bound evidence schema** — G8, G9, G16, G17.
   - Owners: `storage/schema.sql`, `storage/repositories.py`, evidence producers in worker/reviewer/safety/execution.
   - Exit: the evaluator can prove what criterion version, task, artifact, and Git revision every result describes.
3. **Criterion-aware review and canonical evidence policy** — G5-G7, G11.
   - Owners: `actors/reviewer.py`, `actors/safety_reviewer.py`, `actors/base.py`, `execution/supervisor.py`, `dod/evaluator.py`.
   - Exit: configured evidence semantics match plan validation, production, merge, and final evaluation; no generic review is copied across unrelated criteria.
4. **Atomic final evaluation fence** — G9-G10.
   - Owners: evaluator, runtime supervisor, project/task/evidence repositories.
   - Exit: a concurrent task, evidence write, DoD amendment, or new integration commit forces reevaluation instead of allowing stale completion.
5. **DoD correctness test suite** — P0 portion of G20.
   - Exit: unit and PostgreSQL integration tests prove the above failure modes, including concurrency.

### P1 — reliable repair and human control

6. **Versioned/locked criteria and governed amendments** — QuantumByte stable-ID/lock pattern adapted through RAAD governance; G15-G16.
7. **Typed gap model and validated idempotent replanning** — G12-G14 and G18.
8. **Conservative diff/contract invalidation** — SHA, path/glob, artifact, and affected-contract dependency tracking.
9. **Operator visibility** — coherent CLI status, manual reevaluation request through existing control surfaces, and amendment audit history.
10. **Integration-HEAD validation policy** — targeted affected commands on each merge plus a final full required set before completion.

### P2 — efficiency and maintainability

11. **Durable event-triggered coalescing with periodic recovery** — G19.
12. **Single packaged prompt source and prompt-version audit field** — G21.
13. **Golden planning/evaluator datasets** — measure criterion coverage, false satisfaction, false failure, and evidence-quality regressions.
14. **Cost-aware per-criterion audit batching** — bounded concurrency and caching keyed by criterion hash + subject revision, without weakening freshness.

## Validation matrix for future implementation

| Test | Required proof |
|---|---|
| Contract model unit tests | Empty evidence, invalid type/command combinations, unmapped criteria, missing outputs/reviewers, and incompatible security requirements are rejected. |
| Planning fallback test | Provider failure leaves a visible blocked project and never activates the generic fallback DoD. |
| Atomic bootstrap SQL test | A failure during task/dependency persistence rolls back plan, DoD, and backlog together. |
| Evidence authority test | Wrong role, self-review, missing task FK, cross-project artifact/task, wrong criterion version, and unknown evidence type are rejected. |
| Evidence cardinality test | One task's passing evidence cannot satisfy another mapped task unless the criterion explicitly declares criterion-global scope. |
| Criterion-review test | Reviewer receives criterion text/acceptance criteria and separate verdicts can differ for two criteria mapped to one artifact. |
| Revision freshness test | A later merge touching a watched path/affected contract makes older evidence stale; unrelated changes preserve it. Glob/directory dependencies are handled conservatively. |
| Post-merge regression test | A task test that passed before merge but fails on integration HEAD prevents completion. |
| Finalization race test | Concurrent evidence, task creation, DoD amendment, or HEAD advance between evaluation and transition cannot produce `DOD_SATISFIED`. |
| Replanning test | Blocked/unclaimable tasks do not masquerade as runnable work; repeated gap events create one idempotent task set and eventually block visibly when exhausted. |
| Inconclusive test | Provider/object-store/transient verifier failure is persisted as inconclusive/retryable, never as pass and never silently discarded. |
| Resume/recovery test | A restarted supervisor discovers the latest evaluation generation and does not rely on an in-memory completion event for truth. |
| Live integration test | In the real Compose topology, artifact storage, independent review, sandbox command, merge, integration-HEAD validation, evaluator snapshot, and final fence complete end to end. |

## Risks, dependencies, and possible regressions

| Risk/dependency | Why it matters | Mitigation |
|---|---|---|
| PostgreSQL migration and existing rows | New contract/evidence fields cannot be non-null without a backfill; RAAD rejects incompatible legacy schemas rather than silently rewriting them. | Add an explicit schema version/migration with conservative legacy version `0`, unknown freshness, and forced reevaluation before completion. |
| Increased reviewer cost/latency | One isolated review per criterion can multiply provider calls. | Bound concurrency; cache only by criterion hash + exact subject revision; group context loading but keep separate structured verdicts; route through existing budgets. |
| Model nondeterminism | Semantic audits can disagree across reruns. | Treat them as independent review evidence only; retain deterministic artifact/test/integration requirements; record prompt/model/version and `INCONCLUSIVE`. |
| False-negative invalidation | Exact watched-file matching can miss globs, imports, generated files, configuration, or runtime dependencies. | Use path/glob overlap plus `affected_contracts`; fall back to broader invalidation whenever dependency capture is incomplete. |
| Excessive invalidation/thrashing | Conservative freshness may rerun large suites after small changes. | First make correctness conservative, then add measured dependency precision and per-criterion command scoping. Never use age alone as freshness. |
| Completion-lock contention | A strong final fence can contend with task/evidence writers or deadlock if lock order differs. | Define one lock order, keep the final transaction short, use generation compare-and-swap, and cover it with concurrency tests. |
| Human-locked criteria becoming obsolete | A lock can preserve a contradiction after product intent changes. | Surface conflicts, require a governed amendment, and block rather than silently deleting or changing the user's criterion. |
| Inferred requirement overreach | QuantumByte derives “hidden” requirements from product category. Treating these as mandatory can expand scope without authorization. | Store them as proposed/advisory with provenance; require approval before mandatory promotion. |
| Flaky commands/environment | Final integrated validation may expose nondeterministic tests or missing services. | Classify deterministic fail vs infrastructure inconclusive, pin sandbox image/config, cap retries, and surface persistent infrastructure blockers. |
| Replanning loops | Richer gap data can still produce duplicate or circular work. | Gap-generation IDs, deterministic task external keys, dependency validation, bounded attempts, and no criterion mutation during ordinary replanning. |
| Provider/source egress | A fresh read-only audit may send repository content to a provider. | Reuse `ProviderGatewayActor` redaction, allowlists, audit, budget, and routing; never invoke an SDK directly as QuantumByte does. |
| Documentation drift | Current docs already mention mandatory criteria and validation that code does not implement. | Update `goal.md`, `arch_plan.md`, and subsystem docs only when the corresponding behavior and tests land; add doc-contract assertions where practical. |

## QuantumByte limitations and patterns not to copy

The comparison must not treat the reference repository as a production-ready gold standard.

1. **Static source inspection is not world-grounded verification.** QuantumByte explicitly lists running-app evidence, operational requirements, and a structured readiness gate as future milestones in [`docs/roadmap.md`](https://github.com/QuantumByteOSS/quantumbyte/blob/6eaa965a018332dd4407a13466d2a9973a448fc5/docs/roadmap.md#L71-L91). RAAD must retain sandbox commands, artifact integrity, merge evidence, and final integrated validation.
2. **Harness failure is deliberately fail-open.** `run_harness()` catches top-level errors and returns, and Redis failure permits lock-free duplicate runs. That is acceptable for a non-blocking advisory UI but not for RAAD completion.
3. **Positive audit evidence is weak.** `HarnessVerdict.evidence` is designed to be null on success; a `SUCCESS` may therefore have no concrete positive evidence beyond the model verdict. RAAD must require positive, inspectable evidence.
4. **The ship gate is client-side and incomplete.** `computeGate()` exists in a React component, not a transactional backend completion authority. It also treats noncritical uncertainty differently from RAAD's “all mandatory criteria” goal.
5. **Path invalidation is exact-intersection based.** A captured glob or directory need not equal a concrete changed file, so relevant changes can be missed. Adopt the concept, not this algorithm.
6. **Locking can overlap.** The Redis lock has a fixed five-minute TTL with no renewal and degrades open when Redis is unavailable (`apps/worker/redis_lock.py:48-68`). Do not use this for a terminal gate.
7. **Commit failure does not block the turn.** QuantumByte logs commit/push failure, persists success, and schedules the harness against whatever HEAD remains (`apps/worker/main.py:1012-1063`). RAAD's evidence must always describe the durable integrated revision.
8. **There is current source-of-truth drift.** The main-agent prompt and business-requirements skill instruct the builder to edit `_doc/business-requirements.json` and a lock file, while the implemented harness reads/writes PostgreSQL `HarnessRequirement` rows and no runtime code consumes those files. RAAD should have one authoritative contract representation.
9. **Harness logic lacks direct tests.** The repository has serializer tests but no focused tests for `harness.py`, `harness_scheduler.py`, requirement reconciliation, SHA/path invalidation, or readiness behavior. RAAD should not reproduce that coverage gap.
10. **Model/provider isolation differs from RAAD's policy boundary.** QuantumByte's one-shot verifier invokes the Claude Agent SDK directly with bypass permissions. RAAD should route all such calls through its existing provider, audit, redaction, budget, and identity controls.

## Final recommendation

Implement the P0 items as one coherent enhancement of the existing Supervisor/DoD workflow:

1. make the DoD a versioned, source-grounded, validated contract;
2. make evidence criterion-aware, task-bound, artifact-bound, and revision-bound;
3. make review genuinely per criterion;
4. validate the integrated HEAD;
5. atomically fence the final completion snapshot;
6. prove these behaviors with evaluator, repository, and concurrency tests.

Then add QuantumByte-inspired locks, typed uncertainty, diff-scoped invalidation, coalesced evaluation, and operator visibility as P1/P2 improvements. This yields the intended convergence loop—intent, build, evidence, repair, readiness—without adding a second pipeline or surrendering RAAD's deterministic completion authority.

## Implementation completion record (2026-07-23)

All G1-G21 recommendations were implemented in the existing RAAD delivery path. No parallel planner, evidence store, evaluator, API family, legacy mode, or v2 pipeline was introduced.

| Findings | Implemented enhancement and integration point |
|---|---|
| G1 | `agentos/runtime/planning_context.py` builds a clean, bounded, source-revision/tree/docs context. `BootstrapAgentActor.create_team_plan()` uses the one packaged versioned prompt, performs bounded complete-object repair, and raises after exhaustion. `RuntimeSupervisorActor` persists a planning blocker and launches no workers. |
| G2-G3 | `DoDCriterion`, `InitialTask`, and `TeamPlan` now enforce executable evidence, criterion/task/output/contract coverage, stable unique semantics, provenance/locks/severity, artifact-review-integration baselines, reviewer/security unions, bounded paths, dependencies, and a mandatory deliverable. The hardened roster is revalidated through the same `TeamPlan` contract before persistence. |
| G4 | `ProjectRepository.persist_plan_bundle()` atomically writes the contract version, active criteria, backlog/dependencies, agents, resource plan, and runtime/planning snapshot. `dod_contract_versions` is append-only authority; `dod_checks` is its active projection. |
| G5-G6 | `EvidenceType`/`EvidenceScope` is the canonical policy used by plan validation, workers, `ExecutionService._validate_merge_evidence()`, and `DoDEvaluatorActor`. Reviewer and security gates are derived from criterion contracts plus task risk; impossible plans fail before work starts. |
| G7 | `ReviewerAgentActor` and `SafetyReviewerAgentActor` issue isolated strict structured calls per criterion with criterion text, task acceptance, exact artifact checksum/revision, and checksum-bound committed diff. Oversized/mismatched content or provider/tool/parse failure is persisted as `INCONCLUSIVE`; independent identities and self-review rejection remain mandatory. Single-flight waiters are cancellation-isolated. |
| G8, G17 | Schema v4 and `DoDRepository.add_evidence()` bind append-only evidence to project, active criterion version/hash, task, immutable exact-version artifact, producer role, subject/integration commit, canonical command/sandbox and committed-diff digests, watched paths/contracts, and evidence generation. Repository checks and `agentos_validate_dod_evidence` enforce the same authority fail closed, including criterion-global command authority. |
| G9 | The existing merge path validates task-branch evidence, uses an owner-checked renewable lock that cancels work on lease loss, prepares a governed no-fast-forward prospective tree, runs eligible canonical commands there, commits only on success, records integrated-HEAD command/integration evidence, and conservatively invalidates older watched-path/contract evidence. Durable `integration_attempts` recover crashes before/after the Git and database commits. |
| G10 | `dod_evaluation_runs/items`, `DoDRepository.start_evaluation()`, `persist_evaluation()`, and `finalize_project()` fence contract version/hash, integrated HEAD, evidence cutoff/generation, all criterion items, mapped work, and terminal writes. A race persists `STALE` plus `EVALUATION_SNAPSHOT_STALE` reasons and cannot finalize. |
| G11 | `DoDEvaluatorActor._evaluate_criterion()` enforces criterion-, task-, and artifact-scoped cardinality exactly as declared, including every mapped task/artifact, command/sandbox digests, exact MinIO version integrity, committed-diff reviewer authority, integration ancestry, and conservative glob/contract revision freshness. |
| G12 | `DoDGap` retains every typed reason with criterion, evidence type/scope, task, artifact, retryability, and suggested owner role. Replanning events carry these durable reasons instead of criterion IDs alone. |
| G13 | `TaskRepository.create_replan_batch()` accepts only typed `InitialTask` graphs covering the durable evaluation's exact gaps. It validates the still-current generation, current roles/contracts, artifact/contract coverage, reviewers/security, exact dependencies/cycles, and one immutable deterministic batch per evaluation generation; exact redelivery is a no-op and a changed or stale duplicate fails closed. Dynamic task creation uses the same complete contract validation. |
| G14 | `DoDWatchdog` distinguishes executing, dependency-runnable, blocked/unclaimable, and transiently unverifiable work, uses durable evaluation-correlated event IDs, exponential backoff, bounded attempts, and a visible terminal blocker on exhaustion. Retryable operational uncertainty schedules reevaluation instead of manufacturing repair tasks. |
| G15-G16 | Contract source, lock, mandatory, severity, version, and hash semantics are end to end. `amend-dod` requires an exact hash/reason-bound approved next version; `waive-dod` requires an approved active criterion-hash/reason decision. Both advance/invalidate current evaluation truth without weakening unrelated criteria. |
| G18 | `actor_team.yml` explicitly subscribes the PM to `DOD_EVALUATED` and `REPLANNING_TRIGGERED`; deterministic event IDs plus durable receipts keep fallback routing a recovery mechanism only. |
| G19 | Successful integration immediately hands off to the canonical evaluator/supervisor handler. PostgreSQL coalesces exact concurrent evaluations, supersedes changed snapshots, recovers abandoned runs, and leaves periodic watchdog polling as reconciliation. Repeated evaluator failure blocks visibly. |
| G20 | `test_dod_contract.py`, the golden dataset, and PostgreSQL/Compose integration tests cover contract/glob rejection, fail-closed request/source/symlink planning, strict isolated criterion verdicts, cancellation-safe cache behavior, atomic bootstrap rollback, exact artifact/diff/command/sandbox evidence authority, append-only rows, cardinality/freshness classifications, stale-generation exact-gap idempotent replanning, restart recovery, evaluation/finalization races, terminal barriers, restricted sandboxing, and the complete integrated delivery path. |
| G21 | `agentos/config/prompts/` is the only prompt source; `load_prompt()` consumes it as packaged data, plan/evidence rows retain prompt versions, packaging tests prove wheel availability, and the former root prompt shadows were removed. |

### Current validation evidence

- `python -m ruff format --check agentos`: 56 files formatted.
- `python -m ruff check agentos`: passed.
- `python -m mypy agentos`: passed for 47 source files.
- `python -m pytest -q -m "not integration"`: 58 passed; 9 integration tests deselected.
- Full isolated Compose topology (`docker compose --profile test ... integration-test`): 9 integration tests passed, 58 non-integration tests deselected. This includes PostgreSQL, DragonflyDB, MongoDB, MinIO, Milvus, the restricted Docker sandbox, atomic plan/evidence/evaluation controls, exact artifact/diff/command/sandbox authority, stale-generation exact-gap replanning, and a live `write -> Git/MinIO artifact -> sandbox command -> independent checksum-bound review -> prospective merge -> integrated-HEAD evaluator -> atomic DOD_SATISFIED` proof.
- `python -m compileall -q agentos`, wheel/runtime-asset packaging, schema initialization/migration, and `git diff --check`: passed.

### Residual operational risks and dependencies

- Real semantic reviews still depend on configured provider credentials and model availability. Failure remains `INCONCLUSIVE` and cannot satisfy a criterion.
- Schema v4 is additive/idempotent, but production rollout still requires the normal database backup and migration gate, especially for unknown legacy rows that must be reevaluated.
- Conservative path/contract invalidation can increase command and review cost. The revision-bound LRU/single-flight cache and bounded concurrency reduce duplicate provider work without caching uncertainty.
- Integrated validation requires the configured Git workspace, MinIO, PostgreSQL, DragonflyDB, and restricted Docker sandbox to be healthy. Dependency or evaluator failure is bounded and becomes an operator-visible blocker rather than a pass.
