# Governance and Controlled Execution

## Trust model

Agents may reason and propose actions; they are not execution authorities. Deterministic policy, project-bound identities, task ownership, human/reviewer gates, sandbox limits, and durable audit decide whether an action occurs.

## Action request

`ActionRequest` carries:

- project/agent/task IDs;
- action type and description;
- target paths;
- token-array command or one database operation;
- typed payload;
- UTC time and nonce;
- SHA-256 integrity hash over canonical fields.

Any field modification after sealing causes verification failure. The canonical payload must exactly agree with top-level target paths, command tokens, or database query/parameters, and nonces are consumed once to prevent replay.

## Policy order

`PolicyEngine.evaluate_action()` checks, in order:

1. request integrity;
2. quarantine status;
3. authenticated identity and matching agent/project;
4. role action permission;
5. path traversal, global protection, and identity/task ownership;
6. destructive text patterns;
7. security/review/backup/human gates;
8. sandbox-only, low-risk, and constrained-medium policies;
9. fail-closed denial for an unknown action type.

Global protections include environment secret paths, secret/provider-key/audit areas, Git metadata, and Docker sockets. Project source such as schemas/settings/workflows remains implementable when it belongs to the task; it still passes independent review and tests.

## Destructive operations

Known patterns include database/schema/table drops, truncation/deletes, recursive removal, guardrail/log/audit/checkpoint disabling, pipe-to-shell, production mutation/deployment, privilege changes, and host-secret access.

`AGENTOS_ALLOW_DESTRUCTIVE_ACTIONS=false` denies them. If explicitly enabled, they require a project/hash-bound approval plus the configured backup, sandbox, and review evidence. Repeated violations increment a Dragonfly TTL counter and eventually quarantine the agent.

## Audit

Every policy decision creates an `audit_events` record containing actor, action, risk, decision, request hash, previous audit hash, and current hash. Database triggers reject update/delete. Provider calls have separate append-only intent/result tables. Artifacts, DoD contract versions, evidence, and evaluation items are also append-only; insertion constraints independently verify exact object-version binding, criterion revision/type, authenticated producer role, task/artifact project and Git-revision binding, committed-diff provenance, self-review exclusion, canonical command/sandbox digests, result consistency, and integration-supervisor authority.

## File writes

Execution validates the active task lease and path ownership, creates/reuses the task worktree, writes via a temporary file, flushes and `fsync`s, atomically replaces the target, commits Git, uploads the exact bytes to versioned MinIO, verifies the exact committed diff, and records an append-only artifact whose URI `versionId`, version column, full Git revision, byte length, checksum, and diff digest agree. A configured `AGENTOS_SOURCE_REPOSITORY` is validated as Git and cloned locally into the managed integration repository before task branches are created.

The task enters review state. Every produced artifact receives independent review (and security review when required) before missing aggregate expected outputs return it to `PENDING` for more work.

## Reads

Reads are limited to the task worktree and path boundary, reject missing/non-file targets, and cap text size. There is no arbitrary host filesystem reader.

## Command sandbox

Commands are lists such as `['pytest', '-q']`, never shell strings. The first token and image must be allowlisted. The Docker container has:

- no network;
- read-only root;
- bounded `noexec,nosuid` `/tmp`;
- dropped capabilities and no-new-privileges;
- CPU, memory, PIDs, threads, time, and output caps;
- only the assigned worktree mounted read-write.

The runtime reaches Docker through a socket proxy exposing only required image/container/info operations, not the raw socket in the AgentOS container.

## Database sandbox

Database actions use a second PostgreSQL instance and a DSN that production validation requires to differ from the control DSN. Only one `SELECT`, `INSERT`, `UPDATE`, `CREATE`, or `ALTER` statement is accepted; multi-statement strings and destructive statement classes are rejected. Values use parameters.

## Review/test/merge gates

For every mapped criterion, the execution path reads the active contract rather than assuming a fixed evidence trio:

1. Record checksummed artifact evidence.
2. Obtain an isolated independent code-review verdict for that criterion/artifact and any criterion- or risk-required security verdict. The verdict schema is strict and the reviewed content must match the checksum-bound committed diff; provider, parsing, size, or provenance uncertainty is appended as inconclusive evidence and fails the gate.
3. Confirm all expected-output patterns match recorded artifacts.
4. Select the configured criterion verification command (or an explicit allowed task command).
5. Run it in the sandbox and record the canonical token-array command, SHA-256 command digest, sandbox-configuration digest, exit code, and subject revision.
6. Require every configured artifact-, task-, and criterion-scoped pre-merge evidence row to match the active criterion hash, authenticated producer, task/artifact, checksum, task-branch HEAD, and applicable command/sandbox/diff digests. Criterion-global command evidence is accepted only from the integration supervisor.
7. Acquire the owner-checked renewable project merge lock; loss/renewal failure cancels the protected operation. Then persist a `PREPARED` integration attempt.
8. Create a no-commit prospective merge and run every newly eligible unique DoD command in the restricted sandbox against that exact tree. Any failure aborts the merge.
9. Commit only the passing prospective tree, atomically persist its integration HEAD/attempt state, append integrated-HEAD command plus task integration evidence, and transition `UNDER_REVIEW -> COMPLETED`.
10. Trigger the snapshot-fenced evaluator; periodic reconciliation recovers a failed event handoff.

A review/test failure is repairable and returns to `PENDING`. A merge conflict becomes `BLOCKED` with details. Durable `PREPARED`, post-commit/pre-database, and `COMMITTED` pre-evidence states are replayed under the same gate after a crash. A merge lacking a matching durable attempt is rejected; no force merge or silent conflict resolution exists.

## Human approval lifecycle

Approval requests store required gate, canonical request JSON, hash, requester, status, expiry, approver, and decision reason/time. CLI approval/rejection can update only a live `PENDING` row. Execution then revalidates project, hash, identity, and current policy before dispatch.

DoD changes use this same boundary. `DOD_AMENDMENT` approval binds the complete next-version `TeamPlan` hash and reason; applying it increments the version and atomically replaces active criteria/work while retaining append-only contract history. `DOD_WAIVER` binds one active criterion hash and reason; it cannot edit another criterion or survive a later amendment. Ordinary PM replanning has no criterion-mutation API.

## Security limitations

Container isolation is defense in depth, not a proof against every kernel/runtime vulnerability. Production operators must patch Docker/host dependencies, restrict host access, back up durable stores, and conduct environment-specific threat modeling.
