# AgentOS Local Architecture Plan

## 1. Executive architecture summary

AgentOS Local is a local-first, Python-based autonomous software delivery platform. It runs a dynamic team of IT and development agents that collaborate in parallel through guarded event-driven communication, scoped memory, controlled execution, continuous checkpointing, and continuous DoD evaluation.

The platform is designed to run for as long as required until the project Definition of Done is fully satisfied with evidence. It is not time-bound. It is DoD-bound.

The platform is not a general-purpose assistant. It is dedicated to software delivery and IT engineering activities.

## 2. Core operating principle

```text
Agents decide what they want to do.
The runtime decides what they are allowed to do.
The project ends only when DoD is verified.
```

The system should not stop because:

- the current task list is empty
- one agent has no next step
- tests failed
- agents disagree
- integration failed
- current implementation is incomplete

If DoD is incomplete and no active work exists, the platform must trigger DoD gap analysis, replan, create new tasks, and continue.

## 3. Final platform definition

AgentOS Local is a local-first autonomous software delivery runtime that:

- uses Python as the implementation language
- uses a CLI as the control surface
- uses Ray actors as the agent execution model
- uses PostgreSQL as the durable source of truth
- uses pgvector inside PostgreSQL for semantic memory recall
- uses Dragonfly as a Redis-compatible hot coordination layer
- uses Docker for local deployment and execution isolation
- routes all AI provider calls through an internal provider gateway
- uses strict guardrails for agent communication, memory, actions, files, commands, databases, network, provider I/O, approvals, and audit
- dynamically creates an agent team through the first bootstrap PM/Tech Lead agent
- runs continuously until the project DoD is fully satisfied

## 4. Non-goals

The platform should not initially provide:

- web UI
- public REST API
- generic personal assistant behavior
- arbitrary internet-browsing agents
- HR/legal/medical/finance decision agents
- production deployment without approval gates
- direct agent access to shell, filesystem, database, secrets, or provider keys
- unrestricted agent-to-agent chat

## 5. Domain scope

Allowed activities:

- software architecture
- backend development
- frontend development
- database design
- API design
- DevOps and platform engineering
- CI/CD design
- Docker/local deployment
- testing and QA
- security review
- documentation
- dependency management
- debugging
- performance review
- release preparation

Denied or out-of-scope activities:

- general personal task automation
- social media automation
- unrelated browsing
- legal/medical/financial judgment
- HR decisions
- destructive host operations
- uncontrolled production changes

## 6. High-level architecture

```text
CLI
 ↓
Runtime Supervisor Actor
 ↓
Bootstrap PM/Tech Lead Agent
 ↓
Validated Dynamic Team Plan
 ↓
Ray Agent Worker Actors
 ↓
Agent Governance Layer
 ↓
Guarded Communication Bus
 ↓
Scoped Memory Broker
 ↓
Guarded Action Requests
 ↓
Execution Supervisor
 ↓
Docker Sandbox / Git Workspace / Test Database
 ↓
Review / Tests / Security / DoD Evaluation
 ↓
Checkpoints + Summaries + Audit Logs
 ↓
Continue until DoD is satisfied
```

## 7. Application stack

| Concern | Selected technology | Reason |
|---|---|---|
| Main language | Python 3.11+ | Strong ecosystem for agents, Ray, automation, DevOps tooling |
| CLI | Typer + Rich | Simple local control surface with good UX |
| Actor runtime | Ray | Stateful Python actor model, parallelism, independent workers |
| Durable database | PostgreSQL | Strong relational source of truth |
| Vector retrieval | pgvector | Semantic search inside PostgreSQL without separate vector service |
| Hot coordination | Dragonfly | Redis-compatible streams, locks, leases, counters, pub/sub |
| AI provider abstraction | Provider Gateway, planned LiteLLM adapter | External provider isolation and provider portability |
| Execution isolation | Docker | Local sandboxing and app deployment base |
| Configuration | Pydantic Settings | Typed configuration from environment |
| Logging | structlog, planned | Structured auditability |
| Testing | pytest | Standard Python testing |
| Quality | ruff, mypy, black | Linting, type checks, formatting |

## 8. Runtime actor architecture

### 8.1 RuntimeSupervisorActor

Responsibilities:

- start project lifecycle
- start bootstrap agent
- validate team plan against configured limits
- create Ray agent actors
- supervise actor health
- restart failed actors
- coordinate shutdown and resume
- enforce max agent limits

### 8.2 Bootstrap PM/Tech Lead Agent

First agent to run.

Responsibilities:

- understand project request
- define assumptions
- define initial DoD
- propose agent team composition
- propose memory scopes
- propose ownership boundaries
- propose initial task categories
- propose high-level architecture direction
- propose event subscriptions

The bootstrap agent proposes. The runtime validates.

### 8.3 AgentWorkerActors

Long-running Ray actors representing specialized agents.

Each agent has:

- agent identity
- role
- squad
- project ID
- permissions
- memory scopes
- allowed actions
- ownership domains
- active task state
- event subscription profile
- checkpoint pointer

Agents do not directly execute actions. They generate action requests.

### 8.4 TriggerEngineActor

Responsibilities:

- route events to impacted agents
- determine interrupt level
- avoid unnecessary wakeups
- identify downstream consumers of contracts/artifacts
- trigger catch-up packets
- detect agent handoff opportunities

### 8.5 MemoryBrokerActor

Responsibilities:

- enforce memory access control
- retrieve relevant memory
- use PostgreSQL filters and pgvector search
- summarize catch-up packets
- prevent prompt pollution
- prevent restricted data exposure

### 8.6 ProviderGatewayActor

Responsibilities:

- isolate external AI provider access
- hold provider configuration
- perform prompt redaction
- enforce budgets
- route models
- handle fallback models
- log provider calls
- validate provider output

### 8.7 ExecutionSupervisorActor

Responsibilities:

- execute file changes through controlled paths
- execute shell commands through command policies
- manage Git branches/worktrees
- manage Docker sandbox execution
- run tests/lint/builds
- block destructive operations
- return structured execution results

### 8.8 CheckpointManagerActor

Responsibilities:

- create checkpoints after achievements
- persist actor state pointers
- link checkpoints to artifacts
- support recovery and resume

### 8.9 SummaryManagerActor

Responsibilities:

- generate local agent summaries
- generate squad summaries
- generate project summaries
- compress long event history into usable context
- feed validated memory into long-term storage

### 8.10 DoDEvaluatorActor

Responsibilities:

- evaluate current project state against DoD
- verify evidence for each DoD item
- detect gaps
- trigger gap-closure work
- prevent false completion

## 9. Agent lifecycle

```text
STARTING
 ↓
RESTORE_FROM_POSTGRES
 ↓
SUBSCRIBE_TO_EVENTS
 ↓
IDLE
 ↓
TRIGGERED
 ↓
CATCH_UP
 ↓
DECIDE_NEXT_ACTION
 ↓
REQUEST_LOCKS
 ↓
SUBMIT_ACTION_REQUEST
 ↓
EXECUTION_SUPERVISOR_RUNS_IF_ALLOWED
 ↓
PUBLISH_OUTPUT
 ↓
CHECKPOINT
 ↓
SUMMARIZE_IF_NEEDED
 ↓
TRIGGER_RELATED_AGENTS
 ↓
IDLE
```

## 10. Event-driven execution model

The system should not rely on a rigid static graph. Agents are independent actors triggered by events.

Event flow:

```text
Event occurs
 ↓
Event is persisted
 ↓
Trigger Engine identifies impacted agents
 ↓
Memory Broker builds catch-up packets
 ↓
Agent decides next best action
 ↓
Action Guardrails classify and approve/deny/escalate
 ↓
Execution Supervisor performs allowed work
 ↓
Artifacts/events/checkpoints/summaries are written
 ↓
More agents are triggered
```

## 11. Communication architecture

Agents communicate through typed messages and topics, not raw uncontrolled chat.

Message categories:

- TASK_PROPOSAL
- TASK_UPDATE
- CONTRACT_CHANGE
- BLOCKER
- REVIEW_REQUEST
- REVIEW_RESULT
- TEST_RESULT
- SECURITY_ALERT
- CHECKPOINT
- SUMMARY
- ACTION_REQUEST
- APPROVAL_REQUEST

Communication guardrails validate:

- sender identity
- receiver permission
- topic
- message type
- payload schema
- sensitivity
- trigger permission
- privilege escalation attempts
- unsafe embedded instructions
- conflict with accepted decisions

## 12. Topics and routing

Example topic categories:

```text
project.{project_id}.events
project.{project_id}.tasks
project.{project_id}.contracts
project.{project_id}.reviews
project.{project_id}.tests
project.{project_id}.blockers
project.{project_id}.checkpoints
project.{project_id}.summaries
squad.backend.events
squad.frontend.events
squad.platform.events
squad.qa.events
agent.{agent_id}.inbox
```

Dragonfly Streams should be used for catchable events. Pub/Sub should be used only for live wakeup notifications.

## 13. Team formation

The team is identified by the bootstrap PM/Tech Lead agent.

The team plan includes:

- roles
- number of agents per role
- responsibilities
- memory scopes
- allowed actions
- ownership domains
- subscriptions

Runtime validation enforces:

- max total agents
- max active agents
- max parallel code tasks
- role-specific caps
- required minimum safety roles

Example team for a complex ecommerce project:

- 1 PM/Tech Lead
- 2 Solution Architects
- 4 Backend Developers
- 3 Frontend Developers
- 2 Platform Engineers
- 2 QA Engineers
- 1 Security Reviewer
- 1 Code Reviewer

## 14. Parallelism and conflict prevention

Agents must work in parallel without duplicating or conflicting.

Rules:

```text
Parallelize by domain.
Serialize by shared artifact.
```

Agents can work in parallel when:

- tasks have no dependency conflict
- ownership paths do not overlap
- contracts are not exclusively locked
- shared resources are not locked
- agent capacity is available
- policy allows the work

Conflict prevention layers:

1. ownership boundaries
2. allowed paths
3. resource locks
4. branch-per-task
5. contract ownership
6. review gates
7. merge queue
8. CI/test validation

## 15. Ownership model

Each task has:

- owner agent
- allowed paths
- blocked paths
- dependencies
- expected outputs
- required reviewers
- affected contracts
- risk level

Agents cannot modify outside their allowed paths without escalation.

## 16. Guardrails and security architecture

AgentOS Local uses a zero-trust agent runtime.

No agent is trusted by default.
No message is trusted by default.
No tool call is trusted by default.
No memory read is trusted by default.
No provider output is trusted by default.

### 16.1 Governance layer

```text
Agent Governance Layer
├── Identity & Role Registry
├── Communication Guardrails
├── Memory Access Guardrails
├── Action/Tool Guardrails
├── Filesystem Guardrails
├── Database Safety Guardrails
├── Network Egress Guardrails
├── Provider Input/Output Guardrails
├── Approval Matrix
├── Quarantine Manager
├── Audit Log
└── Safety Review Agents
```

### 16.2 Risk levels

LOW:

- read allowed files
- search allowed memory
- run unit tests
- create summary

MEDIUM:

- edit owned files
- create branch
- update documentation
- publish artifact

HIGH:

- add dependency
- modify CI/CD
- change authentication code
- run database migration in sandbox
- modify Docker files

CRITICAL:

- drop database
- drop table
- truncate persistent data
- delete repository
- delete checkpoints
- delete audit logs
- disable guardrails
- expose secrets
- modify provider keys
- deploy to production

### 16.3 Guardrail decisions

- ALLOW
- DENY
- ALLOW_WITH_CONSTRAINTS
- REQUIRE_REVIEW
- REQUIRE_HUMAN_APPROVAL
- REQUIRE_SANDBOX_ONLY
- REQUIRE_BACKUP_FIRST
- REQUIRE_SECURITY_REVIEW
- QUARANTINE_AGENT

### 16.4 Database safety

Separate:

- AgentOS control database
- generated application sandbox/test database

Agents must never directly write to the AgentOS control database.

Allowed autonomously:

- create migration file
- run migration against disposable test database
- create test data
- reset disposable sandbox database

Blocked or approval-required:

- drop persistent database
- drop table
- truncate non-test table
- delete migration history
- remove backups
- modify production-like database

### 16.5 Filesystem safety

Agents receive allowed paths. Sensitive paths are blocked.

Blocked or approval-required:

- `.env`
- secrets directories
- provider keys
- audit logs
- checkpoint files
- CI security gates
- Docker socket configuration
- policy files

### 16.6 Command safety

Agents cannot run commands directly. They submit action requests.

Safe:

- run tests
- run lint
- inspect git diff
- list files
- read logs

Controlled:

- install dependency
- docker build
- docker compose up
- run database migration in sandbox

Blocked or approval-required:

- rm -rf
- sudo
- curl | sh
- wget | sh
- chmod/chown sensitive paths
- drop database
- disable logs
- access host secrets

### 16.7 Provider safety

All external AI calls go through ProviderGateway.

ProviderGateway responsibilities:

- redaction
- budget checks
- model routing
- fallback
- request/response audit
- output validation
- prompt-injection detection hooks
- sensitive data filtering

## 17. Memory architecture

Memory is scoped. Agents share some memory, not all.

### 17.1 Short-term memory

Backed by Dragonfly.

Used for:

- hot event notifications
- active task leases
- locks
- heartbeats
- active context
- provider counters
- temporary coordination

Dragonfly is not the source of truth.

### 17.2 Long-term memory

Backed by PostgreSQL.

Used for:

- events
- tasks
- decisions
- artifacts
- checkpoints
- summaries
- DoD checks
- provider audit records
- memory items
- semantic embeddings

### 17.3 Memory scopes

- private_agent_memory
- squad_memory
- project_memory
- contract_memory
- decision_memory
- security_memory
- provider_audit_memory
- execution_memory
- global_patterns

### 17.4 Vector index with pgvector

The vector index is a semantic recall layer, not the brain and not the source of truth.

Use pgvector for:

1. agent catch-up
2. relevant memory retrieval
3. duplicate task detection
4. similar failure lookup
5. codebase semantic map
6. contract impact analysis
7. agent trigger routing
8. long-term lessons learned
9. DoD gap similarity
10. context compression

Do not embed:

- secrets
- API keys
- credentials
- raw full logs
- raw provider prompts
- raw chain-of-thought
- full repository dumps
- generated vendor files
- node_modules
- lock files unless summarized
- binary files

Store structured records first. Use embeddings for retrieval handles and summaries.

### 17.5 Retrieval pipeline

Memory retrieval should be hybrid:

1. access-control filters
2. project filters
3. scope filters
4. lexical search
5. vector similarity
6. recency scoring
7. importance scoring
8. reranking
9. summary compression

Agents should never query pgvector directly. All retrieval must pass through MemoryBroker.

## 18. Checkpoint and summary architecture

Checkpoint after meaningful achievements:

- task claimed
- task completed
- code patch generated
- tests passed
- tests failed
- review completed
- contract published
- architecture decision accepted
- blocker opened
- blocker resolved
- merge completed
- DoD gap closed

Summary levels:

1. agent local summary
2. squad summary
3. project executive summary

Summaries are the primary context for future prompts. Raw logs are evidence, not primary prompt context.

## 19. DoD model

The project is DoD-bound.

Each DoD item has:

- id
- description
- owner
- verification method
- required artifacts
- status
- evidence
- last checked timestamp

Statuses:

- NOT_STARTED
- IN_PROGRESS
- IMPLEMENTED
- UNDER_REVIEW
- FAILED_VERIFICATION
- SATISFIED
- WAIVED_BY_HUMAN

No item becomes SATISFIED without evidence.

## 20. Run-to-DoD policy

Project states:

- INITIALIZING
- PLANNING
- TEAM_FORMING
- RUNNING
- REPLANNING
- INTEGRATING
- VERIFYING
- BLOCKED_REQUIRES_APPROVAL
- BLOCKED_REQUIRES_INPUT
- DOD_SATISFIED
- FAILED_BY_POLICY
- STOPPED_BY_USER

Only terminal states:

- DOD_SATISFIED
- FAILED_BY_POLICY
- STOPPED_BY_USER

If the system reaches no active tasks and DoD is incomplete, it must trigger replanning.

## 21. Watchdogs

### 21.1 DoD Watchdog

Detects incomplete DoD with no active work.

Action:

- trigger gap analysis
- trigger PM/Tech Lead
- create new work

### 21.2 Stagnation Watchdog

Detects repeated failures or no progress.

Signals:

- same test repeatedly fails
- same files repeatedly rewritten
- no checkpoint for long period
- repeated circular handoffs

Action:

- freeze affected stream
- summarize issue
- trigger architect/reviewer/PM

### 21.3 Deadlock Watchdog

Detects dependency cycles.

Action:

- trigger PM/Architect to break dependency cycle

### 21.4 Safety Watchdog

Detects dangerous behavior.

Action:

- block action
- quarantine agent if needed
- notify security agent

## 22. Persistence architecture

PostgreSQL stores:

- projects
- agents
- events
- tasks
- task dependencies
- artifacts
- checkpoints
- summaries
- memory items
- embeddings
- provider calls
- audit events

Dragonfly stores:

- locks
- leases
- heartbeats
- live event notifications
- short-lived queues
- budget counters

## 23. Deployment architecture

Local Docker Compose services:

- agentos application container
- ray-head
- postgres with pgvector
- dragonfly

Future services:

- Ray worker containers
- sandbox worker service
- optional observability stack
- optional artifact storage service

## 24. CLI architecture

Required commands:

- init
- plan
- run
- status
- logs
- inspect
- pause
- resume
- approve
- reject
- guardrail-check

Starter scaffold includes:

- init
- plan
- run
- status
- guardrail-check

## 25. Implementation plan

### Phase 1: Foundation

- complete PostgreSQL repositories
- add Alembic migrations
- persist runtime state
- persist agents, events, checkpoints, summaries
- connect Docker Compose fully

### Phase 2: Event and trigger runtime

- implement Dragonfly Streams consumer groups
- implement EventStore backed by PostgreSQL
- implement TriggerEngine
- implement subscriptions
- implement agent inboxes

### Phase 3: Provider Gateway

- integrate LiteLLM
- add redaction
- add budget checks
- add provider audit logging
- add output guardrails
- add retry/fallback policy

### Phase 4: Memory

- implement memory ACLs
- implement memory promotion
- implement embedding creation
- implement pgvector retrieval
- implement catch-up packet generation

### Phase 5: Task planning

- implement task graph
- implement ownership boundaries
- implement dependency detection
- implement duplicate task detection
- implement parallel scheduling policies

### Phase 6: Execution

- implement Git workspace manager
- implement branch-per-task
- implement patch application
- implement Docker sandbox commands
- implement test/lint/build runners
- enforce filesystem and command policies

### Phase 7: Review and QA

- implement reviewer agents
- implement QA agents
- implement security reviewer flow
- implement evidence collection
- implement DoD evaluation

### Phase 8: Run-to-DoD

- implement DoD watchdog
- implement stagnation watchdog
- implement deadlock watchdog
- implement replanning
- implement terminal state validation

### Phase 9: Hardening

- append-only audit logs
- agent quarantine
- provider egress policy
- secret scanning
- dependency risk checks
- observability and traces

## 26. Engineering principles

- agents produce artifacts, not just messages
- all high-risk work requires review
- all execution is supervised
- all memory access is scoped
- all provider access is routed through gateway
- all important events are logged
- all meaningful achievements are checkpointed
- all completion claims require evidence
- all destructive actions are denied or escalated
- PostgreSQL is truth
- Dragonfly is coordination
- pgvector is semantic recall
- Ray actors are execution units
- Docker is sandboxing and local deployment base

## 27. Starter skeleton map

```text
agentos/
├── actors/
│   ├── base.py
│   └── bootstrap.py
├── checkpoints/
│   └── manager.py
├── cli/
│   └── main.py
├── config/
│   └── settings.py
├── dod/
│   └── evaluator.py
├── execution/
│   └── supervisor.py
├── governance/
│   ├── models.py
│   └── policy_engine.py
├── memory/
│   └── broker.py
├── messaging/
│   ├── dragonfly_bus.py
│   └── events.py
├── provider/
│   └── gateway.py
├── runtime/
│   ├── supervisor.py
│   └── team_plan.py
├── storage/
│   └── schema.sql
└── watchdogs/
    └── runtime_watchdogs.py
```

## 28. Final architecture commitment

AgentOS Local will be built as a guarded, local-first, Ray-powered autonomous software delivery platform that dynamically forms an IT/development agent team, runs agents as event-driven actors, coordinates them through Dragonfly and PostgreSQL, gives them scoped memory with pgvector semantic recall, executes work only through controlled supervisors, logs and checkpoints everything, and continues until the project DoD is fully satisfied.
