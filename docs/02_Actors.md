# 02 — The Agents Themselves (Actors)

Files covered:
- `agentos/actors/base.py` — the generic "worker" agent
- `agentos/actors/bootstrap.py` — the "project manager" agent that runs first
- `agentos/actors/reviewer.py` — the "code inspector" agent

---

## First, the big picture, with no code at all

Think of AgentOS like a small remote software company:

- You (the human) walk in and say: **"Build me a blog platform."**
- A **Project Manager** (`BootstrapAgentActor`) hears this, thinks about it,
  and writes a plan: what roles do we need? A backend developer? A frontend
  developer? What does "done" actually mean for this project?
- Based on that plan, the company hires some **Developers**
  (`AgentWorkerActor` — one per role). Each developer works completely on
  their own, in their own little office, checking their to-do list over and
  over: "what's the most important thing for me to do right now?"
- Whenever a developer writes code, before it counts as "done," a
  **Code Reviewer** (`ReviewerAgentActor`) checks it for obvious security
  problems first.

Every one of these — the manager, each developer, the reviewer — is a
separate **Ray actor**. Refer back to `00_Python_Fundamentals_Glossary.md`
§12 if you need a refresher, but here's the short version again:

> A Ray actor is like giving each "employee" their own private office with
> their own phone line. You don't walk into their office and do the work
> yourself — you call their phone and ask them to do something, then wait
> for them to call you back with the result. Each actor runs completely
> independently, at the same time as all the others.

---

## `agentos/actors/base.py` — the Developer

This file defines `AgentWorkerActor` — the class used for every "developer"
agent (backend developer, frontend developer, QA, etc). It's the same class
for all of them; only the `role` text passed in differs.

### Setting up the class

```python
@ray.remote(max_restarts=-1, max_task_retries=3)
class AgentWorkerActor:
```

- `@ray.remote` — as covered in the glossary, this turns the class into
  something Ray can run as its own independent, always-on process (its own
  "office").
- `max_restarts=-1` — in plain English: **"if this agent crashes, restart
  it automatically, forever, no limit."** Without this, one bug in one
  agent could permanently take that agent offline.
- `max_task_retries=3` — if you ask this agent to do something and it fails
  partway through (say, due to a flaky network call), Ray will automatically
  try again, up to 3 times, before giving up.

### `__init__` — what happens the instant a developer is "hired"

```python
def __init__(self, agent_id: str, role: str, project_id: str, settings: dict):
    self.agent_id = agent_id
    self.role = role
    self.project_id = project_id
    self.settings = Settings(**settings) if settings else Settings()

    self.db_manager = DatabaseManager(self.settings)
    self.provider = ProviderGateway(self.settings)
    self.checkpoints = CheckpointManager(self.db_manager)

    self.status = "STARTING"
    self.current_task_id: str | None = None
    self.is_running = False
```

In plain terms: this just labels the new agent (its ID, its role, which
project it belongs to) and creates a few helper objects it'll need later:
something to talk to the database (`db_manager`), something to talk to the
AI model (`provider`), and something to record "proof of progress"
(`checkpoints` — explained fully in file 06). Nothing actually *happens*
yet — the agent isn't connected to anything, it isn't listening for work.
That all happens in the next method, `start()`.

Notice `self.status = "STARTING"` and `self.is_running = False` — these are
just plain text/boolean flags the agent uses to track its own state, a bit
like a sign on an office door that says "Away" vs "In — ask me anything."

### `start()` — opening for business

```python
async def start(self) -> dict:
    from agentos.execution.supervisor import ExecutionSupervisor

    await self.db_manager.connect()
    self.memory_broker = MemoryBroker(self.db_manager)
    self.task_repo = TaskRepository(self.db_manager)
    self.audit_repo = AuditEventRepository(self.db_manager)
    self.artifact_repo = ArtifactRepository(self.db_manager)
    self.supervisor = ExecutionSupervisor(self.settings)

    self.provider.db_manager = self.db_manager
    self.provider.call_repo = ProviderCallRepository(self.db_manager)

    self.status = "IDLE"
    self.is_running = True
```

Step by step, in plain English:

1. Actually connect to the Postgres database (`await self.db_manager.connect()`
   — remember `await` means "pause here until this finishes," §Glossary 3).
2. Create a handful of small helper objects the agent will use constantly:
   one for remembering things (`memory_broker`), one for reading/writing
   tasks in the database (`task_repo`), one for logging what it did for
   safety records (`audit_repo`), one for recording files it created
   (`artifact_repo`), and one that actually *carries out* actions safely
   (`supervisor` — this is the guarded gatekeeper explained in file 03).
3. Flip its own status from `"STARTING"` to `"IDLE"` — "I'm hired and ready,
   just waiting for something to do."

```python
    from redis.asyncio import Redis
    self.redis_client = Redis.from_url(self.settings.dragonfly_url, decode_responses=True)
    self.pubsub = self.redis_client.pubsub()

    wakeup_channel = f"agent:{self.agent_id}:wakeup"
    await self.pubsub.subscribe(wakeup_channel)

    self._inbox_task = asyncio.create_task(self._inbox_listening_loop())
```

Now think of this next part like giving the agent **a mailbox and a
doorbell**:

- The **mailbox** (`inbox`) is a simple list stored in Dragonfly/Redis where
  other parts of the system can drop off pending work for this specific
  agent.
- The **doorbell** (`pubsub` on a `wakeup` channel) is a way to instantly
  "ring" this agent and say "hey, something new just arrived, go check your
  mailbox!" — instead of the agent having to constantly open the mailbox
  every second just to see if anything's there (which would be wasteful).
- `asyncio.create_task(self._inbox_listening_loop())` is the important
  line: it starts a **background loop** (§Glossary 3) that runs forever,
  quietly checking the mailbox and listening for the doorbell, *while*
  the rest of the program keeps running normally. This loop is what makes
  the agent "always on."

```python
    logger.info("agent_started", agent_id=self.agent_id, role=self.role, project_id=self.project_id)
    return {
        "agent_id": self.agent_id,
        "role": self.role,
        "project_id": self.project_id,
        "status": self.status,
    }
```

Finally it logs "I'm alive" and hands back a small summary dictionary —
this is the "started" data you saw printed in your terminal logs.

### `_inbox_listening_loop()` — checking the mailbox forever

```python
async def _inbox_listening_loop(self) -> None:
    inbox_key = f"agent:{self.agent_id}:inbox"

    while self.is_running:
        try:
            raw_event_data = await self.redis_client.lpop(inbox_key)
            if not raw_event_data:
                message = await self.pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=cfg["pubsub_poll_timeout_seconds"]
                )
                if message and message["data"] == "NEW_EVENT":
                    continue
                await asyncio.sleep(cfg["empty_inbox_sleep_seconds"])
                continue

            event_dict = json.loads(raw_event_data)
            await self.process_next_step(event_dict.get("event_id"))
        except Exception as e:
            ...
```

Plain-English version of this loop:

> "While I'm still running: check my mailbox. Got something? Great, open it
> and go handle it (`process_next_step`). Mailbox empty? Then just listen
> at the door for a moment (up to `pubsub_poll_timeout_seconds`, about 1
> second) in case the doorbell rings. Still nothing? Take a very short nap
> (`empty_inbox_sleep_seconds`, about 0.1 seconds) and check again. Repeat
> forever."

That "very short nap" matters: without it, this loop would spin as fast as
your computer possibly can, wasting CPU checking an empty mailbox thousands
of times a second. The nap gives the computer a tiny breather each cycle.

The `except Exception` part at the bottom is defensive cleanup: if
something goes wrong badly enough that Python's whole event loop is
shutting down (e.g. the actor itself is being terminated), this notices
that and quietly stops the loop instead of endlessly printing errors into
a void. You actually saw this exact message in your logs —
`actor_loop_terminating_stopping_inbox` — right before the actor was torn
down by the bug we fixed in the previous message.

### `process_next_step()` — the actual "brain" of the agent

This is the most important method in the whole file — it's what the agent
does every single time it has something to react to. Let's go slowly.

```python
async def process_next_step(self, event_id: str) -> dict:
    self.status = "DECIDE_NEXT_ACTION"

    packet = await self.memory_broker.build_catchup_packet(
        project_id=self.project_id, agent_id=self.agent_id, trigger_event_id=event_id, provider_gateway=self.provider
    )
    active_tasks = await self.task_repo.get_active_tasks(self.project_id)
```

In plain English: "Before I decide what to do, let me catch myself up."
It asks the `memory_broker` (file 05) for a summary of recent history —
like reading the last few messages in a group chat before jumping in — and
asks the database for the current to-do list (`active_tasks`) for this
whole project.

```python
    system_prompt = (
        f"You are {self.agent_id}, a {self.role}.\n"
        f"Here are the ongoing uncompleted tasks for this project:\n{json.dumps(active_tasks)}\n"
        "Choose the most critical task from the list above that matches your role.\n"
        "CRITICAL: You must return the exact 'task_id' you are working on in your JSON response.\n\n"
        "SCHEMA LAYOUT:\n"
        "{...}"
    )
```

This builds the actual instructions sent to the AI model (the "prompt").
In plain English, it says: *"You're [agent_id], a [role]. Here's the
to-do list. Pick the most important task for someone in your role, and
tell me exactly what you want to do about it, formatted as this specific
JSON shape."* This is how the agent "thinks" — it doesn't have real
judgment of its own; every decision comes from asking the LLM (via
`ProviderGateway`, file 05) a very structured question and parsing its
JSON answer.

```python
    request = ProviderRequest(...)
    response = await self.provider.get_completion(request, response_format={"type": "json_object"})

    clean_content = response.content.strip()
    if clean_content.startswith("```"):
        clean_content = re.sub(r"^```json\s*|^```\s*", "", clean_content, flags=re.MULTILINE)
        clean_content = re.sub(r"\s*```$", "", clean_content, flags=re.MULTILINE).strip()

    try:
        decision = json.loads(clean_content)
        target_task_id = decision.get("target_task_id")
        action_type = decision.get("action_type", "wait")
        description = decision.get("description", "")
        payload = decision.get("payload", {})
    except Exception:
        action_type = "wait"
        target_task_id = None
        description = "Failed to parse choice structural template response."
        payload = {}
```

Sends the question to the AI, gets back text. The "strip the ```json
fences" bit exists because LLMs frequently wrap their JSON answers in
Markdown code-block formatting (```` ```json ... ``` ````) even when told
not to — this just removes that wrapping so the text underneath can be
parsed as plain JSON. If parsing fails for any reason (the AI said
something malformed), it falls back safely to `action_type = "wait"` —
i.e. "do nothing this round" — rather than crashing the whole agent.

```python
    if action_type != "wait":
        action_req = ActionRequest(...)
        exec_res = await self.supervisor.request_execution(action_req)
```

If the AI decided on a real action (write a file, run a shell command,
etc.), it's packaged up as an `ActionRequest` and handed to the
`ExecutionSupervisor` — the gatekeeper who actually checks if this is
allowed and, if so, carries it out. **The agent itself never touches
files or runs commands directly** — it always has to ask the supervisor,
which is an important safety design (fully explained in file 03).

```python
        if action_type in {"write_file", "write_code"} and exec_res.get("executed"):
            from agentos.actors.reviewer import ReviewerAgentActor
            reviewer = ReviewerAgentActor.options(namespace="agentos").remote(settings_payload=self.settings.model_dump())
            review = await reviewer.review_code_patch.remote(payload.get("file_path", ""), payload.get("content", ""))

            if not review.get("approved", False):
                await self.checkpoints.create(Checkpoint(..., achievement="review_failed", ...))
                self.status = "IDLE"
                return {"status": "BLOCKED_BY_REVIEW"}
```

If the action was writing code, before the agent is allowed to consider
the task complete, it spins up a fresh `ReviewerAgentActor` (a brand new,
temporary actor — one is created for each review, then discarded) and asks
it to check the code. If the reviewer says "not approved," the agent
records a "review_failed" checkpoint (a note explaining what went wrong)
and stops here for this round — the task is **not** marked complete.

```python
        policy_decision = exec_res.get("guardrail", {}).get("decision", "ALLOW")
        await self.audit_repo.log_audit_event(
            project_id=self.project_id, agent_id=self.agent_id, action_type=action_type,
            policy_decision=policy_decision, integrity_hash=action_req.integrity_hash
        )

        if exec_res.get("executed") and target_task_id:
            if action_type == "write_file":
                await self.artifact_repo.create_artifact(...)
            await self.task_repo.update_task_status(target_task_id, "COMPLETED")
            logger.info("task_completed_by_agent", agent_id=self.agent_id, task_id=target_task_id)
```

Whatever the guardrail decided (allow/deny/etc.) gets permanently logged
for safety records. Then, if the action actually executed successfully and
we know which task it was for, the code marks that task `"COMPLETED"` in
the database, and — if it was writing a file — records that file as an
"artifact" (a produced deliverable).

> ⚠️ **A note for you, since you're learning to read code carefully:**
> This call passes a keyword argument named `policy_decision=`. When we
> looked at `watchdogs/runtime_watchdogs.py` earlier, the `SafetyWatchdog`
> queries a database column called `policy_decision`, but the actual SQL
> insert inside `AuditEventRepository.log_audit_event` (file 06) writes to
> a column named `decision`, not `policy_decision`. That mismatch is a
> separate, real bug worth being aware of — it means the safety watchdog's
> query will likely fail at runtime. This is a good example of the kind of
> subtle bug you'll get much better at spotting the more of this codebase
> you read: two files *look* like they agree on a name, but don't.

```python
    checkpoint = await self.checkpoints.create(
        Checkpoint(..., achievement="action_processed", summary=description)
    )
    self.status = "IDLE"
    return {"status": "SUCCESS", "checkpoint_id": checkpoint.checkpoint_id}
```

Whether or not there was a real action this round, the method always ends
by recording a checkpoint (a timestamped note: "here's what I did/decided
this round") and flips the status back to `"IDLE"` — ready for the next
mailbox item.

**One-sentence summary of the whole file:** every developer agent runs a
never-ending loop of "check my mailbox → ask the AI what to do about the
most important task → if it's code, get it reviewed → do it through the
safe execution gatekeeper → write down what happened → go back to
waiting."

---

## `agentos/actors/bootstrap.py` — the Project Manager

This actor only runs **once per project**, right at the very start. Its
only job: turn your plain-English request ("Build a simple blog platform
with posts and comments") into a structured plan.

```python
def __init__(self, project_id: str):
    self.project_id = project_id
    self.settings = load_settings()
    self.provider = ProviderGateway(self.settings)
```

Much simpler setup than the developer agent — it doesn't need a database
connection, a mailbox, or a reviewer. It just needs a way to talk to the AI
(`provider`).

```python
async def create_team_plan(self, user_request: str, max_agents_total: int) -> dict:
    roles_cfg = team_roles()
    role_bullets = "\n".join(f'- {r["role"]}: {r["description"]}' for r in roles_cfg["roles"])
    role_names = " | ".join(f'"{r["role"]}"' for r in roles_cfg["roles"])
```

First, it reads the list of possible agent roles (PM, backend developer,
frontend developer, etc.) from `agentos/config/actor_team.yml`, and turns
that list into two pieces of text for the prompt: a bulleted description
of each role, and a short list of valid role names.

The rest of the method builds a big prompt basically saying: *"You're the
project manager. Here's the user's request, here's the max number of
agents you're allowed to hire, here are the roles you can choose from —
give me back a JSON plan: a project name, a Definition of Done (a checklist
of what 'finished' means), a list of assumptions you're making, and the
team roster."* This is exactly the JSON you saw printed in your terminal
as the "MULTI-AGENT TEAM BLUEPRINT."

```python
    try:
        plan_data = json.loads(clean_content)
    except Exception as e:
        print(f"Failed to parse dynamically generated team plan, falling back to basic skeleton setup: {e}")
        plan_data = {
            "project_name": "emergency-fallback-project",
            "dod": ["Code base executes", "Verify output standards"],
            "assumptions": ["Fallback mode active"],
            "agents": [{"role": "PM_TECH_LEAD", "count": 1, "description": "Fallback coordinator"}]
        }
```

Same defensive pattern as before: if the AI's answer can't be parsed as
JSON, don't crash — fall back to a minimal, safe default plan (just one PM
agent) so the whole system doesn't grind to a halt over one bad AI
response.

```python
    validated_agents = []
    running_count = 0
    for a in plan_data.get("agents", []):
        role_str = a.get("role", "PM_TECH_LEAD")
        count = int(a.get("count", 1))
        if running_count + count > max_agents_total:
            count = max(1, max_agents_total - running_count)
            if running_count >= max_agents_total:
                continue
        running_count += count
        validated_agents.append(AgentSpec(role=AgentRole[role_str], count=count, ...))
```

This is a safety clamp, in plain English: *"Go through the AI's proposed
team one role at a time. Keep a running total of how many agents we've
committed to. If adding the next role would go over the allowed maximum,
shrink it down to whatever's left instead. If we've already hit the
maximum, skip that role entirely."* This is exactly why your log showed
only 2 agents (PM + backend developer) even though a "simple blog
platform" might normally warrant more roles — your `runtime_tuning.yaml`
currently caps `max_agents_total` at 2.

---

## `agentos/actors/reviewer.py` — the Code Inspector

The shortest and simplest of the three. It's spun up **fresh, on demand**,
every time a developer agent writes code — reviewed once, then discarded
(it's not a long-lived actor like the developers).

```python
@ray.remote
class ReviewerAgentActor:
    def __init__(self, settings_payload: dict):
        self.settings = Settings(**settings_payload)
        self.provider = ProviderGateway(self.settings)

    async def review_code_patch(self, file_path: str, code_content: str) -> dict:
        system_prompt = (
            "You are a Senior Security Reviewer Agent.\n"
            "Inspect the provided source code for vulnerabilities, structural syntax flaws, or malicious patterns.\n"
            "Respond with a single raw JSON object matching this schema shape:\n"
            "{ \"approved\": true|false, \"score\": 0-100, \"vulnerabilities_found\": [...] }"
        )
        ...
        response = await self.provider.get_completion(request, response_format={"type": "json_object"})
        try:
            result = json.loads(response.content)
            return result
        except Exception:
            return {"approved": False, "score": 0, "vulnerabilities_found": ["Failed to extract valid review schema."]}
```

In plain English: *"Here's a file path and the code that was just written.
Read it like a security reviewer would, and tell me: is it safe to
approve? Give it a score, and list any specific problems you saw."* Notice
the same defensive fallback pattern one more time — if the AI's response
can't be parsed, default to `"approved": False`. This is a deliberately
**safe failure direction**: when in doubt, reject the code rather than
silently accept it.

---

## What's next

Next file: **`03_Governance_And_Execution.md`** — the safety gatekeeper
system. This is where we explain exactly *how* an action gets checked
before it's allowed to run, what counts as "dangerous," and how the
sandbox actually writes files and runs shell commands safely.
