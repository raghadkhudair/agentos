# 03 — The Safety Gatekeeper (Governance & Execution)

Files covered:
- `agentos/governance/models.py` — the "shapes" of a request and a verdict
- `agentos/governance/policy_engine.py` — the rulebook that decides ALLOW/DENY
- `agentos/execution/supervisor.py` — the only door through which actions actually happen

---

## The big picture, no code yet

Remember from file 02: a developer agent never touches a real file or runs
a real command by itself. Instead, it fills out a **request form**
("I want to write this file with this content") and hands it to a
gatekeeper.

Think of it like an office building with a security desk at the only door:

1. **The form** (`ActionRequest`) — what the agent wants to do, written down
   in a fixed format.
2. **The rulebook** (`PolicyEngine`) — a security guard who reads the form
   and stamps it ALLOW, DENY, or "needs a manager's approval," based on a
   fixed set of rules. The guard never actually *does* anything, they only
   make a decision.
3. **The door itself** (`ExecutionSupervisor`) — checks the form with the
   guard first, and only if it's stamped ALLOW does it actually walk
   through and do the work (write the file / run the command), inside a
   locked sandbox room so nothing outside that room can be touched.

---

## `agentos/governance/models.py` — the paperwork

This file has no real logic in it — it only defines the **shapes** of data
(§Glossary 5, Pydantic `BaseModel`) used everywhere else in this system.

### The two checklists (`Enum`s)

```python
class RiskLevel(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"
```

Just four fixed labels for "how risky is this?" Using an `Enum` here
(§Glossary 6) instead of loose strings means nobody can accidentally type
`"Hihg"` somewhere and have it silently do nothing — Python will error
immediately if you use a value that isn't one of these four.

```python
class PolicyDecision(StrEnum):
    ALLOW = "ALLOW"
    DENY = "DENY"
    ALLOW_WITH_CONSTRAINTS = "ALLOW_WITH_CONSTRAINTS"
    REQUIRE_REVIEW = "REQUIRE_REVIEW"
    REQUIRE_HUMAN_APPROVAL = "REQUIRE_HUMAN_APPROVAL"
    REQUIRE_SANDBOX_ONLY = "REQUIRE_SANDBOX_ONLY"
    REQUIRE_BACKUP_FIRST = "REQUIRE_BACKUP_FIRST"
    REQUIRE_SECURITY_REVIEW = "REQUIRE_SECURITY_REVIEW"
    QUARANTINE_AGENT = "QUARANTINE_AGENT"
```

The full list of possible verdicts the guard can stamp on a form. Not
every one of these is actually used yet in `policy_engine.py` (some are
defined for future use), but `ALLOW`, `DENY`, `ALLOW_WITH_CONSTRAINTS`,
`REQUIRE_REVIEW`, `REQUIRE_HUMAN_APPROVAL`, and `QUARANTINE_AGENT` are.

### `AgentIdentity` — an ID badge

```python
class AgentIdentity(BaseModel):
    agent_id: str
    role: str
    project_id: str
    squad: str | None = None
    memory_scopes: list[str] = Field(default_factory=list)
    allowed_actions: list[str] = Field(default_factory=list)
    allowed_paths: list[str] = Field(default_factory=list)
```

A description of "who is this agent." In the current codebase this shape
is defined but not actually built and passed around anywhere yet — a sign
this project is still a scaffold, with some pieces designed ahead of when
they're wired in. Good to notice, not something you did wrong.

### `ActionRequest` — the request form itself

```python
class ActionRequest(BaseModel):
    project_id: str
    agent_id: str
    action_type: str
    description: str
    target_paths: list[str] = Field(default_factory=list)
    command: str | None = None
    database_operation: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)

    nonce: str = Field(default_factory=lambda: hashlib.sha256(str(json.dumps({})).encode()).hexdigest()[:16])
    integrity_hash: str | None = None
```

This is the actual form every action fills out: which project, which
agent, what type of action (`write_file`, `shell_command`, etc.), a
human-readable description, and a `payload` dictionary carrying the actual
details (file content, the shell command text, etc).

`nonce` is a random-ish short string generated automatically. `integrity_hash`
starts empty (`None`) — it gets filled in automatically right after, by the
next block:

```python
    def model_post_init(self, __context: Any) -> None:
        """Automatically hashes the fields to lock down immutable audit traces."""
        if not self.integrity_hash:
            raw_payload_bytes = f"{self.project_id}:{self.agent_id}:{self.action_type}:{self.description}:{self.nonce}"
            object.__setattr__(self, "integrity_hash", hashlib.sha256(raw_payload_bytes.encode()).hexdigest())
```

`model_post_init` is a special Pydantic hook that automatically runs right
after a new `ActionRequest` object is created — you never call it
yourself. In plain English: *"Take the important identifying fields of
this request, glue them together into one string, and run them through
SHA-256 (a one-way scrambling function) to produce a unique fingerprint."*

Why bother? This `integrity_hash` gets saved permanently in the audit log
(file 06). If someone later tried to tamper with the record of what an
agent did, the fingerprint wouldn't match anymore — it's a simple
tamper-evidence mechanism, common in audit-log design. `object.__setattr__`
is used here instead of the normal `self.integrity_hash = ...` because
Pydantic models can restrict normal attribute assignment in some
configurations; this is a way to set the field that always works.

### `GuardrailResult` — the guard's verdict slip

```python
class GuardrailResult(BaseModel):
    decision: PolicyDecision
    risk_level: RiskLevel
    reasons: list[str] = Field(default_factory=list)
    constraints: list[str] = Field(default_factory=list)
```

Whatever the `PolicyEngine` decides gets packaged into exactly this shape:
the decision itself, how risky it judged the action, a list of reasons
(plain-English explanations), and a list of constraints (extra rules the
action must follow if it's allowed with conditions).

---

## `agentos/governance/policy_engine.py` — the security guard's rulebook

```python
def __init__(self, settings: Settings):
    self.settings = settings
    self._quarantined_agents: set[str] = set()
    cfg = guardrail_policies()
    self.DESTRUCTIVE_PATTERNS = tuple(cfg["destructive_patterns"])
    self._review_types = set(cfg["require_review_action_types"])
    self._low_risk_types = set(cfg["low_risk_action_types"])
    self._medium_risk_types = set(cfg["medium_risk_shell_action_types"])
    self._quarantine_threshold = cfg["safety_watchdog"]["blocked_call_quarantine_threshold"]
```

- `self._quarantined_agents: set[str] = set()` — a **set** is like a list,
  but it can't contain duplicates and checking "is X in here?" is very
  fast. This starts empty — it's the guard's memory of "agents I've
  already caught doing something bad."
- The rest reads several lists out of `guardrail_policies.yaml` (loaded via
  `config/loader.py`, file 01): a list of text patterns that count as
  "destructive" (like `"drop database"` or `"rm -rf"`), which action types
  always need review, which are considered low risk, and which are medium
  risk. Keeping these as an external YAML file (rather than hardcoded in
  Python) means you can tune what's considered dangerous without touching
  code.

### Quarantine — the "banned list"

```python
def quarantine_agent(self, agent_id: str) -> None:
    self._quarantined_agents.add(agent_id)
    print(f"🚨 [POLICY SECURITY BLACKLIST]: Agent '{agent_id}' has been moved to QUARANTINE.")

def lift_quarantine(self, agent_id: str) -> None:
    self._quarantined_agents.discard(agent_id)
```

Two simple methods: add an agent's ID to the banned set, or remove it.
`.add()` and `.discard()` are just the standard Python `set` methods for
"put this in" / "take this out, and don't complain if it wasn't there."

### `evaluate_action()` — the actual decision-making, step by step

This method runs through a fixed checklist, top to bottom, and returns as
soon as it finds a matching rule (this pattern is often called "the first
matching rule wins").

**Step 1 — are you already banned?**

```python
if request.agent_id in self._quarantined_agents:
    return GuardrailResult(
        decision=PolicyDecision.QUARANTINE_AGENT,
        risk_level=RiskLevel.CRITICAL,
        reasons=[f"Execution denied: Agent '{request.agent_id}' is marked as QUARANTINED..."],
        constraints=["Revoke all filesystem access.", "Block outbound provider gateway calls immediately."]
    )
```
If this agent is already on the banned list, the answer is an automatic,
immediate "no" — no further checks needed.

**Step 2 — does the request contain a dangerous phrase?**

```python
text = " ".join(
    part for part in [request.description, request.command, request.database_operation] if part
).lower()

matched = [pattern for pattern in self.DESTRUCTIVE_PATTERNS if pattern in text]
if matched:
    if not self.settings.allow_destructive_actions:
        self.quarantine_agent(request.agent_id)
        return GuardrailResult(decision=PolicyDecision.DENY, risk_level=RiskLevel.CRITICAL, ...)
    return GuardrailResult(decision=PolicyDecision.REQUIRE_HUMAN_APPROVAL, risk_level=RiskLevel.CRITICAL, ...)
```

In plain English:

1. Glue together the description, the shell command (if any), and the
   database operation text (if any) into one lowercase blob of text.
2. Check whether any of the known "destructive patterns" (like
   `"drop database"`, `"rm -rf"`) appear anywhere inside that blob. This is
   a simple **substring search** — it's not smart about intent, it's just
   literally checking "does this text contain this string?"
3. If something dangerous was found: if your settings say destructive
   actions aren't allowed at all (the default), the agent gets **denied
   *and* immediately quarantined** — one strike, banned. If your settings
   explicitly allow destructive actions, it's downgraded to "needs a human
   to approve this first," rather than an outright ban.

This is exactly the check your earlier `agentos guardrail-check` CLI
command runs directly, if you want to experiment with it (see file 01).

**Step 3 — does this action type always need review?**

```python
if request.action_type in self._review_types:
    return GuardrailResult(decision=PolicyDecision.REQUIRE_REVIEW, risk_level=RiskLevel.HIGH, ...)
```

Some action types (configured in the YAML — things like modifying
authentication code or running database migrations) always get flagged
for review, regardless of what the text says.

**Step 4 — is it in the safe list?**

```python
if request.action_type in self._low_risk_types:
    return GuardrailResult(decision=PolicyDecision.ALLOW, risk_level=RiskLevel.LOW)
```
Things like `read_file`, `write_file`, `run_tests` — routine, expected
developer actions — sail straight through with a plain ALLOW.

**Step 5 — is it a shell command?**

```python
if request.action_type in self._medium_risk_types:
    return GuardrailResult(
        decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        risk_level=RiskLevel.MEDIUM,
        constraints=["Execute only inside assigned workspace sandbox environment. Commands must be non-interactive."],
    )
```
Shell commands are allowed, but with a condition attached: they must stay
inside the sandbox and must not require live human input.

**Step 6 — anything else (the fallback)**

```python
return GuardrailResult(
    decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
    risk_level=RiskLevel.MEDIUM,
    constraints=["Execute only inside assigned workspace and allowed paths."],
)
```
If nothing above matched, the default is a cautious "allow, but only
inside the sandbox" — not an outright block, but not a free pass either.

---

## `agentos/execution/supervisor.py` — the locked door

This is the class that actually *carries out* an approved action. Nothing
in the entire codebase touches the filesystem or runs a shell command
except this one file — that's a deliberate design choice, so all
dangerous operations funnel through a single, auditable chokepoint.

### Setting up the sandbox room

```python
def __init__(self, settings: Settings):
    from agentos.governance.policy_engine import PolicyEngine
    self.settings = settings
    self.policy_engine = PolicyEngine(settings)
    self._sandbox_cfg = guardrail_policies()["execution_sandbox"]
    self.workspace_path = os.path.abspath(settings.workspace)

    if not os.path.exists(self.workspace_path):
        try:
            os.makedirs(self.workspace_path, exist_ok=True)
        except Exception:
            pass

    self._initialize_git_workspace_safely()
```

- Creates its own `PolicyEngine` instance (the guard) — so every
  supervisor always checks with a guard before doing anything.
- `self.workspace_path` — the **one folder** all file writes/reads/commands
  are allowed to happen inside. Everything outside this folder is
  completely off-limits, enforced a bit further down.
- Makes sure that folder exists on disk, then calls a method to make sure
  it's also a **git repository** (explained next).

```python
def _initialize_git_workspace_safely(self) -> None:
    git_dir = os.path.join(self.workspace_path, ".git")
    if not os.path.exists(git_dir):
        os.system(f"git init {shlex.quote(self.workspace_path)} > /dev/null 2>&1")
        os.system(f"git -C {shlex.quote(self.workspace_path)} checkout -b {self._sandbox_cfg['default_branch']} > /dev/null 2>&1")
        ...
```

In plain English: "if the workspace folder isn't already a git repository,
turn it into one." Why? Because every change an agent makes gets
automatically committed to git (you'll see this below) — this gives you a
full history of exactly what every agent changed and when, which you can
inspect afterward with normal `git log`, just like a human developer's
work. `shlex.quote(...)` wraps a string safely for use inside a shell
command, so that if the workspace path happened to contain spaces or
special characters, it wouldn't accidentally break the command or (worse)
let something malicious sneak in.

### `request_execution()` — check with the guard, then act

```python
async def request_execution(self, action: ActionRequest) -> dict:
    result: GuardrailResult = self.policy_engine.evaluate_action(action)

    logger.info("policy_guardrail_evaluated", agent_id=action.agent_id, action_type=action.action_type,
                decision=result.decision, risk_level=result.risk_level)

    if result.decision in {PolicyDecision.DENY, PolicyDecision.QUARANTINE_AGENT}:
        return {"executed": False, "guardrail": result.model_dump(), "error": "Action blocked by policy."}

    if result.decision in {PolicyDecision.REQUIRE_HUMAN_APPROVAL, PolicyDecision.REQUIRE_REVIEW, PolicyDecision.REQUIRE_SECURITY_REVIEW}:
        return {"executed": False, "guardrail": result.model_dump(), "pending_approval": True}

    execution_result = await self._route_and_execute(action)
    return {"executed": True, "guardrail": result.model_dump(), "result": execution_result}
```

Every single call to this method follows the same three-way fork:

1. Ask the guard for a verdict.
2. If it's `DENY` or `QUARANTINE_AGENT` → stop here, return "not executed."
3. If it needs a human or review → also stop here, return "not executed,
   pending approval" (note: nothing in the current codebase actually
   *notifies* a human or routes this anywhere yet — it just returns this
   status and the calling agent treats it as "didn't happen this round").
4. Otherwise (ALLOW or ALLOW_WITH_CONSTRAINTS) → actually do the work via
   `_route_and_execute`, then return the real result.

### `_route_and_execute()` — picking the right tool for the job

```python
async def _route_and_execute(self, action: ActionRequest) -> dict:
    action_type = action.action_type
    payload = action.payload or {}
    task_branch_id = f"task-branch-{action.agent_id}"

    if action_type in {"write_file", "write_code"}:
        await self._ensure_branch_context(task_branch_id)
        res = self._write_file_safely(actual_payload)
        if "success" in res:
            await self._commit_branch_changes(task_branch_id, f"Agent {action.agent_id} modification commit.")
        return res
    elif action_type == "read_file":
        return self._read_file_safely(actual_payload)
    elif action_type in {"shell_command", "run_command"}:
        return await self._execute_shell_safely(actual_payload)
    else:
        return {"output": f"Action type '{action_type}' processed successfully, no internal file driver assigned."}
```

Think of this as a receptionist routing your request to the right desk:
writing → the file-writer, reading → the file-reader, running a command →
the shell-runner. Notice each agent gets its **own git branch**, named
`task-branch-<agent_id>` — so if two agents are writing files at the same
time, their changes stay isolated from each other on separate branches
rather than colliding.

### `_write_file_safely()` — the part that stops an agent escaping the sandbox

```python
def _write_file_safely(self, payload: dict) -> dict:
    file_path = payload.get("file_path")
    content = payload.get("content", "")
    if not file_path:
        return {"error": "Missing 'file_path' in payload."}

    full_path = os.path.abspath(os.path.join(self.workspace_path, file_path))
    if not full_path.startswith(self.workspace_path):
        logger.error("path_traversal_attack_blocked", attempted_path=file_path)
        return {"error": "Path traversal detected. Write blocked for safety."}
    ...
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)
    return {"success": True, "path": file_path, "bytes_written": len(content)}
```

This is worth slowing down on, because it's a real, important security
pattern. The agent only ever provides a *relative* file path, like
`"src/app.py"`. The code combines that with the sandbox folder to build a
full path, then — critically — checks that the **resolved** full path
still starts with the sandbox folder's path.

Why does this matter? Because a sneaky (or just confused) agent could ask
to write to a path like `"../../etc/passwd"` — using `..` to "climb out"
of the intended folder. `os.path.abspath` resolves all those `..` parts
down to the real, final location the file would actually land at. If that
final location isn't still inside the sandbox folder, the write is
refused. This exact category of bug is called **path traversal**, and it's
one of the most common real-world security vulnerabilities in file-handling
code — good to remember the name.

`_read_file_safely()` right below it does the exact same check for reads.

### `_execute_shell_safely()` — running a command, with a leash

```python
async def _execute_shell_safely(self, payload: dict) -> dict:
    command_str = payload.get("command")
    if not command_str:
        return {"error": "Missing 'command' string key inside execution payload."}
    try:
        process = await asyncio.create_subprocess_shell(
            command_str, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=self.workspace_path
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), self._sandbox_cfg["shell_command_timeout_seconds"])
        return {"success": True, "exit_code": process.returncode, "stdout": stdout_bytes.decode(...), "stderr": stderr_bytes.decode(...)}
    except asyncio.TimeoutError:
        return {"error": f"Execution timed out after {self._sandbox_cfg['shell_command_timeout_seconds']} seconds."}
    except Exception as e:
        return {"error": f"Failed to execute process shell environment: {str(e)}"}
```

- `asyncio.create_subprocess_shell(...)` starts a real shell process
  running the agent's command — but `cwd=self.workspace_path` pins where
  it runs *from*, so relative commands operate inside the sandbox folder
  (though note: this alone doesn't prevent a command like `cat /etc/passwd`
  using an *absolute* path — the sandboxing here is about the working
  directory, not a full filesystem jail).
- `asyncio.wait_for(..., timeout)` — this is important: it means **no
  shell command can hang forever**. If it doesn't finish within the
  configured timeout (30 seconds by default), Python raises a
  `TimeoutError`, which is caught and turned into a friendly error message
  instead of freezing the whole agent.
- Whatever the command printed (`stdout`) and any errors it printed
  (`stderr`) both get captured and returned, along with the exit code
  (0 usually means success, anything else usually means failure) —
  exactly what you'd see if you'd typed the command yourself in a
  terminal.

**One-sentence summary of this whole file:** the supervisor is the only
thing in AgentOS allowed to touch the real filesystem or run real shell
commands, and before it does anything, it always checks with the guard
first, always stays inside the sandbox folder, and always puts a time
limit on shell commands so nothing can hang forever.

---

## What's next

Next file: **`04_Runtime_Orchestration.md`** — the "hiring manager" class,
`RuntimeSupervisor`, that starts Ray, calls the bootstrap agent, spawns all
the developer agents, and wires up the event-routing system that lets
agents receive work.
