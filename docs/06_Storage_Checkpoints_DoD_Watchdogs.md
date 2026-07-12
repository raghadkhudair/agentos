# 06 — The Filing Cabinet & The Health Inspectors

Files covered:
- `agentos/storage/database.py` — connecting to Postgres
- `agentos/storage/schema.sql` — the shape of every table
- `agentos/storage/repositories.py` — the only place raw SQL lives
- `agentos/checkpoints/manager.py` — "proof of progress" notes
- `agentos/dod/evaluator.py` — deciding if the project is actually done
- `agentos/watchdogs/runtime_watchdogs.py` — the four health inspectors

---

## `agentos/storage/database.py` — the phone line to Postgres

```python
class DatabaseManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.pool = None

    async def connect(self) -> None:
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.settings.database_url)

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
```
A **connection pool** (`asyncpg.create_pool(...)`) is a small, reusable
collection of open connections to the database. Instead of opening a
brand new connection every single time some code needs to talk to
Postgres (slow, wasteful), it "borrows" one from the pool, uses it, and
gives it back — this is exactly what `async with self.db.pool.acquire() as conn:`
does everywhere else in the codebase (§Glossary 10, context managers).
`self.pool = None` at the start, and the `if not self.pool:` check, means
`connect()` is safe to call more than once — it only actually creates the
pool the first time.

```python
    async def initialize_schema(self) -> None:
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
        schema_path = Path(__file__).parent / "schema.sql"
        schema_sql = schema_path.read_text(encoding="utf-8")
        async with self.pool.acquire() as conn:
            await conn.execute(schema_sql)
```
Reads the entire `schema.sql` file as one big block of text and runs it
directly against the database (`conn.execute(...)`). Since every table
in that file is declared with `CREATE TABLE IF NOT EXISTS`, running this
repeatedly is safe — it won't wipe out existing data, it just makes sure
all the expected tables exist.

---

## `agentos/storage/schema.sql` — the blueprint of every table

You don't need to memorize SQL syntax to understand this file — think of
each `CREATE TABLE` block as defining a spreadsheet: the table name is the
spreadsheet's name, and each line inside is a column, with its data type
and any rules attached.

A few details worth calling out:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
```
Turns on two Postgres add-ons: `vector` (pgvector, for the semantic memory
search from file 05) and `uuid-ossp` (lets Postgres auto-generate random
unique IDs with `uuid_generate_v4()`, used as the default value for almost
every table's `id` column).

```sql
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL UNIQUE,
    ...
);
```
`PRIMARY KEY` means "this column uniquely identifies each row, and every
table must have exactly one." `UNIQUE` on `name` means two projects can't
share the same name — this is exactly what powers the
`ON CONFLICT (name) DO UPDATE ...` line you saw in
`ProjectRepository.create_project` (below): if you try to insert a project
with a name that already exists, Postgres updates the existing row instead
of creating a duplicate or erroring.

```sql
CREATE TABLE IF NOT EXISTS tasks (
    ...
    parent_task_id UUID REFERENCES tasks(id),
    ...
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id)
);
```
`REFERENCES tasks(id)` is a **foreign key** — it means "this column must
contain an ID that actually exists in the `tasks` table." This is how
Postgres enforces that you can't casually reference a task that doesn't
exist. `ON DELETE CASCADE` means "if the task this depends on gets
deleted, automatically delete this dependency row too" — cleanup that
happens automatically rather than leaving orphaned, meaningless data
behind. `task_dependencies` is its own small table purely for recording
"task A depends on task B" relationships — this is exactly the graph the
`DeadlockWatchdog` (below) walks through looking for cycles.

```sql
CREATE TABLE IF NOT EXISTS memory_embeddings (
    ...
    embedding vector(768),
    ...
);
CREATE INDEX IF NOT EXISTS memory_embeddings_vector_idx ON memory_embeddings USING hnsw (embedding vector_cosine_ops);
```
`vector(768)` is a pgvector column type — it stores exactly 768 numbers
per row (matching `embedding.dimension` in `runtime_tuning.yaml`, file
01). The `hnsw` index is a special kind of index (a fast lookup structure)
built specifically for doing "find the most similar vectors" searches
quickly, even across millions of rows — without it, the `<=>` similarity
search from file 05 would have to compare against every single row one by
one, which gets very slow as memory grows.

Everything else in the file follows the same pattern: a table per concept
(`agents`, `events`, `artifacts`, `checkpoints`, `provider_calls`,
`audit_events`, etc.), each with an `id`, timestamps, and columns matching
whatever that concept needs to record.

---

## `agentos/storage/repositories.py` — the only place raw SQL lives

This is a deliberate pattern: every table gets its own small "repository"
class, and **all the raw SQL text in the entire codebase lives in this one
file**. Nowhere else writes a `SELECT`/`INSERT` directly against the
database. This makes it much easier to review, audit, or later swap out
the database entirely, since there's exactly one place to look.

They're all similar in shape, so here's one representative example, and
then what's worth noticing about a few specific ones.

```python
class TaskRepository:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def create_task(self, project_id, title, description, owner_agent_id=None, parent_task_id=None, priority=3) -> str:
        query = """
            INSERT INTO tasks (project_id, title, description, owner_agent_id, parent_task_id, priority)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id;
        """
        p_uuid = UUID(parent_task_id) if parent_task_id else None
        async with self.db.pool.acquire() as conn:
            task_id = await conn.fetchval(query, UUID(project_id), title, description, owner_agent_id, p_uuid, priority)
            return str(task_id)
```
`$1, $2, $3...` are placeholders — Postgres fills these in safely with the
values you pass after the query string, in order. This is the same
SQL-injection protection mentioned in file 05: never build a query by
gluing strings together with Python's `f"..."` — always let the database
driver substitute values in safely like this. `RETURNING id` tells
Postgres "after you insert this row, give me back its newly generated
ID" — `conn.fetchval(...)` runs the query and grabs just that one value.

```python
    async def get_active_tasks(self, project_id: str) -> list[dict]:
        query = """
            SELECT
                t.id::text, t.title, t.description, t.status, t.owner_agent_id, t.priority, t.parent_task_id::text,
                COALESCE(
                    ARRAY_AGG(td.depends_on_task_id::text) FILTER (WHERE td.depends_on_task_id IS NOT NULL),
                    '{}'::text[]
                ) as dependencies
            FROM tasks t
            LEFT JOIN task_dependencies td ON t.id = td.task_id
            WHERE t.project_id = $1 AND t.status != 'COMPLETED'
            GROUP BY t.id
            ORDER BY t.priority DESC, t.created_at ASC;
        """
```
This is the most complex query in the file — worth translating into plain
English: *"Get every task for this project that isn't finished yet, along
with a list of every task ID it depends on (gathered from the
`task_dependencies` table), sorted by priority (highest first), then by
age (oldest first)."* `LEFT JOIN` means "combine rows from both tables,
keeping tasks even if they have zero dependencies." `ARRAY_AGG(...)
FILTER (...)` collects all the matching dependency IDs into a single list
per task, skipping any that are empty. This exact query result is what
gets fed into `process_next_step` in file 02, as `active_tasks`.

```python
class AuditEventRepository:
    async def log_audit_event(self, project_id, agent_id, action_type, policy_decision, integrity_hash) -> str:
        query = """
            INSERT INTO audit_events (project_id, agent_id, event_type, decision, details)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id;
        """
        async with self.db.pool.acquire() as conn:
            audit_id = await conn.fetchval(query, UUID(project_id), agent_id, action_type, policy_decision,
                                            json.dumps({"integrity_hash": integrity_hash}))
            return str(audit_id)
```
Notice the Python **parameter name** is `policy_decision`, but it gets
inserted into the SQL column literally named `decision`. That's just a
naming choice (the Python side calls it something more descriptive than
the column name) — not a bug. I want to correct something I flagged for
you earlier in this conversation: I'd previously suspected a mismatch
between this and `SafetyWatchdog`'s query, but on rereading the latest
version of `watchdogs/runtime_watchdogs.py`, its query correctly says
`WHERE ... decision IN ('DENY', 'QUARANTINE_AGENT')` — matching this
`decision` column exactly. That earlier bug I flagged has since been
fixed upstream (or I mis-traced it originally) — good practice for you to
always re-check assumptions against the actual current code, the same way
I'm doing here.

---

## `agentos/checkpoints/manager.py` — "proof of progress" notes

```python
class Checkpoint(BaseModel):
    checkpoint_id: str
    project_id: str
    agent_id: str
    achievement: str
    summary: str
    task_id: str | None = None
    artifacts: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class CheckpointManager:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.repo = CheckpointRepository(self.db)

    async def create(self, checkpoint: Checkpoint) -> Checkpoint:
        await self.repo.save_checkpoint(
            project_id=checkpoint.project_id, agent_id=checkpoint.agent_id,
            achievement=checkpoint.achievement, summary=checkpoint.summary, task_id=checkpoint.task_id
        )
        return checkpoint
```
A `Checkpoint` is a small, timestamped note: "this agent did/decided
this." `CheckpointManager` is a thin wrapper — it just takes a
`Checkpoint` object and saves it via the repository. Every single time an
agent finishes a round of `process_next_step` (file 02), one of these
gets created, whether or not a real action happened. Over time, this
builds up a full history of "what did every agent do, in order" — this
history is exactly what `StagnationWatchdog` and `DoDEvaluator` both read.

---

## `agentos/dod/evaluator.py` — deciding if the project is "done"

Recall the Definition of Done (DoD) is just a list of plain-English
strings, like `["calculator.py", "test_calculator.py", "verify code
output standard"]`. This class's job is to look at everything that's
happened so far and decide, for each item, whether there's enough
evidence it's actually satisfied.

```python
async def evaluate(self, project_id: str, dod: list[str]) -> DoDEvaluation:
    ...
    query_artifacts = "SELECT title, artifact_type, created_at::text FROM artifacts WHERE project_id = $1;"
    ...
    existing_artifacts = {art["title"].lower(): art for art in artifacts_found}
```
First, it fetches all recorded **artifacts** (files that were written,
recorded back in file 02's `process_next_step`) and builds a dictionary
keyed by lowercase title, for fast lookup.

```python
    query_tasks = "SELECT title, status, acceptance_criteria FROM tasks WHERE project_id = $1;"
    completed_criteria = set()
    for trow in task_rows:
        if trow["status"] == "COMPLETED":
            completed_criteria.add(trow["title"].lower())
            if trow["acceptance_criteria"]:
                criteria_data = json.loads(trow["acceptance_criteria"])
                if isinstance(criteria_data, list):
                    for item_str in criteria_data:
                        completed_criteria.add(str(item_str).lower())
```
Second, it gathers every *completed* task's title, plus any explicit
"acceptance criteria" text attached to that task, into one combined set
of lowercase strings.

```python
    for item in dod:
        item_lower = item.lower()
        evidence_list = []

        if item_lower in existing_artifacts:
            evidence_list.append(f"📦 [ARTIFACT VALIDATION] Found verified physical project asset record: '{art['title']}'.")

        if item_lower in completed_criteria or any(item_lower in comp or comp in item_lower for comp in completed_criteria):
            evidence_list.append("🎯 [ACCEPTANCE CRITERIA CHECK] Verified via formal task completion graph requirements rule mappings.")

        for cp in checkpoints_found:
            match_found = item_lower in cp["summary"].lower() or item_lower in cp["achievement"].lower()
            if not match_found and "verify" in item_lower and "output" in item_lower:
                if "shell_command" in cp["summary"].lower() or "python3" in cp["summary"].lower():
                    match_found = True
            if match_found:
                evidence_list.append(f"🏆 [{cp['created_at']}] {cp['achievement'].upper()}: {cp['summary']}")

        if evidence_list:
            status_entry = DoDItemStatus(item=item, status="SATISFIED", evidence=evidence_list)
        else:
            status_entry = DoDItemStatus(item=item, status="MISSING")
            gaps.append(item)
```
This is the heart of the file: for **each** item in your Definition of
Done, it checks three independent sources of evidence — a matching
artifact title, a matching completed task/acceptance criteria, or a
matching checkpoint summary — and if *any* of them found something, the
item is marked `SATISFIED`. If none did, it's added to the `gaps` list.

There's also a specific hand-written special case: if a DoD item's text
contains both the words "verify" and "output" (matching a DoD item
literally worded like `"verify code output standard"`), it'll also count
as satisfied if any checkpoint mentions running `shell_command` or
`python3` — the reasoning being "we can't easily tell from a checkpoint
summary that a *verification* specifically succeeded, so just check that
some kind of verification-flavored command was run at all." This is a
loose heuristic, not a rigorous check — worth knowing if your project's
DoD ever seems to pass more easily than you'd expect.

```python
    all_satisfied = len(gaps) == 0
    return DoDEvaluation(project_id=str(project_id), satisfied=all_satisfied, items=evaluated_items, gaps=gaps)
```
Finally: the whole project is considered `satisfied` only if the `gaps`
list ended up completely empty — every single DoD item found at least one
piece of evidence.

---

## `agentos/watchdogs/runtime_watchdogs.py` — the four health inspectors

Recall from file 04: every 30 seconds, `RuntimeSupervisor.watchdog_loop`
runs all four of these, one after another. Each one answers one specific
question about the project's health.

### `DoDWatchdog` — "are we stuck with nothing left to do, but not actually finished?"

```python
async def inspect(self, project_id: str, project_dod: list[str]) -> dict:
    query_tasks = "SELECT COUNT(*) FROM tasks WHERE project_id = $1 AND status != 'COMPLETED';"
    incomplete_task_count = await conn.fetchval(query_tasks, safe_project_id)
    dod_report = await self.evaluator.evaluate(project_id, project_dod)

    if incomplete_task_count == 0 and not dod_report.satisfied:
        return {"action_required": "TRIGGER_REPLANNING", "reason": "...", "gaps": dod_report.gaps}
    return {"action_required": "NONE", "status": "COMPLIANT"}
```
Counts unfinished tasks, and separately runs the full DoD evaluation. If
there are **zero tasks left in the to-do list, but the DoD still isn't
satisfied**, that's a red flag: the agents have run out of things to do,
yet the project genuinely isn't finished — a sign something needs
replanning (though as noted in file 04, nothing currently *acts* on this
`TRIGGER_REPLANNING` signal yet). This is also the **only** watchdog whose
result the supervisor actually checks (for the `"COMPLIANT"` status) to
decide the whole run is finished.

### `StagnationWatchdog` — "is an agent stuck repeating itself?"

```python
async def inspect(self, project_id: str) -> dict:
    query_checkpoints = """
        SELECT summary FROM checkpoints
        WHERE project_id = $1
        ORDER BY created_at DESC
        LIMIT {cfg['stagnation_watchdog']['checkpoint_history_lookback']};
    """
    ...
    counter = Counter(summaries)
    most_common_action, count = counter.most_common(1)[0]
    if count >= cfg['stagnation_watchdog']['repeated_action_threshold']:
        return {"action_required": "FREEZE_STREAM", "reason": f"Agent is stuck repeating...", "repeated_action": most_common_action}
```

The idea, in plain English: *"look at the last N checkpoint summaries; if
the exact same summary text shows up too many times in a row, the agent
is probably looping — doing the same thing over and over without making
progress."* `Counter` (from Python's standard `collections` module) is a
handy tool that counts how many times each item appears in a list;
`.most_common(1)` gives you the single most frequent item and its count.

> ⚠️ **A real bug worth learning from.** Look closely at the SQL string:
> ```python
> query_checkpoints = """
>     SELECT summary FROM checkpoints
>     WHERE project_id = $1
>     ORDER BY created_at DESC
>     LIMIT {cfg['stagnation_watchdog']['checkpoint_history_lookback']};
> """
> ```
> That `{cfg[...]}` looks like it should insert a number — but the string
> is **missing the `f` prefix** (compare to `f"..."` strings elsewhere in
> this codebase, §Glossary 8). Without the `f`, Python treats
> `{cfg['stagnation_watchdog']['checkpoint_history_lookback']}` as
> **literal text**, not something to fill in. So Postgres receives a query
> that literally ends with
> `LIMIT {cfg['stagnation_watchdog']['checkpoint_history_lookback']};` —
> which isn't valid SQL, and this query will fail with a syntax error
> every single time `StagnationWatchdog.inspect()` runs. Because
> `watchdog_loop` in file 04 wraps every watchdog call in a bare
> `except Exception: pass`, this failure is currently completely silent —
> stagnation detection has likely never actually worked, and there's no
> visible symptom telling you that. **The fix is a one-character change:**
> add `f` right before the opening `"""` on the `query_checkpoints ="""` line.
> This is a great, very common real-world bug to have found by reading
> code carefully — forgetting an `f` prefix on a string that needs
> variable interpolation is one of the most common small mistakes in
> Python, and it often doesn't announce itself loudly.

### `SafetyWatchdog` — "has this project racked up too many blocked/denied actions?"

```python
async def inspect(self, project_id: str) -> dict:
    query_audit = "SELECT COUNT(*) FROM audit_events WHERE project_id = $1 AND decision IN ('DENY', 'QUARANTINE_AGENT');"
    blocked_call_count = await conn.fetchval(query_audit, safe_project_id)
    if blocked_call_count >= cfg['safety_watchdog']['blocked_call_quarantine_threshold']:
        return {"action_required": "QUARANTINE_AGENT", "reason": f"...Found {blocked_call_count} violations."}
    return {"action_required": "NONE", "status": "SECURE"}
```
Straightforward: count how many audit log entries recorded a `DENY` or
`QUARANTINE_AGENT` decision (file 03) for this project. If that count
crosses a configured threshold, flag it — a project with lots of blocked
actions might indicate something's gone wrong (a misbehaving or confused
agent repeatedly trying disallowed things).

### `DeadlockWatchdog` — "are any tasks blocking each other in a circle?"

```python
graph = {}
for row in rows:
    graph.setdefault(row["task_id"], []).append(row["depends_on_task_id"])

visited = set()
path = set()

def has_cycle(node):
    if node in path: return True
    if node in visited: return False
    path.add(node)
    for neighbor in graph.get(node, []):
        if has_cycle(neighbor): return True
    path.remove(node)
    visited.add(node)
    return False

if any(has_cycle(task) for task in graph):
    return {"action_required": "RESOLVE_DEADLOCK", "reason": "Circular task dependencies detected..."}
```
This builds a "dependency graph" — a dictionary where each task points to
the list of tasks it depends on — then runs a classic algorithm called
**depth-first cycle detection**. Plain-English version:

> "Starting from a task, walk down its chain of dependencies. Keep track
> of which tasks are currently on your 'walking path.' If you ever land
> back on a task that's already on your current path, you've found a
> loop — Task A depends on B, which depends on C, which depends back on
> A, so none of them can ever be marked done first."

`has_cycle` is a **recursive function** — a function that calls itself,
each time on a "smaller" version of the problem (here, one step deeper
into the dependency chain), which is a common and useful way to walk
through tree/graph-shaped data like this. `visited` remembers nodes we've
already fully explored (and cleared) so we don't waste time re-checking
them; `path` tracks only the nodes currently "in progress" on this
specific walk.

**One-sentence summary of the whole file:** four independent inspectors —
"are we out of tasks but not done," "is someone looping," "are there too
many blocked actions," "are tasks deadlocked in a circle" — run on a
timer, and only the first one's "we're actually finished" signal is
currently wired up to anything (ending the whole `agentos run` command).

---

## What's next

Next file: **`07_Tests_And_Full_Walkthrough.md`** — the test file, and a
complete start-to-finish trace of exactly what happens, file by file, when
you run `agentos run "Build a simple blog platform..."` — tying together
everything from files 01 through 06 into one story.
