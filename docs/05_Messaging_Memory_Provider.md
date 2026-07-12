# 05 — Messages, Memory, and Talking to the AI

Files covered:
- `agentos/messaging/events.py` — the shape of a "note" passed between agents
- `agentos/messaging/dragonfly_bus.py` — the mail truck
- `agentos/memory/broker.py` — how an agent "catches up" before deciding
- `agentos/provider/gateway.py` — the only door to the AI model

---

## `agentos/messaging/events.py` — what a "note" looks like

```python
class EventType(StrEnum):
    PROJECT_CREATED = "PROJECT_CREATED"
    TEAM_PLAN_CREATED = "TEAM_PLAN_CREATED"
    AGENT_CREATED = "AGENT_CREATED"
    AGENT_TRIGGERED = "AGENT_TRIGGERED"
    ACTION_REQUESTED = "ACTION_REQUESTED"
    ACTION_ALLOWED = "ACTION_ALLOWED"
    ACTION_DENIED = "ACTION_DENIED"
    TASK_CREATED = "TASK_CREATED"
    TASK_CLAIMED = "TASK_CLAIMED"
    TASK_COMPLETED = "TASK_COMPLETED"
    CONTRACT_PUBLISHED = "CONTRACT_PUBLISHED"
    REVIEW_REQUESTED = "REVIEW_REQUESTED"
    TEST_RESULT = "TEST_RESULT"
    CHECKPOINT_CREATED = "CHECKPOINT_CREATED"
    SUMMARY_CREATED = "SUMMARY_CREATED"
    BLOCKER_CREATED = "BLOCKER_CREATED"
    DOD_EVALUATED = "DOD_EVALUATED"
    AGENT_QUARANTINED = "AGENT_QUARANTINED"
```
The complete, fixed vocabulary of "things that can happen" in AgentOS.
Worth knowing: as of the code we've walked through so far, only
`PROJECT_CREATED` is actually created and sent anywhere (in
`runtime/supervisor.py`, file 04). All the others are defined and ready
to use, but nothing currently creates a `TASK_COMPLETED` or
`ACTION_DENIED` event, for instance — those parts of the system aren't
wired up yet. Another sign of this being an early scaffold.

```python
class Event(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    project_id: str
    event_type: EventType
    producer_agent_id: str | None = None
    target_agent_id: str | None = None
    topic: str
    payload: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    causation_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
```
The actual "note" shape. `event_id` auto-generates a unique ID
(`uuid4()` — a random, essentially-never-repeats identifier) for every new
event. `target_agent_id` is what lets an event be addressed to one
specific agent instead of broadcast to everyone (used by the trigger
engine's routing, file 04). `payload` is a free-form dictionary for
whatever extra data this specific event needs to carry.
`correlation_id`/`causation_id` are designed for tracing "this event
happened *because of* that earlier event" — again, defined for future use,
not populated anywhere yet.

---

## `agentos/messaging/dragonfly_bus.py` — the mail truck

```python
class DragonflyBus:
    def __init__(self, url: str):
        self.redis = Redis.from_url(url, decode_responses=True)

    async def publish_event(self, stream: str, event: Event) -> str:
        return await self.redis.xadd(stream, {"event": event.model_dump_json()})

    async def read_latest(self, stream: str, count: int = 10) -> list[dict]:
        items = await self.redis.xrevrange(stream, count=count)
        return [{"id": item_id, "event": json.loads(fields["event"])} for item_id, fields in items]
```
A small, deliberately thin wrapper around the Redis/Dragonfly connection.
`publish_event` turns an `Event` object into JSON text and appends it to a
stream (`xadd` = "add to stream"). `read_latest` reads the most recent
entries back out (`xrevrange` = "read range, reverse order" — newest
first), turning the JSON text back into Python dictionaries. This class
doesn't decide *what* to send or *who* should receive it — that's the
`TriggerEngine`'s job (file 04). This class is purely "how do I physically
put a note on the truck / take a note off the truck."

---

## `agentos/memory/broker.py` — helping an agent "catch up"

Recall from file 02: every time an agent is about to decide what to do
next, it first asks the `MemoryBroker` for a "catch-up packet" — a
summary of relevant context, like reading recent messages before you
speak up in a meeting.

```python
@dataclass(frozen=True)
class CatchUpPacket:
    project_id: str
    agent_id: str
    trigger_event_id: str
    relevant_events: list[str] = field(default_factory=list)
    active_tasks: list[str] = field(default_factory=list)
    relevant_memories: list[str] = field(default_factory=list)
    recommended_next_actions: list[str] = field(default_factory=list)
```
A simple, immutable (§Glossary 4, `frozen=True`) data container — what
we're going to hand the agent: recent events, the current to-do list,
relevant long-term memories, and a couple of generic suggested next
actions.

### `build_catchup_packet()` — gathering the context, in three parts

**Part 1 — what just happened recently?**
```python
query_events = """
    SELECT event_type, topic, payload
    FROM events
    WHERE project_id = $1
    ORDER BY created_at DESC
    LIMIT 5;
"""
recent_events = []
trigger_message = ""
async with self.db.pool.acquire() as conn:
    rows = await conn.fetch(query_events, safe_project_id)
    for row in rows:
        recent_events.append(f"[{row['event_type']}] {row['topic']}: {row['payload']}")
        if not trigger_message:
            payload_dict = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
            trigger_message = payload_dict.get("message", "")
```
Asks Postgres for the 5 most recent events for this project (`$1` is a
placeholder Postgres fills in safely with `safe_project_id` — this
prevents a category of attack called **SQL injection**, where someone
sneaks SQL commands into a text value; always using placeholders like this
instead of gluing text together is the safe way to build queries). While
looping through, it also grabs the very first `"message"` field it finds
in any event's payload and remembers it as `trigger_message` — this later
becomes the search text used for the semantic memory lookup in Part 3.

**Part 2 — what's still left to do?**
```python
live_tasks = await self.task_repo.get_active_tasks(str(project_id))
formatted_tasks = []
for t in live_tasks:
    dep_list = t.get("dependencies", [])
    dep_str = ", ".join(dep_list) if dep_list else "NONE"
    formatted_tasks.append(
        f"- [Task ID: {t['id']}] {t['title']} (Status: {t['status']})\n"
        f"  Description: {t['description']}\n"
        f"  Blocked By Tasks: {dep_str}"
    )
```
Fetches the live task list and formats each one into a readable block of
text, including which other tasks it's blocked by — this becomes part of
what gets shown to the LLM inside `process_next_step` (file 02).

**Part 3 — is there anything relevant from long-term memory?**
```python
relevant_memories = []
if provider_gateway and trigger_message:
    query_vector = await provider_gateway.get_embedding(trigger_message)
    query_memories = """
        SELECT mi.title, mi.content, (me.embedding <=> $2::vector) as distance
        FROM memory_items mi
        JOIN memory_embeddings me ON me.memory_item_id = mi.id
        WHERE mi.project_id = $1 AND mi.scope = ANY($3)
        ORDER BY distance ASC
        LIMIT 3;
    """
    async with self.db.pool.acquire() as conn:
        mem_rows = await conn.fetch(query_memories, safe_project_id, query_vector, scopes)
        for m in mem_rows:
            relevant_memories.append(f"🔍 [Memory ({scopes})]: {m['title']} - {m['content']}")
```
This is the semantic ("meaning-based") memory search, and it's worth
slowing down on since it's a different idea from a normal database search.

- `provider_gateway.get_embedding(trigger_message)` sends the recent
  trigger text to the AI model and gets back an **embedding** — a long
  list of numbers (a "vector") that represents the *meaning* of that text
  in a mathematical space. Texts with similar meaning end up with
  mathematically similar vectors, even if they don't share any of the same
  words.
- `(me.embedding <=> $2::vector) as distance` — the `<=>` operator is
  provided by the **pgvector** Postgres extension (§Glossary 13): it
  computes "how mathematically different are these two vectors?" A small
  distance means "very similar meaning."
- `ORDER BY distance ASC LIMIT 3` — sort by "most similar meaning first,"
  and take the top 3. This is how the system finds memories that are
  *conceptually* related to what's currently happening, not just memories
  that happen to share the same exact words.
- `mi.scope = ANY($3)` restricts the search to only memories tagged with
  scopes this agent is allowed to see — a basic access-control boundary,
  so one agent's private notes aren't automatically visible to every other
  agent.

**Putting it together**
```python
return CatchUpPacket(
    project_id=str(project_id), agent_id=agent_id, trigger_event_id=trigger_event_id,
    relevant_events=recent_events, active_tasks=formatted_tasks, relevant_memories=relevant_memories,
    recommended_next_actions=[
        "Verify task hierarchy sequences before generating code requests.",
        "Ensure standard verification checks pass via sandboxed execution paths.",
    ],
)
```
All three parts get bundled into the `CatchUpPacket` and handed back to
whichever agent asked for it.

---

## `agentos/provider/gateway.py` — the only door to the AI model

Just like `execution/supervisor.py` is the *only* thing allowed to touch
files/shell commands, `ProviderGateway` is the *only* thing allowed to
actually call the AI model. Every agent goes through it.

### Setup

```python
class ProviderGateway:
    def __init__(self, settings: Settings, db_manager: DatabaseManager | None = None):
        self.settings = settings
        self.db_manager = db_manager
        self.call_repo = ProviderCallRepository(db_manager) if db_manager else None
        tuning = runtime_tuning()
        model_cfg = tuning.get("models", {})

        self.default_model = model_cfg.get("primary", "gemini/gemini-1.5-pro")
        self.fallback_model = model_cfg.get("fallback", "gemini/gemini-1.5-flash")
        self.embedding_model = model_cfg.get("embedding", "gemini/text-embedding-004")
```
Reads which model to use for normal requests (`default_model`), which
model to fall back to if that fails (`fallback_model`), and which model
to use specifically for embeddings, all from `runtime_tuning.yaml`
(file 01) rather than from `settings.py`, even though `settings.py` also
defines similar-looking fields — the YAML values are what's actually used.
`call_repo` will later log every single AI call made, for cost tracking.

### `_sanitize_prompt_input()` — a basic first line of defense

```python
def _sanitize_prompt_input(self, text: str) -> str:
    patterns = guardrail_policies()["prompt_sanitization_patterns"]
    sanitized = text
    for pattern in patterns:
        sanitized = re.sub(pattern, "[REDACTED_SECURITY_VIOLATION]", sanitized, flags=re.IGNORECASE)
    return sanitized
```
`re.sub(pattern, replacement, text)` searches `text` for anything matching
`pattern` (a "regular expression," a mini pattern-matching language for
text) and replaces every match with the replacement string.
`flags=re.IGNORECASE` makes the matching case-insensitive.

In plain English: before any text is sent to the AI, scan it for a list of
known suspicious phrases (things like attempts to reference `.env` files
or say "ignore previous instructions" — a technique called **prompt
injection**, where malicious text tries to trick an AI model into
ignoring its actual instructions) and blank them out. This is a simple,
useful first layer, but it's important to understand its limits: it only
catches phrases that were specifically anticipated and added to the list
in `guardrail_policies.yaml` — it's not a smart, general-purpose defense.

### `_check_budget_allowance()` — don't overspend

```python
async def _check_budget_allowance(self, project_id: str) -> bool:
    if not self.db_manager or not self.db_manager.pool:
        return True
    try:
        query = "SELECT COALESCE(SUM(cost_usd), 0.0) FROM provider_calls WHERE project_id = $1"
        total_spent = await self.db_manager.pool.fetchval(query, uuid.UUID(project_id))
        max_budget = getattr(self.settings, "daily_budget_usd", 10.0)
        return float(total_spent) < float(max_budget)
    except Exception:
        return True
```
Adds up every dollar spent so far on this project (`SUM(cost_usd)`;
`COALESCE(..., 0.0)` just means "if there are no rows yet, treat the sum
as 0 instead of nothing") and checks whether it's still under your
configured `daily_budget_usd`. Notice the `except Exception: return True`
at the bottom — this is a deliberate choice called **"fail open"**: if
checking the budget itself breaks somehow, allow the call to proceed
rather than blocking all AI usage over an unrelated bug. This is a
reasonable trade-off for a budget check (you'd rather occasionally overpay
than have the whole system freeze), but it's the opposite choice from,
say, the code reviewer in file 02, which deliberately "fails closed"
(defaults to rejecting code if something goes wrong) — worth noticing that
different situations call for different defaults.

### `get_completion()` — the main event: asking the AI something

```python
async def get_completion(self, request: ProviderRequest, **kwargs) -> ProviderResponse:
    if not await self._check_budget_allowance(request.budget_key):
        raise RuntimeError("API Request blocked: Project budget cap has been exceeded.")

    sanitized_messages = []
    for msg in request.messages:
        sanitized_messages.append({"role": msg["role"], "content": self._sanitize_prompt_input(msg["content"])})

    used_model = self.default_model
    try:
        response = await litellm.acompletion(model=self.default_model, messages=sanitized_messages, **kwargs)
    except Exception as e:
        print(f"Primary model ({self.default_model}) failed. Fallback triggered... Error: {e}")
        used_model = self.fallback_model
        response = await litellm.acompletion(model=used_model, messages=sanitized_messages, **kwargs)

    content = response.choices[0].message.content
```
Step by step: check the budget first (raise an error and stop entirely if
over budget). Sanitize every message. Try the primary model. If that call
fails for *any* reason (network issue, bad API key, model overloaded), the
`except` block automatically retries the exact same request against the
fallback model instead — this is the retry logic you saw referenced in
your logs as `"Fallback triggered."` `litellm` is a library that provides
one consistent interface (`.acompletion(...)`) for calling many different
AI providers (Gemini, OpenAI, Anthropic, etc.) without needing different
code for each one — you just change the `model` string.

```python
    try:
        cost = litellm.completion_cost(completion_response=response)
    except Exception:
        cost = 0.0

    if self.call_repo:
        try:
            await self.call_repo.log_call(project_id=request.budget_key, purpose=request.purpose,
                                           provider="litellm", model=used_model, cost_usd=float(cost or 0.0))
        except Exception as log_error:
            print(f"Failed to save provider call log: {log_error}")

    return ProviderResponse(content=content, model=used_model, provider="litellm", estimated_cost_usd=cost)
```
`litellm.completion_cost(...)` estimates how much this specific call cost
in real dollars, based on the model and how much text was involved. That
cost gets logged to the database (feeding back into the budget check
above, next time it runs), and the actual AI-generated text
(`response.choices[0].message.content` — this specific nested structure
is just how LiteLLM/OpenAI-style APIs shape their responses) gets wrapped
up and returned to whoever asked.

### `get_embedding()` — turning text into a "meaning vector"

```python
async def get_embedding(self, text: str) -> list[float]:
    try:
        response = await litellm.aembedding(model=self.embedding_model, input=[text])
        return response['data'][0]['embedding']
    except Exception as e:
        print(f"Failed to fetch embedding: {e}")
        return [0.0] * runtime_tuning()["embedding"]["dimension"]
```
This is what `memory/broker.py` calls to get the "meaning vector"
described earlier. If it fails for any reason, instead of crashing, it
returns a list of zeros of the right length (`embedding.dimension` from
`runtime_tuning.yaml`, currently `768`) — a harmless placeholder that lets
the rest of the code keep running (it just won't find any meaningfully
similar memories that round, since a zero-vector isn't close to anything
in particular).

---

## What's next

Next file: **`06_Storage_Checkpoints_DoD_Watchdogs.md`** — the database
layer itself (`storage/database.py`, `repositories.py`, `schema.sql`), how
"proof of progress" checkpoints are recorded, how the system decides the
project is actually *done*, and exactly what each of the four watchdogs
checks for.
