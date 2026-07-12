# 00 — Python Fundamentals Glossary (Read This First)

This file explains every Python concept, library, and pattern that shows up
repeatedly in the AgentOS codebase. Every other file in this series will
assume you've read this one. When you hit something unfamiliar in the code
walkthroughs, come back here.

---

## 1. Classes, `self`, and `__init__`

```python
class Settings:
    def __init__(self, name):
        self.name = name
```

- `class Settings:` defines a **blueprint** for objects. Nothing happens yet —
  you're just describing what a `Settings` object *looks like*.
- `def __init__(self, name):` is the **constructor** — the function that runs
  automatically the moment you create a new object with `Settings("foo")`.
  `__init__` is a "dunder" (double-underscore) method; Python calls it
  automatically, you never call it directly.
- `self` is the object being built. It's the first parameter of every method
  in a class, and Python passes it automatically. When you write
  `self.name = name`, you're saying "store `name` as a property on this
  specific object." Every other method in the class can then read `self.name`.
- Creating an object: `s = Settings("myproject")` → Python calls
  `__init__(self=s, name="myproject")` behind the scenes.

You'll see this pattern in almost every file in this repo — `Settings`,
`AgentWorkerActor`, `PolicyEngine`, `DatabaseManager`, etc. are all classes.

---

## 2. Type hints (`str`, `int`, `str | None`, `list[dict]`)

```python
def evaluate(self, action: ActionRequest) -> GuardrailResult:
```

- `action: ActionRequest` means "the `action` parameter is *expected* to be
  an `ActionRequest` object." Python does **not** enforce this at runtime by
  itself — it's documentation for humans and tools (like your editor's
  autocomplete), not a hard rule like in Java or C++.
- `-> GuardrailResult` means "this function is expected to return a
  `GuardrailResult` object."
- `str | None` means "either a string, or `None` (nothing)." This is a
  **union type** — the value could be one type or another.
- `list[dict]` means "a list where every item is a dictionary."
- `from __future__ import annotations` (seen at the top of almost every file)
  is a compatibility switch that lets you write modern type-hint syntax
  (like `str | None`) even on older Python versions.

---

## 3. `async def`, `await`, and `asyncio`

This is the single most important concept to understand in this codebase,
because **almost everything in AgentOS is asynchronous**.

Normal ("synchronous") Python runs one line at a time, and if a line is slow
(like "wait for the database to respond"), your whole program just sits there
frozen until it's done.

**Async** Python lets you say: "start this slow operation, and while you're
waiting for it, let other things happen." This matters enormously for
AgentOS because it's constantly waiting on network calls: talking to
Postgres, talking to Redis/Dragonfly, talking to the Gemini LLM API.

```python
async def connect(self):
    self.pool = await asyncpg.create_pool(self.url)
```

- `async def` marks a function as a **coroutine** — a special kind of
  function that can be paused and resumed. You cannot call it like a normal
  function; you have to `await` it (or run it via `asyncio.run(...)`).
- `await` means "pause here, let other code run, and resume me when this
  finishes." You can only use `await` inside an `async def` function.
- If you see `asyncio.run(some_coroutine())` — that's how you kick off async
  code from *regular*, synchronous code (like the very start of a CLI
  command). It creates an "event loop" (the engine that manages all the
  pausing/resuming) and runs until the coroutine finishes.
- `asyncio.create_task(some_coroutine())` starts a coroutine running **in the
  background**, without waiting for it to finish. This is how AgentOS starts
  its long-running background loops (like an agent's inbox listener) without
  blocking everything else.
- `asyncio.sleep(1.0)` pauses *this* coroutine for 1 second, but lets other
  coroutines keep running during that second — unlike `time.sleep(1.0)`,
  which would freeze the entire program.

**Analogy:** imagine a chef (your program) cooking multiple dishes at once.
Synchronous code = the chef stands and stares at the oven until the bread is
done before doing anything else. Async code = the chef puts the bread in,
sets a timer, and goes chops vegetables for another dish, coming back the
moment the timer rings.

---

## 4. Decorators (`@dataclass`, `@ray.remote`, `@app.command()`)

```python
@ray.remote
class AgentWorkerActor:
    ...
```

A decorator is a function that **wraps** another function or class to add
behavior, without you having to modify the original code. The `@` symbol
applies it.

- `@ray.remote` — turns a normal Python class into a **Ray actor**: instead
  of running in your current process, Ray will run instances of this class
  in their own separate processes (potentially on different machines),
  and lets you call their methods remotely. More on this in the Actors file.
- `@dataclass(frozen=True)` — auto-generates boilerplate for a simple class
  that just holds data (`__init__`, `__repr__`, equality checks). `frozen=True`
  means the object can't be modified after creation (immutable).
- `@app.command()` — from the `typer` library; marks a Python function as a
  CLI command, so running `agentos <function_name> ...` in the terminal
  calls it.

---

## 5. Pydantic: `BaseModel`, `BaseSettings`, `Field`

AgentOS uses **Pydantic** everywhere to define the "shape" of data — think of
it as a stricter, self-validating version of a class.

```python
from pydantic import BaseModel, Field

class ActionRequest(BaseModel):
    project_id: str
    risk_level: str = Field(default="LOW")
```

- Any class inheriting from `BaseModel` automatically gets: input
  validation (it'll raise an error if you pass a number where a string was
  expected), easy conversion to/from JSON (`.model_dump()`, `.model_dump_json()`,
  `.model_validate(...)`), and auto-generated `__init__`.
- `Field(default=...)` lets you set defaults and add metadata (like an
  "alias" — an alternate name used when reading from JSON or environment
  variables).
- `BaseSettings` (used in `config/settings.py`) is a special version of
  `BaseModel` designed specifically for **loading configuration from
  environment variables and `.env` files**. Each field's `alias="SOME_ENV_VAR"`
  tells Pydantic which environment variable to read.

---

## 6. Enums

```python
from enum import Enum

class RiskLevel(str, Enum):
    LOW = "LOW"
    HIGH = "HIGH"
```

An `Enum` (enumeration) is a fixed set of named constants. Instead of using
raw strings like `"HIGH"` scattered through the code (easy to typo), you use
`RiskLevel.HIGH`. Inheriting from `str` as well as `Enum` means it behaves
like a string too (so it's easy to store/serialize).

---

## 7. `import` statements and modules

```python
from agentos.governance.models import ActionRequest
```

This means: "go into the folder `agentos/governance/`, open the file
`models.py`, and bring the `ActionRequest` class into this file so I can use
it." Every folder in the repo with an `__init__.py` file (even an empty one)
is a **package** — a folder Python recognizes as importable.

`from __future__ import annotations` (mentioned above) is technically also
an import, just a special compatibility one.

---

## 8. F-strings

```python
f"Agent {agent_id} started with role {role}"
```

An f-string (`f"..."`) lets you embed variables directly inside a string
using `{curly_braces}`. It's just string formatting — very common for
building log messages and prompts throughout this codebase.

---

## 9. List/dict comprehensions

```python
[agent.id for agent in agents if agent.active]
```

This is a compact way to build a new list by looping over another list and
optionally filtering. It reads right-to-left in spirit: "for each `agent` in
`agents`, if `agent.active` is true, include `agent.id`." You'll see this a
lot instead of writing a full `for` loop with `.append()`.

---

## 10. Context managers (`async with`)

```python
async with self.pool.acquire() as conn:
    await conn.execute(...)
```

`with` (and `async with`) guarantees cleanup happens automatically — in this
case, "borrow a database connection from the pool, and no matter what
happens (even an error), return it to the pool when this block ends."
Without this, you'd have to manually remember to release the connection
every single time, including in error cases.

---

## 11. Exceptions (`try` / `except`)

```python
try:
    result = await risky_call()
except Exception as e:
    print(f"Failed: {e}")
```

Code inside `try:` runs normally. If it raises an error (an "exception"),
Python immediately jumps to the matching `except:` block instead of
crashing the whole program. `Exception` is the base class that catches
almost any error type.

---

## 12. What is Ray? (high-level, before the deep dive)

**Ray** is a Python framework for distributing work across multiple
processes or machines. The core building block AgentOS uses is the **actor**:
a class decorated with `@ray.remote`. When you do:

```python
actor = MyActor.remote(...)       # creates the actor (runs in its own process)
result = await actor.some_method.remote(...)   # calls a method on it remotely
```

Each actor is like its own tiny independent program with its own memory,
running in parallel with everything else, that you communicate with by
sending it method calls. This is exactly how AgentOS represents each AI
"agent" (PM, backend developer, etc.) — each one is a separate Ray actor
running independently, deciding on its own what to do next.

---

## 13. What is Postgres / pgvector?

**PostgreSQL** ("Postgres") is a relational (table-based) database — AgentOS
uses it as the permanent record of everything: projects, tasks, events,
checkpoints. **pgvector** is an extension that adds the ability to store and
search "embeddings" (long lists of numbers representing the *meaning* of a
piece of text) — this is what powers the semantic memory search described in
the Memory file.

## 14. What is Redis / Dragonfly?

**Redis** (and **Dragonfly**, a faster Redis-compatible alternative used
here) is an in-memory data store — much faster than Postgres, but not meant
for permanent storage. AgentOS uses it for short-lived, fast coordination:
each agent's "inbox" (a queue of pending events) and "wake up" notifications
live here.

---

You now have the vocabulary you need. Move on to the next files — each one
will reference back to sections here (e.g. "see §3 on async/await") instead
of re-explaining these basics every time.
