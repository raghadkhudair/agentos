# 04 — The Hiring Manager (Runtime Orchestration)

Files covered:
- `agentos/runtime/team_plan.py` — the shape of a "team plan"
- `agentos/runtime/trigger_engine.py` — the mail sorting office
- `agentos/runtime/supervisor.py` — the class that starts everything

---

## The big picture, no code yet

If file 02's agents are "employees" and file 03's supervisor is "building
security," then `RuntimeSupervisor` in this file is the **hiring manager**
who runs the whole opening-day sequence:

1. Turn on the building's power (start Ray).
2. Call in the Project Manager agent, get the team plan.
3. Make sure the plan doesn't ask for more staff than allowed.
4. Actually hire everyone (create the Ray actors).
5. Set up the internal mail system so employees can send each other notes
   (the "trigger engine").
6. Tell the Project Manager "go, you're on the clock."
7. Wait around until the health inspectors (watchdogs) say the whole
   project is finished.

This is the class the CLI (`agentos run "..."`) actually calls.

---

## `agentos/runtime/team_plan.py` — describing "a team" in code

This file, like `governance/models.py`, has no real logic — just shapes.

```python
class AgentRole(StrEnum):
    PM_TECH_LEAD = "pm_tech_lead"
    SOLUTION_ARCHITECT = "solution_architect"
    BACKEND_DEVELOPER = "backend_developer"
    FRONTEND_DEVELOPER = "frontend_developer"
    PLATFORM_ENGINEER = "platform_engineer"
    INFRA_ENGINEER = "infra_engineer"
    QA_ENGINEER = "qa_engineer"
    CODE_REVIEWER = "code_reviewer"
    SECURITY_REVIEWER = "security_reviewer"
```
The fixed, complete list of "job titles" the bootstrap PM agent is allowed
to choose from when building a team.

```python
class AgentSpec(BaseModel):
    role: AgentRole
    count: int = Field(ge=1)
    description: str
    memory_scopes: list[str] = Field(default_factory=list)
    allowed_action_categories: list[str] = Field(default_factory=list)
    ownership_domains: list[str] = Field(default_factory=list)
```
One "job posting": which role, how many people in that role
(`Field(ge=1)` means "must be greater than or equal to 1" — Pydantic will
reject `0` or negative numbers automatically), and a description of their
focus.

```python
class TeamPlan(BaseModel):
    project_name: str
    user_request: str
    dod: list[str]
    assumptions: list[str] = Field(default_factory=list)
    agents: list[AgentSpec]
    max_requested_agents: int

    @property
    def total_agents(self) -> int:
        return sum(agent.count for agent in self.agents)
```
The whole plan: a project name, the original request text, the DoD
checklist, assumptions, and the list of job postings.

`@property` is a decorator that lets you call `total_agents` like it's just
a normal field (`plan.total_agents`) even though it's actually calculated
on the fly by adding up every job posting's `count`. You never have to
remember to keep this number "in sync" — it's always freshly computed.

```python
class ValidatedTeamPlan(BaseModel):
    original: TeamPlan
    agents: list[AgentSpec]
    total_agents: int
    reduced: bool
    reduction_reason: str | None = None
```
The result *after* the safety clamp we saw briefly in file 02 (shrinking
the team if it asked for too many agents). It keeps both the AI's
`original` plan and the possibly-smaller `agents` list that's actually
going to be used, plus a flag (`reduced`) telling you whether a clamp
happened.

---

## `agentos/runtime/trigger_engine.py` — the mail sorting office

Recall from file 02: each agent has a personal mailbox and doorbell. But
*something* has to actually decide "this event should go to that agent's
mailbox." That's the `TriggerEngine`'s entire job — think of it as the
mail sorting office that reads the address on every envelope and delivers
it to the right desk.

```python
def __init__(self, bus: DragonflyBus):
    self.bus = bus
    self.subscriptions: Dict[str, Set[str]] = {}
    self.is_running = False
```
`self.subscriptions` is a dictionary that maps "type of event" → "set of
agent IDs who care about that type of event." For example:
`{"TASK_COMPLETED": {"backend_developer-1", "qa_engineer-1"}}`.

```python
def register_subscription(self, event_type: EventType, agent_id: str) -> None:
    if event_type not in self.subscriptions:
        self.subscriptions[event_type] = set()
    self.subscriptions[event_type].add(agent_id)
```
"Sign this agent up to be notified whenever this type of event happens."
If nobody's signed up for this event type yet, first create an empty set
for it, then add the agent to it.

### `start_routing_loop()` — the never-ending sorting job

```python
async def start_routing_loop(self, project_id: str) -> None:
    self.is_running = True
    stream_key = f"project:{project_id}:events"
    group_name = "trigger_engine_group"
    consumer_name = "main_engine_processor"

    try:
        await self.bus.redis.xgroup_create(stream_key, group_name, mkstream=True)
    except Exception:
        pass
```
This connects to a Dragonfly/Redis **stream** — think of a stream as a
running, ordered log of every event that's ever happened for this project
(like a group chat's full message history, that new readers can "catch up"
on). A **consumer group** (`xgroup_create`) is a Redis feature that lets
multiple readers cooperatively work through a stream without
double-processing the same message — here there's only one reader (the
trigger engine itself), but this pattern also makes it safe to restart the
trigger engine later without losing track of where it left off.

```python
    while self.is_running:
        try:
            response = await self.bus.redis.xreadgroup(
                groupname=group_name, consumername=consumer_name,
                streams={stream_key: ">"}, count=5, block=1000
            )
            if not response:
                await asyncio.sleep(0.1)
                continue

            for _, items in response:
                for message_id, fields in items:
                    raw_event = fields.get("event")
                    if not raw_event:
                        continue
                    event_dict = json.loads(raw_event)
                    event = Event(**event_dict)
                    await self._route_event(event)
                    await self.bus.redis.xack(stream_key, group_name, message_id)
        except asyncio.CancelledError:
            self.is_running = False
            break
        except Exception as e:
            print(f"Trigger Engine processing loop exception encountered: {e}")
            await asyncio.sleep(1.0)
```
In plain English: *"Forever: ask Redis for up to 5 new, not-yet-seen
events, waiting up to 1 second if there aren't any yet. For each event
that arrives, figure out where it should be delivered (`_route_event`),
then tell Redis 'I've handled this one' (`xack`, short for
'acknowledge')."* The acknowledgment step matters — if the trigger engine
crashed mid-way through handling an event *before* acknowledging it,
Redis would know that event was never actually finished, and a future
reader could pick it back up rather than silently losing it.

### `_route_event()` — the actual sorting decision

```python
async def _route_event(self, event: Event) -> None:
    subscribers = self.subscriptions.get(event.event_type, set())
    if event.target_agent_id:
        subscribers = subscribers.intersection({event.target_agent_id})

    for agent_id in subscribers:
        inbox_key = f"agent:{agent_id}:inbox"
        await self.bus.redis.rpush(inbox_key, event.model_dump_json())
        await self.bus.redis.publish(f"agent:{agent_id}:wakeup", "NEW_EVENT")
        print(f"📡 [TRIGGER ENGINE]: Dispatched event {event.event_type} down to agent inbox: {agent_id}")
```
1. Look up who's subscribed to this event's type.
2. If the event was addressed to one *specific* agent
   (`event.target_agent_id`), narrow the delivery list down to just that
   one agent using `.intersection(...)` — a set operation meaning "keep
   only what's in both sets."
3. For every agent on the final delivery list: drop the event into their
   inbox list (`rpush` = "push onto the right/end of the list") and ring
   their doorbell (`publish(..., "NEW_EVENT")`) so they wake up and check
   it right away instead of waiting for their next lazy poll.

---

## `agentos/runtime/supervisor.py` — the hiring manager, start to finish

### `__init__` — getting the office ready

```python
def __init__(self, settings: Settings):
    self.settings = settings
    self.db_manager = DatabaseManager(settings)
    self.dragonfly = DragonflyBus(settings.dragonfly_url)
    self.trigger_engine = TriggerEngine(self.dragonfly)
    self._project_complete = asyncio.Event()
    self._actor_handles: list = []
```
Sets up its own database connection manager, its own Dragonfly connection,
and its own `TriggerEngine` (the sorting office). `self._actor_handles`
is the fix we applied together in the previous session — a list that keeps
every hired agent's Ray handle alive for the whole run, so Ray doesn't
accidentally fire them the moment nobody's "holding" their reference.

`self._project_complete = asyncio.Event()` — an `asyncio.Event` is like a
simple traffic light with two states: red (not set) and green (set). Any
part of the code can `await event.wait()`, which pauses until someone else
calls `event.set()` to flip it green. This is exactly how the supervisor
knows when to stop waiting — more on this below.

### `connect_ray()` — turning on the power

```python
def connect_ray(self) -> None:
    if ray.is_initialized():
        return
    logger.info("initializing_standalone_local_ray_cluster")
    ray.init(
        ignore_reinit_error=True,
        num_cpus=tuning_cfg["ray"]["num_cpus"],
        namespace="agentos",
        include_dashboard=False,
        object_store_memory=tuning_cfg["ray"]["object_store_memory"],
        _system_config={"gcs_rpc_server_reconnect_timeout_s": tuning_cfg["ray"]["gcs_rpc_server_reconnect_timeout_s"]}
    )
```
If Ray's already running, do nothing. Otherwise, start a fresh local Ray
instance, using the CPU count and memory limits from
`runtime_tuning.yaml` (file 01) rather than hardcoded numbers — this is
the exact fix that resolved your earlier connection-timeout problem,
switching from "try to connect to a separate `ray-head` container" to
"just start Ray right here, inside this same container."

### `bootstrap_project()` — the whole opening sequence, step by step

This is the single longest, most important method in the entire codebase
— it's what actually runs when you type `agentos run "..."`. Let's take it
piece by piece.

**Step 1 — power on, connect to the database**
```python
self.connect_ray()
await self.db_manager.connect()
project_repo = ProjectRepository(self.db_manager)
event_repo = EventRepository(self.db_manager)
```

**Step 2 — call in the Project Manager, get the plan**
```python
bootstrap = BootstrapAgentActor.options(namespace="agentos").remote(project_id=self.settings.project_name)
raw_plan = await bootstrap.create_team_plan.remote(user_request, tuning_cfg["agent_limits"]["max_agents_total"])
plan = TeamPlan.model_validate(raw_plan)
validated = self.validate_team_plan(plan)
actual_project_name = plan.project_name
```
Creates the `BootstrapAgentActor` from file 02, asks it for a plan, then
turns the raw dictionary it returned into a real, validated `TeamPlan`
object (`.model_validate(...)` is Pydantic checking the shape matches),
and runs it through `validate_team_plan` (shown further below) to clamp
the team size if needed.

**Step 3 — print the blueprint you saw in your terminal**
```python
print("\n" + "="*60)
print(f" 🚀 MULTI-AGENT TEAM BLUEPRINT FOR: {actual_project_name.upper()} ")
...
```
Exactly the banner you saw in your run logs — just formatted printing, no
new logic.

**Step 4 — save the project to the database**
```python
db_project_id = await project_repo.create_project(name=actual_project_name, request=user_request, dod=validated.original.dod)
```
This is the moment the project gets a permanent database ID
(`db_project_id`) — everything else from here on (tasks, checkpoints,
events) is tagged with this ID.

**Step 5 — actually hire everyone**
```python
actors = await self.create_agent_actors(validated.agents, actual_project_name)
```
Explained in its own section below.

**Step 6 — sign everyone up for mail delivery**
```python
first_pm_identity = None
for spec in validated.agents:
    for index in range(1, spec.count + 1):
        agent_id = f"{spec.role.value}-{index}"
        if spec.role.value == "pm_tech_lead" and not first_pm_identity:
            first_pm_identity = agent_id
        for e_type in EventType:
            self.trigger_engine.register_subscription(e_type, agent_id)
```
Loops through every hired agent and subscribes them to **every** possible
event type (`for e_type in EventType:`) — a simple, broad approach: every
agent hears about everything happening in the project, rather than a
finer-grained "only tell backend developers about backend events" setup.
Along the way, it also remembers the agent ID of the very first PM
agent — needed for the next step.

**Step 7 — start the background daemons**
```python
asyncio.create_task(self.trigger_engine.start_routing_loop(db_project_id))
asyncio.create_task(self.watchdog_loop(db_project_id, validated.original.dod))
```
Starts two background loops (§Glossary 3) running forever alongside
everything else: the mail sorting office, and the health-check watchdogs
(fully explained in file 06).

**Step 8 — announce the project has started**
```python
init_event = Event(project_id=db_project_id, event_type=EventType.PROJECT_CREATED, topic=unified_stream_key,
                    payload={"user_request": user_request, "dod": validated.original.dod})
await event_repo.save_event(db_project_id, init_event)
await self.dragonfly.publish_event(unified_stream_key, init_event)
```
Creates a `PROJECT_CREATED` event, saves a permanent copy to Postgres, and
also publishes it to the Dragonfly stream — this is the very first event
the trigger engine will pick up and route to everyone's mailbox.

**Step 9 — personally kick off the PM agent**
```python
target_pm_name = first_pm_identity if first_pm_identity else "pm_tech_lead-1"
pm_actor = ray.get_actor(target_pm_name, namespace="agentos")
execution_trigger = await pm_actor.process_next_step.remote(str(init_event.event_id))
```
Rather than waiting for the mail system to eventually deliver the
`PROJECT_CREATED` event through the normal mailbox flow, the supervisor
directly and immediately calls the PM agent's `process_next_step` — a
manual "get moving right now" nudge to kick things off without delay. This
is the exact line that failed with `ValueError: Failed to look up actor`
in your earlier error — `ray.get_actor(...)` needs the named actor to
still exist at this point, which is why the actor-handle-GC bug we fixed
mattered so much.

**Step 10 — wait for the project to actually finish**
```python
logger.info("supervisor_blocking_main_thread_awaiting_agent_completion")
await self._project_complete.wait()

await self.db_manager.disconnect()
return {...}
```
Remember the traffic-light analogy from `__init__`. This line pauses here
— potentially for a long time — until something else calls
`self._project_complete.set()`. Looking ahead to `watchdog_loop` below,
that happens when the `DoDWatchdog` reports the project's Definition of
Done is fully satisfied. Only then does this method finally return, and
your terminal command finishes.

### `validate_team_plan()` — the safety clamp

```python
def validate_team_plan(self, plan: TeamPlan) -> ValidatedTeamPlan:
    max_allowed = tuning_cfg["agent_limits"]["max_agents_total"]
    if plan.total_agents <= max_allowed:
        return ValidatedTeamPlan(original=plan, agents=plan.agents, total_agents=plan.total_agents, reduced=False)

    reduced_agents: list[AgentSpec] = []
    remaining = max_allowed
    for spec in plan.agents:
        if remaining <= 0:
            break
        count = min(spec.count, remaining)
        reduced_agents.append(spec.model_copy(update={"count": count}))
        remaining -= count
    return ValidatedTeamPlan(original=plan, agents=reduced_agents, total_agents=sum(a.count for a in reduced_agents),
                              reduced=True, reduction_reason="Configured max_agents_total constraint enforced.")
```
If the AI's proposed team fits within the limit, use it as-is. Otherwise,
walk through the job postings one at a time, taking as many as still fit
(`min(spec.count, remaining)`), until the budget runs out — any roles that
don't fit at all get dropped entirely. `spec.model_copy(update={"count": count})`
makes a copy of a Pydantic object with one field changed, without touching
the original (Pydantic objects are usually treated as "don't mutate
directly, make a modified copy instead").

### `create_agent_actors()` — actually hiring people

```python
async def create_agent_actors(self, specs: Iterable[AgentSpec], project_name: str) -> list[dict]:
    from agentos.actors.base import AgentWorkerActor
    created: list[dict] = []
    settings_payload = self.settings.model_dump(by_alias=False)
    for spec in specs:
        for index in range(1, spec.count + 1):
            agent_id = f"{spec.role.value}-{index}"
            actor = AgentWorkerActor.options(name=agent_id, namespace="agentos",
                                              max_concurrency=tuning_cfg["agent_limits"]["max_parallel_code_tasks"]).remote(
                agent_id=agent_id, role=spec.role.value, project_id=project_name, settings=settings_payload,
            )
            started = await actor.start.remote()
            self._actor_handles.append(actor)
            created.append(started)
    return created
```
For every job posting (e.g. "backend_developer, count=1"), and for every
number up to that count, this builds a unique agent ID (like
`backend_developer-1`, and `backend_developer-2` if count were 2), creates
that agent as a real Ray actor, calls its `start()` method (file 02), and
— thanks to the fix — keeps a permanent reference to it.

### `watchdog_loop()` — the health inspectors, and how the whole thing ends

```python
async def watchdog_loop(self, project_id: str, dod: list[str]) -> None:
    from agentos.watchdogs.runtime_watchdogs import DoDWatchdog, StagnationWatchdog, SafetyWatchdog, DeadlockWatchdog
    dod_wd = DoDWatchdog(self.db_manager)
    stag_wd = StagnationWatchdog(self.db_manager)
    safety_wd = SafetyWatchdog(self.db_manager)
    deadlock_wd = DeadlockWatchdog(self.db_manager)

    while True:
        await asyncio.sleep(tuning_cfg["watchdog_loop"]["interval_seconds"])
        for wd, args in [(dod_wd, (project_id, dod)), (stag_wd, (project_id,)),
                        (safety_wd, (project_id,)), (deadlock_wd, (project_id,))]:
            try:
                result = await wd.inspect(*args)
                if wd.__class__.__name__ == "DoDWatchdog" and result.get("status") == "COMPLIANT":
                    self._project_complete.set()
                    return
            except Exception:
                pass
```
Every 30 seconds (by default), this loop runs all four watchdogs
(explained fully in file 06) one after another. The important line for
understanding *why your `run` command hangs* is:

```python
if wd.__class__.__name__ == "DoDWatchdog" and result.get("status") == "COMPLIANT":
    self._project_complete.set()
    return
```

This is the **only** place in the entire codebase that flips the traffic
light to green. In plain English: *"If the watchdog that just ran was
specifically the `DoDWatchdog`, and it reported the project's Definition
of Done is fully satisfied, flip the 'we're done' switch and stop this
loop entirely."* Until that exact condition is met, this loop just keeps
quietly running forever, and — as flagged before — the bare
`except Exception: pass` at the bottom means if any watchdog is throwing
an error every single cycle, you'll never see it printed anywhere; the
loop just keeps silently swallowing it and trying again 30 seconds later.

---

## What's next

Next file: **`05_Messaging_Memory_Provider.md`** — how events are shaped
and passed around (`messaging/events.py`, `dragonfly_bus.py`), how an
agent "remembers" recent history (`memory/broker.py`), and exactly how
AgentOS actually talks to the AI model, including its budget and
prompt-safety checks (`provider/gateway.py`).
