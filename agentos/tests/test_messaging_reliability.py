from __future__ import annotations

import asyncio
from collections import defaultdict
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from agentos.actors.base import AgentWorkerActor
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType
from agentos.runtime.trigger_engine import TriggerEngineActor


def _actor_implementation(actor: Any) -> type[Any]:
    return actor.__ray_metadata__.modified_class


class FakeRedis:
    def __init__(self) -> None:
        self.added: list[tuple[str, dict[str, str]]] = []
        self.published: list[tuple[str, str]] = []
        self.acked: list[tuple[str, str, str]] = []
        self.autoclaim_response: list[Any] = ["0-0", [], []]
        self.autoclaim_calls: list[tuple[Any, ...]] = []

    async def xadd(self, key: str, fields: dict[str, str]) -> str:
        self.added.append((key, fields))
        return f"{len(self.added)}-0"

    async def publish(self, key: str, value: str) -> int:
        self.published.append((key, value))
        return 1

    async def xack(self, key: str, group: str, message_id: str) -> int:
        self.acked.append((key, group, message_id))
        return 1

    async def xautoclaim(self, *args: Any, **kwargs: Any) -> list[Any]:
        self.autoclaim_calls.append((*args, kwargs))
        return self.autoclaim_response


class FakeDragonflyClient:
    def __init__(self) -> None:
        self.redis = FakeRedis()
        self.groups: list[tuple[str, str]] = []

    @staticmethod
    def key(*parts: object) -> str:
        return ":".join(["agentos", *(str(part) for part in parts)])

    async def ensure_consumer_group(self, stream: str, group: str) -> None:
        self.groups.append((stream, group))


def _fake_bus() -> DragonflyBus:
    bus = object.__new__(DragonflyBus)
    bus.client = FakeDragonflyClient()
    bus.redis = bus.client.redis
    return bus


@pytest.mark.asyncio
async def test_per_agent_inbox_is_a_project_scoped_consumer_group_stream() -> None:
    bus = _fake_bus()
    project_id = str(uuid4())
    event = Event(
        project_id=project_id,
        event_type=EventType.TASK_CREATED,
        producer_agent_id="runtime_supervisor",
        payload={"task_id": str(uuid4())},
    )

    group = await bus.ensure_inbox_group(project_id, "backend-1")
    message_id = await bus.send_to_inbox(project_id, "backend-1", event)

    expected_key = f"agentos:project:{project_id}:agent:backend-1:inbox"
    assert group == f"agent-inbox-{project_id}-backend-1"
    assert bus.client.groups == [(expected_key, group)]
    assert bus.redis.added[0][0] == expected_key
    assert Event.model_validate_json(bus.redis.added[0][1]["event"]).event_id == event.event_id
    assert message_id == "1-0"


@pytest.mark.asyncio
async def test_stale_pending_inbox_entries_are_reclaimed_with_xautoclaim() -> None:
    bus = _fake_bus()
    project_id = str(uuid4())
    bus.redis.autoclaim_response = [
        "0-0",
        [("9-0", {"event": "payload"})],
        [],
    ]

    messages = await bus.reclaim_inbox(
        project_id,
        "backend-1",
        "consumer-2",
        min_idle_milliseconds=120_000,
        count=5,
    )

    assert messages == [("9-0", {"event": "payload"})]
    assert bus.redis.autoclaim_calls


class FakeReceiptRepository:
    def __init__(self) -> None:
        self.statuses: dict[tuple[str, str, str], str] = {}
        self.failures: list[str] = []

    @staticmethod
    def _key(project_id: str, event_id: str, agent_id: str) -> tuple[str, str, str]:
        return project_id, event_id, agent_id

    async def event_receipt_status(
        self, project_id: str, event_id: str, agent_id: str
    ) -> str | None:
        return self.statuses.get(self._key(project_id, event_id, agent_id))

    async def record_event_delivery(
        self, project_id: str, event_id: str, agent_id: str, stream_id: str
    ) -> bool:
        del stream_id
        key = self._key(project_id, event_id, agent_id)
        if key in self.statuses:
            return False
        self.statuses[key] = "DELIVERED"
        return True

    async def claim_event_receipt(
        self,
        project_id: str,
        event_id: str,
        agent_id: str,
        consumer_name: str,
        lease_seconds: int,
    ) -> bool:
        del consumer_name, lease_seconds
        key = self._key(project_id, event_id, agent_id)
        if self.statuses.get(key) not in {"DELIVERED", "FAILED"}:
            return False
        self.statuses[key] = "PROCESSING"
        return True

    async def complete_event_receipt(
        self, project_id: str, event_id: str, agent_id: str, consumer_name: str
    ) -> bool:
        del consumer_name
        key = self._key(project_id, event_id, agent_id)
        if self.statuses.get(key) != "PROCESSING":
            return False
        self.statuses[key] = "PROCESSED"
        return True

    async def fail_event_receipt(
        self,
        project_id: str,
        event_id: str,
        agent_id: str,
        consumer_name: str,
        error: str,
    ) -> bool:
        del consumer_name
        self.statuses[self._key(project_id, event_id, agent_id)] = "FAILED"
        self.failures.append(error)
        return True


class FakeInboxBus:
    def __init__(self) -> None:
        self.acked: list[str] = []

    async def acknowledge_inbox(self, project_id: str, agent_id: str, message_id: str) -> int:
        del project_id, agent_id
        self.acked.append(message_id)
        return 1


@pytest.mark.asyncio
@pytest.mark.parametrize("result_status", ["BUSY", "THROTTLED"])
async def test_worker_does_not_ack_retryable_results(result_status: str) -> None:
    worker_class = _actor_implementation(AgentWorkerActor)
    worker = object.__new__(worker_class)
    worker.project_id = str(uuid4())
    worker.agent_id = "backend-1"
    worker.events = FakeReceiptRepository()
    worker.bus = FakeInboxBus()

    async def process(event: Event) -> dict[str, str]:
        del event
        return {"status": result_status}

    worker._process_event = process
    event = Event(
        project_id=worker.project_id,
        event_type=EventType.TASK_CREATED,
        producer_agent_id="runtime_supervisor",
        payload={"task_id": str(uuid4())},
    )
    handled = await worker._handle_inbox_message(
        "1-0",
        {"event": event.model_dump_json()},
        group="group",
        consumer_name="consumer",
        lease_seconds=120,
    )

    assert handled is False
    assert worker.bus.acked == []
    assert worker.events.failures == [result_status]


@pytest.mark.asyncio
async def test_worker_acks_only_after_durable_success_and_deduplicates_replay() -> None:
    worker_class = _actor_implementation(AgentWorkerActor)
    worker = object.__new__(worker_class)
    worker.project_id = str(uuid4())
    worker.agent_id = "backend-1"
    worker.events = FakeReceiptRepository()
    worker.bus = FakeInboxBus()
    calls = 0

    async def process(event: Event) -> dict[str, str]:
        nonlocal calls
        del event
        calls += 1
        return {"status": "COMPLETED"}

    worker._process_event = process
    event = Event(
        project_id=worker.project_id,
        event_type=EventType.TASK_CREATED,
        producer_agent_id="runtime_supervisor",
        payload={"task_id": str(uuid4())},
    )
    fields = {"event": event.model_dump_json()}

    assert await worker._handle_inbox_message(
        "1-0", fields, group="group", consumer_name="consumer", lease_seconds=120
    )
    assert await worker._handle_inbox_message(
        "2-0", fields, group="group", consumer_name="consumer", lease_seconds=120
    )
    assert calls == 1
    assert worker.bus.acked == ["1-0", "2-0"]


@pytest.mark.asyncio
async def test_worker_records_failure_and_leaves_stream_entry_pending_on_error() -> None:
    worker_class = _actor_implementation(AgentWorkerActor)
    worker = object.__new__(worker_class)
    worker.project_id = str(uuid4())
    worker.agent_id = "backend-1"
    worker.events = FakeReceiptRepository()
    worker.bus = FakeInboxBus()

    async def process(event: Event) -> dict[str, str]:
        del event
        raise RuntimeError("transient handler failure")

    worker._process_event = process
    event = Event(
        project_id=worker.project_id,
        event_type=EventType.TASK_CREATED,
        producer_agent_id="runtime_supervisor",
        payload={"task_id": str(uuid4())},
    )
    handled = await worker._handle_inbox_message(
        "1-0",
        {"event": event.model_dump_json()},
        group="group",
        consumer_name="consumer",
        lease_seconds=120,
    )

    assert handled is False
    assert worker.bus.acked == []
    assert worker.events.failures == ["RuntimeError"]


class FakeRoutingBus:
    def __init__(self, *, fail: bool = False) -> None:
        self.sent = 0
        self.fail = fail

    async def send_to_inbox(self, project_id: str, agent_id: str, event: Event) -> str:
        del project_id, agent_id, event
        self.sent += 1
        if self.fail:
            raise RuntimeError("stream unavailable")
        return f"{self.sent}-0"


@pytest.mark.asyncio
async def test_trigger_records_recipient_dedupe_after_successful_stream_send() -> None:
    trigger_class = _actor_implementation(TriggerEngineActor)
    trigger = object.__new__(trigger_class)
    trigger.registered_agents = {"backend-1"}
    trigger.subscriptions = defaultdict(set, {EventType.TASK_CREATED.value: {"backend-1"}})
    trigger.allowed_producers = defaultdict(set)
    trigger.fallback_coordinator = None
    trigger.bus = FakeRoutingBus()
    trigger.receipts = FakeReceiptRepository()
    event = Event(
        project_id=str(uuid4()),
        event_type=EventType.TASK_CREATED,
        producer_agent_id="runtime_supervisor",
        payload={"task_id": str(uuid4())},
    )

    await trigger._route_event(event)
    await trigger._route_event(event)

    assert trigger.bus.sent == 1
    assert (
        await trigger.receipts.event_receipt_status(
            event.project_id, str(event.event_id), "backend-1"
        )
        == "DELIVERED"
    )


@pytest.mark.asyncio
async def test_trigger_does_not_write_recipient_dedupe_when_stream_send_fails() -> None:
    trigger_class = _actor_implementation(TriggerEngineActor)
    trigger = object.__new__(trigger_class)
    trigger.registered_agents = {"backend-1"}
    trigger.subscriptions = defaultdict(set, {EventType.TASK_CREATED.value: {"backend-1"}})
    trigger.allowed_producers = defaultdict(set)
    trigger.fallback_coordinator = None
    trigger.bus = FakeRoutingBus(fail=True)
    trigger.receipts = FakeReceiptRepository()
    event = Event(
        project_id=str(uuid4()),
        event_type=EventType.TASK_CREATED,
        producer_agent_id="runtime_supervisor",
        payload={"task_id": str(uuid4())},
    )

    with pytest.raises(RuntimeError, match="stream unavailable"):
        await trigger._route_event(event)

    assert (
        await trigger.receipts.event_receipt_status(
            event.project_id, str(event.event_id), "backend-1"
        )
        is None
    )


@pytest.mark.asyncio
async def test_worker_start_is_idempotent_when_already_running() -> None:
    worker_class = _actor_implementation(AgentWorkerActor)
    worker = object.__new__(worker_class)
    worker._start_lock = asyncio.Lock()
    worker.running = True
    worker.agent_id = "backend-1"
    worker.role = "backend_developer"
    worker.project_id = str(uuid4())
    worker.squad = "backend"
    worker.status = "IDLE"
    worker.current_task_id = None
    worker.provider_assignment = {}
    worker.resource_allocation = {}

    async def must_not_start() -> dict[str, Any]:
        raise AssertionError("idempotent start attempted to create duplicate loops")

    worker._start_once = must_not_start
    snapshot = await worker.start()
    assert snapshot["status"] == "IDLE"


def test_schema_has_durable_unique_recipient_receipts() -> None:
    schema = (Path(__file__).resolve().parents[1] / "storage" / "schema.sql").read_text(
        encoding="utf-8"
    )
    assert "CREATE TABLE IF NOT EXISTS event_receipts" in schema
    assert "PRIMARY KEY (project_id, event_id, agent_id)" in schema
    for field in ("attempt_count", "lease_expires_at", "status", "last_error"):
        assert field in schema
