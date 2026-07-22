from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from agentos.config.loader import guardrail_policies
from agentos.config.settings import Settings
from agentos.messaging.events import Event, EventType
from agentos.storage.clients.dragonfly import DragonflyClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import EventRepository, ProjectRepository

logger = structlog.get_logger()
POLICY = guardrail_policies()


class _Watchdog:
    PRODUCER_IDS = {
        "DoDWatchdog": "dod_watchdog",
        "StagnationWatchdog": "stagnation_watchdog",
        "DeadlockWatchdog": "deadlock_watchdog",
        "SafetyWatchdog": "safety_watchdog",
    }

    def __init__(self, database: PostgresClient):
        self.db = database
        self.events = EventRepository(database)

    async def emit(self, project_id: str, event_type: EventType, payload: dict[str, Any]) -> None:
        await self.events.save_event(
            project_id,
            Event(
                project_id=project_id,
                event_type=event_type,
                producer_agent_id=self.PRODUCER_IDS[self.__class__.__name__],
                payload=payload,
            ),
        )


class DoDWatchdog(_Watchdog):
    """Triggers durable replanning when the backlog is empty but DoD is incomplete."""

    def __init__(self, database: PostgresClient):
        super().__init__(database)
        self.projects = ProjectRepository(database)

    async def inspect_and_act(
        self, project_id: str, dod_satisfied: bool, dod_gaps: list[str]
    ) -> dict[str, Any]:
        active = await self.db.fetchval(
            """
            SELECT count(*) FROM tasks
            WHERE project_id=$1::uuid AND status NOT IN ('COMPLETED','CANCELLED')
            """,
            project_id,
        )
        if dod_satisfied or int(active or 0) > 0:
            return {"action_required": "NONE", "status": "COMPLIANT"}
        await self.projects.update_status(project_id, "REPLANNING")
        await self.emit(
            project_id,
            EventType.REPLANNING_TRIGGERED,
            {"reason": "empty_backlog_with_unsatisfied_dod", "gaps": dod_gaps},
        )
        return {"action_required": "TRIGGER_REPLANNING", "gaps": dod_gaps}


class StagnationWatchdog(_Watchdog):
    """Detects stale progress or repeated outcomes and records a blocker."""

    async def inspect_and_act(self, project_id: str) -> dict[str, Any]:
        config = POLICY["stagnation_watchdog"]
        rows = await self.db.fetch(
            """
            SELECT summary,created_at FROM checkpoints
            WHERE project_id=$1::uuid ORDER BY created_at DESC LIMIT $2
            """,
            project_id,
            int(config["checkpoint_history_lookback"]),
        )
        if not rows:
            return {"action_required": "NONE", "status": "STABLE"}
        stale_after = timedelta(seconds=int(config.get("staleness_seconds", 300)))
        latest = rows[0]["created_at"]
        reason: str | None = None
        if datetime.now(UTC) - latest > stale_after:
            reason = f"no checkpoint for {int(stale_after.total_seconds())} seconds"
        else:
            summary, count = Counter(str(row["summary"]) for row in rows).most_common(1)[0]
            if count >= int(config["repeated_action_threshold"]):
                reason = f"checkpoint outcome repeated {count} times: {summary[:500]}"
        if reason is None:
            return {"action_required": "NONE", "status": "STABLE"}
        await self.emit(project_id, EventType.BLOCKER_CREATED, {"reason": reason})
        return {"action_required": "REPLAN", "reason": reason}


class SafetyWatchdog(_Watchdog):
    """Quarantines identities that repeatedly receive deny decisions."""

    def __init__(self, database: PostgresClient, settings: Settings):
        super().__init__(database)
        self.dragonfly = DragonflyClient(settings)

    async def inspect_and_act(self, project_id: str) -> dict[str, Any]:
        row = await self.db.fetchrow(
            """
            SELECT agent_id,count(*) violation_count FROM audit_events
            WHERE project_id=$1::uuid AND decision IN ('DENY','QUARANTINE_AGENT')
            GROUP BY agent_id ORDER BY violation_count DESC LIMIT 1
            """,
            project_id,
        )
        threshold = int(POLICY["safety_watchdog"]["blocked_call_quarantine_threshold"])
        if row is None or int(row["violation_count"]) < threshold:
            return {"action_required": "NONE", "status": "SECURE"}
        agent_id = str(row["agent_id"])
        await self.dragonfly.redis.sadd(
            self.dragonfly.key("governance", "quarantined_agents"), agent_id
        )
        await self.emit(
            project_id,
            EventType.AGENT_QUARANTINED,
            {"agent_id": agent_id, "violation_count": int(row["violation_count"])},
        )
        return {"action_required": "QUARANTINE_AGENT", "agent_id": agent_id}


class DeadlockWatchdog(_Watchdog):
    """Finds dependency cycles without mutating tasks automatically."""

    async def inspect_and_act(self, project_id: str) -> dict[str, Any]:
        rows = await self.db.fetch(
            """
            SELECT d.task_id::text,d.depends_on_task_id::text
            FROM task_dependencies d JOIN tasks t ON t.id=d.task_id
            WHERE t.project_id=$1::uuid AND t.status NOT IN ('COMPLETED','CANCELLED')
            """,
            project_id,
        )
        graph: dict[str, list[str]] = {}
        for row in rows:
            graph.setdefault(str(row["task_id"]), []).append(str(row["depends_on_task_id"]))
        visited: set[str] = set()
        visiting: set[str] = set()

        def cyclic(node: str) -> bool:
            if node in visiting:
                return True
            if node in visited:
                return False
            visiting.add(node)
            found = any(cyclic(child) for child in graph.get(node, []))
            visiting.remove(node)
            visited.add(node)
            return found

        if not any(cyclic(node) for node in graph):
            return {"action_required": "NONE", "status": "STABLE"}
        await self.emit(
            project_id,
            EventType.BLOCKER_CREATED,
            {"reason": "circular task dependency detected"},
        )
        return {"action_required": "RESOLVE_DEADLOCK"}
