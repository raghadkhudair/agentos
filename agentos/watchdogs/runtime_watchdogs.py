from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import structlog

from agentos.config.loader import guardrail_policies, runtime_tuning
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

    async def emit(
        self,
        project_id: str,
        event_type: EventType,
        payload: dict[str, Any],
        *,
        correlation_id: str | None = None,
    ) -> None:
        event_id = (
            uuid5(
                NAMESPACE_URL,
                f"agentos:{project_id}:{event_type.value}:{correlation_id}",
            )
            if correlation_id
            else None
        )
        event_payload: dict[str, Any] = {
            "project_id": project_id,
            "event_type": event_type,
            "producer_agent_id": self.PRODUCER_IDS[self.__class__.__name__],
            "payload": payload,
            "correlation_id": correlation_id,
        }
        if event_id is not None:
            event_payload["event_id"] = event_id
        await self.events.save_event(
            project_id,
            Event.model_validate(event_payload),
        )


class DoDWatchdog(_Watchdog):
    """Triggers durable replanning when the backlog is empty but DoD is incomplete."""

    TRANSIENT_EVALUATION_CODES = {
        "ARTIFACT_STORE_INCONCLUSIVE",
        "EVALUATION_SNAPSHOT_STALE",
        "EVIDENCE_FRESHNESS_INCONCLUSIVE",
        "INTEGRATION_ANCESTRY_INCONCLUSIVE",
    }

    def __init__(self, database: PostgresClient):
        super().__init__(database)
        self.projects = ProjectRepository(database)

    async def inspect_and_act(
        self,
        project_id: str,
        dod_satisfied: bool,
        dod_gaps: list[dict[str, Any]],
        *,
        evaluation_run_id: str | None = None,
    ) -> dict[str, Any]:
        counts = await self.db.fetchrow(
            """
            SELECT
              count(*) FILTER(WHERE t.status IN ('CLAIMED','IN_PROGRESS','UNDER_REVIEW')) executing,
              count(*) FILTER(
                WHERE t.status='PENDING' AND NOT EXISTS(
                  SELECT 1 FROM task_dependencies td
                  JOIN tasks dependency ON dependency.id=td.depends_on_task_id
                  WHERE td.task_id=t.id AND dependency.status<>'COMPLETED'
                )
              ) runnable,
              count(*) FILTER(WHERE t.status NOT IN ('COMPLETED','CANCELLED')) nonterminal
            FROM tasks t WHERE t.project_id=$1::uuid
            """,
            project_id,
        )
        assert counts is not None
        if dod_satisfied or int(counts["executing"] or 0) > 0 or int(counts["runnable"] or 0) > 0:
            return {"action_required": "NONE", "status": "COMPLIANT"}
        retry_evaluation = bool(dod_gaps) and all(
            bool(gap.get("retryable", True))
            and str(gap.get("code")) in self.TRANSIENT_EVALUATION_CODES
            for gap in dod_gaps
        )
        tuning = runtime_tuning()["dod"]
        maximum = int(tuning["max_replan_attempts"])
        delay = int(tuning["replan_backoff_seconds"])
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT status,replan_attempts,next_replan_at FROM projects WHERE id=$1::uuid FOR UPDATE",
                project_id,
            )
            if project is None:
                raise LookupError(f"project not found: {project_id}")
            if project["next_replan_at"] and project["next_replan_at"] > datetime.now(UTC):
                return {"action_required": "NONE", "status": "BACKOFF"}
            attempts = int(project["replan_attempts"] or 0)
            if attempts >= maximum:
                await connection.execute(
                    "UPDATE projects SET status='BLOCKED_REQUIRES_INPUT' WHERE id=$1::uuid",
                    project_id,
                )
                exhausted = True
            else:
                exhausted = False
            if not exhausted and evaluation_run_id:
                duplicate = await connection.fetchval(
                    """
                    SELECT 1 FROM events WHERE project_id=$1::uuid
                      AND event_type='REPLANNING_TRIGGERED' AND correlation_id=$2 LIMIT 1
                    """,
                    project_id,
                    evaluation_run_id,
                )
                if duplicate:
                    return {"action_required": "NONE", "status": "COALESCED"}
            if not exhausted:
                attempts += 1
                await connection.execute(
                    """
                    UPDATE projects SET status=$4,replan_attempts=$2,
                      next_replan_at=now()+($3*interval '1 second') WHERE id=$1::uuid
                    """,
                    project_id,
                    attempts,
                    delay * (2 ** (attempts - 1)),
                    "VERIFYING" if retry_evaluation else "REPLANNING",
                )
        if exhausted:
            await self.emit(
                project_id,
                EventType.BLOCKER_CREATED,
                {
                    "reason": (
                        "bounded_evaluation_retries_exhausted"
                        if retry_evaluation
                        else "bounded_replanning_exhausted"
                    ),
                    "attempts": attempts,
                    "gaps": dod_gaps,
                },
                correlation_id=evaluation_run_id,
            )
            return {"action_required": "BLOCK", "attempts": attempts, "gaps": dod_gaps}
        if retry_evaluation:
            return {
                "action_required": "RETRY_EVALUATION",
                "attempt": attempts,
                "retry_at_backoff": True,
                "gaps": dod_gaps,
            }
        await self.emit(
            project_id,
            EventType.REPLANNING_TRIGGERED,
            {
                "reason": "no_runnable_work_with_unsatisfied_dod",
                "gaps": dod_gaps,
                "evaluation_run_id": evaluation_run_id,
                "attempt": attempts,
                "nonterminal_tasks": int(counts["nonterminal"] or 0),
            },
            correlation_id=evaluation_run_id,
        )
        return {
            "action_required": "TRIGGER_REPLANNING",
            "gaps": dod_gaps,
            "attempt": attempts,
        }


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
