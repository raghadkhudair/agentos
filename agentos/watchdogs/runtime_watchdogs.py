from __future__ import annotations

import uuid
import json
from datetime import datetime, timedelta, timezone
from collections import Counter
import structlog
from agentos.storage.database import DatabaseManager
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType
from agentos.storage.repositories import EventRepository, ProjectRepository
from agentos.config.loader import guardrail_policies

cfg = guardrail_policies()
logger = structlog.get_logger()


class DoDWatchdog:
    """
    Detects incomplete DoD with no active work, and now also performs
    the plan's required actions itself: trigger gap analysis (delegated to
    DoDEvaluatorActor, which already computed gaps/similar-task-check),
    trigger PM/Tech Lead (via a REPLANNING_TRIGGERED event), and mark the
    project as REPLANNING so the state is visible in the database too.
    """
    def __init__(self, db_manager: DatabaseManager, dragonfly_url: str):
        self.db = db_manager
        self.bus = DragonflyBus(dragonfly_url)
        self.event_repo = EventRepository(db_manager)
        self.project_repo = ProjectRepository(db_manager)

    async def inspect_and_act(self, project_id: str, dod_satisfied: bool, dod_gaps: list[str]) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        query_tasks = "SELECT COUNT(*) FROM tasks WHERE project_id = $1 AND status != 'COMPLETED';"
        async with self.db.pool.acquire() as conn:
            incomplete_task_count = await conn.fetchval(query_tasks, safe_project_id)

        if incomplete_task_count == 0 and not dod_satisfied:
            logger.error("watchdog_dod_stalled", project_id=project_id, gaps=dod_gaps)

            unified_stream_key = f"project:{project_id}:events"
            replanning_event = Event(
                project_id=project_id,
                event_type=EventType.REPLANNING_TRIGGERED,
                topic=unified_stream_key,
                payload={
                    "message": "CRITICAL STALL: Backlog is empty but project goals are unfulfilled.",
                    "missing_dod_items": dod_gaps
                }
            )
            await self.event_repo.save_event(project_id, replanning_event)
            await self.bus.publish_event(unified_stream_key, replanning_event, claimed_agent_id="dod_watchdog")
            await self.project_repo.update_status(project_id, "REPLANNING")

            return {
                "action_required": "TRIGGER_REPLANNING",
                "reason": "Incomplete DoD milestones found with zero outstanding active database tasks.",
                "gaps": dod_gaps
            }

        return {"action_required": "NONE", "status": "COMPLIANT"}


class StagnationWatchdog:
    """
     Detects repeated failures or no progress. If a project is stuck repeating the same action
       or has no checkpoints for a long time, 
       it triggers a BLOCKER_CREATED event and 
       recommends freezing the project stream for PM/Architect review.
    """
    def __init__(self, db_manager: DatabaseManager, dragonfly_url: str):
        self.db = db_manager
        self.bus = DragonflyBus(dragonfly_url)
        self.event_repo = EventRepository(db_manager)

    async def inspect_and_act(self, project_id: str, summary_manager=None, provider_gateway=None) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        query_checkpoints = f"""
            SELECT summary, created_at FROM checkpoints
            WHERE project_id = $1
            ORDER BY created_at DESC
            LIMIT {cfg['stagnation_watchdog']['checkpoint_history_lookback']};
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query_checkpoints, safe_project_id)

        if not rows:
            return {"action_required": "NONE", "status": "STABLE"}

        # Signal 1: no checkpoint for a long period
        most_recent = rows[0]["created_at"]
        staleness_limit = timedelta(seconds=cfg['stagnation_watchdog'].get('staleness_seconds', 300))
        if datetime.now(timezone.utc) - most_recent > staleness_limit:
            return await self._freeze_and_notify(
                project_id,
                reason=f"No checkpoint logged in over {staleness_limit.total_seconds():.0f} seconds — the project may be stuck.",
                context_lines=[f"Last checkpoint: {rows[0]['summary']} at {most_recent.isoformat()}"],
                summary_manager=summary_manager, provider_gateway=provider_gateway
            )

        # Signal 2: repeated identical checkpoint summary
        summaries = [row["summary"] for row in rows]
        counter = Counter(summaries)
        most_common_action, count = counter.most_common(1)[0]
        if count >= cfg['stagnation_watchdog']['repeated_action_threshold']:
            return await self._freeze_and_notify(
                project_id,
                reason=f"Agent is stuck repeating the exact same execution step: {most_common_action}",
                context_lines=[f"Repeated {count} times: {most_common_action}"],
                summary_manager=summary_manager, provider_gateway=provider_gateway
            )

        # Signal 3: repeated circular handoffs — same task bouncing between 2+ agents
        query_handoffs = """
            SELECT task_id, agent_id, created_at FROM checkpoints
            WHERE project_id = $1 AND achievement = 'task_claimed' AND task_id IS NOT NULL
            ORDER BY task_id, created_at ASC;
        """
        async with self.db.pool.acquire() as conn:
            handoff_rows = await conn.fetch(query_handoffs, safe_project_id)

        claims_by_task: dict[str, list[str]] = {}
        for row in handoff_rows:
            claims_by_task.setdefault(str(row["task_id"]), []).append(row["agent_id"])

        threshold = cfg['stagnation_watchdog'].get('circular_handoff_claim_threshold', 4)
        for task_id, claimants in claims_by_task.items():
            distinct_agents = set(claimants)
            if len(claimants) >= threshold and 2 <= len(distinct_agents) < len(claimants):
                return await self._freeze_and_notify(
                    project_id,
                    reason=f"Task {task_id} has bounced between agents {list(distinct_agents)} {len(claimants)} times without completing.",
                    context_lines=[f"Claim order: {' -> '.join(claimants)}"],
                    summary_manager=summary_manager, provider_gateway=provider_gateway
                )

        return {"action_required": "NONE", "status": "STABLE"}

    async def _freeze_and_notify(self, project_id: str, reason: str, context_lines: list[str], summary_manager=None, provider_gateway=None) -> dict:
        logger.warning("watchdog_stagnation_detected", project_id=project_id, reason=reason)

        written_summary = reason
        if summary_manager and provider_gateway:
            try:
                written_summary = await summary_manager.generate_stagnation_summary.remote(
                    project_id, reason, context_lines, provider_gateway
                )
                from agentos.storage.repositories import SummaryRepository
                summary_repo = SummaryRepository(self.db)
                await summary_repo.save_summary(project_id, "stagnation_alert", "stagnation_watchdog", written_summary)
            except Exception as e:
                logger.error("stagnation_summary_generation_failed", error=str(e))

        unified_stream_key = f"project:{project_id}:events"
        stagnation_event = Event(
            project_id=project_id,
            event_type=EventType.BLOCKER_CREATED,
            topic=unified_stream_key,
            payload={"message": f"STAGNATION DETECTED: {written_summary}", "reason": reason}
        )
        await self.event_repo.save_event(project_id, stagnation_event)
        await self.bus.publish_event(unified_stream_key, stagnation_event, claimed_agent_id="stagnation_watchdog")
        return {"action_required": "FREEZE_STREAM", "reason": reason, "summary": written_summary}
    
class SafetyWatchdog:
    """
     Monitors audit logs to isolate policy-violating agents.
    """
    def __init__(self, db_manager: DatabaseManager, dragonfly_url: str):
        self.db = db_manager
        self.bus = DragonflyBus(dragonfly_url)
        self.event_repo = EventRepository(db_manager)

    async def inspect_and_act(self, project_id: str) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        query_audit = """
            SELECT agent_id, COUNT(*) as violation_count FROM audit_events
            WHERE project_id = $1 AND decision IN ('DENY', 'QUARANTINE_AGENT')
            GROUP BY agent_id
            ORDER BY violation_count DESC
            LIMIT 1;
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query_audit, safe_project_id)

        if row and row["violation_count"] >= cfg['safety_watchdog']['blocked_call_quarantine_threshold']:
            logger.critical("watchdog_safety_quarantine_triggered", project_id=project_id, agent_id=row["agent_id"], violations=row["violation_count"])

            unified_stream_key = f"project:{project_id}:events"
            alert_event = Event(
                project_id=project_id,
                event_type=EventType.SECURITY_ALERT,
                topic=unified_stream_key,
                payload={
                    "message": f"Agent '{row['agent_id']}' exceeded safety violation threshold and requires quarantine.",
                    "agent_id": row["agent_id"],
                    "violation_count": row["violation_count"]
                }
            )
            await self.event_repo.save_event(project_id, alert_event)
            await self.bus.publish_event(unified_stream_key, alert_event, claimed_agent_id="safety_watchdog")

            return {
                "action_required": "QUARANTINE_AGENT",
                "agent_id": row["agent_id"],
                "reason": f"Agent surpassed maximum safety boundary constraints. Found {row['violation_count']} violations."
            }

        return {"action_required": "NONE", "status": "SECURE"}


class DeadlockWatchdog:
    """
    Scans task dependencies for cycles, and now also triggers
    PM/Architect via an event, instead of just detecting and logging.
    """
    def __init__(self, db_manager: DatabaseManager, dragonfly_url: str):
        self.db = db_manager
        self.bus = DragonflyBus(dragonfly_url)
        self.event_repo = EventRepository(db_manager)

    async def inspect_and_act(self, project_id: str) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        query_deps = """
            SELECT task_id::text, depends_on_task_id::text
            FROM task_dependencies
            WHERE task_id IN (SELECT id FROM tasks WHERE project_id = $1 AND status != 'COMPLETED');
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query_deps, safe_project_id)

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
            logger.critical("watchdog_deadlock_detected", project_id=project_id)

            unified_stream_key = f"project:{project_id}:events"
            deadlock_event = Event(
                project_id=project_id,
                event_type=EventType.BLOCKER_CREATED,
                topic=unified_stream_key,
                payload={"message": "DEADLOCK DETECTED: Circular task dependencies are blocking progress. PM/Architect review needed."}
            )
            await self.event_repo.save_event(project_id, deadlock_event)
            await self.bus.publish_event(unified_stream_key, deadlock_event, claimed_agent_id="deadlock_watchdog")

            return {
                "action_required": "RESOLVE_DEADLOCK",
                "reason": "Circular task dependencies detected. Tasks are blocking each other indefinitely."
            }

        return {"action_required": "NONE", "status": "STABLE"}