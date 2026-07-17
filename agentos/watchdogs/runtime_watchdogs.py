from __future__ import annotations

import uuid
import json
from collections import Counter
import structlog
from agentos.storage.database import DatabaseManager
from agentos.config.loader import guardrail_policies
cfg = guardrail_policies()

logger = structlog.get_logger()


class DoDWatchdog:
    """
    Detects incomplete Definition of Done criteria when no active database
    tasks are currently running — i.e. the project is STUCK, not just unfinished.

    This is intentionally different from DoDEvaluatorActor: the evaluator answers
    "is the project done?"; this watchdog answers the narrower question
    "are we stuck with nothing left to do, even though we're not done?"
    It relies on an already-computed evaluation result rather than re-evaluating,
    to avoid duplicating the DB/LLM work DoDEvaluatorActor already just did.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def inspect(self, project_id: str, dod_satisfied: bool, dod_gaps: list[str]) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id

        query_tasks = "SELECT COUNT(*) FROM tasks WHERE project_id = $1 AND status != 'COMPLETED';"
        async with self.db.pool.acquire() as conn:
            incomplete_task_count = await conn.fetchval(query_tasks, safe_project_id)

        if incomplete_task_count == 0 and not dod_satisfied:
            logger.error("watchdog_dod_stalled", project_id=project_id, gaps=dod_gaps)
            return {
                "action_required": "TRIGGER_REPLANNING",
                "reason": "Incomplete DoD milestones found with zero outstanding active database tasks.",
                "gaps": dod_gaps
            }

        return {"action_required": "NONE", "status": "COMPLIANT"}
    
class StagnationWatchdog:
    """
    Analyzes historical database checkpoints to identify loops, repeated 
    file rewrites, or circular execution failures across agent tasks.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def inspect(self, project_id: str) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        query_checkpoints = f"""
            SELECT summary FROM checkpoints 
            WHERE project_id = $1 
            ORDER BY created_at DESC 
            LIMIT {cfg['stagnation_watchdog']['checkpoint_history_lookback']};
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query_checkpoints, safe_project_id)
            
        summaries = [row["summary"] for row in rows]
        if not summaries:
            return {"action_required": "NONE", "status": "STABLE"}
            
        counter = Counter(summaries)
        most_common_action, count = counter.most_common(1)[0]
        
        if count >= cfg['stagnation_watchdog']['repeated_action_threshold']:
            logger.warning("watchdog_stagnation_loop_detected", project_id=project_id, action=most_common_action, count=count)
            return {
                "action_required": "FREEZE_STREAM",
                "reason": f"Agent is stuck repeating the exact same execution step: {most_common_action}",
                "repeated_action": most_common_action
            }
            
        return {"action_required": "NONE", "status": "STABLE"}


class SafetyWatchdog:
    """
    Monitors append-only audit tracking logs to isolate malicious or 
    policy-violating agents from the system workspace.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def inspect(self, project_id: str) -> dict:
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
            return {"action_required": "QUARANTINE_AGENT", "agent_id": row["agent_id"], "reason": f"Agent surpassed maximum safety boundary constraints. Found {row['violation_count']} violations."}

        return {"action_required": "NONE", "status": "SECURE"}
    

class DeadlockWatchdog:
    """
    Scans relational task tables to detect cyclic dependency deadlocks 
    that prevent agents from progressing.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def inspect(self, project_id: str) -> dict:
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
            return {
                "action_required": "RESOLVE_DEADLOCK",
                "reason": "Circular task dependencies detected. Tasks are blocking each other indefinitely."
            }

        return {"action_required": "NONE", "status": "STABLE"}