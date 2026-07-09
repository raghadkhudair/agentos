from __future__ import annotations

import uuid
import json
from collections import Counter
from agentos.storage.database import DatabaseManager
from agentos.dod.evaluator import DoDEvaluator


class DoDWatchdog:
    """
    Detects incomplete Definition of Done criteria when no active database 
    tasks are currently running, and automatically triggers a replanning sequence.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.evaluator = DoDEvaluator(db_manager)

    async def inspect(self, project_id: str, project_dod: list[str]) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        # 1. Query the database to see if any tasks are currently incomplete or in progress
        query_tasks = "SELECT COUNT(*) FROM tasks WHERE project_id = $1 AND status != 'COMPLETED';"
        async with self.db.pool.acquire() as conn:
            incomplete_task_count = await conn.fetchval(query_tasks, safe_project_id)
            
        # 2. Run the actual Quality Contract Evaluation
        dod_report = await self.evaluator.evaluate(project_id, project_dod)
        
        # Condition: No active tasks remain, but the Quality Contract is STILL not satisfied!
        if incomplete_task_count == 0 and not dod_report.satisfied:
            print(f"⚠️ [WATCHDOG ALERT]: Project {project_id} has stalled with 0 active tasks but unfulfilled DoD items!")
            return {
                "action_required": "TRIGGER_REPLANNING",
                "reason": "Incomplete DoD milestones found with zero outstanding active database tasks.",
                "gaps": dod_report.gaps
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
        
        # Fetch recent checkpoints to check for repetitive patterns
        query_checkpoints = """
            SELECT summary FROM checkpoints 
            WHERE project_id = $1 
            ORDER BY created_at DESC 
            LIMIT 10;
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query_checkpoints, safe_project_id)
            
        summaries = [row["summary"] for row in rows]
        if not summaries:
            return {"action_required": "NONE", "status": "STABLE"}
            
        # Count identical consecutive agent action executions
        counter = Counter(summaries)
        most_common_action, count = counter.most_common(1)[0]
        
        # Trigger an alert if the same file modification action occurs more than 3 times consecutively
        if count >= 4:
            print(f"🛑 [STAGNATION ALERT]: Circular execution loop detected on action: {most_common_action}")
            return {
                "action_required": "FREEZE_STREAM",
                "reason": f"Agent is stuck repeating the exact same execution step: {most_common_action}",
                "repeated_action": most_common_action
            }
            
        return {"action_required": "NONE", "status": "STABLE"}


class SafetyWatchdog:
    """
    Monitors security and provider call logs for prompt-injection symptoms, 
    repeated policy engine denials, or budget overflows.
    """
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def inspect(self, project_id: str) -> dict:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        # Query audit logs for recent policy engine governance rejections
        query_audit = """
            SELECT COUNT(*) FROM provider_calls 
            WHERE project_id = $1 AND purpose = 'decide_next_action' AND cost_usd = 0.0;
        """
        async with self.db.pool.acquire() as conn:
            blocked_call_count = await conn.fetchval(query_audit, safe_project_id)
            
        # If policy violations are stacking up, flag the agent worker as a security risk
        if blocked_call_count >= 5:
            print(f"🚨 [SAFETY ALERT]: Excessive policy violations detected for project context: {project_id}")
            return {
                "action_required": "QUARANTINE_AGENT",
                "reason": "Agent has breached corporate security policies or triggered repeated block boundaries."
            }
            
        return {"action_required": "NONE", "status": "SECURE"}