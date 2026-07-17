from __future__ import annotations

import json
import uuid
import ray
import structlog
from pydantic import BaseModel, Field

from agentos.storage.database import DatabaseManager
from agentos.messaging.dragonfly_bus import DragonflyBus
from agentos.messaging.events import Event, EventType

logger = structlog.get_logger()


class DoDItemStatus(BaseModel):
    item: str
    status: str = "NOT_STARTED"
    evidence: list[str] = Field(default_factory=list)


class DoDEvaluation(BaseModel):
    project_id: str
    satisfied: bool
    items: list[DoDItemStatus]
    gaps: list[str] = Field(default_factory=list)


@ray.remote(namespace="agentos")
class DoDEvaluatorActor:
    """Runtime completion gate.

    Verifies project completeness by cross-referencing requirements with completed
    artifacts, verifying failure records to prevent false completion, and triggering gap-closure events.
    """

    def __init__(self, settings_payload: dict):
        from agentos.config.settings import Settings
        self.settings = Settings(**settings_payload) if settings_payload else Settings()
        self.db = DatabaseManager(self.settings)
        self.bus = DragonflyBus(self.settings.dragonfly_url)
        self._connected = False

    async def _ensure_connected(self):
        """Ensures the actor process has active system connections on demand."""
        if not self._connected:
            await self.db.connect()
            self._connected = True

    async def evaluate(self, project_id: str, dod: list[str]) -> dict:
        """Evaluates DoD completion, blocks compromised items, and triggers gap-closure tasks."""
        await self._ensure_connected()
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        logger.info("evaluating_definition_of_done_compliance", project_id=project_id)

        # Fetch live artifacts
        query_artifacts = "SELECT title, artifact_type, created_at::text FROM artifacts WHERE project_id = $1;"
        artifacts_found = []
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query_artifacts, safe_project_id)
                artifacts_found = [dict(row) for row in rows]
        except Exception as e:
            logger.error("failed_to_query_artifacts_for_dod", error=str(e))

        existing_artifacts = {art["title"].lower(): art for art in artifacts_found}

        # Fetch checkpoint history logs (for audit failure checking)
        query_checkpoints = "SELECT achievement, summary, created_at::text FROM checkpoints WHERE project_id = $1;"
        checkpoints_found = []
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query_checkpoints, safe_project_id)
                checkpoints_found = [dict(row) for row in rows]
        except Exception as e:
            logger.error("failed_to_query_checkpoints_for_dod", error=str(e))

        # Check for any active 'review_failed' checkpoint logs to prevent false completions
        failed_reviews = {
            cp["summary"].lower() for cp in checkpoints_found 
            if cp["achievement"].lower() in {"review_failed", "verification_failed"}
        }

        # Gather completed tasks criteria mappings
        query_tasks = "SELECT title, status, acceptance_criteria FROM tasks WHERE project_id = $1;"
        completed_criteria = set()
        try:
            async with self.db.pool.acquire() as conn:
                task_rows = await conn.fetch(query_tasks, safe_project_id)
                for trow in task_rows:
                    if trow["status"] == "COMPLETED":
                        completed_criteria.add(trow["title"].lower())
                        if trow["acceptance_criteria"]:
                            try:
                                criteria_data = json.loads(trow["acceptance_criteria"])
                                if isinstance(criteria_data, list):
                                    for item_str in criteria_data:
                                        completed_criteria.add(str(item_str).lower())
                            except Exception:
                                pass
        except Exception as e:
            logger.error("failed_to_parse_completed_criteria", error=str(e))

        evaluated_items: list[DoDItemStatus] = []
        gaps: list[str] = []
        
        for item in dod:
            item_lower = item.lower()
            evidence_list = []
            is_compromised = False
            compromise_reason = ""

            # Check if this item has triggered any active review_failed checkpoints
            for failed_desc in failed_reviews:
                if item_lower in failed_desc:
                    is_compromised = True
                    compromise_reason = failed_desc
                    break

            if is_compromised:
                evidence_list.append(
                    f"⚠️ [FALSE COMPLETION BLOCKED] Rejection detected: '{compromise_reason}'"
                )
                evaluated_items.append(
                    DoDItemStatus(item=item, status="BLOCKED_REJECTED", evidence=evidence_list)
                )
                gaps.append(item)
                continue

            #  Verify physical project assets
            if item_lower in existing_artifacts:
                art = existing_artifacts[item_lower]
                evidence_list.append(
                    f"📦 [ARTIFACT VALIDATION] Found verified physical asset: '{art['title']}'."
                )
            
            # Validate task acceptance criteria mappings
            if item_lower in completed_criteria or any(item_lower in comp or comp in item_lower for comp in completed_criteria):
                evidence_list.append(
                    f"🎯 [ACCEPTANCE CRITERIA CHECK] Verified via formal task completion graph mappings."
                )

            # Parse checkpoint history entries
            for cp in checkpoints_found:
                summary_lower = cp["summary"].lower()
                achievement_lower = cp["achievement"].lower()
                match_found = item_lower in summary_lower or item_lower in achievement_lower
                
                if match_found:
                    evidence_list.append(
                        f"🏆 [{cp['created_at']}] {cp['achievement'].upper()}: {cp['summary']}"
                    )
            
            if evidence_list:
                status_entry = DoDItemStatus(item=item, status="SATISFIED", evidence=evidence_list)
            else:
                status_entry = DoDItemStatus(item=item, status="MISSING")
                gaps.append(item)
                
            evaluated_items.append(status_entry)

        all_satisfied = len(gaps) == 0

        if not all_satisfied:
            logger.warning("definition_of_done_has_unresolved_gaps", missing=gaps)
            unified_stream_key = f"project:{project_id}:events"
            
            # Publish a GAP_DETECTED event onto the Dragonfly event bus stream
            gap_event = Event(
                project_id=project_id,
                event_type=EventType.TASK_CREATED, 
                topic=unified_stream_key,
                payload={
                    "message": f"Definition of Done Gaps Detected. Please resolve missing items: {', '.join(gaps)}",
                    "missing_do_items": gaps
                }
            )
            try:
                await self.bus.publish_event(unified_stream_key, gap_event)
                logger.info("gap_closure_event_successfully_dispatched", stream=unified_stream_key)
            except Exception as e:
                logger.error("failed_to_dispatch_gap_closure_event", error=str(e))

        evaluation = DoDEvaluation(
            project_id=str(project_id),
            satisfied=all_satisfied,
            items=evaluated_items,
            gaps=gaps
        )
        return evaluation.model_dump()