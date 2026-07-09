from __future__ import annotations
import uuid
from pydantic import BaseModel, Field
from agentos.storage.database import DatabaseManager


class DoDItemStatus(BaseModel):
    item: str
    status: str = "NOT_STARTED"
    evidence: list[str] = Field(default_factory=list)


class DoDEvaluation(BaseModel):
    project_id: str
    satisfied: bool
    items: list[DoDItemStatus]
    gaps: list[str] = Field(default_factory=list)


class DoDEvaluator:
    """Runtime completion gate.

    Verifies project completeness by cross-referencing requirements with live database artifacts.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def evaluate(self, project_id: str, dod: list[str]) -> DoDEvaluation:
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        # 1. Fetch real physical artifact models saved by agents during the engineering lifecycle
        query_artifacts = "SELECT title, artifact_type, created_at::text FROM artifacts WHERE project_id = $1;"
        artifacts_found = []
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query_artifacts, safe_project_id)
                artifacts_found = [dict(row) for row in rows]
        except Exception as e:
            print(f"DoDEvaluator failed to query artifacts database: {e}")

        # 2. Extract title mappings for direct verification checks
        existing_artifacts = {art["title"].lower(): art for art in artifacts_found}

        # 3. Gather checkpoints history logs as secondary fallback tracking
        query_checkpoints = "SELECT achievement, summary, created_at::text FROM checkpoints WHERE project_id = $1;"
        checkpoints_found = []
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query_checkpoints, safe_project_id)
                checkpoints_found = [dict(row) for row in rows]
        except Exception as e:
            print(f"Failed to query checkpoints loop: {e}")

        evaluated_items: list[DoDItemStatus] = []
        gaps: list[str] = []
        
        for item in dod:
            item_lower = item.lower()
            evidence_list = []
            
            # CRITICAL CHECKPOINT MATCH: Check if an exact artifact match exists in our verified registry table
            if item_lower in existing_artifacts:
                art = existing_artifacts[item_lower]
                evidence_list.append(
                    f"📦 [ARTIFACT VALIDATION - {art['created_at']}] Found verified physical project asset record."
                )
            
            # Check secondary text checkpoints history log for confirmation entries
            for cp in checkpoints_found:
                summary_lower = cp["summary"].lower()
                achievement_lower = cp["achievement"].lower()
                
                match_found = item_lower in summary_lower or item_lower in achievement_lower
                
                if not match_found and "verify" in item_lower and "output" in item_lower:
                    if "shell_command" in summary_lower or "python3" in summary_lower:
                        match_found = True
                
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
        
        return DoDEvaluation(
            project_id=str(project_id),
            satisfied=all_satisfied,
            items=evaluated_items,
            gaps=gaps
        )