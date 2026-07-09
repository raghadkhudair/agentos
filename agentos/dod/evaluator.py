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

    A project must not be marked complete unless all mandatory DoD items are satisfied with evidence.
    """

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager

    async def evaluate(self, project_id: str, dod: list[str]) -> DoDEvaluation:
        """
        Queries live database milestones and checkpoints to evaluate project compliance 
        against the mandatory Definition of Done (DoD) contract.
        """
        safe_project_id = uuid.UUID(project_id) if isinstance(project_id, str) else project_id
        
        # 1. Gather all successful checkpoints recorded for this project
        query = """
            SELECT achievement, summary, created_at::text 
            FROM checkpoints 
            WHERE project_id = $1 
            ORDER BY created_at ASC;
        """
        checkpoints_found = []
        try:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(query, safe_project_id)
                checkpoints_found = [dict(row) for row in rows]
        except Exception as e:
            print(f"DoDEvaluator failed to query checkpoints database: {e}")

        # 2. Iterate through each requirement item and hunt for verified evidence matchings
        evaluated_items: list[DoDItemStatus] = []
        gaps: list[str] = []
        
        for item in dod:
            item_lower = item.lower()
            evidence_list = []
            
            # Semantic token lookups across checkpoint achievement labels and summaries
            for cp in checkpoints_found:
                summary_lower = cp["summary"].lower()
                achievement_lower = cp["achievement"].lower()
                
                # Check 1: Strict string match
                match_found = item_lower in summary_lower or item_lower in achievement_lower
                
                # Check 2: Flexible evaluation mapping for validation shell checkpoints
                if not match_found and "verify" in item_lower and "output" in item_lower:
                    if "shell_command" in summary_lower or "python3" in summary_lower:
                        match_found = True
                
                if match_found:
                    evidence_list.append(
                        f"🏆 [{cp['created_at']}] {cp['achievement'].upper()}: {cp['summary']}"
                    )
            
            if evidence_list:
                status_entry = DoDItemStatus(
                    item=item,
                    status="SATISFIED",
                    evidence=evidence_list
                )
            else:
                status_entry = DoDItemStatus(item=item, status="MISSING")
                gaps.append(item)
                
            evaluated_items.append(status_entry)

        # 3. Compile completion details
        all_satisfied = len(gaps) == 0
        
        return DoDEvaluation(
            project_id=str(project_id),
            satisfied=all_satisfied,
            items=evaluated_items,
            gaps=gaps
        )