from __future__ import annotations

from pydantic import BaseModel, Field


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

    async def evaluate(self, project_id: str, dod: list[str]) -> DoDEvaluation:
        items = [DoDItemStatus(item=item) for item in dod]
        return DoDEvaluation(project_id=project_id, satisfied=False, items=items, gaps=dod)
