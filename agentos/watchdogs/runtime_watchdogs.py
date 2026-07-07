from __future__ import annotations


class DoDWatchdog:
    """Detects incomplete DoD with no active work and triggers replanning."""

    async def inspect(self, project_id: str) -> dict:
        return {"project_id": project_id, "status": "not_implemented"}


class StagnationWatchdog:
    """Detects repeated failures or lack of checkpoints."""

    async def inspect(self, project_id: str) -> dict:
        return {"project_id": project_id, "status": "not_implemented"}


class SafetyWatchdog:
    """Detects repeated denied actions, prompt-injection symptoms, and unsafe behavior."""

    async def inspect(self, project_id: str) -> dict:
        return {"project_id": project_id, "status": "not_implemented"}
