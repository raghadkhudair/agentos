from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CatchUpPacket:
    project_id: str
    agent_id: str
    trigger_event_id: str
    relevant_events: list[str] = field(default_factory=list)
    relevant_memories: list[str] = field(default_factory=list)
    relevant_artifacts: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)
    recommended_next_actions: list[str] = field(default_factory=list)


class MemoryBroker:
    """Scoped memory gateway.

    Responsibilities:
    - enforce memory ACLs
    - combine structured filters with vector retrieval
    - build concise catch-up packets
    - prevent raw log or secret leakage into prompts
    """

    async def build_catchup_packet(
        self,
        *,
        project_id: str,
        agent_id: str,
        trigger_event_id: str,
    ) -> CatchUpPacket:
        return CatchUpPacket(
            project_id=project_id,
            agent_id=agent_id,
            trigger_event_id=trigger_event_id,
            relevant_events=["Starter packet: event retrieval is not implemented yet."],
            recommended_next_actions=["Decide whether to wait, plan, implement, review, or escalate."],
        )
