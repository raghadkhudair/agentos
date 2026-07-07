from __future__ import annotations

from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision
from agentos.governance.policy_engine import PolicyEngine


class ExecutionSupervisor:
    """Only execution boundary for agents.

    Agents do not run shell commands, mutate files, or touch databases directly.
    They submit ActionRequest objects. This supervisor applies policy first.
    """

    def __init__(self, settings: Settings):
        self.policy_engine = PolicyEngine(settings)

    async def request_execution(self, action: ActionRequest) -> dict:
        result: GuardrailResult = self.policy_engine.evaluate_action(action)
        if result.decision in {PolicyDecision.DENY, PolicyDecision.QUARANTINE_AGENT}:
            return {"executed": False, "guardrail": result.model_dump()}
        if result.decision in {
            PolicyDecision.REQUIRE_HUMAN_APPROVAL,
            PolicyDecision.REQUIRE_REVIEW,
            PolicyDecision.REQUIRE_SECURITY_REVIEW,
        }:
            return {"executed": False, "guardrail": result.model_dump(), "pending_approval": True}
        return {"executed": True, "guardrail": result.model_dump(), "result": "starter-noop"}
