from __future__ import annotations

from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision, RiskLevel
from agentos.config.loader import guardrail_policies


class PolicyEngine:

    def __init__(self, settings: Settings):
        self.settings = settings
        self._quarantined_agents: set[str] = set()
        cfg = guardrail_policies()
        self.DESTRUCTIVE_PATTERNS = tuple(cfg["destructive_patterns"])
        self._review_types = set(cfg["require_review_action_types"])
        self._low_risk_types = set(cfg["low_risk_action_types"])
        self._medium_risk_types = set(cfg["medium_risk_shell_action_types"])
        self._quarantine_threshold = cfg["safety_watchdog"]["blocked_call_quarantine_threshold"]

    def quarantine_agent(self, agent_id: str) -> None:
        self._quarantined_agents.add(agent_id)
        print(f"🚨 [POLICY SECURITY BLACKLIST]: Agent '{agent_id}' has been moved to QUARANTINE.")

    def lift_quarantine(self, agent_id: str) -> None:
        self._quarantined_agents.discard(agent_id)

    def evaluate_action(self, request: ActionRequest) -> GuardrailResult:
        if request.agent_id in self._quarantined_agents:
            return GuardrailResult(
                decision=PolicyDecision.QUARANTINE_AGENT,
                risk_level=RiskLevel.CRITICAL,
                reasons=[f"Execution denied: Agent '{request.agent_id}' is marked as QUARANTINED due to security anomalies."],
                constraints=["Revoke all filesystem access.", "Block outbound provider gateway calls immediately."]
            )

        text = " ".join(
            part for part in [request.description, request.command, request.database_operation] if part
        ).lower()

        matched = [pattern for pattern in self.DESTRUCTIVE_PATTERNS if pattern in text]
        if matched:
            if not self.settings.allow_destructive_actions:
                self.quarantine_agent(request.agent_id)
                return GuardrailResult(
                    decision=PolicyDecision.DENY,
                    risk_level=RiskLevel.CRITICAL,
                    reasons=[f"Blocked destructive pattern: {pattern}" for pattern in matched],
                    constraints=["Destructive actions require explicit out-of-band approval."],
                )
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_HUMAN_APPROVAL,
                risk_level=RiskLevel.CRITICAL,
                reasons=[f"Critical destructive pattern detected: {pattern}" for pattern in matched],
                constraints=["Require backup proof.", "Require sandbox proof.", "Require human approval."],
            )

        if request.action_type in self._review_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=["High-impact engineering action requires independent review."],
            )

        if request.action_type in self._low_risk_types:
            return GuardrailResult(decision=PolicyDecision.ALLOW, risk_level=RiskLevel.LOW)
        
        if request.action_type in self._medium_risk_types:
            return GuardrailResult(
                decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
                risk_level=RiskLevel.MEDIUM,
                constraints=["Execute only inside assigned workspace sandbox environment. Commands must be non-interactive."],
            )
        
        return GuardrailResult(
            decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
            risk_level=RiskLevel.MEDIUM,
            constraints=["Execute only inside assigned workspace and allowed paths."],
        )