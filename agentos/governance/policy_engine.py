from __future__ import annotations

from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision, RiskLevel


class PolicyEngine:
    """Deterministic governance layer for agent actions.

    Enforces immutable zero-trust filters and tracks stateful 
    quarantine boundaries to isolate compromised agents.
    """

    DESTRUCTIVE_PATTERNS = (
        "drop database",
        "drop schema",
        "drop table",
        "truncate",
        "delete from",
        "rm -rf",
        "disable guardrail",
        "delete audit",
        "delete checkpoint",
        "delete event log",
        "curl | sh",
        "wget | sh",
    )

    def __init__(self, settings: Settings):
        self.settings = settings
        self._quarantined_agents: set[str] = set()

    def quarantine_agent(self, agent_id: str) -> None:
        """Forcibly isolates an agent worker and blocks all future execution permissions."""
        self._quarantined_agents.add(agent_id)
        print(f"🚨 [POLICY SECURITY BLACKLIST]: Agent '{agent_id}' has been moved to QUARANTINE.")

    def lift_quarantine(self, agent_id: str) -> None:
        """Manually releases an agent from quarantine following out-of-band remediation."""
        self._quarantined_agents.discard(agent_id)

    def evaluate_action(self, request: ActionRequest) -> GuardrailResult:
        # 1. Stateful Quarantine Interception Gate
        if request.agent_id in self._quarantined_agents:
            return GuardrailResult(
                decision=PolicyDecision.QUARANTINE_AGENT,
                risk_level=RiskLevel.CRITICAL,
                reasons=[f"Execution denied: Agent '{request.agent_id}' is marked as QUARANTINED due to security anomalies."],
                constraints=["Revoke all filesystem access.", "Block outbound provider gateway calls immediately."]
            )

        # 2. Destructive Command Extraction Filter Scanning
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

        if request.action_type in {"modify_auth", "modify_ci", "run_migration", "add_dependency"}:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=["High-impact engineering action requires independent review."],
            )

        # 3. Low Risk: Static, passive read/write actions
        if request.action_type in {"read_file", "write_file", "write_code", "run_tests", "create_summary", "search_memory"}:
            return GuardrailResult(decision=PolicyDecision.ALLOW, risk_level=RiskLevel.LOW)
        
        # 4. Medium Risk: Shell execution commands
        if request.action_type in {"shell_command", "run_command"}:
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