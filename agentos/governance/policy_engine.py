from __future__ import annotations

from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision, RiskLevel


class PolicyEngine:
    """Deterministic governance layer for agent actions.

    Agents can propose actions. This engine decides whether the runtime may execute them.
    The initial implementation is intentionally conservative.
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

    def evaluate_action(self, request: ActionRequest) -> GuardrailResult:
        text = " ".join(
            part for part in [request.description, request.command, request.database_operation] if part
        ).lower()

        matched = [pattern for pattern in self.DESTRUCTIVE_PATTERNS if pattern in text]
        if matched:
            if not self.settings.allow_destructive_actions:
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

        if request.action_type in {"read_file", "run_tests", "create_summary", "search_memory"}:
            return GuardrailResult(decision=PolicyDecision.ALLOW, risk_level=RiskLevel.LOW)

        return GuardrailResult(
            decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
            risk_level=RiskLevel.MEDIUM,
            constraints=["Execute only inside assigned workspace and allowed paths."],
        )
