from __future__ import annotations

import json
from redis.asyncio import Redis
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, GuardrailResult, PolicyDecision, RiskLevel
from agentos.config.loader import guardrail_policies


class PolicyEngine:

    def __init__(self, settings: Settings):
        self.settings = settings
        self.redis_client = Redis.from_url(settings.dragonfly_url, decode_responses=True)
        
        cfg = guardrail_policies()
        self.DESTRUCTIVE_PATTERNS = tuple(cfg["destructive_patterns"])
        self._review_types = set(cfg["require_review_action_types"])
        self._low_risk_types = set(cfg["low_risk_action_types"])
        self._medium_risk_types = set(cfg["medium_risk_shell_action_types"])
        self._security_review_types = set(cfg.get("require_security_review_action_types", []))
        self._backup_types = set(cfg.get("require_backup_action_types", []))
        self._sandbox_only_types = set(cfg.get("sandbox_only_action_types", []))
        self._controlled_keywords = cfg.get("controlled_command_keywords", [])
        self._quarantine_threshold = cfg["safety_watchdog"]["blocked_call_quarantine_threshold"]
        
        self.QUARANTINE_SET_KEY = "governance:global:quarantined_agents"

    async def quarantine_agent(self, agent_id: str) -> None:
        """Persists the quarantine state globally inside the Dragonfly cache layer."""
        await self.redis_client.sadd(self.QUARANTINE_SET_KEY, agent_id)
        print(f"🚨 [POLICY SECURITY BLACKLIST]: Agent '{agent_id}' has been moved to GLOBAL QUARANTINE.")

    async def lift_quarantine(self, agent_id: str) -> None:
        """Removes an agent from the quarantine blacklist and clears their violation records."""
        await self.redis_client.srem(self.QUARANTINE_SET_KEY, agent_id)
        await self.redis_client.delete(f"agent:{agent_id}:violation_count")
        print(f"✅ [POLICY SECURITY PRIVILEGES]: Quarantine lifted for Agent '{agent_id}'.")

    async def evaluate_action(self, request: ActionRequest, auth_identity: any = None) -> GuardrailResult:
        """Asynchronously evaluates an incoming task against global security compliance policies."""
        
        is_quarantined = await self.redis_client.sismember(self.QUARANTINE_SET_KEY, request.agent_id)
        if is_quarantined:
            return GuardrailResult(
                decision=PolicyDecision.QUARANTINE_AGENT,
                risk_level=RiskLevel.CRITICAL,
                reasons=[f"Execution denied: Agent '{request.agent_id}' is marked as QUARANTINED."],
                constraints=["Revoke all filesystem access.", "Block outbound provider gateway calls immediately."]
            )

        if auth_identity and request.action_type not in auth_identity.allowed_actions:
            counter_key = f"agent:{request.agent_id}:violation_count"
            current_violations = await self.redis_client.incr(counter_key)
            
            if current_violations >= self._quarantine_threshold:
                await self.quarantine_agent(request.agent_id)
                return GuardrailResult(
                    decision=PolicyDecision.QUARANTINE_AGENT,
                    risk_level=RiskLevel.CRITICAL,
                    reasons=[f"Threshold breached: Agent quarantined after {current_violations} violations."],
                    constraints=["Revoke all access."]
                )
            return GuardrailResult(
                decision=PolicyDecision.DENY,
                risk_level=RiskLevel.HIGH,
                reasons=[f"Role Violation: Agent role {auth_identity.role} is not authorized to execute action category '{request.action_type}'."]
            )

        text = " ".join(
            part for part in [request.description, request.command, request.database_operation] if part
        ).lower()

        matched = [pattern for pattern in self.DESTRUCTIVE_PATTERNS if pattern in text]
        if matched:
            counter_key = f"agent:{request.agent_id}:violation_count"
            current_violations = await self.redis_client.incr(counter_key)
            
            if current_violations >= self._quarantine_threshold:
                await self.quarantine_agent(request.agent_id)
                return GuardrailResult(
                    decision=PolicyDecision.QUARANTINE_AGENT,
                    risk_level=RiskLevel.CRITICAL,
                    reasons=[f"Threshold breached: Agent quarantined after {current_violations} violations."],
                    constraints=["Revoke all access."]
                )

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


        matched_controlled = [kw for kw in self._controlled_keywords if kw in text]
        if matched_controlled:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=[f"Controlled Command Gating: Operation involves a restricted utility phrase: '{kw}'." for kw in matched_controlled],
                constraints=["Requires explicit administrative review sign-off before sandbox execution."]
            )

        if request.action_type in self._review_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=["High-impact engineering action requires independent review."]
            )
        
        if request.action_type in self._security_review_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_SECURITY_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=["Critical authentication or security path modification requires dedicated security review."]
            )

        if request.action_type in self._backup_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_BACKUP_FIRST,
                risk_level=RiskLevel.HIGH,
                reasons=["Data modification action requires verified system backup state verification before execution."]
            )

        if request.action_type in self._sandbox_only_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_SANDBOX_ONLY,
                risk_level=RiskLevel.MEDIUM,
                constraints=["Action strictly constrained. Network operations and host filesystem hooks are disabled."]
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