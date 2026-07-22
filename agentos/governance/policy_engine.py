from __future__ import annotations

import fnmatch
import json
from pathlib import PurePosixPath

from agentos.config.loader import guardrail_policies
from agentos.config.settings import Settings
from agentos.governance.models import (
    ActionRequest,
    AgentIdentity,
    GuardrailResult,
    PolicyDecision,
    RiskLevel,
)
from agentos.storage.clients.dragonfly import DragonflyClient


class PolicyEngine:
    """Deterministic, fail-closed action policy boundary."""

    def __init__(self, settings: Settings, dragonfly: DragonflyClient | None = None):
        self.settings = settings
        self.dragonfly = dragonfly or DragonflyClient(settings)
        self.redis_client = self.dragonfly.redis
        config = guardrail_policies()
        self.destructive_patterns = tuple(
            str(item).lower() for item in config["destructive_patterns"]
        )
        self.review_types = set(config["require_review_action_types"])
        self.low_risk_types = set(config["low_risk_action_types"])
        self.medium_risk_types = set(config["medium_risk_shell_action_types"])
        self.security_review_types = set(config.get("require_security_review_action_types", []))
        self.backup_types = set(config.get("require_backup_action_types", []))
        self.sandbox_only_types = set(config.get("sandbox_only_action_types", []))
        self.controlled_keywords = tuple(
            str(item).lower() for item in config.get("controlled_command_keywords", [])
        )
        self.blocked_paths = tuple(
            str(item).replace("\\", "/").strip("/").lower()
            for item in config.get("filesystem_safety", {}).get("blocked_global_paths", [])
        )
        self.quarantine_threshold = int(
            config["safety_watchdog"]["blocked_call_quarantine_threshold"]
        )
        self.quarantine_key = self.dragonfly.key("governance", "quarantined_agents")

    async def quarantine_agent(self, agent_id: str) -> None:
        await self.redis_client.sadd(self.quarantine_key, agent_id)

    async def lift_quarantine(self, agent_id: str) -> None:
        await self.redis_client.srem(self.quarantine_key, agent_id)
        await self.redis_client.delete(self.dragonfly.key("agent", agent_id, "violation_count"))

    async def _violation(self, agent_id: str, reason: str) -> GuardrailResult:
        key = self.dragonfly.key("agent", agent_id, "violation_count")
        count = int(await self.redis_client.incr(key))
        await self.redis_client.expire(key, 86_400)
        if count >= self.quarantine_threshold:
            await self.quarantine_agent(agent_id)
            return GuardrailResult(
                decision=PolicyDecision.QUARANTINE_AGENT,
                risk_level=RiskLevel.CRITICAL,
                reasons=[reason, f"quarantine threshold reached after {count} violations"],
                constraints=["All execution and provider access is revoked."],
            )
        return GuardrailResult(
            decision=PolicyDecision.DENY,
            risk_level=RiskLevel.HIGH,
            reasons=[reason],
        )

    @staticmethod
    def _path_allowed(path: str, allowed_paths: list[str], blocked_paths: tuple[str, ...]) -> bool:
        normalized = PurePosixPath(path.replace("\\", "/")).as_posix().strip("/").lower()
        if normalized.startswith("../") or "/../" in f"/{normalized}/":
            return False
        if any(
            normalized == blocked or normalized.startswith(f"{blocked}/")
            for blocked in blocked_paths
        ):
            return False
        if not allowed_paths:
            return True
        normalized_allowed = [item.replace("\\", "/").strip("/").lower() for item in allowed_paths]
        return any(
            normalized == item
            or normalized.startswith(f"{item}/")
            or fnmatch.fnmatch(normalized, item)
            for item in normalized_allowed
        )

    async def evaluate_action(
        self, request: ActionRequest, auth_identity: AgentIdentity | None = None
    ) -> GuardrailResult:
        if not request.verify_integrity():
            return await self._violation(request.agent_id, "action integrity verification failed")
        if await self.redis_client.sismember(self.quarantine_key, request.agent_id):
            return GuardrailResult(
                decision=PolicyDecision.QUARANTINE_AGENT,
                risk_level=RiskLevel.CRITICAL,
                reasons=["agent is quarantined"],
            )
        if auth_identity is None or auth_identity.agent_id != request.agent_id:
            return await self._violation(
                request.agent_id, "unknown or mismatched execution identity"
            )
        if auth_identity.project_id != request.project_id:
            return await self._violation(request.agent_id, "cross-project execution request")
        if request.action_type not in auth_identity.allowed_actions:
            return await self._violation(
                request.agent_id,
                f"role {auth_identity.role} cannot perform {request.action_type}",
            )
        for target in request.target_paths:
            if not self._path_allowed(target, auth_identity.allowed_paths, self.blocked_paths):
                return await self._violation(
                    request.agent_id, f"target path is outside policy: {target}"
                )

        material = " ".join(
            [
                request.description,
                " ".join(request.command or []),
                request.database_operation or "",
                json.dumps(request.payload, sort_keys=True, default=str),
            ]
        ).lower()
        destructive = [pattern for pattern in self.destructive_patterns if pattern in material]
        if destructive:
            if not self.settings.allow_destructive_actions:
                return await self._violation(
                    request.agent_id,
                    "destructive operation denied: " + ", ".join(destructive),
                )
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_HUMAN_APPROVAL,
                risk_level=RiskLevel.CRITICAL,
                reasons=["destructive operation requires explicit human approval"],
                constraints=[
                    "Verified backup",
                    "Disposable sandbox proof",
                    "Independent security review",
                ],
            )

        if request.action_type in self.security_review_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_SECURITY_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=["security-sensitive change requires independent security review"],
            )
        if request.action_type in self.backup_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_BACKUP_FIRST,
                risk_level=RiskLevel.HIGH,
                reasons=["data-changing operation requires backup evidence"],
            )
        if request.action_type in self.review_types or any(
            keyword in material for keyword in self.controlled_keywords
        ):
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_REVIEW,
                risk_level=RiskLevel.HIGH,
                reasons=["high-impact action requires independent review"],
            )
        if request.action_type in self.sandbox_only_types:
            return GuardrailResult(
                decision=PolicyDecision.REQUIRE_SANDBOX_ONLY,
                risk_level=RiskLevel.MEDIUM,
                constraints=["No network", "Bounded CPU/memory/PIDs", "Non-interactive command"],
            )
        if request.action_type in self.low_risk_types:
            return GuardrailResult(decision=PolicyDecision.ALLOW, risk_level=RiskLevel.LOW)
        if request.action_type in self.medium_risk_types:
            return GuardrailResult(
                decision=PolicyDecision.ALLOW_WITH_CONSTRAINTS,
                risk_level=RiskLevel.MEDIUM,
                constraints=["Assigned workspace only", "Task path ownership enforced"],
            )
        return GuardrailResult(
            decision=PolicyDecision.DENY,
            risk_level=RiskLevel.HIGH,
            reasons=[f"action type {request.action_type!r} has no explicit policy"],
        )
