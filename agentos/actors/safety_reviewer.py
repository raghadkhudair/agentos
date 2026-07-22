from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

import ray
import structlog

from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.provider.gateway import ProviderRequest
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import DoDRepository

logger = structlog.get_logger()


def _json_object(content: str) -> dict[str, Any]:
    clean = content.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", clean, flags=re.IGNORECASE)
    value = json.loads(clean)
    if not isinstance(value, dict):
        raise ValueError("safety review response must be a JSON object")
    return value


@ray.remote(num_cpus=0.2, max_concurrency=4)  # type: ignore[call-overload]
class SafetyReviewerAgentActor:
    def __init__(self, settings_payload: dict[str, Any], provider_actor_name: str):
        self.settings = Settings(**settings_payload)
        self.provider = ray.get_actor(provider_actor_name, namespace="agentos")
        self.db = PostgresClient(self.settings)
        self.dod = DoDRepository(self.db)

    async def review_code_change(
        self,
        *,
        project_id: str,
        task_id: str,
        criterion_ids: list[str],
        artifact_id: str,
        file_path: str,
        diff_content: str,
        risk_level: str,
    ) -> dict[str, Any]:
        deterministic_findings: list[str] = []
        patterns = {
            r"(?i)(password|api[_-]?key|secret)\s*[:=]\s*['\"][^'\"]+": "embedded credential",
            r"(?i)verify\s*=\s*false": "TLS verification disabled",
            r"(?i)subprocess\.[^(]+\([^)]*shell\s*=\s*true": "shell-enabled subprocess",
            r"(?i)(chmod\s+777|allow_origins\s*=\s*\[?['\"]\*)": "over-broad permission",
        }
        for pattern, finding in patterns.items():
            if re.search(pattern, diff_content):
                deterministic_findings.append(finding)

        request = ProviderRequest(
            purpose="safety_behavior_review",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Perform an independent application-security review of this exact Git diff. "
                        "Assess authentication, authorization, injection, secrets, transport, data "
                        "isolation, unsafe execution, and fail-open behavior. Return JSON only: "
                        '{"safe":bool,"findings":[str]}.'
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Risk: {risk_level}\nFile: {file_path}\nDiff:\n{diff_content[:100_000]}"
                    ),
                },
            ],
            budget_key=UUID(project_id),
            agent_id="security_reviewer-1",
            agent_role="security_reviewer",
            complexity=TaskComplexity.CRITICAL,
            required_capabilities={"chat", "json"},
        )
        try:
            response = await self.provider.get_completion.remote(
                request.model_dump(mode="json"), response_format={"type": "json_object"}
            )
            result = _json_object(response["content"])
            raw_findings = result.get("findings", [])
            provider_findings = (
                [str(item) for item in raw_findings] if isinstance(raw_findings, list) else []
            )
            findings = [*deterministic_findings, *provider_findings]
            safe = bool(result.get("safe")) and not deterministic_findings
        except Exception as error:
            safe = False
            findings = [f"security review failed closed: {type(error).__name__}"]

        for criterion_id in criterion_ids:
            await self.dod.add_evidence(
                project_id,
                criterion_id,
                "security_review",
                "security_reviewer-1",
                summary="Security review approved" if safe else "; ".join(findings)[:2000],
                passed=safe,
                artifact_id=artifact_id,
                metadata={
                    "task_id": task_id,
                    "artifact_id": artifact_id,
                    "file_path": file_path,
                    "risk_level": risk_level,
                },
            )
        return {"safe": safe, "findings": findings}

    async def review_agent_behavior(
        self,
        project_id: str,
        action_type: str,
        description: str,
        recent_violation_count: int,
    ) -> dict[str, Any]:
        request = ProviderRequest(
            purpose="safety_behavior_review",
            messages=[
                {
                    "role": "system",
                    "content": 'Assess policy-bypass risk. Return JSON: {"safe":bool,"reason":str}.',
                },
                {
                    "role": "user",
                    "content": (
                        f"Action: {action_type}\nDescription: {description}\n"
                        f"Recent deterministic policy violations: {recent_violation_count}"
                    ),
                },
            ],
            budget_key=UUID(project_id),
            agent_id="security_reviewer-1",
            agent_role="security_reviewer",
            complexity=TaskComplexity.CRITICAL,
            required_capabilities={"chat", "json"},
        )
        try:
            response = await self.provider.get_completion.remote(
                request.model_dump(mode="json"), response_format={"type": "json_object"}
            )
            result = _json_object(response["content"])
            return {"safe": bool(result.get("safe")), "reason": str(result.get("reason", ""))}
        except Exception as error:
            logger.error("safety_review_failed_closed", error_type=type(error).__name__)
            return {"safe": False, "reason": "safety reviewer unavailable; failed closed"}
