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
        raise ValueError("review response must be a JSON object")
    return value


@ray.remote(num_cpus=0.2, max_concurrency=4)  # type: ignore[call-overload]
class ReviewerAgentActor:
    """Independent code-review actor that records review evidence."""

    def __init__(self, settings_payload: dict[str, Any], provider_actor_name: str):
        self.settings = Settings(**settings_payload)
        self.provider = ray.get_actor(provider_actor_name, namespace="agentos")
        self.db = PostgresClient(self.settings)
        self.dod = DoDRepository(self.db)

    async def review_code_patch(
        self,
        *,
        project_id: str,
        task_id: str,
        criterion_ids: list[str],
        artifact_id: str,
        file_path: str,
        code_content: str,
    ) -> dict[str, Any]:
        deterministic_findings: list[str] = []
        patterns = {
            r"\beval\s*\(": "dynamic eval",
            r"\bexec\s*\(": "dynamic exec",
            r"subprocess\..*shell\s*=\s*True": "shell-enabled subprocess",
            r"verify\s*=\s*False": "TLS verification disabled",
            r"(?i)(password|api_key|secret)\s*=\s*['\"][^'\"]+": "embedded credential",
        }
        for pattern, finding in patterns.items():
            if re.search(pattern, code_content):
                deterministic_findings.append(finding)

        request = ProviderRequest(
            purpose="review_code_patch",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Review correctness, maintainability, security, and missing tests. "
                        'Return JSON: {"approved":bool,"score":0-100,"findings":[str]}.'
                    ),
                },
                {"role": "user", "content": f"File: {file_path}\n{code_content}"},
            ],
            budget_key=UUID(project_id),
            agent_id="code_reviewer-1",
            agent_role="code_reviewer",
            complexity=TaskComplexity.HIGH,
            required_capabilities={"chat", "json"},
        )
        result: dict[str, Any]
        try:
            response = await self.provider.get_completion.remote(
                request.model_dump(mode="json"), response_format={"type": "json_object"}
            )
            result = _json_object(response["content"])
        except Exception as error:
            result = {
                "approved": False,
                "score": 0,
                "findings": [f"review provider failed closed: {type(error).__name__}"],
            }
        raw_findings = result.get("findings", [])
        provider_findings = (
            [str(item) for item in raw_findings] if isinstance(raw_findings, list) else []
        )
        findings = [*deterministic_findings, *provider_findings]
        approved = bool(result.get("approved")) and not deterministic_findings
        raw_score = result.get("score", 0)
        score = int(raw_score) if isinstance(raw_score, (str, int, float)) else 0
        for criterion_id in criterion_ids:
            await self.dod.add_evidence(
                project_id,
                criterion_id,
                "review",
                "code_reviewer-1",
                summary="Review approved" if approved else "; ".join(findings)[:2000],
                passed=approved,
                artifact_id=artifact_id,
                metadata={
                    "task_id": task_id,
                    "file_path": file_path,
                    "artifact_id": artifact_id,
                    "score": score,
                },
            )
        return {"approved": approved, "score": score, "findings": findings}
