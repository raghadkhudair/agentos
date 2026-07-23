from __future__ import annotations

import asyncio
import hashlib
import json
import re
from typing import Any
from uuid import UUID

import ray
import structlog
from pydantic import BaseModel, ConfigDict, Field

from agentos.actors.review_cache import CriterionReviewCache
from agentos.config.loader import runtime_tuning
from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.provider.gateway import ProviderRequest
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import DoDRepository

logger = structlog.get_logger()
_SECURITY_REVIEW_PROMPT_VERSION = "criterion-security-review-v1"
_MAX_REVIEW_CONTEXT_CHARACTERS = 100_000


class _SecurityReviewVerdict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    safe: bool
    findings: list[str] = Field(default_factory=list, max_length=100)


class _BehaviorReviewVerdict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    safe: bool
    reason: str = Field(min_length=1, max_length=2000)


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
        tuning = runtime_tuning()["review"]
        self.review_cache = CriterionReviewCache(
            int(tuning["max_parallel_criterion_reviews"]),
            int(tuning["revision_cache_entries"]),
        )

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

        project_uuid = UUID(project_id)
        task = await self.db.fetchrow(
            "SELECT * FROM tasks WHERE id=$1 AND project_id=$2", UUID(task_id), project_uuid
        )
        artifact = await self.db.fetchrow(
            "SELECT * FROM artifacts WHERE id=$1 AND project_id=$2",
            UUID(artifact_id),
            project_uuid,
        )
        if task is None or artifact is None or artifact["task_id"] != task["id"]:
            raise ValueError("security review requires a task-bound durable artifact")
        if str(artifact["title"]).replace("\\", "/") != file_path.replace("\\", "/"):
            raise ValueError("security-review file path does not identify the durable artifact")
        artifact_metadata = artifact["metadata"] or {}
        subject_commit = artifact_metadata.get("git_commit")
        if not isinstance(subject_commit, str) or not subject_commit:
            raise ValueError(
                "security-review artifact lacks its authoritative Git subject revision"
            )
        review_digest = artifact_metadata.get("review_diff_sha256")
        review_characters = artifact_metadata.get("review_diff_characters")
        if (
            not isinstance(review_digest, str)
            or len(review_digest) != 64
            or not isinstance(review_characters, int)
            or review_characters < 0
        ):
            raise ValueError("security-review artifact lacks its exact diff provenance")
        context_too_large = review_characters > _MAX_REVIEW_CONTEXT_CHARACTERS
        content_digest = hashlib.sha256(diff_content.encode("utf-8")).hexdigest()
        if not context_too_large and (
            len(diff_content) != review_characters or content_digest != review_digest
        ):
            raise ValueError("security-review content does not match the exact committed diff")
        checks = {
            str(item["criterion_id"]): item
            for item in await self.dod.get_checks(project_id, criterion_ids)
            if "security_review" in (item["required_evidence_types"] or [])
        }
        if not checks:
            raise ValueError("security review requires at least one criterion-authorized gate")

        async def review_one(criterion_id: str, criterion: dict[str, Any]) -> dict[str, Any]:
            request = ProviderRequest(
                purpose="safety_behavior_review",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Perform an independent application-security review of this exact "
                            "artifact against only the supplied DoD criterion. Assess authentication, "
                            "authorization, injection, secrets, transport, data isolation, unsafe "
                            "execution, and fail-open behavior. Return JSON only: "
                            '{"safe":bool,"findings":[str]}.'
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Criterion ID: {criterion_id}\nCriterion: {criterion['description']}\n"
                            f"Risk: {risk_level}\nTask acceptance: "
                            f"{json.dumps(task['acceptance_criteria'])}\nArtifact: {file_path} "
                            f"({artifact['checksum_sha256']})\nCriterion affected contracts: "
                            f"{json.dumps(criterion.get('affected_contracts') or [])}\n"
                            f"Task affected contracts: "
                            f"{json.dumps(task['affected_contracts'] or [])}\nDiff:\n{diff_content}"
                        ),
                    },
                ],
                budget_key=UUID(project_id),
                agent_id="security_reviewer-1",
                agent_role="security_reviewer",
                complexity=TaskComplexity.CRITICAL,
                required_capabilities={"chat", "json"},
            )
            cache_key = hashlib.sha256(
                json.dumps(
                    {
                        "kind": "security-review-v1",
                        "criterion_hash": criterion["criterion_hash"],
                        "subject_commit": subject_commit,
                        "artifact_checksum": artifact["checksum_sha256"],
                        "content_hash": content_digest,
                        "risk_level": risk_level,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()

            async def invoke_provider() -> dict[str, Any]:
                response = await self.provider.get_completion.remote(
                    request.model_dump(mode="json"), response_format={"type": "json_object"}
                )
                result = _SecurityReviewVerdict.model_validate(_json_object(response["content"]))
                return {
                    "safe": result.safe,
                    "findings": result.findings,
                    "run_status": "OK",
                    "provider": str(response.get("provider") or "unknown"),
                    "model": str(response.get("model") or "unknown"),
                }

            if context_too_large:
                safe = False
                cache_hit = False
                decision = {"run_status": "INCONCLUSIVE"}
                findings = [
                    "exact artifact diff exceeds the bounded independent-review context limit"
                ]
            else:
                try:
                    decision, cache_hit = await self.review_cache.get_or_run(
                        cache_key, invoke_provider
                    )
                    findings = [*deterministic_findings, *decision["findings"]]
                    safe = bool(decision["safe"]) and not deterministic_findings
                except Exception as error:
                    safe = False
                    cache_hit = False
                    decision = {"run_status": "INCONCLUSIVE"}
                    findings = [f"independent security review unavailable: {type(error).__name__}"]
            run_status = str(decision["run_status"])
            summary = (
                f"Criterion {criterion_id} independently security-reviewed for artifact "
                f"{artifact_id} ({artifact['checksum_sha256']}); no blocking findings"
                if safe
                else f"Criterion {criterion_id}: {'; '.join(findings)[:1800]}"
            )
            await self.dod.add_evidence(
                project_id,
                criterion_id,
                "security_review",
                "security_reviewer-1",
                summary=summary,
                passed=safe,
                artifact_id=artifact_id,
                task_id=task_id,
                source_role="security_reviewer",
                subject_commit=subject_commit,
                watched_paths=list(task["allowed_paths"] or []),
                affected_contracts=list(task["affected_contracts"] or []),
                run_status=run_status,
                metadata={
                    "file_path": file_path,
                    "risk_level": risk_level,
                    "cache_hit": cache_hit,
                    "review_cache_key": cache_key,
                    "prompt_version": _SECURITY_REVIEW_PROMPT_VERSION,
                    "provider": decision.get("provider"),
                    "model": decision.get("model"),
                },
            )
            return {
                "criterion_id": criterion_id,
                "safe": safe,
                "findings": findings,
                "run_status": run_status,
                "cache_hit": cache_hit,
            }

        results = list(
            await asyncio.gather(
                *(review_one(criterion_id, criterion) for criterion_id, criterion in checks.items())
            )
        )
        return {
            "safe": bool(results) and all(item["safe"] for item in results),
            "reviews": results,
            "findings": [finding for item in results for finding in item["findings"]],
        }

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
            result = _BehaviorReviewVerdict.model_validate(_json_object(response["content"]))
            return result.model_dump()
        except Exception as error:
            logger.error("safety_review_failed_closed", error_type=type(error).__name__)
            return {"safe": False, "reason": "safety reviewer unavailable; failed closed"}
