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
_REVIEW_PROMPT_VERSION = "criterion-code-review-v1"
_MAX_REVIEW_CONTEXT_CHARACTERS = 100_000


class _CodeReviewVerdict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    approved: bool
    score: int = Field(ge=0, le=100)
    findings: list[str] = Field(default_factory=list, max_length=100)


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
        tuning = runtime_tuning()["review"]
        self.review_cache = CriterionReviewCache(
            int(tuning["max_parallel_criterion_reviews"]),
            int(tuning["revision_cache_entries"]),
        )

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
            raise ValueError("review requires a task-bound durable artifact")
        if str(artifact["title"]).replace("\\", "/") != file_path.replace("\\", "/"):
            raise ValueError("review file path does not identify the durable artifact")
        artifact_metadata = artifact["metadata"] or {}
        subject_commit = artifact_metadata.get("git_commit")
        if not isinstance(subject_commit, str) or not subject_commit:
            raise ValueError("review artifact lacks its authoritative Git subject revision")
        review_digest = artifact_metadata.get("review_diff_sha256")
        review_characters = artifact_metadata.get("review_diff_characters")
        if (
            not isinstance(review_digest, str)
            or len(review_digest) != 64
            or not isinstance(review_characters, int)
            or review_characters < 0
        ):
            raise ValueError("review artifact lacks its exact diff provenance")
        context_too_large = review_characters > _MAX_REVIEW_CONTEXT_CHARACTERS
        content_digest = hashlib.sha256(code_content.encode("utf-8")).hexdigest()
        if not context_too_large and (
            len(code_content) != review_characters or content_digest != review_digest
        ):
            raise ValueError("review content does not match the artifact's exact committed diff")
        requested_criteria = set(dict.fromkeys(criterion_ids))
        checks = {
            str(item["criterion_id"]): item
            for item in await self.dod.get_checks(project_id, criterion_ids)
            if "review" in (item["required_evidence_types"] or [])
        }
        if set(checks) != requested_criteria:
            raise ValueError("every requested criterion must authorize independent review evidence")

        async def review_one(criterion_id: str, criterion: dict[str, Any]) -> dict[str, Any]:
            request = ProviderRequest(
                purpose="review_code_patch",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Independently decide whether this one DoD criterion is satisfied by "
                            "the exact artifact and diff. Review correctness, maintainability, and "
                            "test adequacy. Return JSON only: "
                            '{"approved":bool,"score":0-100,"findings":[str]}.'
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Criterion ID: {criterion_id}\nCriterion: {criterion['description']}\n"
                            f"Task acceptance: {json.dumps(task['acceptance_criteria'])}\n"
                            f"Criterion affected contracts: "
                            f"{json.dumps(criterion.get('affected_contracts') or [])}\n"
                            f"Task affected contracts: "
                            f"{json.dumps(task['affected_contracts'] or [])}\n"
                            f"Artifact: {file_path} ({artifact['checksum_sha256']})\n"
                            f"Diff:\n{code_content}"
                        ),
                    },
                ],
                budget_key=UUID(project_id),
                agent_id="code_reviewer-1",
                agent_role="code_reviewer",
                complexity=TaskComplexity.HIGH,
                required_capabilities={"chat", "json"},
            )
            cache_key = hashlib.sha256(
                json.dumps(
                    {
                        "kind": "code-review-v1",
                        "criterion_hash": criterion["criterion_hash"],
                        "subject_commit": subject_commit,
                        "artifact_checksum": artifact["checksum_sha256"],
                        "content_hash": content_digest,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()

            async def invoke_provider() -> dict[str, Any]:
                response = await self.provider.get_completion.remote(
                    request.model_dump(mode="json"), response_format={"type": "json_object"}
                )
                result = _CodeReviewVerdict.model_validate(_json_object(response["content"]))
                return {
                    "approved": result.approved,
                    "score": result.score,
                    "findings": result.findings,
                    "run_status": "OK",
                    "provider": str(response.get("provider") or "unknown"),
                    "model": str(response.get("model") or "unknown"),
                }

            if context_too_large:
                decision = {"run_status": "INCONCLUSIVE"}
                cache_hit = False
                approved = False
                score = 0
                findings = [
                    "exact artifact diff exceeds the bounded independent-review context limit"
                ]
            else:
                try:
                    decision, cache_hit = await self.review_cache.get_or_run(
                        cache_key, invoke_provider
                    )
                    findings = [*deterministic_findings, *decision["findings"]]
                    approved = bool(decision["approved"]) and not deterministic_findings
                    score = int(decision["score"])
                except Exception as error:
                    approved = False
                    score = 0
                    cache_hit = False
                    decision = {"run_status": "INCONCLUSIVE"}
                    findings = [f"independent review unavailable: {type(error).__name__}"]
            run_status = str(decision["run_status"])
            summary = (
                f"Criterion {criterion_id} independently reviewed for artifact "
                f"{artifact_id} ({artifact['checksum_sha256']}); score={score}; no blocking findings"
                if approved
                else f"Criterion {criterion_id}: {'; '.join(findings)[:1800]}"
            )
            await self.dod.add_evidence(
                project_id,
                criterion_id,
                "review",
                "code_reviewer-1",
                summary=summary,
                passed=approved,
                artifact_id=artifact_id,
                task_id=task_id,
                source_role="code_reviewer",
                subject_commit=subject_commit,
                watched_paths=list(task["allowed_paths"] or []),
                affected_contracts=list(task["affected_contracts"] or []),
                run_status=run_status,
                metadata={
                    "file_path": file_path,
                    "score": score,
                    "cache_hit": cache_hit,
                    "review_cache_key": cache_key,
                    "prompt_version": _REVIEW_PROMPT_VERSION,
                    "provider": decision.get("provider"),
                    "model": decision.get("model"),
                },
            )
            return {
                "criterion_id": criterion_id,
                "approved": approved,
                "score": score,
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
            "approved": bool(results) and all(item["approved"] for item in results),
            "reviews": results,
            "findings": [finding for item in results for finding in item["findings"]],
        }
