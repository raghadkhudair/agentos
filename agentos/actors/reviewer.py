from __future__ import annotations

import asyncio
import hashlib
import json
import re
from typing import Any
from uuid import UUID

import ray
import structlog

from agentos.actors.review_cache import CriterionReviewCache
from agentos.config.loader import runtime_tuning
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

        task = await self.db.fetchrow("SELECT * FROM tasks WHERE id=$1", UUID(task_id))
        artifact = await self.db.fetchrow("SELECT * FROM artifacts WHERE id=$1", UUID(artifact_id))
        if task is None or artifact is None or artifact["task_id"] != task["id"]:
            raise ValueError("review requires a task-bound durable artifact")
        checks = {
            str(item["criterion_id"]): item
            for item in await self.dod.get_checks(project_id, criterion_ids)
            if "review" in (item["required_evidence_types"] or [])
        }

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
                            f"Artifact: {file_path} ({artifact['checksum_sha256']})\n"
                            f"Diff:\n{code_content[:100_000]}"
                        ),
                    },
                ],
                budget_key=UUID(project_id),
                agent_id="code_reviewer-1",
                agent_role="code_reviewer",
                complexity=TaskComplexity.HIGH,
                required_capabilities={"chat", "json"},
            )
            subject_commit = (artifact["metadata"] or {}).get("git_commit")
            cache_key = hashlib.sha256(
                json.dumps(
                    {
                        "kind": "code-review-v1",
                        "criterion_hash": criterion["criterion_hash"],
                        "subject_commit": subject_commit,
                        "artifact_checksum": artifact["checksum_sha256"],
                        "content_hash": hashlib.sha256(code_content.encode()).hexdigest(),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()

            async def invoke_provider() -> dict[str, Any]:
                response = await self.provider.get_completion.remote(
                    request.model_dump(mode="json"), response_format={"type": "json_object"}
                )
                result = _json_object(response["content"])
                raw_findings = result.get("findings", [])
                return {
                    "approved": bool(result.get("approved")),
                    "score": result.get("score", 0),
                    "findings": (
                        [str(item) for item in raw_findings]
                        if isinstance(raw_findings, list)
                        else []
                    ),
                    "run_status": "OK",
                }

            try:
                decision, cache_hit = await self.review_cache.get_or_run(cache_key, invoke_provider)
                findings = [*deterministic_findings, *decision["findings"]]
                approved = bool(decision["approved"]) and not deterministic_findings
                raw_score = decision["score"]
                score = int(raw_score) if isinstance(raw_score, (str, int, float)) else 0
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
