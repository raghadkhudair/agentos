from __future__ import annotations

import fnmatch
import json
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

import ray
import structlog
from pydantic import BaseModel, Field

from agentos.config.settings import Settings
from agentos.messaging.events import Event, EventType
from agentos.storage.clients.minio import MinioObjectClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import DoDRepository, EventRepository

logger = structlog.get_logger()


class DoDItemStatus(BaseModel):
    criterion_id: str
    description: str
    status: str = "NOT_STARTED"
    evidence_summary: str = ""


class DoDEvaluation(BaseModel):
    project_id: str
    satisfied: bool
    items: list[DoDItemStatus]
    gaps: list[str] = Field(default_factory=list)


@ray.remote(num_cpus=0.1, max_concurrency=4)  # type: ignore[call-overload]
class DoDEvaluatorActor:
    """Evidence-only completion gate; model output is never completion evidence."""

    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.db = PostgresClient(self.settings)
        self.repo = DoDRepository(self.db)
        self.events = EventRepository(self.db)
        self.minio = MinioObjectClient(self.settings)

    async def _validate_artifact_evidence(self, project_id: str) -> list[tuple[str, str]]:
        rows = await self.db.fetch(
            """
            WITH current_artifacts AS (
              SELECT DISTINCT ON (
                e.criterion_id, COALESCE(e.metadata->>'task_id',e.source_agent_id), a.title
              ) e.*
              FROM dod_evidence e
              JOIN artifacts a ON a.id=e.artifact_id AND a.project_id=e.project_id
              WHERE e.project_id=$1 AND e.evidence_type='artifact'
              ORDER BY e.criterion_id,COALESCE(e.metadata->>'task_id',e.source_agent_id),
                       a.title,e.created_at DESC
            )
            SELECT e.criterion_id,a.id,a.object_uri,a.checksum_sha256,a.content_length
            FROM current_artifacts e
            JOIN artifacts a ON a.id=e.artifact_id AND a.project_id=e.project_id
            WHERE e.passed
            """,
            UUID(project_id),
        )
        failures: list[tuple[str, str]] = []
        for row in rows:
            parsed = urlparse(row["object_uri"])
            if parsed.scheme != "minio":
                failures.append((row["criterion_id"], "artifact URI is not a MinIO object"))
                continue
            try:
                version = parse_qs(parsed.query).get("versionId", [None])[0]
                metadata = await self.minio.stat(
                    bucket=parsed.netloc,
                    object_name=parsed.path.lstrip("/"),
                    version_id=version,
                )
                if metadata.size != row["content_length"]:
                    failures.append(
                        (row["criterion_id"], "artifact length does not match durable record")
                    )
                if not metadata.sha256 or metadata.sha256 != row["checksum_sha256"]:
                    failures.append(
                        (row["criterion_id"], "artifact checksum does not match durable record")
                    )
            except Exception as error:
                failures.append(
                    (row["criterion_id"], f"artifact unavailable: {type(error).__name__}")
                )
        return failures

    async def _validate_criterion_contracts(self, project_id: str) -> list[tuple[str, str]]:
        checks = await self.repo.get_project_dod_status(project_id)
        failures: list[tuple[str, str]] = []
        for check in checks:
            criterion_id = str(check["criterion_id"])
            required_artifacts = list(check.get("required_artifacts") or [])
            if required_artifacts:
                rows = await self.db.fetch(
                    """
                    WITH current_artifacts AS (
                      SELECT DISTINCT ON (
                        e.criterion_id,COALESCE(e.metadata->>'task_id',e.source_agent_id),a.title
                      ) e.*,a.title
                      FROM dod_evidence e
                      JOIN artifacts a ON a.id=e.artifact_id AND a.project_id=e.project_id
                      WHERE e.project_id=$1 AND e.criterion_id=$2
                        AND e.evidence_type='artifact'
                      ORDER BY e.criterion_id,COALESCE(e.metadata->>'task_id',e.source_agent_id),
                               a.title,e.created_at DESC
                    )
                    SELECT DISTINCT a.title FROM current_artifacts e
                    JOIN artifacts a ON a.id=e.artifact_id
                      AND a.project_id=e.project_id
                    WHERE e.passed
                    """,
                    UUID(project_id),
                    criterion_id,
                )
                titles = [str(row["title"]).replace("\\", "/") for row in rows]
                missing = [
                    pattern
                    for pattern in required_artifacts
                    if not any(
                        fnmatch.fnmatch(title, str(pattern).replace("\\", "/")) for title in titles
                    )
                ]
                if missing:
                    failures.append(
                        (criterion_id, f"required artifacts missing: {', '.join(missing)}")
                    )
            required_command = list(check.get("verification_command") or [])
            if required_command:
                command_row = await self.db.fetchrow(
                    """
                    SELECT command,passed FROM dod_evidence
                    WHERE project_id=$1 AND criterion_id=$2 AND evidence_type IN ('test','command')
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    UUID(project_id),
                    criterion_id,
                )
                actual_command: list[str] = []
                if command_row and command_row["command"]:
                    try:
                        parsed = json.loads(command_row["command"])
                        if isinstance(parsed, list):
                            actual_command = [str(item) for item in parsed]
                    except json.JSONDecodeError:
                        actual_command = []
                if (
                    not command_row
                    or not command_row["passed"]
                    or actual_command != required_command
                ):
                    failures.append((criterion_id, "required verification command has not passed"))
        return failures

    async def _validate_current_execution_evidence(self, project_id: str) -> list[tuple[str, str]]:
        failures: list[tuple[str, str]] = []
        rows = await self.db.fetch(
            """
            WITH latest AS (
              SELECT DISTINCT ON (
                project_id,criterion_id,evidence_type,
                COALESCE(metadata->>'task_id',source_agent_id)
              ) *
              FROM dod_evidence
              WHERE project_id=$1 AND evidence_type IN ('test','command','review','security_review')
              ORDER BY project_id,criterion_id,evidence_type,
                       COALESCE(metadata->>'task_id',source_agent_id),created_at DESC
            )
            SELECT e.*,t.owner_agent_id,a.role AS reviewer_role
            FROM latest e
            LEFT JOIN tasks t
              ON t.id=(NULLIF(e.metadata->>'task_id',''))::uuid AND t.project_id=e.project_id
            LEFT JOIN agents a
              ON a.project_id=e.project_id AND a.id=e.source_agent_id
            """,
            UUID(project_id),
        )
        for row in rows:
            criterion_id = str(row["criterion_id"])
            evidence_type = str(row["evidence_type"])
            if evidence_type in {"test", "command"} and (
                not row["passed"] or row["exit_code"] != 0
            ):
                failures.append((criterion_id, "latest command evidence did not exit successfully"))
            if evidence_type in {"review", "security_review"} and row["passed"]:
                expected_role = (
                    "security_reviewer" if evidence_type == "security_review" else "code_reviewer"
                )
                if row["source_agent_id"] == row["owner_agent_id"]:
                    failures.append((criterion_id, "review evidence is not independent"))
                elif row["reviewer_role"] != expected_role:
                    failures.append((criterion_id, f"{evidence_type} producer role is invalid"))
        return failures

    async def _validate_mapped_tasks(self, project_id: str) -> list[tuple[str, str]]:
        rows = await self.db.fetch(
            """
            SELECT c.criterion_id,t.id AS task_id,t.status,
              EXISTS(
                SELECT 1 FROM dod_evidence e
                WHERE e.project_id=t.project_id AND e.criterion_id=c.criterion_id
                  AND e.metadata->>'task_id'=t.id::text
                  AND e.evidence_type='integration' AND e.passed
              ) AS integrated
            FROM dod_checks c
            JOIN tasks t ON t.project_id=c.project_id AND c.criterion_id=ANY(t.dod_criteria)
            WHERE c.project_id=$1
            """,
            UUID(project_id),
        )
        failures: list[tuple[str, str]] = []
        for row in rows:
            if row["status"] != "COMPLETED" or not row["integrated"]:
                failures.append(
                    (
                        str(row["criterion_id"]),
                        f"mapped task {row['task_id']} is not completed and integrated",
                    )
                )
        return failures

    async def evaluate(self, project_id: str, dod: list[Any] | None = None) -> dict[str, Any]:
        del dod
        rows = await self.repo.evaluate_and_persist(project_id)
        contract_failures = [
            *await self._validate_artifact_evidence(project_id),
            *await self._validate_criterion_contracts(project_id),
            *await self._validate_current_execution_evidence(project_id),
            *await self._validate_mapped_tasks(project_id),
        ]
        for criterion_id, reason in contract_failures:
            await self.repo.update_criterion_status(
                project_id,
                criterion_id,
                "FAILED_VERIFICATION",
                "dod_evaluator",
                reason,
            )
        failure_map = {criterion: reason for criterion, reason in contract_failures}
        items: list[DoDItemStatus] = []
        gaps: list[str] = []
        for row in rows:
            status = "FAILED_VERIFICATION" if row["criterion_id"] in failure_map else row["status"]
            summary = failure_map.get(row["criterion_id"], row.get("evidence_summary", ""))
            item = DoDItemStatus(
                criterion_id=row["criterion_id"],
                description=row["description"],
                status=status,
                evidence_summary=summary,
            )
            items.append(item)
            if status != "SATISFIED":
                gaps.append(row["criterion_id"])
        satisfied = bool(items) and not gaps
        event = Event(
            project_id=project_id,
            event_type=EventType.DOD_EVALUATED,
            producer_agent_id="dod_evaluator",
            payload={"satisfied": satisfied, "gaps": gaps},
        )
        await self.events.save_event(project_id, event)
        logger.info("dod_evaluated", project_id=project_id, satisfied=satisfied, gaps=gaps)
        return DoDEvaluation(
            project_id=project_id,
            satisfied=satisfied,
            items=items,
            gaps=gaps,
        ).model_dump(mode="json")
