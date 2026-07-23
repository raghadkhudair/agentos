from __future__ import annotations

import asyncio
import fnmatch
import hashlib
import json
from collections import defaultdict
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import ray
import structlog
from git import Repo
from pydantic import BaseModel, Field

from agentos.config.loader import runtime_tuning
from agentos.config.settings import Settings
from agentos.messaging.events import Event, EventType
from agentos.runtime.team_plan import EvidenceScope, EvidenceType, _patterns_overlap
from agentos.storage.clients.minio import MinioObjectClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import DoDRepository, EventRepository

logger = structlog.get_logger()


def _path_dependency_overlap(path: str, dependency: str) -> bool:
    """Conservatively match concrete changed paths to files, directories, or globs."""

    changed = path.replace("\\", "/").strip("/")
    pattern = dependency.replace("\\", "/").strip("/")
    literal_prefix = pattern.split("*", 1)[0].split("?", 1)[0].split("[", 1)[0]
    return bool(
        changed == pattern
        or changed.startswith(f"{pattern}/")
        or fnmatch.fnmatch(changed, pattern)
        or (literal_prefix and changed.startswith(literal_prefix))
    )


def _revision_freshness_reason(
    repository: Repo,
    row: dict[str, Any],
    integration_head: str | None,
    evidence: list[dict[str, Any]],
) -> tuple[str, str] | None:
    """Return a typed reason when revision-bound artifact/review evidence is not fresh."""

    subject_commit = row.get("subject_commit")
    if not subject_commit or not integration_head:
        return (
            "EVIDENCE_FRESHNESS_INCONCLUSIVE",
            "evidence or evaluation snapshot lacks a resolvable subject revision",
        )
    if subject_commit == integration_head:
        return None
    try:
        subject = repository.commit(str(subject_commit))
        head = repository.commit(str(integration_head))
        if not repository.is_ancestor(subject, head):
            return (
                "EVIDENCE_STALE_REVISION",
                "evidence subject commit is not an ancestor of the integrated HEAD",
            )
        changed_paths = [
            item.replace("\\", "/")
            for item in repository.git.diff(
                "--name-only", subject.hexsha, head.hexsha, "--"
            ).splitlines()
            if item
        ]
    except Exception:
        return (
            "EVIDENCE_FRESHNESS_INCONCLUSIVE",
            "the evidence revision could not be compared with the integrated HEAD",
        )

    watched_paths = [str(item) for item in row.get("watched_paths") or []]
    touched = sorted(
        path
        for path in changed_paths
        if any(_path_dependency_overlap(path, dependency) for dependency in watched_paths)
    )
    if touched:
        return (
            "EVIDENCE_STALE_PATH",
            f"later integrated changes overlap watched paths: {', '.join(touched[:20])}",
        )

    affected_contracts = [str(item) for item in row.get("affected_contracts") or []]
    if affected_contracts:
        for candidate in evidence:
            if candidate.get("evidence_type") != EvidenceType.INTEGRATION.value:
                continue
            if candidate.get("task_id") == row.get("task_id"):
                continue
            candidate_contracts = [str(item) for item in candidate.get("affected_contracts") or []]
            if not any(
                _patterns_overlap(expected, changed)
                for expected in affected_contracts
                for changed in candidate_contracts
            ):
                continue
            candidate_commit = candidate.get("integration_commit")
            if not candidate_commit:
                continue
            try:
                integrated = repository.commit(str(candidate_commit))
                if (
                    integrated.hexsha != subject.hexsha
                    and repository.is_ancestor(subject, integrated)
                    and repository.is_ancestor(integrated, head)
                ):
                    return (
                        "EVIDENCE_STALE_CONTRACT",
                        "a later task changed an affected contract used by this evidence",
                    )
            except Exception:
                return (
                    "EVIDENCE_FRESHNESS_INCONCLUSIVE",
                    "an affected-contract revision could not be resolved",
                )

    if changed_paths and not watched_paths and not affected_contracts:
        return (
            "EVIDENCE_STALE_UNSCOPED",
            "unscoped evidence predates later integrated changes and was invalidated conservatively",
        )
    return None


def _classify_item_status(reason_codes: set[str]) -> str:
    """Apply the fail-closed precedence used by every persisted criterion verdict."""

    if any(code.endswith("INCONCLUSIVE") for code in reason_codes):
        return "INCONCLUSIVE"
    if any("STALE" in code for code in reason_codes):
        return "STALE"
    if reason_codes.intersection(
        {
            "EVIDENCE_FAILED",
            "ARTIFACT_LENGTH_MISMATCH",
            "ARTIFACT_CHECKSUM_MISMATCH",
            "ARTIFACT_URI_INVALID",
            "ARTIFACT_VERSION_MISMATCH",
            "ARTIFACT_REVISION_MISMATCH",
            "COMMAND_CONTRACT_MISMATCH",
            "COMMAND_DIGEST_MISMATCH",
            "SANDBOX_DIGEST_MISSING",
            "REVIEW_DIFF_PROVENANCE_MISSING",
            "WAIVER_INVALID",
        }
    ):
        return "FAILED"
    if reason_codes:
        return "MISSING"
    return "SATISFIED"


class DoDGap(BaseModel):
    criterion_id: str | None = None
    code: str
    message: str
    evidence_type: str | None = None
    scope: str | None = None
    task_id: str | None = None
    artifact_id: str | None = None
    retryable: bool = True
    suggested_owner_role: str = "pm_tech_lead"


class DoDItemStatus(BaseModel):
    criterion_id: str
    criterion_hash: str
    description: str
    status: str
    reasons: list[DoDGap] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)


class DoDEvaluation(BaseModel):
    project_id: str
    evaluation_run_id: str
    contract_version: int
    contract_hash: str
    integration_head: str | None
    evidence_generation: int
    satisfied: bool
    status: str
    items: list[DoDItemStatus]
    gaps: list[DoDGap] = Field(default_factory=list)


def _gap(
    code: str,
    message: str,
    *,
    evidence_type: str | None = None,
    scope: str | None = None,
    task_id: str | None = None,
    artifact_id: str | None = None,
    retryable: bool | None = None,
    suggested_owner_role: str | None = None,
) -> DoDGap:
    if suggested_owner_role is None:
        if evidence_type == EvidenceType.SECURITY_REVIEW.value:
            suggested_owner_role = "security_reviewer"
        elif evidence_type == EvidenceType.REVIEW.value:
            suggested_owner_role = "code_reviewer"
        elif evidence_type in {EvidenceType.TEST.value, EvidenceType.COMMAND.value}:
            suggested_owner_role = "qa_engineer"
        elif "INCONCLUSIVE" in code or code in {
            "REPOSITORY_HEAD_DRIFT",
            "ARTIFACT_URI_INVALID",
        }:
            suggested_owner_role = "platform_engineer"
        else:
            suggested_owner_role = "pm_tech_lead"
    return DoDGap(
        code=code,
        message=message,
        evidence_type=evidence_type,
        scope=scope,
        task_id=task_id,
        artifact_id=artifact_id,
        retryable=(code != "WAIVER_INVALID" if retryable is None else retryable),
        suggested_owner_role=suggested_owner_role,
    )


@ray.remote(num_cpus=0.1, max_concurrency=1)  # type: ignore[call-overload]
class DoDEvaluatorActor:
    """Evaluate one immutable contract/evidence/HEAD snapshot and persist every reason."""

    def __init__(self, settings_payload: dict[str, Any]):
        self.settings = Settings(**settings_payload)
        self.db = PostgresClient(self.settings)
        self.repo = DoDRepository(self.db)
        self.events = EventRepository(self.db)
        self.minio = MinioObjectClient(self.settings)
        self.evaluator_instance_id = f"dod_evaluator:{uuid4()}"

    def _is_ancestor(self, project_id: str, commit: str | None, head: str | None) -> bool | None:
        if not commit or not head:
            return None
        if commit == head:
            return True
        try:
            repository = Repo(self.settings.workspace / project_id / "repository")
            return bool(repository.is_ancestor(repository.commit(commit), repository.commit(head)))
        except Exception:
            return None

    def _revision_gap(
        self,
        project_id: str,
        row: dict[str, Any],
        integration_head: str | None,
        evidence: list[dict[str, Any]],
        *,
        evidence_type: EvidenceType,
        scope: EvidenceScope,
        task_id: str,
        artifact_id: str,
    ) -> DoDGap | None:
        try:
            repository = Repo(self.settings.workspace / project_id / "repository")
            result = _revision_freshness_reason(repository, row, integration_head, evidence)
        except Exception:
            result = (
                "EVIDENCE_FRESHNESS_INCONCLUSIVE",
                "the managed repository is unavailable for evidence freshness validation",
            )
        if result is None:
            return None
        code, message = result
        return _gap(
            code,
            message,
            evidence_type=evidence_type.value,
            scope=scope.value,
            task_id=task_id,
            artifact_id=artifact_id,
        )

    async def _artifact_health(self, artifact: dict[str, Any]) -> DoDGap | None:
        parsed = urlparse(str(artifact["object_uri"]))
        if parsed.scheme != "minio":
            return _gap(
                "ARTIFACT_URI_INVALID",
                "artifact URI is not a versioned MinIO object",
                evidence_type=EvidenceType.ARTIFACT.value,
                scope=EvidenceScope.ARTIFACT.value,
                artifact_id=str(artifact["id"]),
            )
        try:
            versions = parse_qs(parsed.query).get("versionId", [])
            if len(versions) != 1 or versions[0] != artifact.get("object_version_id"):
                return _gap(
                    "ARTIFACT_VERSION_MISMATCH",
                    "artifact URI and durable record do not identify one exact object version",
                    evidence_type=EvidenceType.ARTIFACT.value,
                    scope=EvidenceScope.ARTIFACT.value,
                    artifact_id=str(artifact["id"]),
                )
            version = versions[0]
            metadata = await self.minio.stat(
                bucket=parsed.netloc,
                object_name=parsed.path.lstrip("/"),
                version_id=version,
            )
        except Exception as error:
            return _gap(
                "ARTIFACT_STORE_INCONCLUSIVE",
                f"artifact store could not be verified: {type(error).__name__}",
                evidence_type=EvidenceType.ARTIFACT.value,
                scope=EvidenceScope.ARTIFACT.value,
                artifact_id=str(artifact["id"]),
            )
        if metadata.size != artifact["content_length"]:
            return _gap(
                "ARTIFACT_LENGTH_MISMATCH",
                "artifact length does not match the durable record",
                evidence_type=EvidenceType.ARTIFACT.value,
                scope=EvidenceScope.ARTIFACT.value,
                artifact_id=str(artifact["id"]),
            )
        if not metadata.sha256 or metadata.sha256 != artifact["checksum_sha256"]:
            return _gap(
                "ARTIFACT_CHECKSUM_MISMATCH",
                "artifact checksum does not match the durable record",
                evidence_type=EvidenceType.ARTIFACT.value,
                scope=EvidenceScope.ARTIFACT.value,
                artifact_id=str(artifact["id"]),
            )
        if metadata.version_id != version:
            return _gap(
                "ARTIFACT_VERSION_MISMATCH",
                "artifact store returned a different object version",
                evidence_type=EvidenceType.ARTIFACT.value,
                scope=EvidenceScope.ARTIFACT.value,
                artifact_id=str(artifact["id"]),
            )
        return None

    @staticmethod
    def _latest(
        evidence: list[dict[str, Any]],
        evidence_type: str,
        task_id: str | None = None,
        artifact_id: str | None = None,
    ) -> dict[str, Any] | None:
        matches = [
            row
            for row in evidence
            if row["evidence_type"] == evidence_type
            and (task_id is None or str(row.get("task_id")) == task_id)
            and (artifact_id is None or str(row.get("artifact_id")) == artifact_id)
        ]
        return (
            max(matches, key=lambda row: (row["created_at"], str(row["id"]))) if matches else None
        )

    async def _evaluate_criterion(
        self,
        project_id: str,
        run: dict[str, Any],
        check: dict[str, Any],
        tasks: list[dict[str, Any]],
        evidence: list[dict[str, Any]],
        artifacts: list[dict[str, Any]],
        all_evidence: list[dict[str, Any]],
    ) -> DoDItemStatus:
        criterion_id = str(check["criterion_id"])
        reasons: list[DoDGap] = []
        used: set[str] = set()
        required = [EvidenceType(item) for item in check["required_evidence_types"] or []]
        scopes = dict(check.get("evidence_scopes") or {})
        mapped_tasks = [task for task in tasks if criterion_id in (task["dod_criteria"] or [])]
        if check["status"] == "WAIVED_BY_HUMAN":
            approval = await self.db.fetchval(
                "SELECT status FROM approval_requests WHERE id=$1",
                check["waiver_approval_id"],
            )
            if approval == "APPROVED":
                return DoDItemStatus(
                    criterion_id=criterion_id,
                    criterion_hash=str(check["criterion_hash"]),
                    description=str(check["description"]),
                    status="WAIVED_BY_HUMAN",
                )
            reasons.append(_gap("WAIVER_INVALID", "waiver lacks a current approved decision"))
        if check["mandatory"] and not mapped_tasks:
            reasons.append(_gap("TASK_MAPPING_MISSING", "mandatory criterion has no mapped tasks"))
        for task in mapped_tasks:
            task_id = str(task["id"])
            if task["status"] != "COMPLETED":
                reasons.append(
                    _gap(
                        "TASK_INCOMPLETE",
                        f"mapped task is {task['status']}",
                        scope=EvidenceScope.TASK.value,
                        task_id=task_id,
                    )
                )

        criterion_artifacts = [
            artifact
            for artifact in artifacts
            if any(artifact["task_id"] == task["id"] for task in mapped_tasks)
        ]
        required_patterns = list(check.get("required_artifacts") or [])
        for pattern in required_patterns:
            matches = [
                artifact
                for artifact in criterion_artifacts
                if fnmatch.fnmatch(
                    str(artifact["title"]).replace("\\", "/"), str(pattern).replace("\\", "/")
                )
            ]
            if not matches:
                reasons.append(
                    _gap(
                        "ARTIFACT_PATTERN_MISSING",
                        f"required artifact pattern has no durable match: {pattern}",
                        evidence_type=EvidenceType.ARTIFACT.value,
                        scope=EvidenceScope.ARTIFACT.value,
                    )
                )

        for evidence_type in required:
            scope = EvidenceScope(scopes.get(evidence_type.value, EvidenceScope.CRITERION.value))
            if scope == EvidenceScope.CRITERION:
                row = self._latest(evidence, evidence_type.value)
                if row is None:
                    reasons.append(
                        _gap(
                            "EVIDENCE_MISSING",
                            f"criterion-scoped {evidence_type.value} evidence is missing",
                            evidence_type=evidence_type.value,
                            scope=scope.value,
                        )
                    )
                    continue
                used.add(str(row["id"]))
                if row["run_status"] == "INCONCLUSIVE":
                    reasons.append(
                        _gap(
                            "EVIDENCE_INCONCLUSIVE",
                            row["summary"],
                            evidence_type=evidence_type.value,
                            scope=scope.value,
                        )
                    )
                elif not row["passed"]:
                    reasons.append(
                        _gap(
                            "EVIDENCE_FAILED",
                            row["summary"],
                            evidence_type=evidence_type.value,
                            scope=scope.value,
                        )
                    )
                if evidence_type in {EvidenceType.TEST, EvidenceType.COMMAND}:
                    try:
                        actual_command = json.loads(row["command"] or "[]")
                    except json.JSONDecodeError:
                        actual_command = []
                    if actual_command != list(check["verification_command"] or []):
                        reasons.append(
                            _gap(
                                "COMMAND_CONTRACT_MISMATCH",
                                "executed command differs from the criterion contract",
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                            )
                        )
                    canonical_command = json.dumps(actual_command, separators=(",", ":"))
                    expected_digest = hashlib.sha256(canonical_command.encode("utf-8")).hexdigest()
                    if row.get("command_digest") != expected_digest:
                        reasons.append(
                            _gap(
                                "COMMAND_DIGEST_MISMATCH",
                                "recorded command digest does not match the executed token array",
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                            )
                        )
                    sandbox_digest = str(row.get("sandbox_digest") or "")
                    if len(sandbox_digest) != 64 or any(
                        character not in "0123456789abcdef" for character in sandbox_digest.lower()
                    ):
                        reasons.append(
                            _gap(
                                "SANDBOX_DIGEST_MISSING",
                                "verification lacks a valid governed sandbox configuration digest",
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                            )
                        )
                    if row["subject_commit"] != run["integration_head"]:
                        reasons.append(
                            _gap(
                                "EVIDENCE_STALE_HEAD",
                                "verification did not run against the current integrated HEAD",
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                            )
                        )
            elif scope == EvidenceScope.TASK:
                for task in mapped_tasks:
                    task_id = str(task["id"])
                    row = self._latest(evidence, evidence_type.value, task_id=task_id)
                    if row is None:
                        reasons.append(
                            _gap(
                                "EVIDENCE_MISSING",
                                f"task-scoped {evidence_type.value} evidence is missing",
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                                task_id=task_id,
                            )
                        )
                        continue
                    used.add(str(row["id"]))
                    if row["run_status"] == "INCONCLUSIVE":
                        reasons.append(
                            _gap(
                                "EVIDENCE_INCONCLUSIVE",
                                row["summary"],
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                                task_id=task_id,
                            )
                        )
                    elif not row["passed"]:
                        reasons.append(
                            _gap(
                                "EVIDENCE_FAILED",
                                row["summary"],
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                                task_id=task_id,
                            )
                        )
                    if evidence_type == EvidenceType.INTEGRATION:
                        integration_ancestry = self._is_ancestor(
                            project_id, row["integration_commit"], run["integration_head"]
                        )
                        subject_ancestry = self._is_ancestor(
                            project_id, row["subject_commit"], row["integration_commit"]
                        )
                        if integration_ancestry is None or subject_ancestry is None:
                            reasons.append(
                                _gap(
                                    "INTEGRATION_ANCESTRY_INCONCLUSIVE",
                                    "integration or task subject ancestry could not be verified",
                                    evidence_type=evidence_type.value,
                                    scope=scope.value,
                                    task_id=task_id,
                                )
                            )
                        elif not integration_ancestry or not subject_ancestry:
                            reasons.append(
                                _gap(
                                    "INTEGRATION_STALE_HEAD",
                                    "task subject and integration commits are not ancestors of current HEAD",
                                    evidence_type=evidence_type.value,
                                    scope=scope.value,
                                    task_id=task_id,
                                )
                            )
            else:
                for task in mapped_tasks:
                    task_id = str(task["id"])
                    task_artifacts = [
                        artifact
                        for artifact in criterion_artifacts
                        if artifact["task_id"] == task["id"]
                    ]
                    if not task_artifacts:
                        reasons.append(
                            _gap(
                                "TASK_ARTIFACT_MISSING",
                                "mapped task has no durable artifact",
                                evidence_type=evidence_type.value,
                                scope=scope.value,
                                task_id=task_id,
                            )
                        )
                    for artifact in task_artifacts:
                        artifact_id = str(artifact["id"])
                        row = self._latest(
                            evidence,
                            evidence_type.value,
                            task_id=task_id,
                            artifact_id=artifact_id,
                        )
                        if row is None:
                            reasons.append(
                                _gap(
                                    "EVIDENCE_MISSING",
                                    f"artifact-scoped {evidence_type.value} evidence is missing",
                                    evidence_type=evidence_type.value,
                                    scope=scope.value,
                                    task_id=task_id,
                                    artifact_id=artifact_id,
                                )
                            )
                            continue
                        used.add(str(row["id"]))
                        if row["run_status"] == "INCONCLUSIVE":
                            reasons.append(
                                _gap(
                                    "EVIDENCE_INCONCLUSIVE",
                                    row["summary"],
                                    evidence_type=evidence_type.value,
                                    scope=scope.value,
                                    task_id=task_id,
                                    artifact_id=artifact_id,
                                )
                            )
                        elif not row["passed"]:
                            reasons.append(
                                _gap(
                                    "EVIDENCE_FAILED",
                                    row["summary"],
                                    evidence_type=evidence_type.value,
                                    scope=scope.value,
                                    task_id=task_id,
                                    artifact_id=artifact_id,
                                )
                            )
                        if evidence_type in {
                            EvidenceType.ARTIFACT,
                            EvidenceType.REVIEW,
                            EvidenceType.SECURITY_REVIEW,
                        }:
                            revision_gap = self._revision_gap(
                                project_id,
                                row,
                                run["integration_head"],
                                all_evidence,
                                evidence_type=evidence_type,
                                scope=scope,
                                task_id=task_id,
                                artifact_id=artifact_id,
                            )
                            if revision_gap:
                                reasons.append(revision_gap)
                            artifact_commit = (artifact.get("metadata") or {}).get("git_commit")
                            if not artifact_commit or row.get("subject_commit") != artifact_commit:
                                reasons.append(
                                    _gap(
                                        "ARTIFACT_REVISION_MISMATCH",
                                        "artifact evidence does not target the artifact's Git revision",
                                        evidence_type=evidence_type.value,
                                        scope=scope.value,
                                        task_id=task_id,
                                        artifact_id=artifact_id,
                                    )
                                )
                            if evidence_type in {
                                EvidenceType.REVIEW,
                                EvidenceType.SECURITY_REVIEW,
                            }:
                                artifact_metadata = artifact.get("metadata") or {}
                                review_digest = artifact_metadata.get("review_diff_sha256")
                                review_characters = artifact_metadata.get("review_diff_characters")
                                if (
                                    not isinstance(review_digest, str)
                                    or len(review_digest) != 64
                                    or not isinstance(review_characters, int)
                                    or review_characters < 0
                                ):
                                    reasons.append(
                                        _gap(
                                            "REVIEW_DIFF_PROVENANCE_MISSING",
                                            "review evidence lacks checksum-bound committed diff provenance",
                                            evidence_type=evidence_type.value,
                                            scope=scope.value,
                                            task_id=task_id,
                                            artifact_id=artifact_id,
                                        )
                                    )
                        if evidence_type == EvidenceType.ARTIFACT:
                            health_gap = await self._artifact_health(artifact)
                            if health_gap:
                                health_gap.task_id = task_id
                                reasons.append(health_gap)

        status = _classify_item_status({reason.code for reason in reasons})
        for reason in reasons:
            reason.criterion_id = criterion_id
            if reason.task_id:
                responsible_task: dict[str, Any] | None = None
                for candidate in mapped_tasks:
                    if str(candidate["id"]) == reason.task_id:
                        responsible_task = candidate
                        break
                if responsible_task and reason.suggested_owner_role == "pm_tech_lead":
                    reason.suggested_owner_role = str(
                        responsible_task.get("owner_role") or "pm_tech_lead"
                    )
        return DoDItemStatus(
            criterion_id=criterion_id,
            criterion_hash=str(check["criterion_hash"]),
            description=str(check["description"]),
            status=status,
            reasons=reasons,
            evidence_ids=sorted(used),
        )

    async def evaluate(self, project_id: str, dod: list[Any] | None = None) -> dict[str, Any]:
        del dod
        run = await self.repo.start_evaluation(project_id, self.evaluator_instance_id)
        if run.get("reused_running"):
            deadline = asyncio.get_running_loop().time() + float(
                runtime_tuning()["dod"]["recovery_scan_seconds"]
            )
            while asyncio.get_running_loop().time() < deadline:
                current_status = await self.db.fetchval(
                    "SELECT status FROM dod_evaluation_runs WHERE id=$1", run["id"]
                )
                if current_status != "RUNNING":
                    return await self.evaluate(project_id)
                await asyncio.sleep(0.5)
            raise TimeoutError("coalesced DoD evaluation did not reach a durable terminal state")
        if run.get("reused"):
            rows = await self.db.fetch(
                """
                SELECT i.*,c.description FROM dod_evaluation_items i
                JOIN dod_checks c ON c.project_id=i.project_id
                  AND c.criterion_id=i.criterion_id AND c.active
                WHERE i.evaluation_run_id=$1 ORDER BY i.created_at
                """,
                run["id"],
            )
            items = [
                DoDItemStatus(
                    criterion_id=str(row["criterion_id"]),
                    criterion_hash=str(row["criterion_hash"]),
                    description=str(row["description"]),
                    status=str(row["status"]),
                    reasons=[DoDGap.model_validate(item) for item in row["reasons"] or []],
                    evidence_ids=[str(item) for item in row["evidence_ids"] or []],
                )
                for row in rows
            ]
            gaps = [reason for item in items for reason in item.reasons]
            return DoDEvaluation(
                project_id=project_id,
                evaluation_run_id=str(run["id"]),
                contract_version=int(run["contract_version"]),
                contract_hash=str(run["contract_hash"]),
                integration_head=run["integration_head"],
                evidence_generation=int(run["evidence_generation"]),
                satisfied=run["status"] == "SATISFIED",
                status=str(run["status"]),
                items=items,
                gaps=gaps,
            ).model_dump(mode="json")
        checks = [
            dict(row)
            for row in await self.db.fetch(
                """
                SELECT * FROM dod_checks WHERE project_id=$1 AND active AND contract_version=$2
                ORDER BY created_at
                """,
                UUID(project_id),
                run["contract_version"],
            )
        ]
        tasks = [
            dict(row)
            for row in await self.db.fetch(
                "SELECT * FROM tasks WHERE project_id=$1 AND dod_contract_version=$2",
                UUID(project_id),
                run["contract_version"],
            )
        ]
        evidence_rows = [
            dict(row)
            for row in await self.db.fetch(
                """
                SELECT * FROM dod_evidence WHERE project_id=$1 AND contract_version=$2
                  AND evidence_generation<=$3 AND created_at<=$4 ORDER BY created_at
                """,
                UUID(project_id),
                run["contract_version"],
                run["evidence_generation"],
                run["evidence_cutoff"],
            )
        ]
        evidence_by_criterion: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in evidence_rows:
            evidence_by_criterion[str(row["criterion_id"])].append(row)
        artifacts = [
            dict(row)
            for row in await self.db.fetch(
                "SELECT * FROM artifacts WHERE project_id=$1", UUID(project_id)
            )
        ]
        items = [
            await self._evaluate_criterion(
                project_id,
                run,
                check,
                tasks,
                evidence_by_criterion[str(check["criterion_id"])],
                artifacts,
                evidence_rows,
            )
            for check in checks
        ]
        try:
            observed_head = Repo(
                self.settings.workspace / project_id / "repository"
            ).head.commit.hexsha
        except Exception:
            observed_head = None
        if run["integration_head"] and observed_head != run["integration_head"]:
            for item in items:
                check = next(row for row in checks if row["criterion_id"] == item.criterion_id)
                if not check["mandatory"]:
                    continue
                item.reasons.append(
                    _gap(
                        "REPOSITORY_HEAD_DRIFT",
                        "managed repository HEAD differs from the fenced integration HEAD",
                        scope=EvidenceScope.CRITERION.value,
                    )
                )
                item.reasons[-1].criterion_id = item.criterion_id
                item.status = "STALE"
        mandatory_ids = {str(check["criterion_id"]) for check in checks if check["mandatory"]}
        mandatory_items = [item for item in items if item.criterion_id in mandatory_ids]
        satisfied = bool(mandatory_items) and all(
            item.status in {"SATISFIED", "WAIVED_BY_HUMAN"} for item in mandatory_items
        )
        all_gaps = [reason for item in items for reason in item.reasons]
        if satisfied:
            status = "SATISFIED"
        elif any(item.status == "INCONCLUSIVE" for item in mandatory_items):
            status = "INCONCLUSIVE"
        elif any(item.status == "STALE" for item in mandatory_items):
            status = "STALE"
        else:
            status = "UNSATISFIED"
        persisted = await self.repo.persist_evaluation(
            str(run["id"]),
            [item.model_dump(mode="json") for item in items],
            status,
            [item.model_dump(mode="json") for item in all_gaps],
        )
        if persisted["stale"]:
            satisfied = False
            status = "STALE"
            snapshot_gap = _gap("EVALUATION_SNAPSHOT_STALE", "project changed during evaluation")
            all_gaps.append(snapshot_gap)
            for item in items:
                item.status = "STALE"
                item_gap = snapshot_gap.model_copy(deep=True)
                item_gap.criterion_id = item.criterion_id
                item.reasons.append(item_gap)
        event = Event(
            project_id=project_id,
            event_type=EventType.DOD_EVALUATED,
            producer_agent_id="dod_evaluator",
            payload={
                "evaluation_run_id": str(run["id"]),
                "contract_version": run["contract_version"],
                "contract_hash": run["contract_hash"],
                "integration_head": run["integration_head"],
                "evidence_generation": run["evidence_generation"],
                "satisfied": satisfied,
                "status": status,
                "gaps": [item.model_dump(mode="json") for item in all_gaps],
            },
        )
        await self.events.save_event(project_id, event)
        logger.info("dod_evaluated", project_id=project_id, satisfied=satisfied, status=status)
        return DoDEvaluation(
            project_id=project_id,
            evaluation_run_id=str(run["id"]),
            contract_version=int(run["contract_version"]),
            contract_hash=str(run["contract_hash"]),
            integration_head=run["integration_head"],
            evidence_generation=int(run["evidence_generation"]),
            satisfied=satisfied,
            status=status,
            items=items,
            gaps=all_gaps,
        ).model_dump(mode="json")
