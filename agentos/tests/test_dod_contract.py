from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from git import Repo
from pydantic import ValidationError

from agentos.actors.bootstrap import BootstrapAgentActor
from agentos.actors.review_cache import CriterionReviewCache
from agentos.actors.reviewer import ReviewerAgentActor, _CodeReviewVerdict
from agentos.actors.safety_reviewer import _SecurityReviewVerdict
from agentos.config.loader import load_prompt, runtime_tuning
from agentos.config.settings import Settings
from agentos.dod.evaluator import _classify_item_status, _revision_freshness_reason
from agentos.runtime.planning_context import PlanningContextError, build_planning_context
from agentos.runtime.team_plan import (
    AgentRole,
    AgentSpec,
    CriterionSeverity,
    CriterionSource,
    DoDCriterion,
    EvidenceScope,
    EvidenceType,
    InitialTask,
    TeamPlan,
    _pattern_within_boundary,
    _patterns_overlap,
)
from agentos.storage.clients.dragonfly import DragonflyClient
from agentos.storage.repositories import TaskRepository

ROOT = Path(__file__).resolve().parents[2]


def criterion(**overrides: object) -> DoDCriterion:
    payload: dict[str, object] = {
        "criterion_id": "delivery",
        "description": "The requested implementation passes its repository test suite.",
        "verification_command": ["pytest", "-q"],
        "required_artifacts": ["src/result.py"],
        "required_evidence_types": ["artifact", "test", "review", "integration"],
        "source": "user",
        "locked": True,
        "mandatory": True,
        "severity": "required",
    }
    payload.update(overrides)
    return DoDCriterion.model_validate(payload)


def task(**overrides: object) -> InitialTask:
    payload: dict[str, object] = {
        "title": "Deliver result",
        "description": "Implement and verify the requested result.",
        "owner_role": "backend_developer",
        "acceptance_criteria": ["The repository test suite passes."],
        "allowed_paths": ["src"],
        "blocked_paths": [".env"],
        "expected_outputs": ["src/result.py"],
        "required_reviewers": ["code_reviewer"],
        "dod_criteria": ["delivery"],
    }
    payload.update(overrides)
    return InitialTask.model_validate(payload)


def plan(**overrides: object) -> TeamPlan:
    payload: dict[str, object] = {
        "project_name": "contract-proof",
        "user_request": "Deliver a verified result.",
        "high_level_architecture": "One governed task produces and verifies one artifact.",
        "dod": [criterion().model_dump(mode="json")],
        "agents": [
            AgentSpec(
                role=AgentRole.BACKEND_DEVELOPER,
                count=1,
                description="Implement the result.",
            ).model_dump(mode="json")
        ],
        "initial_backlog": [task().model_dump(mode="json")],
        "max_requested_agents": 5,
        "contract_version": 1,
        "source_revision": "a" * 40,
        "planning_context_hash": "b" * 64,
        "prompt_version": "bootstrap-dod-v1",
    }
    payload.update(overrides)
    return TeamPlan.model_validate(payload)


def test_evidence_contract_populates_canonical_cardinality() -> None:
    item = criterion()
    assert item.evidence_scopes == {
        EvidenceType.ARTIFACT: EvidenceScope.ARTIFACT,
        EvidenceType.TEST: EvidenceScope.CRITERION,
        EvidenceType.REVIEW: EvidenceScope.ARTIFACT,
        EvidenceType.INTEGRATION: EvidenceScope.TASK,
    }
    assert len(item.contract_hash) == 64


def test_criterion_rejects_fail_open_or_ambiguous_evidence() -> None:
    with pytest.raises(ValidationError, match="deterministic"):
        criterion(required_evidence_types=["artifact", "review", "integration"])
    with pytest.raises(ValidationError, match="not both"):
        criterion(required_evidence_types=["artifact", "test", "command", "review", "integration"])
    with pytest.raises(ValidationError, match="artifact evidence requires"):
        criterion(required_artifacts=[])


def test_inferred_criteria_cannot_be_mandatory_or_locked() -> None:
    with pytest.raises(ValidationError, match="inferred criteria"):
        criterion(source=CriterionSource.INFERRED)
    advisory = criterion(
        source=CriterionSource.INFERRED,
        locked=False,
        mandatory=False,
        severity=CriterionSeverity.ADVISORY,
    )
    assert advisory.mandatory is False
    with pytest.raises(ValidationError, match="must remain locked"):
        criterion(source=CriterionSource.USER, locked=False)


def test_team_plan_rejects_uncovered_criteria_and_artifacts() -> None:
    extra = criterion(
        criterion_id="second",
        description="A second requested artifact is independently delivered and verified.",
        required_artifacts=["src/second.py"],
    )
    with pytest.raises(ValidationError, match="lack implementation tasks"):
        plan(dod=[criterion().model_dump(mode="json"), extra.model_dump(mode="json")])
    with pytest.raises(ValidationError, match="not covered"):
        plan(dod=[criterion(required_artifacts=["src/another.py"]).model_dump(mode="json")])
    with pytest.raises(ValidationError, match="duplicate the same normalized requirement"):
        plan(
            dod=[
                criterion().model_dump(mode="json"),
                criterion(criterion_id="duplicate").model_dump(mode="json"),
            ]
        )


def test_path_contract_matching_does_not_treat_empty_glob_prefix_as_unbounded() -> None:
    assert _pattern_within_boundary("src/result.py", "src") is True
    assert _pattern_within_boundary("src/*.py", "src") is True
    assert _pattern_within_boundary("src*/escaped.py", "src") is False
    assert _pattern_within_boundary("docs/readme.md", "*.py") is False
    assert _patterns_overlap("src/*.py", "src/result.py") is True
    assert _patterns_overlap("*.py", "docs/readme.md") is False
    assert _patterns_overlap("src/*.py", "src/*.md") is False
    with pytest.raises(ValidationError, match="outside"):
        task(allowed_paths=["*.py"], expected_outputs=["docs/readme.md"])
    patterned_contract = criterion(affected_contracts=["public-api/*"])
    assert plan(
        dod=[patterned_contract.model_dump(mode="json")],
        initial_backlog=[task(affected_contracts=["public-api/users"]).model_dump(mode="json")],
    )
    with pytest.raises(ValidationError, match="affected contracts"):
        plan(
            dod=[patterned_contract.model_dump(mode="json")],
            initial_backlog=[task(affected_contracts=["internal-jobs"]).model_dump(mode="json")],
        )


@pytest.mark.asyncio
async def test_lost_distributed_lock_cancels_the_protected_operation() -> None:
    class RedisWithLostRenewal:
        async def set(self, *_args: object, **_kwargs: object) -> bool:
            return True

        async def eval(self, _script: str, _keys: int, *_args: object) -> int:
            # Renewal includes the TTL argument; release does not.
            return 0 if len(_args) == 3 else 1

    client = object.__new__(DragonflyClient)
    client.settings = Settings(AGENTOS_ENV="development")
    client.redis = RedisWithLostRenewal()  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="distributed lock renewal failed"):
        async with client.lock("test-lost-lease", ttl_seconds=1) as acquired:
            assert acquired is True
            await asyncio.sleep(2)


def test_task_security_gate_is_union_of_risk_and_criterion() -> None:
    secure = criterion(
        severity="critical",
        required_evidence_types=[
            "artifact",
            "test",
            "review",
            "security_review",
            "integration",
        ],
    )
    with pytest.raises(ValidationError, match="security reviewer"):
        plan(
            dod=[secure.model_dump(mode="json")],
            initial_backlog=[task(risk_level="HIGH").model_dump(mode="json")],
        )


def test_contract_hash_changes_with_explicit_version() -> None:
    first = plan()
    second_payload = first.model_dump(mode="json")
    second_payload["contract_version"] = 2
    second = TeamPlan.model_validate(second_payload)
    assert first.contract_hash != second.contract_hash
    assert first.contract_hash == plan().contract_hash


def test_planning_context_is_revision_bound_and_rejects_dirty_sources(tmp_path: Path) -> None:
    repository = Repo.init(tmp_path, initial_branch="main")
    with repository.config_writer() as writer:
        writer.set_value("user", "name", "AgentOS Test")
        writer.set_value("user", "email", "agentos@example.invalid")
    (tmp_path / "README.md").write_text("# Grounded project\n", encoding="utf-8")
    (tmp_path / "src.py").write_text("value = 1\n", encoding="utf-8")
    repository.index.add(["README.md", "src.py"])
    commit = repository.index.commit("initial")

    context = build_planning_context(tmp_path, "Change the value")
    assert context["source_revision"] == commit.hexsha
    assert context["tracked_tree"] == ["README.md", "src.py"]
    assert context["documents"]["README.md"] == "# Grounded project\n"
    assert len(context["planning_context_hash"]) == 64

    (tmp_path / "src.py").write_text("value = 2\n", encoding="utf-8")
    with pytest.raises(PlanningContextError, match="uncommitted changes"):
        build_planning_context(tmp_path, "Change the value")


def test_planning_context_never_follows_tracked_document_symlinks(tmp_path: Path) -> None:
    repository = Repo.init(tmp_path, initial_branch="main")
    with repository.config_writer() as writer:
        writer.set_value("user", "name", "AgentOS Test")
        writer.set_value("user", "email", "agentos@example.invalid")
    outside = tmp_path.parent / f"outside-{uuid4().hex}.md"
    outside.write_text("provider-secret", encoding="utf-8")
    link = tmp_path / "README.md"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("symlink creation is unavailable on this host")
    repository.index.add(["README.md"])
    repository.index.commit("tracked symlink")

    context = build_planning_context(tmp_path, "Inspect the repository safely.")
    assert context["tracked_tree"] == ["README.md"]
    assert context["documents"] == {}
    outside.unlink()


def test_planning_context_rejects_oversized_requests_before_provider_egress() -> None:
    with pytest.raises(PlanningContextError, match="planning limit"):
        build_planning_context(None, "x" * 100_001)


def test_prompts_are_packaged_versioned_and_have_no_root_shadow() -> None:
    assert "prompt-version: bootstrap-dod-v1" in load_prompt("bootstrap_pm.md")
    assert "prompt-version: worker-action-v1" in load_prompt("agent_worker.md")
    assert not (ROOT / "prompts" / "bootstrap_pm.md").exists()
    assert not (ROOT / "prompts" / "agent_worker.md").exists()


@pytest.mark.asyncio
async def test_bootstrap_is_bounded_and_fail_closed_without_fallback_plan() -> None:
    source = (ROOT / "agentos" / "actors" / "bootstrap.py").read_text(encoding="utf-8")
    assert "max_validation_attempts" in source
    assert "bootstrap_plan_failed_closed" in source
    assert "raise RuntimeError" in source
    assert "def _fallback" not in source
    assert "fallback_team" not in source

    class InvalidCompletion:
        def __init__(self) -> None:
            self.calls = 0

        async def remote(self, *_: object, **__: object) -> dict[str, str]:
            self.calls += 1
            return {"content": "{}"}

    completion = InvalidCompletion()
    bootstrap_class = BootstrapAgentActor.__ray_metadata__.modified_class
    bootstrap = bootstrap_class.__new__(bootstrap_class)
    bootstrap.project_id = str(uuid4())
    bootstrap.settings = Settings(environment="test")
    bootstrap.provider = type("FakeProvider", (), {"get_completion": completion})()
    context = {
        "user_request": "Deliver a result.",
        "source_revision": "a" * 40,
        "tracked_tree": [],
        "documents": {},
        "planning_context_hash": "b" * 64,
    }
    with pytest.raises(RuntimeError, match="planning failed after"):
        await bootstrap.create_team_plan("Deliver a result.", 5, context)
    assert completion.calls == int(runtime_tuning()["planning"]["max_validation_attempts"])


def test_task_state_machine_has_terminal_write_barriers() -> None:
    assert TaskRepository.STATUS_TRANSITIONS["COMPLETED"] == set()
    assert TaskRepository.STATUS_TRANSITIONS["CANCELLED"] == set()
    assert "COMPLETED" in TaskRepository.STATUS_TRANSITIONS["UNDER_REVIEW"]
    assert "COMPLETED" not in TaskRepository.STATUS_TRANSITIONS["PENDING"]


def test_schema_declares_versioned_append_only_fenced_dod() -> None:
    schema = (ROOT / "agentos" / "storage" / "schema.sql").read_text(encoding="utf-8")
    for contract in (
        "CREATE TABLE IF NOT EXISTS dod_contract_versions",
        "CREATE TABLE IF NOT EXISTS dod_evaluation_runs",
        "CREATE TABLE IF NOT EXISTS dod_evaluation_items",
        "CREATE TABLE IF NOT EXISTS integration_attempts",
        "CREATE TRIGGER dod_evidence_append_only",
        "CREATE TRIGGER artifacts_append_only",
        "criterion-global command evidence requires the integration supervisor",
        "VALUES (4, 'Versioned DoD contracts",
    ):
        assert contract in schema


def test_watchdog_uses_runnable_work_and_bounded_replanning() -> None:
    source = (ROOT / "agentos" / "watchdogs" / "runtime_watchdogs.py").read_text(encoding="utf-8")
    assert "runnable" in source
    assert "max_replan_attempts" in source
    assert "bounded_replanning_exhausted" in source
    assert "evaluation_run_id" in source


def test_golden_dod_status_and_revision_freshness_dataset(tmp_path: Path) -> None:
    cases = json.loads(
        (ROOT / "agentos/tests/fixtures/dod_golden_cases.json").read_text(encoding="utf-8")
    )
    for case in cases["status_cases"]:
        assert _classify_item_status(set(case["codes"])) == case["expected"], case["name"]

    repository = Repo.init(tmp_path, initial_branch="main")
    with repository.config_writer() as writer:
        writer.set_value("user", "name", "AgentOS Test")
        writer.set_value("user", "email", "agentos@example.invalid")
    source_dir = tmp_path / "src"
    source_dir.mkdir()
    (source_dir / "result.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("# Proof\n", encoding="utf-8")
    repository.index.add(["src/result.py", "README.md"])
    base = repository.index.commit("base").hexsha
    (tmp_path / "README.md").write_text("# Proof\n\nDocs change.\n", encoding="utf-8")
    repository.index.add(["README.md"])
    docs_head = repository.index.commit("docs").hexsha
    (source_dir / "result.py").write_text("VALUE = 2\n", encoding="utf-8")
    repository.index.add(["src/result.py"])
    source_head = repository.index.commit("source").hexsha
    heads = {"docs": docs_head, "source": source_head}
    for case in cases["freshness_cases"]:
        result = _revision_freshness_reason(
            repository,
            {
                "subject_commit": base,
                "watched_paths": case["watched_paths"],
                "affected_contracts": case.get("affected_contracts", []),
                "task_id": "task-a",
            },
            heads[case["head"]],
            (
                [
                    {
                        "evidence_type": "integration",
                        "task_id": "task-b",
                        "affected_contracts": ["public-api"],
                        "integration_commit": docs_head,
                    }
                ]
                if case.get("later_contract_change")
                else []
            ),
        )
        assert (result[0] if result else None) == case["expected"], case["name"]
    patterned_contract_result = _revision_freshness_reason(
        repository,
        {
            "subject_commit": base,
            "watched_paths": [],
            "affected_contracts": ["public-api/*"],
            "task_id": "task-a",
        },
        docs_head,
        [
            {
                "evidence_type": "integration",
                "task_id": "task-b",
                "affected_contracts": ["public-api/users"],
                "integration_commit": docs_head,
            }
        ],
    )
    assert patterned_contract_result and patterned_contract_result[0] == "EVIDENCE_STALE_CONTRACT"


@pytest.mark.asyncio
async def test_revision_review_cache_coalesces_only_successful_exact_snapshots() -> None:
    cache = CriterionReviewCache(max_concurrency=1, max_entries=2)
    calls = 0

    async def successful() -> dict[str, object]:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return {"run_status": "OK", "approved": True}

    first, second = await asyncio.gather(
        cache.get_or_run("criterion-hash:commit:artifact", successful),
        cache.get_or_run("criterion-hash:commit:artifact", successful),
    )
    assert calls == 1
    assert first[0] == second[0]
    cached, cache_hit = await cache.get_or_run("criterion-hash:commit:artifact", successful)
    assert cached["approved"] is True and cache_hit is True
    assert calls == 1

    async def inconclusive() -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"run_status": "INCONCLUSIVE", "approved": False}

    await cache.get_or_run("transient", inconclusive)
    await cache.get_or_run("transient", inconclusive)
    assert calls == 3


@pytest.mark.asyncio
async def test_review_cache_waiter_cancellation_does_not_cancel_shared_review() -> None:
    cache = CriterionReviewCache(max_concurrency=1, max_entries=2)
    release = asyncio.Event()

    async def operation() -> dict[str, object]:
        await release.wait()
        return {"run_status": "OK", "approved": True}

    owner = asyncio.create_task(cache.get_or_run("shared", operation))
    await asyncio.sleep(0)
    waiter = asyncio.create_task(cache.get_or_run("shared", operation))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    release.set()
    result, _ = await owner
    assert result["approved"] is True


def test_reviewer_verdicts_reject_truthy_string_booleans() -> None:
    with pytest.raises(ValidationError):
        _CodeReviewVerdict.model_validate({"approved": "false", "score": 95, "findings": []})
    with pytest.raises(ValidationError):
        _SecurityReviewVerdict.model_validate({"safe": "false", "findings": []})


@pytest.mark.asyncio
async def test_reviewer_isolates_criterion_context_and_persists_distinct_verdicts() -> None:
    project_id = str(uuid4())
    task_id = str(uuid4())
    artifact_id = str(uuid4())
    patch_content = "VALUE = 1\n"

    class FakeDB:
        async def fetchrow(self, query: str, *_: object) -> dict[str, Any]:
            if "FROM tasks" in query:
                return {
                    "id": task_id,
                    "acceptance_criteria": ["Each criterion is reviewed independently."],
                    "allowed_paths": ["src"],
                    "affected_contracts": ["public-api"],
                }
            return {
                "id": artifact_id,
                "task_id": task_id,
                "title": "src/result.py",
                "checksum_sha256": "a" * 64,
                "metadata": {
                    "git_commit": "b" * 40,
                    "review_diff_sha256": hashlib.sha256(patch_content.encode()).hexdigest(),
                    "review_diff_characters": len(patch_content),
                },
            }

    class FakeDoD:
        def __init__(self) -> None:
            self.evidence: list[dict[str, Any]] = []

        async def get_checks(self, *_: object) -> list[dict[str, Any]]:
            return [
                {
                    "criterion_id": "first",
                    "description": "The first behavior is correct.",
                    "criterion_hash": "1" * 64,
                    "required_evidence_types": ["review"],
                },
                {
                    "criterion_id": "second",
                    "description": "The second behavior is correct.",
                    "criterion_hash": "2" * 64,
                    "required_evidence_types": ["review"],
                },
            ]

        async def add_evidence(self, *_: object, **kwargs: Any) -> str:
            self.evidence.append(kwargs)
            return str(uuid4())

    class FakeCompletion:
        def __init__(self) -> None:
            self.requests: list[dict[str, Any]] = []

        async def remote(self, request: dict[str, Any], **_: object) -> dict[str, str]:
            self.requests.append(request)
            user_message = request["messages"][1]["content"]
            approved = "Criterion ID: first" in user_message
            return {
                "content": json.dumps(
                    {
                        "approved": approved,
                        "score": 95 if approved else 25,
                        "findings": [] if approved else ["second criterion is not proven"],
                    }
                )
            }

    reviewer_class = ReviewerAgentActor.__ray_metadata__.modified_class
    reviewer = reviewer_class.__new__(reviewer_class)
    reviewer.db = FakeDB()
    reviewer.dod = FakeDoD()
    completion = FakeCompletion()
    reviewer.provider = type("FakeProvider", (), {"get_completion": completion})()
    reviewer.review_cache = CriterionReviewCache(max_concurrency=2, max_entries=8)

    result = await reviewer.review_code_patch(
        project_id=project_id,
        task_id=task_id,
        criterion_ids=["first", "second"],
        artifact_id=artifact_id,
        file_path="src/result.py",
        code_content=patch_content,
    )

    assert result["approved"] is False
    assert [item["approved"] for item in result["reviews"]] == [True, False]
    assert len(completion.requests) == 2
    supplied_contexts = [item["messages"][1]["content"] for item in completion.requests]
    assert any("The first behavior is correct." in context for context in supplied_contexts)
    assert any("The second behavior is correct." in context for context in supplied_contexts)
    assert all("Task affected contracts" in context for context in supplied_contexts)
    assert [item["passed"] for item in reviewer.dod.evidence] == [True, False]
