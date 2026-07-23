from __future__ import annotations

import hashlib
import json
import os
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from agentos.config.runtime import ResourcePlanner
from agentos.config.settings import Settings
from agentos.dod.evaluator import DoDEvaluatorActor
from agentos.execution.supervisor import ExecutionService
from agentos.governance.models import ActionRequest, AgentIdentity
from agentos.memory.broker import MemoryService
from agentos.messaging.events import Event, EventType
from agentos.runtime.team_plan import AgentRole, AgentSpec, DoDCriterion, InitialTask, TeamPlan
from agentos.storage.clients import (
    DragonflyClient,
    MilvusVectorClient,
    MinioObjectClient,
    MongoDocumentClient,
    PostgresClient,
    VectorRecord,
)
from agentos.storage.repositories import (
    AgentRepository,
    ArtifactRepository,
    DoDRepository,
    EventRepository,
    ProjectRepository,
    TaskRepository,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_AGENTOS_INTEGRATION") != "1",
        reason="set RUN_AGENTOS_INTEGRATION=1 with the Compose stack running",
    ),
]


class _FailingRemoteEmbedding:
    async def remote(self, *_: Any, **__: Any) -> list[float]:
        raise RuntimeError("embedding deliberately unavailable for storage saga proof")


class _ProviderWithoutEmbedding:
    get_embedding = _FailingRemoteEmbedding()


def _test_plan(
    name: str,
    request: str,
    *,
    allowed_paths: list[str] | None = None,
) -> tuple[TeamPlan, AgentSpec]:
    criterion = DoDCriterion(
        criterion_id="proof",
        description="The integration proof is delivered and verified.",
        verification_command=["python", "-c", "print('proof')"],
        required_artifacts=["proof/result.txt"],
        required_evidence_types=["artifact", "test", "review", "integration"],
        source="system",
    )
    task = InitialTask(
        title="Integration proof task",
        description="Produce the bounded integration proof.",
        owner_role=AgentRole.QA_ENGINEER,
        acceptance_criteria=["The proof command succeeds."],
        allowed_paths=allowed_paths or ["proof"],
        expected_outputs=["proof/result.txt"],
        required_reviewers=["code_reviewer"],
        dod_criteria=["proof"],
    )
    spec = AgentSpec(role=AgentRole.QA_ENGINEER, count=1, description="Verify the proof.")
    plan = TeamPlan(
        project_name=name,
        user_request=request,
        high_level_architecture="One bounded proof task and its independent evidence.",
        dod=[criterion],
        agents=[spec],
        initial_backlog=[task],
        max_requested_agents=5,
        source_revision="EMPTY_WORKSPACE",
        planning_context_hash="a" * 64,
        prompt_version="integration-test-v1",
    )
    return plan, spec


async def _create_planned_test_project(
    postgres: PostgresClient,
    settings: Settings,
    name: str,
    request: str,
    *,
    allowed_paths: list[str] | None = None,
) -> tuple[str, str]:
    plan, spec = _test_plan(name, request, allowed_paths=allowed_paths)
    project_id = await ProjectRepository(postgres).create_project(name, request, [])
    await ProjectRepository(postgres).update_status(project_id, "PLANNING")
    runtime = ResourcePlanner(settings).build_runtime_config(
        [("qa_engineer-1", AgentRole.QA_ENGINEER.value)]
    )
    persisted = await ProjectRepository(postgres).persist_plan_bundle(
        project_id,
        plan,
        [spec],
        runtime,
        settings.safe_snapshot(),
        {
            "user_request": request,
            "source_revision": "EMPTY_WORKSPACE",
            "tracked_tree": [],
            "documents": {},
            "planning_context_hash": "a" * 64,
        },
    )
    return project_id, persisted["task_ids"][plan.initial_backlog[0].title]


@pytest.mark.asyncio
async def test_plan_bundle_rolls_back_all_contract_state_on_late_failure() -> None:
    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    postgres = PostgresClient(settings)
    await postgres.initialize_schema()
    plan, spec = _test_plan("atomic-plan-proof", "Prove initial plan rollback.")
    project_id = await ProjectRepository(postgres).create_project(
        plan.project_name, plan.user_request, []
    )
    await ProjectRepository(postgres).update_status(project_id, "PLANNING")
    runtime = ResourcePlanner(settings).build_runtime_config(
        [("qa_engineer-1", AgentRole.QA_ENGINEER.value)]
    )
    broken_runtime = runtime.model_copy(update={"allocations": []})
    with pytest.raises(KeyError, match="qa_engineer-1"):
        await ProjectRepository(postgres).persist_plan_bundle(
            project_id,
            plan,
            [spec],
            broken_runtime,
            settings.safe_snapshot(),
            {
                "user_request": plan.user_request,
                "source_revision": "EMPTY_WORKSPACE",
                "tracked_tree": [],
                "documents": {},
                "planning_context_hash": "a" * 64,
            },
        )
    project = await ProjectRepository(postgres).get(project_id)
    assert project is not None and project["dod_contract_version"] == 0
    for table in (
        "dod_contract_versions",
        "dod_checks",
        "tasks",
        "resource_plans",
        "runtime_config_snapshots",
        "agents",
    ):
        count = await postgres.fetchval(
            f"SELECT count(*) FROM {table} WHERE project_id=$1",  # noqa: S608 - fixed test table list
            UUID(project_id),
        )
        assert count == 0, table
    await postgres.disconnect()


@pytest.mark.asyncio
async def test_evidence_authority_and_provenance_are_fail_closed() -> None:
    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    postgres = PostgresClient(settings)
    await postgres.initialize_schema()
    project_id, task_id = await _create_planned_test_project(
        postgres,
        settings,
        "evidence-authority-proof",
        "Reject unauthenticated and mismatched evidence.",
    )
    await ProjectRepository(postgres).update_status(project_id, "RUNNING")
    claimed = await TaskRepository(postgres).claim_next(
        project_id, "qa_engineer-1", AgentRole.QA_ENGINEER.value
    )
    assert claimed is not None
    await AgentRepository(postgres).register_agent(
        "code_reviewer-1", project_id, "code_reviewer", "review"
    )
    checksum = hashlib.sha256(b"proof").hexdigest()
    artifact_id = await ArtifactRepository(postgres).create_artifact(
        project_id,
        "FILE",
        "proof/result.txt",
        object_uri="minio://agentos-artifacts/proof?versionId=1",
        checksum_sha256=checksum,
        content_length=5,
        task_id=task_id,
        metadata={"git_commit": "a" * 40},
    )
    dod = DoDRepository(postgres)
    await dod.add_evidence(
        project_id,
        "proof",
        "artifact",
        "qa_engineer-1",
        "Task owner produced the checksum-bound artifact.",
        True,
        artifact_id=artifact_id,
        checksum_sha256=checksum,
        task_id=task_id,
        source_role="qa_engineer",
        subject_commit="a" * 40,
        watched_paths=["proof/result.txt"],
    )
    with pytest.raises(ValueError, match="author cannot approve"):
        await dod.add_evidence(
            project_id,
            "proof",
            "review",
            "qa_engineer-1",
            "self review",
            True,
            artifact_id=artifact_id,
            task_id=task_id,
            source_role="qa_engineer",
            subject_commit="a" * 40,
        )
    with pytest.raises(ValueError, match="authenticated.*identity"):
        await dod.add_evidence(
            project_id,
            "proof",
            "review",
            "unknown-reviewer",
            "unknown reviewer",
            True,
            artifact_id=artifact_id,
            task_id=task_id,
            source_role="code_reviewer",
            subject_commit="a" * 40,
        )
    with pytest.raises(ValueError, match="stale criterion hash"):
        await dod.add_evidence(
            project_id,
            "proof",
            "test",
            "qa_engineer-1",
            "wrong criterion revision",
            True,
            task_id=task_id,
            source_role="qa_engineer",
            criterion_hash="0" * 64,
            command=json.dumps(["python", "-c", "print('proof')"]),
            exit_code=0,
            subject_commit="a" * 40,
        )
    with pytest.raises(ValueError, match="integration supervisor"):
        await dod.add_evidence(
            project_id,
            "proof",
            "integration",
            "qa_engineer-1",
            "forged integration",
            True,
            task_id=task_id,
            source_role="qa_engineer",
            subject_commit="a" * 40,
            integration_commit="b" * 40,
        )
    await postgres.disconnect()


@pytest.mark.asyncio
async def test_replanning_is_gap_bound_graph_validated_and_generation_idempotent() -> None:
    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    postgres = PostgresClient(settings)
    await postgres.initialize_schema()
    project_id, task_id = await _create_planned_test_project(
        postgres,
        settings,
        "replan-contract-proof",
        "Create only one validated repair batch for an evaluated gap.",
    )
    projects = ProjectRepository(postgres)
    tasks = TaskRepository(postgres)
    dod = DoDRepository(postgres)
    await projects.update_status(project_id, "RUNNING")
    claimed = await tasks.claim_next(project_id, "qa_engineer-1", "qa_engineer")
    assert claimed is not None and str(claimed["id"]) == task_id
    await tasks.update_task_status(task_id, "IN_PROGRESS")
    await tasks.update_task_status(task_id, "BLOCKED")
    check = await postgres.fetchrow(
        "SELECT criterion_hash FROM dod_checks WHERE project_id=$1 AND criterion_id='proof'",
        UUID(project_id),
    )
    assert check is not None
    run = await dod.start_evaluation(project_id, "replan-test")
    await dod.persist_evaluation(
        str(run["id"]),
        [
            {
                "criterion_id": "proof",
                "criterion_hash": check["criterion_hash"],
                "status": "MISSING",
                "reasons": [
                    {
                        "criterion_id": "proof",
                        "code": "TASK_INCOMPLETE",
                        "message": "The original task is blocked.",
                    }
                ],
                "evidence_ids": [],
            }
        ],
        "UNSATISFIED",
        [{"criterion_id": "proof", "code": "TASK_INCOMPLETE"}],
    )
    recovered = await dod.start_evaluation(project_id, "restarted-supervisor")
    assert recovered["reused"] is True and recovered["id"] == run["id"]
    proposal = InitialTask(
        title="Repair integration proof",
        description="Repair the exact durable DoD gap without changing the contract.",
        owner_role=AgentRole.QA_ENGINEER,
        acceptance_criteria=["The contracted proof command passes."],
        allowed_paths=["proof"],
        expected_outputs=["proof/result.txt"],
        required_reviewers=["code_reviewer"],
        dod_criteria=["proof"],
    )
    first = await tasks.create_replan_batch(project_id, str(run["id"]), ["proof"], [proposal])
    generation = await postgres.fetchval(
        "SELECT evidence_generation FROM projects WHERE id=$1", UUID(project_id)
    )
    second = await tasks.create_replan_batch(project_id, str(run["id"]), ["proof"], [proposal])
    assert second == first
    assert (
        await postgres.fetchval(
            "SELECT evidence_generation FROM projects WHERE id=$1", UUID(project_id)
        )
        == generation
    )
    changed = proposal.model_copy(update={"description": "A conflicting duplicate repair."})
    with pytest.raises(ValueError, match="idempotency key conflicts"):
        await tasks.create_replan_batch(project_id, str(run["id"]), ["proof"], [changed])
    cycle_a = proposal.model_copy(update={"title": "Cycle A", "depends_on": ["Cycle B"]})
    cycle_b = proposal.model_copy(update={"title": "Cycle B", "depends_on": ["Cycle A"]})
    with pytest.raises(ValueError, match="dependency cycle"):
        await tasks.create_replan_batch(project_id, str(run["id"]), ["proof"], [cycle_a, cycle_b])
    await postgres.disconnect()


@pytest.mark.asyncio
async def test_all_storage_clients_round_trip() -> None:
    """Exercise every required data system through the production client classes."""

    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    postgres = PostgresClient(settings)
    dragonfly = DragonflyClient(settings)
    mongodb = MongoDocumentClient(settings)
    minio = MinioObjectClient(settings)
    milvus = MilvusVectorClient(settings)
    proof_id = str(uuid4())
    payload = f"agentos-storage-proof:{proof_id}".encode()

    try:
        await postgres.initialize_schema()
        assert (await postgres.healthcheck())["healthy"] is True
        assert await postgres.fetchval("SELECT $1::text", proof_id) == proof_id
        native_json = await postgres.fetchrow(
            "SELECT jsonb_build_object('proof', $1::text) AS object_value, "
            "json_build_array(1, 2, 3) AS array_value",
            proof_id,
        )
        assert native_json is not None
        assert native_json["object_value"] == {"proof": proof_id}
        assert native_json["array_value"] == [1, 2, 3]

        project_id, _ = await _create_planned_test_project(
            postgres,
            settings,
            "outbox-codec-proof",
            "Verify native JSON outbox delivery",
        )
        event_repository = EventRepository(postgres)
        event = Event(
            project_id=project_id,
            event_type=EventType.PROJECT_CREATED,
            producer_agent_id="runtime_supervisor",
            payload={"proof_id": proof_id},
        )
        await event_repository.save_event(project_id, event)
        outbox = await event_repository.claim_outbox(project_id, limit=1)
        assert outbox
        assert Event.model_validate(outbox[0]["payload"]) == event
        await event_repository.mark_outbox_published(outbox[0]["id"])

        other_project_id, other_task_id = await _create_planned_test_project(
            postgres,
            settings,
            "isolation-proof",
            "Reject cross-project task references",
        )
        with pytest.raises(asyncpg.RaiseError, match="same project"):
            await ArtifactRepository(postgres).create_artifact(
                project_id,
                "FILE",
                "cross-project.txt",
                object_uri="minio://invalid/cross-project.txt",
                checksum_sha256="0" * 64,
                content_length=0,
                task_id=other_task_id,
            )

        assert (await dragonfly.healthcheck())["healthy"] is True
        cache_key = f"integration:{proof_id}"
        await dragonfly.set_json(cache_key, payload.decode(), ttl_seconds=300)
        assert await dragonfly.redis.get(dragonfly.key(cache_key)) == payload.decode()

        await mongodb.initialize()
        assert (await mongodb.healthcheck())["healthy"] is True
        await mongodb.append_memory(
            project_id=proof_id,
            agent_id="integration-agent",
            scope="shared_project_memory",
            kind="integration_proof",
            content=payload.decode(),
            ttl_seconds=300,
        )
        documents = await mongodb.recent_memories(
            project_id=proof_id,
            requester_agent_id="integration-agent",
            scopes=["shared_project_memory"],
        )
        assert documents[0]["content"] == payload.decode()

        await minio.initialize()
        object_name = f"integration/{proof_id}/proof.txt"
        metadata = await minio.put_bytes(
            bucket=settings.minio_artifacts_bucket,
            object_name=object_name,
            data=payload,
            content_type="text/plain",
        )
        assert (
            await minio.get_bytes(
                bucket=settings.minio_artifacts_bucket,
                object_name=object_name,
                version_id=metadata.version_id,
            )
            == payload
        )

        await milvus.initialize()
        vector = [0.0] * settings.embedding_dimension
        vector[0] = 1.0
        await milvus.upsert(
            VectorRecord(
                record_id=proof_id,
                vector=vector,
                project_id=proof_id,
                agent_id="integration-agent",
                scope="shared_project_memory",
                kind="integration_proof",
                content_ref=metadata.uri,
            )
        )
        hits = await milvus.search(
            vector=vector,
            project_id=proof_id,
            scopes=["shared_project_memory"],
            requester_agent_id="integration-agent",
            kinds=["integration_proof"],
            limit=1,
        )
        assert hits and hits[0].record_id == proof_id

        memory_service = MemoryService(settings)
        await memory_service.initialize()
        long_content = f"{payload.decode()}\n" + ("durable-memory\n" * 2_000)
        try:
            memory_result = await memory_service.remember(
                project_id=project_id,
                agent_id="integration-agent",
                scope="project_memory",
                kind="integration_proof",
                title="Lossless polyglot memory",
                content=long_content,
                importance=5,
                provider_gateway=_ProviderWithoutEmbedding(),
                promote_long_term=True,
            )
            assert memory_result["midterm_saved"] is True
            assert memory_result["longterm_saved"] is True
            memory_row = await postgres.fetchrow(
                "SELECT * FROM memory_items WHERE id=$1",
                UUID(memory_result["memory_id"]),
            )
            assert memory_row is not None
            assert memory_row["storage_status"] == "READY"
            assert memory_row["content_object_uri"].startswith("minio://")
            assert memory_row["content_length"] == len(long_content.encode())
            assert memory_row["content_hash"] == hashlib.sha256(long_content.encode()).hexdigest()
            hydrated = await memory_service._hydrate_longterm_row(dict(memory_row))
            assert hydrated["content"] == long_content
        finally:
            await memory_service.close()
    finally:
        await postgres.disconnect()
        await mongodb.close()
        await dragonfly.close()


@pytest.mark.asyncio
async def test_controlled_execution_uses_restricted_docker_sandbox() -> None:
    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    service = ExecutionService(settings)
    await service.db.initialize_schema()
    project_id, task_id = await _create_planned_test_project(
        service.db,
        settings,
        "sandbox-proof",
        "verify restricted execution",
        allowed_paths=["proof"],
    )
    await ProjectRepository(service.db).update_status(project_id, "RUNNING")
    identity = AgentIdentity(
        project_id=project_id,
        agent_id="integration-agent",
        role="qa_engineer",
        allowed_actions=["shell_command"],
        allowed_paths=["proof"],
    )
    await AgentRepository(service.db).register_agent(
        identity.agent_id,
        project_id,
        identity.role,
        "integration",
        memory_scopes=identity.memory_scopes,
        permissions={
            "allowed_actions": identity.allowed_actions,
            "ownership_domains": identity.allowed_paths,
        },
    )
    claimed = await TaskRepository(service.db).claim_next(
        project_id, identity.agent_id, identity.role
    )
    assert claimed is not None and str(claimed["id"]) == task_id
    await service.register_identity(identity)
    command = ["python", "-c", "print('agentos-sandbox-ok')"]
    action = ActionRequest(
        project_id=project_id,
        agent_id=identity.agent_id,
        task_id=task_id,
        action_type="shell_command",
        description="bounded integration proof",
        command=command,
        payload={"command": command},
    )
    try:
        result = await service.request_execution(action.model_dump(mode="json"))
        assert result["executed"] is True
        assert result["result"]["exit_code"] == 0
        assert "agentos-sandbox-ok" in result["result"]["output"]
    finally:
        await service.db.disconnect()
        await service.dragonfly.close()


@pytest.mark.asyncio
async def test_live_delivery_path_reaches_only_a_fenced_integrated_dod() -> None:
    """Prove the canonical artifact -> review -> sandbox -> merge -> evaluator path."""

    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    service = ExecutionService(settings)
    evaluator = None
    await service.db.initialize_schema()
    project_id, task_id = await _create_planned_test_project(
        service.db,
        settings,
        "live-dod-delivery-proof",
        "Complete the real governed delivery path.",
        allowed_paths=["proof"],
    )
    await ProjectRepository(service.db).update_status(project_id, "RUNNING")
    identity = AgentIdentity(
        project_id=project_id,
        agent_id="delivery-qa-1",
        role="qa_engineer",
        allowed_actions=["write_file", "shell_command"],
        allowed_paths=["proof"],
    )
    await AgentRepository(service.db).register_agent(
        identity.agent_id,
        project_id,
        identity.role,
        "delivery",
        permissions={
            "allowed_actions": identity.allowed_actions,
            "ownership_domains": identity.allowed_paths,
        },
    )
    await AgentRepository(service.db).register_agent(
        "code_reviewer-1", project_id, "code_reviewer", "review"
    )
    claimed = await TaskRepository(service.db).claim_next(
        project_id, identity.agent_id, identity.role
    )
    assert claimed is not None and str(claimed["id"]) == task_id
    await TaskRepository(service.db).update_task_status(task_id, "IN_PROGRESS")
    await service.register_identity(identity)
    try:
        write = await service.request_execution(
            ActionRequest(
                project_id=project_id,
                agent_id=identity.agent_id,
                task_id=task_id,
                action_type="write_file",
                description="Write the checksum-bound delivery proof.",
                target_paths=["proof/result.txt"],
                payload={"file_path": "proof/result.txt", "content": "proof\n"},
            ).model_dump(mode="json")
        )
        assert write["executed"] is True
        artifact = write["result"]
        command = ["python", "-c", "print('proof')"]
        command_result = await service.request_execution(
            ActionRequest(
                project_id=project_id,
                agent_id=identity.agent_id,
                task_id=task_id,
                action_type="shell_command",
                description="Run the exact contracted verification command.",
                command=command,
                payload={"command": command},
            ).model_dump(mode="json")
        )
        assert command_result["executed"] is True
        assert command_result["result"]["exit_code"] == 0

        dod = DoDRepository(service.db)
        await dod.add_evidence(
            project_id,
            "proof",
            "artifact",
            identity.agent_id,
            "The task owner produced a durable checksum-bound MinIO artifact.",
            True,
            artifact_id=artifact["artifact_id"],
            checksum_sha256=artifact["checksum_sha256"],
            task_id=task_id,
            source_role=identity.role,
            subject_commit=artifact["git_commit"],
            watched_paths=["proof/result.txt"],
        )
        await dod.add_evidence(
            project_id,
            "proof",
            "test",
            identity.agent_id,
            "The exact contracted command passed in the restricted task sandbox.",
            True,
            task_id=task_id,
            source_role=identity.role,
            command=json.dumps(command),
            exit_code=0,
            subject_commit=command_result["result"]["git_commit"],
            sandbox_digest=command_result["result"]["sandbox_digest"],
            watched_paths=["proof/result.txt"],
        )
        await dod.add_evidence(
            project_id,
            "proof",
            "review",
            "code_reviewer-1",
            "An authenticated independent reviewer approved this exact artifact and revision.",
            True,
            artifact_id=artifact["artifact_id"],
            task_id=task_id,
            source_role="code_reviewer",
            subject_commit=artifact["git_commit"],
            watched_paths=["proof/result.txt"],
        )

        merged = await service.merge_task(project_id, task_id, identity.agent_id)
        assert merged["success"] is True, merged
        assert merged["integrated_commit"]
        task_row = await TaskRepository(service.db).get(task_id)
        assert task_row is not None and task_row["status"] == "COMPLETED"

        evaluator_class = DoDEvaluatorActor.__ray_metadata__.modified_class
        evaluator = evaluator_class(settings.model_dump(mode="python"))
        evaluation = await evaluator.evaluate(project_id)
        assert evaluation["status"] == "SATISFIED"
        assert evaluation["satisfied"] is True
        assert evaluation["integration_head"] == merged["integrated_commit"]
        assert evaluation["gaps"] == []
        assert await dod.finalize_project(project_id, evaluation["evaluation_run_id"]) is True
        project = await ProjectRepository(service.db).get(project_id)
        assert project is not None and project["status"] == "DOD_SATISFIED"
    finally:
        if evaluator is not None:
            await evaluator.db.disconnect()
        await service.db.disconnect()
        await service.dragonfly.close()


@pytest.mark.asyncio
async def test_dod_evaluation_snapshot_fence_and_terminal_write_barrier() -> None:
    settings = Settings(environment="test", postgres_pool_min_size=1, postgres_pool_max_size=2)
    postgres = PostgresClient(settings)
    await postgres.initialize_schema()
    project_id, task_id = await _create_planned_test_project(
        postgres,
        settings,
        "dod-fence-proof",
        "Prove that finalization and late writes are fenced.",
    )
    projects = ProjectRepository(postgres)
    tasks = TaskRepository(postgres)
    dod = DoDRepository(postgres)
    await projects.update_status(project_id, "RUNNING")
    check = await postgres.fetchrow(
        "SELECT * FROM dod_checks WHERE project_id=$1 AND criterion_id='proof'",
        UUID(project_id),
    )
    assert check is not None

    stale_run = await dod.start_evaluation(project_id, "integration-test")
    coalesced = await dod.start_evaluation(project_id, "integration-test-concurrent")
    assert coalesced["id"] == stale_run["id"]
    assert coalesced["reused_running"] is True
    claimed = await tasks.claim_next(project_id, "qa_engineer-1", "qa_engineer")
    assert claimed is not None and str(claimed["id"]) == task_id
    await tasks.update_task_status(task_id, "IN_PROGRESS")
    stale = await dod.persist_evaluation(
        str(stale_run["id"]),
        [
            {
                "criterion_id": "proof",
                "criterion_hash": check["criterion_hash"],
                "status": "MISSING",
                "reasons": [{"code": "TEST_MUTATION", "message": "generation changed"}],
                "evidence_ids": [],
            }
        ],
        "UNSATISFIED",
        [{"code": "TEST_MUTATION", "message": "generation changed"}],
    )
    assert stale == {
        "evaluation_run_id": str(stale_run["id"]),
        "status": "STALE",
        "stale": True,
    }
    stale_item = await postgres.fetchrow(
        "SELECT status,reasons FROM dod_evaluation_items WHERE evaluation_run_id=$1",
        stale_run["id"],
    )
    stale_summary = await postgres.fetchval(
        "SELECT failure_summary FROM dod_evaluation_runs WHERE id=$1", stale_run["id"]
    )
    assert stale_item is not None and stale_item["status"] == "STALE"
    assert any(reason["code"] == "EVALUATION_SNAPSHOT_STALE" for reason in stale_item["reasons"])
    assert any(reason["code"] == "EVALUATION_SNAPSHOT_STALE" for reason in stale_summary)

    evidence_id = await dod.add_evidence(
        project_id,
        "proof",
        "test",
        "qa_engineer-1",
        "The exact proof command exited zero on the task branch.",
        True,
        task_id=task_id,
        source_role="qa_engineer",
        command=json.dumps(["python", "-c", "print('proof')"]),
        exit_code=0,
        subject_commit="c" * 40,
    )
    with pytest.raises(asyncpg.RaiseError, match="append-only"):
        await postgres.execute(
            "UPDATE dod_evidence SET summary='mutated' WHERE id=$1", UUID(evidence_id)
        )

    await tasks.update_task_status(task_id, "UNDER_REVIEW")
    await tasks.update_task_status(task_id, "COMPLETED")
    await postgres.execute(
        "UPDATE projects SET integration_head=$2,evidence_generation=evidence_generation+1 WHERE id=$1",
        UUID(project_id),
        "d" * 40,
    )
    satisfied_run = await dod.start_evaluation(project_id, "dod_evaluator")
    persisted = await dod.persist_evaluation(
        str(satisfied_run["id"]),
        [
            {
                "criterion_id": "proof",
                "criterion_hash": check["criterion_hash"],
                "status": "SATISFIED",
                "reasons": [],
                "evidence_ids": [evidence_id],
            }
        ],
        "SATISFIED",
        [],
    )
    assert persisted["status"] == "SATISFIED"
    newer_evidence_id = await dod.add_evidence(
        project_id,
        "proof",
        "test",
        "qa_engineer-1",
        "A newer result advanced the evidence generation before finalization.",
        True,
        task_id=task_id,
        source_role="qa_engineer",
        command=json.dumps(["python", "-c", "print('proof')"]),
        exit_code=0,
        subject_commit="d" * 40,
    )
    assert await dod.finalize_project(project_id, str(satisfied_run["id"])) is False
    final_run = await dod.start_evaluation(project_id, "dod_evaluator")
    final_persisted = await dod.persist_evaluation(
        str(final_run["id"]),
        [
            {
                "criterion_id": "proof",
                "criterion_hash": check["criterion_hash"],
                "status": "SATISFIED",
                "reasons": [],
                "evidence_ids": [newer_evidence_id],
            }
        ],
        "SATISFIED",
        [],
    )
    assert final_persisted["status"] == "SATISFIED"
    assert await dod.finalize_project(project_id, str(final_run["id"])) is True
    with pytest.raises(ValueError, match="immutable after project finalization"):
        await dod.add_evidence(
            project_id,
            "proof",
            "test",
            "qa_engineer-1",
            "late evidence",
            True,
            task_id=task_id,
            source_role="qa_engineer",
            command=json.dumps(["python", "-c", "print('proof')"]),
            exit_code=0,
            subject_commit="d" * 40,
        )
    await postgres.disconnect()
