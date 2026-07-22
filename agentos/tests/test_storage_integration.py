from __future__ import annotations

import hashlib
import os
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from agentos.config.settings import Settings
from agentos.execution.supervisor import ExecutionService
from agentos.governance.models import ActionRequest, AgentIdentity
from agentos.memory.broker import MemoryService
from agentos.messaging.events import Event, EventType
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

        project_id = await ProjectRepository(postgres).create_project(
            "outbox-codec-proof", "Verify native JSON outbox delivery", []
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

        other_project_id = await ProjectRepository(postgres).create_project(
            "isolation-proof", "Reject cross-project task references", []
        )
        other_task_id = await TaskRepository(postgres).create_task(
            other_project_id, "Other task", "Belongs to the second project"
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
    project_id = await ProjectRepository(service.db).create_project(
        "sandbox-proof", "verify restricted execution", []
    )
    task_id = await TaskRepository(service.db).create_task(
        project_id,
        "Sandbox proof",
        "Run one bounded command through the Docker socket proxy",
        owner_role="qa_engineer",
        allowed_paths=["proof"],
    )
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
