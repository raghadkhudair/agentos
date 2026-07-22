from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import ray
import structlog
from pydantic import BaseModel, Field

from agentos.config.loader import runtime_tuning
from agentos.config.settings import Settings
from agentos.governance.models import AgentIdentity
from agentos.storage.clients.milvus import MilvusVectorClient, VectorRecord
from agentos.storage.clients.minio import MinioObjectClient
from agentos.storage.clients.mongodb import MongoDocumentClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import EventRepository, MemoryRepository, TaskRepository

logger = structlog.get_logger()


class CatchUpPacket(BaseModel):
    project_id: str
    agent_id: str
    trigger_event_id: str
    recent_events: list[dict[str, Any]] = Field(default_factory=list)
    active_tasks: list[dict[str, Any]] = Field(default_factory=list)
    midterm_memories: list[dict[str, Any]] = Field(default_factory=list)
    longterm_memories: list[dict[str, Any]] = Field(default_factory=list)
    recommended_next_actions: list[str] = Field(default_factory=list)


class MemoryService:
    """Coordinates short-, mid-, and long-term memory without merging their stores."""

    _SECRET = re.compile(
        r"(?i)(api[_-]?key|password|secret|token|private[_-]?key)\s*[:=]\s*['\"]?[^\s'\"]{8,}"
    )

    def __init__(self, settings: Settings):
        self.settings = settings
        self.postgres = PostgresClient(settings)
        self.mongo = MongoDocumentClient(settings)
        self.milvus = MilvusVectorClient(settings)
        self.minio = MinioObjectClient(settings)
        self.memory_repo = MemoryRepository(self.postgres)
        self.event_repo = EventRepository(self.postgres)
        self.task_repo = TaskRepository(self.postgres)
        self.tuning = runtime_tuning().get("memory", {})

    @classmethod
    def scrub(cls, text: str) -> str:
        return cls._SECRET.sub("[REDACTED_BY_MEMORY_BROKER]", text)

    async def initialize(self) -> None:
        await self.postgres.connect()
        await self.mongo.initialize()
        await self.minio.initialize()
        await self.milvus.initialize()

    async def close(self) -> None:
        await self.postgres.disconnect()
        await self.mongo.close()

    async def remember(
        self,
        *,
        project_id: str,
        agent_id: str,
        scope: str,
        kind: str,
        title: str,
        content: str,
        importance: int,
        provider_gateway: Any,
        metadata: dict[str, Any] | None = None,
        promote_long_term: bool = True,
    ) -> dict[str, Any]:
        safe_content = self.scrub(content)
        result: dict[str, Any] = {"midterm_saved": False, "longterm_saved": False}
        if not promote_long_term:
            await self.mongo.append_memory(
                project_id=project_id,
                agent_id=agent_id,
                scope=scope,
                kind=kind,
                content=safe_content,
                metadata=metadata,
            )
            result["midterm_saved"] = True
            return result

        encoded = safe_content.encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        large_content = len(encoded) > 16_384
        memory_id = await self.memory_repo.save_memory_item(
            project_id=project_id,
            scope=scope,
            owner_agent_id=agent_id,
            memory_type=kind,
            title=self.scrub(title),
            content=safe_content,
            importance_score=importance,
            content_hash=digest,
            content_length=len(encoded),
            storage_status="PENDING_OBJECT" if large_content else "READY",
            metadata=metadata,
        )
        content_object_uri: str | None = None
        if large_content:
            try:
                object_name = f"{project_id}/memory/{uuid4()}.txt"
                obj = await self.minio.put_bytes(
                    bucket=self.settings.minio_memory_bucket,
                    object_name=object_name,
                    data=encoded,
                    content_type="text/plain; charset=utf-8",
                    metadata={"project-id": project_id, "agent-id": agent_id, "scope": scope},
                )
                if obj.sha256 != digest or obj.size != len(encoded):
                    raise OSError("MinIO memory object verification failed")
                content_object_uri = obj.uri
                await self.memory_repo.attach_memory_object(
                    memory_id,
                    object_uri=obj.uri,
                    object_version_id=obj.version_id,
                    content_hash=digest,
                    content_length=len(encoded),
                    preview=safe_content[:16_384],
                )
            except Exception:
                await self.memory_repo.mark_memory_object_failed(memory_id)
                raise
        await self.mongo.append_memory(
            project_id=project_id,
            agent_id=agent_id,
            scope=scope,
            kind=kind,
            content=safe_content,
            metadata={**(metadata or {}), "longterm_memory_id": memory_id},
        )
        result.update(
            midterm_saved=True,
            longterm_saved=True,
            memory_id=memory_id,
            content_object_uri=content_object_uri,
            semantic_indexed=False,
        )
        try:
            vector = await provider_gateway.get_embedding.remote(
                f"{title}\n{safe_content[:8000]}", project_id
            )
            milvus_id = str(uuid4())
            await self.milvus.upsert(
                VectorRecord(
                    record_id=milvus_id,
                    vector=vector,
                    project_id=project_id,
                    agent_id=agent_id,
                    scope=scope,
                    kind=kind,
                    content_ref=memory_id,
                    importance=importance,
                    created_at_epoch=int(datetime.now(UTC).timestamp()),
                )
            )
            await self.memory_repo.link_vector(memory_id, milvus_id, self.settings.embedding_model)
            result.update(semantic_indexed=True, milvus_record_id=milvus_id)
        except Exception as error:
            logger.warning(
                "semantic_memory_indexing_unavailable",
                project_id=project_id,
                error_type=type(error).__name__,
            )
        return result

    async def _hydrate_longterm_row(self, row: dict[str, Any]) -> dict[str, Any]:
        safe_row = dict(row)
        uri = safe_row.get("content_object_uri")
        if not uri:
            return safe_row
        parsed = urlparse(str(uri))
        if parsed.scheme != "minio" or not parsed.netloc or not parsed.path.lstrip("/"):
            raise ValueError("invalid stored MinIO memory URI")
        version_id = safe_row.get("content_object_version_id")
        if not version_id:
            version_values = parse_qs(parsed.query).get("versionId", [])
            version_id = version_values[-1] if version_values else None
        data = await self.minio.get_bytes(
            bucket=parsed.netloc,
            object_name=parsed.path.lstrip("/"),
            version_id=str(version_id) if version_id else None,
        )
        if len(data) != int(safe_row.get("content_length") or 0):
            raise OSError("stored memory length does not match PostgreSQL metadata")
        if hashlib.sha256(data).hexdigest() != str(safe_row.get("content_hash")):
            raise OSError("stored memory checksum does not match PostgreSQL metadata")
        safe_row["content"] = data.decode("utf-8")
        return safe_row

    async def build_packet(
        self,
        *,
        identity: AgentIdentity,
        trigger_event_id: str,
        scopes: list[str],
        provider_gateway: Any,
        query_text: str,
    ) -> CatchUpPacket:
        recent_events = await self.event_repo.recent(
            identity.project_id, int(self.tuning.get("recent_event_limit", 20))
        )
        active_tasks = await self.task_repo.get_active_tasks(identity.project_id)
        midterm = await self.mongo.recent_memories(
            project_id=identity.project_id,
            requester_agent_id=identity.agent_id,
            scopes=scopes,
            limit=int(self.tuning.get("recent_midterm_limit", 20)),
        )

        longterm: list[dict[str, Any]] = []
        if query_text.strip():
            semantic_rows: list[dict[str, Any]] = []
            try:
                vector = await provider_gateway.get_embedding.remote(
                    query_text, identity.project_id
                )
                hits = await self.milvus.search(
                    vector=vector,
                    project_id=identity.project_id,
                    scopes=scopes,
                    requester_agent_id=identity.agent_id,
                    limit=int(self.tuning.get("semantic_result_limit", 8)),
                )
                semantic_rows = await self.memory_repo.get_by_ids(
                    [hit.content_ref for hit in hits],
                    project_id=identity.project_id,
                    agent_id=identity.agent_id,
                    scopes=scopes,
                )
            except Exception as error:
                logger.warning(
                    "semantic_memory_retrieval_unavailable",
                    project_id=identity.project_id,
                    error_type=type(error).__name__,
                )
            lexical_rows = await self.memory_repo.lexical_search(
                identity.project_id,
                identity.agent_id,
                scopes,
                query_text,
                limit=int(self.tuning.get("semantic_result_limit", 8)),
            )
            seen: set[str] = set()
            for row in [*semantic_rows, *lexical_rows]:
                key = str(row["id"])
                if key in seen:
                    continue
                seen.add(key)
                safe_row = await self._hydrate_longterm_row(dict(row))
                safe_row["content"] = self.scrub(str(safe_row.get("content", "")))
                longterm.append(safe_row)

        max_chars = int(self.tuning.get("max_prompt_memory_characters", 16_000))
        consumed = 0
        bounded_longterm: list[dict[str, Any]] = []
        for row in longterm:
            size = len(str(row.get("content", "")))
            if consumed + size > max_chars:
                break
            bounded_longterm.append(row)
            consumed += size

        return CatchUpPacket(
            project_id=identity.project_id,
            agent_id=identity.agent_id,
            trigger_event_id=trigger_event_id,
            recent_events=[self._json_safe_event(row) for row in recent_events],
            active_tasks=[self._json_safe(row) for row in active_tasks],
            midterm_memories=[self._json_safe(row) for row in midterm],
            longterm_memories=[self._json_safe(row) for row in bounded_longterm],
            recommended_next_actions=[
                "Select only an unblocked task inside the agent ownership boundary.",
                "Publish a typed collaboration update after a meaningful state change.",
                "Record test and review evidence before claiming completion.",
            ],
        )

    @staticmethod
    def _json_safe(row: dict[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(json.dumps(row, default=str)))

    @classmethod
    def _json_safe_event(cls, row: dict[str, Any]) -> dict[str, Any]:
        safe = cls._json_safe(row)
        if "payload" in safe:
            safe["payload"] = json.loads(cls.scrub(json.dumps(safe["payload"], default=str)))
        return safe


@ray.remote(num_cpus=0.2, max_concurrency=32)  # type: ignore[call-overload]
class MemoryBrokerActor:
    def __init__(self, settings_payload: dict[str, Any]):
        self.service = MemoryService(Settings(**settings_payload))
        self.identities: dict[str, AgentIdentity] = {}

    async def register_agent_identity(self, identity_data: dict[str, Any]) -> None:
        identity = AgentIdentity.model_validate(identity_data)
        row = await self.service.postgres.fetchrow(
            "SELECT role,project_id,memory_scopes FROM agents WHERE id=$1 AND project_id=$2",
            identity.agent_id,
            identity.project_id,
        )
        if row is None:
            raise PermissionError("memory identity is not registered in PostgreSQL")
        if str(row["role"]) != identity.role or str(row["project_id"]) != identity.project_id:
            raise PermissionError("memory identity does not match the persisted agent")
        persisted_scopes = set(row["memory_scopes"] or [])
        if not set(identity.memory_scopes).issubset(persisted_scopes):
            raise PermissionError("memory identity claims unregistered scopes")
        existing = self.identities.get(identity.agent_id)
        if existing is not None and existing != identity:
            raise PermissionError("memory identity is already registered with different claims")
        self.identities[identity.agent_id] = identity

    async def record_memory(self, **kwargs: Any) -> dict[str, Any]:
        agent_id = kwargs.get("agent_id")
        if not isinstance(agent_id, str):
            raise PermissionError("memory writer identity is required")
        identity = self.identities.get(agent_id)
        if not identity:
            raise PermissionError("unknown memory writer identity")
        if kwargs.get("project_id") != identity.project_id:
            raise PermissionError("memory writer cannot cross project boundaries")
        scope = kwargs.get("scope")
        if scope not in identity.memory_scopes:
            raise PermissionError(f"agent cannot write memory scope {scope!r}")
        return await self.service.remember(**kwargs)

    async def build_catchup_packet(
        self,
        *,
        project_id: str,
        agent_id: str,
        trigger_event_id: str,
        requested_scopes: list[str] | None = None,
        provider_gateway: Any,
        query_text: str = "",
    ) -> dict[str, Any]:
        identity = self.identities.get(agent_id)
        if identity is None or identity.project_id != project_id:
            raise PermissionError("unknown or cross-project memory requester")
        scopes = list(identity.memory_scopes)
        if requested_scopes is not None:
            scopes = [scope for scope in requested_scopes if scope in identity.memory_scopes]
        if not scopes:
            raise PermissionError("memory request has no authorized scope")
        packet = await self.service.build_packet(
            identity=identity,
            trigger_event_id=trigger_event_id,
            scopes=scopes,
            provider_gateway=provider_gateway,
            query_text=query_text,
        )
        return packet.model_dump(mode="json")


MemoryBroker = MemoryService

__all__ = ["CatchUpPacket", "MemoryBroker", "MemoryBrokerActor", "MemoryService"]
