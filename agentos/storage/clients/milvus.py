from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any

from pymilvus import DataType, MilvusClient

from agentos.config.settings import Settings

_FILTER_VALUE = re.compile(r"^[A-Za-z0-9_.:@/\-]{1,128}$")


@dataclass(frozen=True)
class VectorRecord:
    record_id: str
    vector: list[float]
    project_id: str
    agent_id: str
    scope: str
    kind: str
    content_ref: str
    importance: int = 3
    created_at_epoch: int = 0


@dataclass(frozen=True)
class VectorHit:
    record_id: str
    similarity: float
    project_id: str
    agent_id: str
    scope: str
    kind: str
    content_ref: str
    importance: int


class MilvusVectorClient:
    """Milvus semantic-index client.

    Milvus stores retrieval indexes and references only. PostgreSQL remains the
    authoritative memory record store.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        kwargs: dict[str, Any] = {
            "uri": settings.milvus_uri,
            "db_name": settings.milvus_database,
        }
        if settings.milvus_token and settings.milvus_token.get_secret_value():
            kwargs["token"] = settings.milvus_token.get_secret_value()
        self.client = MilvusClient(**kwargs)
        self.collection_name = f"{settings.milvus_collection_prefix}_semantic_memory"
        self._initialized = False
        self._initialize_lock = asyncio.Lock()

    @staticmethod
    def _filter_literal(value: str) -> str:
        if not _FILTER_VALUE.fullmatch(value):
            raise ValueError("Milvus filter value contains unsupported characters")
        return value

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._initialize_lock:
            if self._initialized:
                return
            exists = await asyncio.to_thread(
                self.client.has_collection, collection_name=self.collection_name
            )
            if not exists:
                schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
                schema.add_field(
                    field_name="id", datatype=DataType.VARCHAR, is_primary=True, max_length=64
                )
                schema.add_field(
                    field_name="vector",
                    datatype=DataType.FLOAT_VECTOR,
                    dim=self.settings.embedding_dimension,
                )
                schema.add_field(field_name="project_id", datatype=DataType.VARCHAR, max_length=64)
                schema.add_field(field_name="agent_id", datatype=DataType.VARCHAR, max_length=128)
                schema.add_field(field_name="scope", datatype=DataType.VARCHAR, max_length=64)
                schema.add_field(field_name="kind", datatype=DataType.VARCHAR, max_length=64)
                schema.add_field(
                    field_name="content_ref", datatype=DataType.VARCHAR, max_length=512
                )
                schema.add_field(field_name="importance", datatype=DataType.INT64)
                schema.add_field(field_name="created_at", datatype=DataType.INT64)
                index_params = self.client.prepare_index_params()
                index_params.add_index(
                    field_name="vector", index_type="AUTOINDEX", metric_type="COSINE"
                )
                await asyncio.to_thread(
                    self.client.create_collection,
                    collection_name=self.collection_name,
                    schema=schema,
                    index_params=index_params,
                    consistency_level="Strong",
                )
            await asyncio.to_thread(
                self.client.load_collection, collection_name=self.collection_name
            )
            self._initialized = True

    async def healthcheck(self) -> dict[str, Any]:
        collections = await asyncio.to_thread(self.client.list_collections)
        return {
            "service": "milvus",
            "healthy": True,
            "collections": collections,
        }

    def _validate_vector(self, vector: list[float]) -> None:
        if len(vector) != self.settings.embedding_dimension:
            raise ValueError(
                f"embedding dimension {len(vector)} does not match configured "
                f"dimension {self.settings.embedding_dimension}"
            )
        if not all(isinstance(value, (int, float)) for value in vector):
            raise TypeError("embedding contains a non-numeric value")

    async def upsert(self, record: VectorRecord) -> None:
        await self.initialize()
        self._validate_vector(record.vector)
        payload = {
            "id": record.record_id,
            "vector": [float(value) for value in record.vector],
            "project_id": record.project_id,
            "agent_id": record.agent_id,
            "scope": record.scope,
            "kind": record.kind,
            "content_ref": record.content_ref,
            "importance": record.importance,
            "created_at": record.created_at_epoch or int(time.time()),
        }
        await asyncio.to_thread(
            self.client.upsert, collection_name=self.collection_name, data=[payload]
        )

    async def search(
        self,
        *,
        vector: list[float],
        project_id: str,
        scopes: list[str],
        requester_agent_id: str,
        kinds: list[str] | None = None,
        limit: int = 8,
    ) -> list[VectorHit]:
        await self.initialize()
        self._validate_vector(vector)
        safe_project = self._filter_literal(project_id)
        safe_agent = self._filter_literal(requester_agent_id)
        safe_scopes = [self._filter_literal(scope) for scope in scopes]
        if not safe_scopes:
            return []
        scope_expression = ", ".join(f'"{scope}"' for scope in safe_scopes)
        expression = (
            f'project_id == "{safe_project}" and scope in [{scope_expression}] '
            f'and (scope != "private_agent_memory" or agent_id == "{safe_agent}")'
        )
        if kinds:
            safe_kinds = [self._filter_literal(kind) for kind in kinds]
            kind_expression = ", ".join(f'"{kind}"' for kind in safe_kinds)
            expression += f" and kind in [{kind_expression}]"

        results = await asyncio.to_thread(
            self.client.search,
            collection_name=self.collection_name,
            data=[vector],
            anns_field="vector",
            filter=expression,
            limit=max(1, min(limit, 100)),
            output_fields=[
                "project_id",
                "agent_id",
                "scope",
                "kind",
                "content_ref",
                "importance",
            ],
            search_params={"metric_type": "COSINE", "params": {}},
            consistency_level="Strong",
        )
        hits: list[VectorHit] = []
        for hit in results[0] if results else []:
            entity = hit.get("entity", {})
            hits.append(
                VectorHit(
                    record_id=str(hit.get("id")),
                    similarity=float(hit.get("distance", 0.0)),
                    project_id=str(entity.get("project_id", "")),
                    agent_id=str(entity.get("agent_id", "")),
                    scope=str(entity.get("scope", "")),
                    kind=str(entity.get("kind", "")),
                    content_ref=str(entity.get("content_ref", "")),
                    importance=int(entity.get("importance", 0)),
                )
            )
        return hits

    async def delete_project(self, project_id: str) -> None:
        safe_project = self._filter_literal(project_id)
        await self.initialize()
        await asyncio.to_thread(
            self.client.delete,
            collection_name=self.collection_name,
            filter=f'project_id == "{safe_project}"',
        )
