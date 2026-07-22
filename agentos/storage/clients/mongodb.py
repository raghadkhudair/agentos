from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.server_api import ServerApi

from agentos.config.settings import Settings


class MongoDocumentClient:
    """Async MongoDB client for expiring mid-term agent working memory."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
            settings.mongodb_dsn,
            server_api=ServerApi("1"),
            appname="agentos",
            tz_aware=True,
            connectTimeoutMS=5_000,
            serverSelectionTimeoutMS=5_000,
            retryWrites=True,
            retryReads=True,
        )
        self.database = self.client[settings.mongodb_database]
        self.memory = self.database["agent_working_memory"]
        self.agent_state = self.database["agent_runtime_state"]
        self._initialized = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        await self.memory.create_index([("expires_at", ASCENDING)], expireAfterSeconds=0)
        await self.memory.create_index(
            [("project_id", ASCENDING), ("scope", ASCENDING), ("created_at", DESCENDING)]
        )
        await self.memory.create_index(
            [("project_id", ASCENDING), ("agent_id", ASCENDING), ("created_at", DESCENDING)]
        )
        await self.agent_state.create_index(
            [("project_id", ASCENDING), ("agent_id", ASCENDING)], unique=True
        )
        self._initialized = True

    async def close(self) -> None:
        await self.client.close()

    async def healthcheck(self) -> dict[str, Any]:
        response = await self.database.command("ping")
        build = await self.database.command("buildInfo")
        return {
            "service": "mongodb",
            "healthy": response.get("ok") == 1.0,
            "version": build.get("version"),
        }

    async def append_memory(
        self,
        *,
        project_id: str,
        agent_id: str,
        scope: str,
        kind: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        ttl_seconds: int | None = None,
    ) -> str:
        await self.initialize()
        now = datetime.now(UTC)
        result = await self.memory.insert_one(
            {
                "project_id": project_id,
                "agent_id": agent_id,
                "scope": scope,
                "kind": kind,
                "content": content,
                "metadata": metadata or {},
                "created_at": now,
                "expires_at": now
                + timedelta(seconds=ttl_seconds or self.settings.midterm_memory_ttl_seconds),
            }
        )
        return str(result.inserted_id)

    async def recent_memories(
        self,
        *,
        project_id: str,
        requester_agent_id: str,
        scopes: list[str],
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        await self.initialize()
        allowed = [scope for scope in scopes if scope]
        if not allowed:
            return []
        access_filter: dict[str, Any] = {
            "project_id": project_id,
            "scope": {"$in": allowed},
            "$or": [
                {"scope": {"$ne": "private_agent_memory"}},
                {"agent_id": requester_agent_id},
            ],
        }
        cursor = self.memory.find(
            access_filter, {"content": 1, "scope": 1, "kind": 1, "agent_id": 1, "created_at": 1}
        )
        rows = await cursor.sort("created_at", DESCENDING).limit(max(1, min(limit, 100))).to_list()
        for row in rows:
            row["_id"] = str(row["_id"])
        return rows

    async def save_agent_state(
        self, *, project_id: str, agent_id: str, state: dict[str, Any]
    ) -> None:
        await self.initialize()
        await self.agent_state.update_one(
            {"project_id": project_id, "agent_id": agent_id},
            {
                "$set": {
                    "state": state,
                    "updated_at": datetime.now(UTC),
                }
            },
            upsert=True,
        )

    async def load_agent_state(self, *, project_id: str, agent_id: str) -> dict[str, Any] | None:
        await self.initialize()
        row = await self.agent_state.find_one({"project_id": project_id, "agent_id": agent_id})
        return row.get("state") if row else None
