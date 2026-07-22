from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from agentos.messaging.events import Event
from agentos.runtime.team_plan import DoDCriterion
from agentos.storage.clients.postgres import PostgresClient


def _uuid(value: str | UUID) -> UUID:
    return value if isinstance(value, UUID) else UUID(str(value))


class ProjectState(StrEnum):
    INITIALIZING = "INITIALIZING"
    PLANNING = "PLANNING"
    TEAM_FORMING = "TEAM_FORMING"
    RUNNING = "RUNNING"
    REPLANNING = "REPLANNING"
    INTEGRATING = "INTEGRATING"
    VERIFYING = "VERIFYING"
    PAUSED = "PAUSED"
    BLOCKED_REQUIRES_APPROVAL = "BLOCKED_REQUIRES_APPROVAL"
    BLOCKED_REQUIRES_INPUT = "BLOCKED_REQUIRES_INPUT"
    DOD_SATISFIED = "DOD_SATISFIED"
    FAILED_BY_POLICY = "FAILED_BY_POLICY"
    STOPPED_BY_USER = "STOPPED_BY_USER"


class ProjectRepository:
    TERMINAL_STATES = {
        ProjectState.DOD_SATISFIED,
        ProjectState.FAILED_BY_POLICY,
        ProjectState.STOPPED_BY_USER,
    }

    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def create_project(
        self,
        name: str,
        request: str,
        dod: list[Any],
        *,
        project_key: str | None = None,
        architecture: str = "",
        assumptions: list[str] | None = None,
    ) -> str:
        key = project_key or f"{name.lower().replace(' ', '-')}-{uuid4().hex[:12]}"
        row = await self.db.fetchrow(
            """
            INSERT INTO projects(project_key, name, request, dod, architecture, assumptions)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb)
            RETURNING id
            """,
            key,
            name,
            request,
            json.dumps(dod),
            architecture,
            json.dumps(assumptions or []),
        )
        assert row is not None
        return str(row["id"])

    async def update_plan(
        self,
        project_id: str,
        *,
        name: str,
        dod: list[Any],
        architecture: str,
        assumptions: list[str],
    ) -> None:
        await self.db.execute(
            """
            UPDATE projects
            SET name=$2, dod=$3::jsonb, architecture=$4, assumptions=$5::jsonb
            WHERE id=$1
            """,
            _uuid(project_id),
            name,
            json.dumps(dod),
            architecture,
            json.dumps(assumptions),
        )

    async def update_status(self, project_id: str, status: str | ProjectState) -> None:
        target = ProjectState(str(status)) if not isinstance(status, ProjectState) else status
        async with self.db.transaction() as connection:
            current = await connection.fetchval(
                "SELECT status FROM projects WHERE id=$1 FOR UPDATE", _uuid(project_id)
            )
            if current is None:
                raise LookupError(f"project not found: {project_id}")
            if ProjectState(current) in self.TERMINAL_STATES and current != target.value:
                raise ValueError(f"terminal project state {current} is immutable")
            await connection.execute(
                "UPDATE projects SET status=$2 WHERE id=$1", _uuid(project_id), target.value
            )

    async def get(self, project_id: str) -> dict[str, Any] | None:
        row = await self.db.fetchrow("SELECT * FROM projects WHERE id=$1", _uuid(project_id))
        return dict(row) if row else None


class AgentRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def register_agent(
        self,
        agent_id: str,
        project_id: str,
        role: str,
        squad: str,
        *,
        status: str = "ACTIVE",
        memory_scopes: list[str] | None = None,
        permissions: dict[str, Any] | None = None,
        provider_assignment: dict[str, Any] | None = None,
        resource_allocation: dict[str, Any] | None = None,
    ) -> None:
        await self.db.execute(
            """
            INSERT INTO agents(
                project_id,id,role,squad,status,permissions,memory_scopes,
                provider_assignment,resource_allocation,last_heartbeat_at
            ) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8::jsonb,$9::jsonb,now())
            ON CONFLICT(project_id,id) DO UPDATE SET
                role=EXCLUDED.role,squad=EXCLUDED.squad,status=EXCLUDED.status,
                permissions=EXCLUDED.permissions,memory_scopes=EXCLUDED.memory_scopes,
                provider_assignment=EXCLUDED.provider_assignment,
                resource_allocation=EXCLUDED.resource_allocation,last_heartbeat_at=now()
            """,
            _uuid(project_id),
            agent_id,
            role,
            squad,
            status,
            json.dumps(permissions or {}),
            memory_scopes or [],
            json.dumps(provider_assignment or {}),
            json.dumps(resource_allocation or {}),
        )

    async def heartbeat(self, project_id: str, agent_id: str, status: str) -> None:
        await self.db.execute(
            "UPDATE agents SET status=$3,last_heartbeat_at=now() WHERE project_id=$1 AND id=$2",
            _uuid(project_id),
            agent_id,
            status,
        )

    async def list_project_agents(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            "SELECT * FROM agents WHERE project_id=$1 ORDER BY id", _uuid(project_id)
        )
        return [dict(row) for row in rows]


class EventRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def save_event(self, project_id: str, event: Event) -> None:
        payload = event.model_dump(mode="json")
        async with self.db.transaction() as connection:
            await connection.execute(
                """
                INSERT INTO events(
                    id,project_id,event_type,topic,producer_agent_id,target_agent_id,
                    payload,payload_object_uri,correlation_id,causation_id,schema_version,created_at
                ) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12)
                ON CONFLICT(id) DO NOTHING
                """,
                event.event_id,
                _uuid(project_id),
                event.event_type.value,
                event.topic,
                event.producer_agent_id,
                event.target_agent_id,
                json.dumps(event.payload),
                event.payload_object_uri,
                event.correlation_id,
                event.causation_id,
                event.schema_version,
                event.created_at,
            )
            await connection.execute(
                """
                INSERT INTO event_outbox(event_id,project_id,topic,payload)
                VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(event_id) DO NOTHING
                """,
                event.event_id,
                _uuid(project_id),
                event.topic,
                json.dumps(payload),
            )

    async def get_event(self, event_id: str) -> dict[str, Any] | None:
        row = await self.db.fetchrow("SELECT * FROM events WHERE id=$1", _uuid(event_id))
        return dict(row) if row else None

    async def recent(self, project_id: str, limit: int = 20) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            "SELECT * FROM events WHERE project_id=$1 ORDER BY created_at DESC LIMIT $2",
            _uuid(project_id),
            max(1, min(limit, 200)),
        )
        return [dict(row) for row in rows]

    async def claim_outbox(self, project_id: str, limit: int = 100) -> list[dict[str, Any]]:
        async with self.db.transaction() as connection:
            rows = await connection.fetch(
                """
                SELECT * FROM event_outbox
                WHERE project_id=$1 AND published_at IS NULL AND available_at <= now()
                ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $2
                """,
                _uuid(project_id),
                max(1, min(limit, 1000)),
            )
            ids = [row["id"] for row in rows]
            if ids:
                await connection.execute(
                    """
                    UPDATE event_outbox
                    SET attempt_count=attempt_count+1, available_at=now()+interval '5 minutes'
                    WHERE id=ANY($1::bigint[])
                    """,
                    ids,
                )
            return [dict(row) for row in rows]

    async def mark_outbox_published(self, outbox_id: int) -> None:
        await self.db.execute(
            "UPDATE event_outbox SET published_at=now(),last_error=NULL WHERE id=$1", outbox_id
        )

    async def mark_outbox_failed(self, outbox_id: int, error: str, attempt: int) -> None:
        delay = min(300, 2 ** min(attempt, 8))
        await self.db.execute(
            """
            UPDATE event_outbox SET last_error=$2,available_at=now()+($3*interval '1 second')
            WHERE id=$1
            """,
            outbox_id,
            error[:2000],
            delay,
        )

    async def event_receipt_status(
        self, project_id: str, event_id: str, agent_id: str
    ) -> str | None:
        value = await self.db.fetchval(
            """
            SELECT status FROM event_receipts
            WHERE project_id=$1 AND event_id=$2 AND agent_id=$3
            """,
            _uuid(project_id),
            _uuid(event_id),
            agent_id,
        )
        return str(value) if value is not None else None

    async def record_event_delivery(
        self,
        project_id: str,
        event_id: str,
        agent_id: str,
        stream_id: str,
    ) -> bool:
        result = await self.db.execute(
            """
            INSERT INTO event_receipts(project_id,event_id,agent_id,stream_id,status)
            VALUES($1,$2,$3,$4,'DELIVERED')
            ON CONFLICT(project_id,event_id,agent_id) DO NOTHING
            """,
            _uuid(project_id),
            _uuid(event_id),
            agent_id,
            stream_id,
        )
        return result.endswith(" 1")

    async def claim_event_receipt(
        self,
        project_id: str,
        event_id: str,
        agent_id: str,
        consumer_name: str,
        lease_seconds: int,
    ) -> bool:
        row = await self.db.fetchrow(
            """
            UPDATE event_receipts
            SET status='PROCESSING',consumer_name=$4,attempt_count=attempt_count+1,
                lease_expires_at=now()+($5*interval '1 second'),last_error=NULL
            WHERE project_id=$1 AND event_id=$2 AND agent_id=$3
              AND (
                status IN ('DELIVERED','FAILED')
                OR (status='PROCESSING' AND lease_expires_at < now())
              )
            RETURNING attempt_count
            """,
            _uuid(project_id),
            _uuid(event_id),
            agent_id,
            consumer_name,
            max(1, lease_seconds),
        )
        return row is not None

    async def complete_event_receipt(
        self, project_id: str, event_id: str, agent_id: str, consumer_name: str
    ) -> bool:
        result = await self.db.execute(
            """
            UPDATE event_receipts
            SET status='PROCESSED',processed_at=now(),lease_expires_at=NULL,last_error=NULL
            WHERE project_id=$1 AND event_id=$2 AND agent_id=$3
              AND status='PROCESSING' AND consumer_name=$4
            """,
            _uuid(project_id),
            _uuid(event_id),
            agent_id,
            consumer_name,
        )
        return result.endswith(" 1")

    async def fail_event_receipt(
        self,
        project_id: str,
        event_id: str,
        agent_id: str,
        consumer_name: str,
        error: str,
    ) -> bool:
        result = await self.db.execute(
            """
            UPDATE event_receipts
            SET status='FAILED',lease_expires_at=NULL,last_error=$5
            WHERE project_id=$1 AND event_id=$2 AND agent_id=$3
              AND status='PROCESSING' AND consumer_name=$4
            """,
            _uuid(project_id),
            _uuid(event_id),
            agent_id,
            consumer_name,
            error[:2000],
        )
        return result.endswith(" 1")

    async def list_pending_approvals(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            "SELECT * FROM approval_requests WHERE project_id=$1 AND status='PENDING' ORDER BY created_at",
            _uuid(project_id),
        )
        return [dict(row) for row in rows]


class TaskRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def create_task(
        self,
        project_id: str,
        title: str,
        description: str,
        owner_agent_id: str | None = None,
        owner_role: str | None = None,
        parent_task_id: str | None = None,
        priority: int = 3,
        acceptance_criteria: list[str] | None = None,
        allowed_paths: list[str] | None = None,
        blocked_paths: list[str] | None = None,
        expected_outputs: list[str] | None = None,
        required_reviewers: list[str] | None = None,
        dod_criteria: list[str] | None = None,
        affected_contracts: list[str] | None = None,
        risk_level: str = "LOW",
        complexity: str = "standard",
        external_key: str | None = None,
        embedding: list[float] | None = None,
    ) -> str:
        del embedding  # semantic task records are indexed by MemoryService in Milvus
        row = await self.db.fetchrow(
            """
            INSERT INTO tasks(
                project_id,parent_task_id,external_key,title,description,owner_agent_id,owner_role,
                priority,complexity,acceptance_criteria,allowed_paths,blocked_paths,
                expected_outputs,required_reviewers,dod_criteria,affected_contracts,risk_level
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,$17)
            ON CONFLICT(project_id,external_key) WHERE external_key IS NOT NULL
            DO UPDATE SET updated_at=now() RETURNING id
            """,
            _uuid(project_id),
            _uuid(parent_task_id) if parent_task_id else None,
            external_key,
            title,
            description,
            owner_agent_id,
            owner_role,
            priority,
            complexity,
            json.dumps(acceptance_criteria or []),
            allowed_paths or [],
            blocked_paths or [],
            expected_outputs or [],
            required_reviewers or [],
            dod_criteria or [],
            affected_contracts or [],
            risk_level,
        )
        assert row is not None
        return str(row["id"])

    async def add_dependency(self, task_id: str, depends_on_task_id: str) -> None:
        await self.db.execute(
            "INSERT INTO task_dependencies VALUES($1,$2) ON CONFLICT DO NOTHING",
            _uuid(task_id),
            _uuid(depends_on_task_id),
        )

    async def get_active_tasks(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            """
            SELECT t.*,
              COALESCE(array_agg(td.depends_on_task_id::text)
              FILTER(WHERE td.depends_on_task_id IS NOT NULL),'{}'::text[]) dependencies
            FROM tasks t LEFT JOIN task_dependencies td ON td.task_id=t.id
            WHERE t.project_id=$1 AND t.status NOT IN ('COMPLETED','CANCELLED')
            GROUP BY t.id ORDER BY t.priority DESC,t.created_at
            """,
            _uuid(project_id),
        )
        return [dict(row) for row in rows]

    async def claim_next(
        self, project_id: str, agent_id: str, agent_role: str
    ) -> dict[str, Any] | None:
        async with self.db.transaction() as connection:
            await connection.execute(
                """
                UPDATE tasks SET status='PENDING',owner_agent_id=NULL,lease_expires_at=NULL
                WHERE project_id=$1 AND status IN ('CLAIMED','IN_PROGRESS','UNDER_REVIEW')
                  AND lease_expires_at < now()
                """,
                _uuid(project_id),
            )
            row = await connection.fetchrow(
                """
                SELECT t.* FROM tasks t
                WHERE t.project_id=$1 AND t.status='PENDING'
                  AND (t.owner_role IS NULL OR t.owner_role=$2)
                  AND NOT EXISTS(
                    SELECT 1 FROM task_dependencies td JOIN tasks dependency ON dependency.id=td.depends_on_task_id
                    WHERE td.task_id=t.id AND dependency.status <> 'COMPLETED'
                  )
                ORDER BY t.priority DESC,t.created_at FOR UPDATE SKIP LOCKED LIMIT 1
                """,
                _uuid(project_id),
                agent_role,
            )
            if row is None:
                return None
            await connection.execute(
                """
                UPDATE tasks SET status='CLAIMED',owner_agent_id=$2,lease_expires_at=now()+interval '2 minutes'
                WHERE id=$1
                """,
                row["id"],
                agent_id,
            )
            return dict(row)

    async def renew_lease(self, task_id: str, agent_id: str) -> bool:
        result = await self.db.execute(
            """
            UPDATE tasks SET lease_expires_at=now()+interval '2 minutes'
            WHERE id=$1 AND owner_agent_id=$2
              AND status IN ('CLAIMED','IN_PROGRESS','UNDER_REVIEW')
            """,
            _uuid(task_id),
            agent_id,
        )
        return result.endswith(" 1")

    async def get(self, task_id: str) -> dict[str, Any] | None:
        row = await self.db.fetchrow("SELECT * FROM tasks WHERE id=$1", _uuid(task_id))
        return dict(row) if row else None

    async def update_task_status(self, task_id: str, status: str) -> None:
        completed = datetime.now(UTC) if status == "COMPLETED" else None
        await self.db.execute(
            "UPDATE tasks SET status=$2,completed_at=COALESCE($3,completed_at) WHERE id=$1",
            _uuid(task_id),
            status,
            completed,
        )

    update_status = update_task_status

    async def update_task_affected_contracts(self, task_id: str, contracts: list[str]) -> None:
        await self.db.execute(
            "UPDATE tasks SET affected_contracts=$2 WHERE id=$1", _uuid(task_id), contracts
        )

    async def find_similar_task(self, project_id: str, embedding: list[float]) -> None:
        del project_id, embedding
        return None

    async def artifact_titles(self, task_id: str) -> list[str]:
        rows = await self.db.fetch(
            "SELECT title FROM artifacts WHERE task_id=$1 ORDER BY created_at", _uuid(task_id)
        )
        return [str(row["title"]) for row in rows]


class ArtifactRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def create_artifact(
        self,
        project_id: str,
        artifact_type: str,
        title: str,
        *,
        object_uri: str,
        checksum_sha256: str,
        content_length: int,
        content_type: str = "application/octet-stream",
        object_version_id: str | None = None,
        task_id: str | None = None,
        summary: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        row = await self.db.fetchrow(
            """
            INSERT INTO artifacts(
                project_id,task_id,artifact_type,title,object_uri,object_version_id,
                checksum_sha256,content_length,content_type,summary,metadata
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) RETURNING id
            """,
            _uuid(project_id),
            _uuid(task_id) if task_id else None,
            artifact_type,
            title,
            object_uri,
            object_version_id,
            checksum_sha256,
            content_length,
            content_type,
            summary,
            json.dumps(metadata or {}),
        )
        assert row is not None
        return str(row["id"])


class CheckpointRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def save_checkpoint(
        self,
        project_id: str,
        agent_id: str,
        achievement: str,
        summary: str,
        task_id: str | None = None,
        agent_state_snapshot: dict[str, Any] | None = None,
        artifacts: list[str] | None = None,
    ) -> str:
        row = await self.db.fetchrow(
            """
            INSERT INTO checkpoints(project_id,agent_id,task_id,achievement,summary,state_pointer,artifacts)
            VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb) RETURNING id
            """,
            _uuid(project_id),
            agent_id,
            _uuid(task_id) if task_id else None,
            achievement,
            summary,
            json.dumps(agent_state_snapshot or {}),
            json.dumps(artifacts or []),
        )
        assert row is not None
        return str(row["id"])

    async def latest(self, project_id: str, agent_id: str) -> dict[str, Any] | None:
        row = await self.db.fetchrow(
            """
            SELECT * FROM checkpoints WHERE project_id=$1 AND agent_id=$2
            ORDER BY created_at DESC LIMIT 1
            """,
            _uuid(project_id),
            agent_id,
        )
        return dict(row) if row else None


class ProviderCallRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def log_intent(
        self,
        *,
        project_id: str,
        purpose: str,
        provider: str,
        model: str,
        prompt_hash: str,
        agent_id: str | None = None,
        redaction_status: str = "APPLIED",
    ) -> str:
        row = await self.db.fetchrow(
            """
            INSERT INTO provider_call_intents(
                project_id,agent_id,purpose,provider,model,prompt_hash,redaction_status
            ) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id
            """,
            _uuid(project_id),
            agent_id,
            purpose,
            provider,
            model,
            prompt_hash,
            redaction_status,
        )
        assert row is not None
        return str(row["id"])

    async def log_call(
        self,
        project_id: str,
        purpose: str,
        provider: str,
        model: str,
        cost_usd: float,
        prompt_hash: str,
        response_hash: str | None = None,
        *,
        agent_id: str | None = None,
        redaction_status: str = "APPLIED",
        token_usage: dict[str, Any] | None = None,
        latency_ms: int | None = None,
        status: str = "COMPLETED",
        error_code: str | None = None,
        intent_id: str | None = None,
    ) -> str:
        row = await self.db.fetchrow(
            """
            INSERT INTO provider_calls(
                intent_id,project_id,agent_id,purpose,provider,model,prompt_hash,response_hash,
                redaction_status,token_usage,cost_usd,latency_ms,status,error_code
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14) RETURNING id
            """,
            _uuid(intent_id) if intent_id else None,
            _uuid(project_id),
            agent_id,
            purpose,
            provider,
            model,
            prompt_hash,
            response_hash,
            redaction_status,
            json.dumps(token_usage or {}),
            cost_usd,
            latency_ms,
            status,
            error_code,
        )
        assert row is not None
        return str(row["id"])


class MemoryRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def save_memory_item(
        self,
        project_id: str | None,
        scope: str,
        owner_agent_id: str,
        memory_type: str,
        title: str,
        content: str,
        importance_score: float = 3,
        *,
        content_object_uri: str | None = None,
        content_object_version_id: str | None = None,
        content_hash: str | None = None,
        content_length: int | None = None,
        storage_status: str = "READY",
        source_event_id: str | None = None,
        source_artifact_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        encoded = content.encode("utf-8")
        digest = content_hash or hashlib.sha256(encoded).hexdigest()
        row = await self.db.fetchrow(
            """
            INSERT INTO memory_items(
                project_id,scope,owner_agent_id,memory_type,title,content,content_object_uri,
                content_object_version_id,content_hash,content_length,storage_status,
                source_event_id,source_artifact_id,importance,metadata
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
            RETURNING id
            """,
            _uuid(project_id) if project_id else None,
            scope,
            owner_agent_id,
            memory_type,
            title,
            content,
            content_object_uri,
            content_object_version_id,
            digest,
            len(encoded) if content_length is None else content_length,
            storage_status,
            _uuid(source_event_id) if source_event_id else None,
            _uuid(source_artifact_id) if source_artifact_id else None,
            max(1, min(5, round(importance_score))),
            json.dumps(metadata or {}),
        )
        assert row is not None
        return str(row["id"])

    async def attach_memory_object(
        self,
        memory_id: str,
        *,
        object_uri: str,
        object_version_id: str | None,
        content_hash: str,
        content_length: int,
        preview: str,
    ) -> None:
        await self.db.execute(
            """
            UPDATE memory_items
            SET content=$2,content_object_uri=$3,content_object_version_id=$4,
                content_hash=$5,content_length=$6,storage_status='READY'
            WHERE id=$1
            """,
            _uuid(memory_id),
            preview,
            object_uri,
            object_version_id,
            content_hash,
            content_length,
        )

    async def mark_memory_object_failed(self, memory_id: str) -> None:
        await self.db.execute(
            "UPDATE memory_items SET storage_status='OBJECT_FAILED' WHERE id=$1",
            _uuid(memory_id),
        )

    async def link_vector(self, memory_id: str, record_id: str, model: str) -> None:
        await self.db.execute(
            "UPDATE memory_items SET milvus_record_id=$2,embedding_model=$3 WHERE id=$1",
            _uuid(memory_id),
            record_id,
            model,
        )

    async def get_by_ids(
        self, memory_ids: Iterable[str], *, project_id: str, agent_id: str, scopes: list[str]
    ) -> list[dict[str, Any]]:
        ids = [_uuid(item) for item in memory_ids]
        if not ids:
            return []
        rows = await self.db.fetch(
            """
            SELECT * FROM memory_items WHERE id=ANY($1::uuid[])
              AND (project_id=$2 OR scope='global_patterns') AND scope=ANY($3::text[])
              AND (scope<>'private_agent_memory' OR owner_agent_id=$4)
            """,
            ids,
            _uuid(project_id),
            scopes,
            agent_id,
        )
        ordering = {item: index for index, item in enumerate(ids)}
        return sorted((dict(row) for row in rows), key=lambda row: ordering[row["id"]])

    async def lexical_search(
        self,
        project_id: str,
        agent_id: str,
        scopes: list[str],
        query_text: str,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            """
            SELECT *,ts_rank(to_tsvector('english',title||' '||content),
              websearch_to_tsquery('english',$4)) lexical_score
            FROM memory_items WHERE (project_id=$1 OR scope='global_patterns')
              AND scope=ANY($2::text[])
              AND (scope<>'private_agent_memory' OR owner_agent_id=$3)
              AND to_tsvector('english',title||' '||content) @@ websearch_to_tsquery('english',$4)
            ORDER BY lexical_score DESC,importance DESC,created_at DESC LIMIT $5
            """,
            _uuid(project_id),
            scopes,
            agent_id,
            query_text,
            max(1, min(limit, 100)),
        )
        return [dict(row) for row in rows]

    async def search_hybrid_memories(self, **kwargs: Any) -> list[dict[str, Any]]:
        return await self.lexical_search(
            kwargs["project_id"],
            kwargs["agent_id"],
            kwargs["allowed_scopes"],
            kwargs["query_text"],
            kwargs.get("limit", 5),
        )

    async def find_similar_failures(
        self, project_id: str, error_embedding: list[float]
    ) -> list[dict[str, Any]]:
        del error_embedding
        rows = await self.db.fetch(
            """
            SELECT * FROM memory_items WHERE project_id=$1 AND memory_type='execution_failure'
            ORDER BY created_at DESC LIMIT 2
            """,
            _uuid(project_id),
        )
        return [dict(row) for row in rows]

    async def find_affected_contracts(
        self, project_id: str, change_embedding: list[float]
    ) -> list[str]:
        del change_embedding
        rows = await self.db.fetch(
            "SELECT title FROM memory_items WHERE project_id=$1 AND scope='contract_memory' ORDER BY created_at DESC LIMIT 3",
            _uuid(project_id),
        )
        return [row["title"] for row in rows]

    async def save_global_lesson(
        self, owner_agent_id: str, title: str, lesson_content: str, lesson_embedding: list[float]
    ) -> str:
        del lesson_embedding
        return await self.save_memory_item(
            None, "global_patterns", owner_agent_id, "long_term_lesson", title, lesson_content
        )


class SummaryRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def save_summary(self, project_id: str, scope: str, owner_id: str, summary: str) -> str:
        row = await self.db.fetchrow(
            "INSERT INTO summaries(project_id,scope,owner_id,summary) VALUES($1,$2,$3,$4) RETURNING id",
            _uuid(project_id),
            scope,
            owner_id,
            summary,
        )
        assert row is not None
        return str(row["id"])


class AuditEventRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def log_audit_event(
        self,
        project_id: str,
        agent_id: str,
        action_type: str,
        policy_decision: str,
        integrity_hash: str,
        *,
        risk_level: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> str:
        async with self.db.transaction() as connection:
            previous = await connection.fetchval(
                "SELECT integrity_hash FROM audit_events WHERE project_id=$1 ORDER BY created_at DESC,id DESC LIMIT 1 FOR UPDATE",
                _uuid(project_id),
            )
            chain_hash = hashlib.sha256(
                f"{previous or ''}:{integrity_hash}:{policy_decision}".encode()
            ).hexdigest()
            row = await connection.fetchrow(
                """
                INSERT INTO audit_events(project_id,agent_id,event_type,risk_level,decision,integrity_hash,previous_hash,details)
                VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb) RETURNING id
                """,
                _uuid(project_id),
                agent_id,
                action_type,
                risk_level,
                policy_decision,
                chain_hash,
                previous,
                json.dumps({"request_hash": integrity_hash, **(details or {})}),
            )
            assert row is not None
            return str(row["id"])


class DoDRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def add_dod_check(self, project_id: str, criterion: DoDCriterion | str, **_: Any) -> str:
        item = (
            criterion
            if isinstance(criterion, DoDCriterion)
            else DoDCriterion.from_text(criterion, 1)
        )
        row = await self.db.fetchrow(
            """
            INSERT INTO dod_checks(
              project_id,criterion_id,description,verification_type,verification_command,
              required_artifacts,required_evidence_types
            ) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7)
            ON CONFLICT(project_id,criterion_id) DO UPDATE SET description=EXCLUDED.description
            RETURNING id
            """,
            _uuid(project_id),
            item.criterion_id,
            item.description,
            item.verification_type.value,
            json.dumps(item.verification_command),
            json.dumps(item.required_artifacts),
            item.required_evidence_types,
        )
        assert row is not None
        return str(row["id"])

    async def add_evidence(
        self,
        project_id: str,
        criterion_id: str,
        evidence_type: str,
        source_agent_id: str,
        summary: str,
        passed: bool,
        *,
        artifact_id: str | None = None,
        command: str | None = None,
        exit_code: int | None = None,
        checksum_sha256: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        row = await self.db.fetchrow(
            """
            INSERT INTO dod_evidence(
              project_id,criterion_id,evidence_type,source_agent_id,artifact_id,command,
              exit_code,checksum_sha256,summary,passed,metadata
            ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) RETURNING id
            """,
            _uuid(project_id),
            criterion_id,
            evidence_type,
            source_agent_id,
            _uuid(artifact_id) if artifact_id else None,
            command,
            exit_code,
            checksum_sha256,
            summary,
            passed,
            json.dumps(metadata or {}),
        )
        assert row is not None
        return str(row["id"])

    async def evaluate_and_persist(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            """
            WITH latest_evidence AS (
              SELECT DISTINCT ON (
                project_id, criterion_id, evidence_type,
                COALESCE(metadata->>'task_id', source_agent_id)
              ) *
              FROM dod_evidence
              WHERE project_id=$1
              ORDER BY project_id, criterion_id, evidence_type,
                COALESCE(metadata->>'task_id', source_agent_id), created_at DESC
            )
            SELECT c.*,
              COALESCE(array_agg(DISTINCT e.evidence_type) FILTER(WHERE e.passed),'{}'::text[]) passed_types,
              bool_or(e.passed=false) FILTER(WHERE e.id IS NOT NULL) has_failure,
              count(e.id) FILTER(WHERE e.passed) passed_count
            FROM dod_checks c LEFT JOIN latest_evidence e
              ON e.project_id=c.project_id AND e.criterion_id=c.criterion_id
            WHERE c.project_id=$1 GROUP BY c.id ORDER BY c.created_at
            """,
            _uuid(project_id),
        )
        evaluated: list[dict[str, Any]] = []
        async with self.db.transaction() as connection:
            for record in rows:
                item = dict(record)
                required = set(item["required_evidence_types"] or [])
                passed_types = set(item["passed_types"] or [])
                if item["has_failure"]:
                    status = "FAILED_VERIFICATION"
                elif required and required.issubset(passed_types):
                    status = "SATISFIED"
                elif item["passed_count"]:
                    status = "UNDER_REVIEW"
                else:
                    status = "NOT_STARTED"
                summary = f"passed evidence types: {', '.join(sorted(passed_types)) or 'none'}"
                await connection.execute(
                    "UPDATE dod_checks SET status=$3,evidence_summary=$4,verified_by_agent_id='dod_evaluator' WHERE project_id=$1 AND criterion_id=$2",
                    _uuid(project_id),
                    item["criterion_id"],
                    status,
                    summary,
                )
                item.update(status=status, evidence_summary=summary)
                evaluated.append(item)
        return evaluated

    async def update_criterion_status(
        self, project_id: str, criterion: str, status: str, agent_id: str, evidence: str = ""
    ) -> None:
        if status == "SATISFIED" and not evidence.strip():
            raise ValueError("SATISFIED requires evidence")
        await self.db.execute(
            "UPDATE dod_checks SET status=$3,verified_by_agent_id=$4,evidence_summary=$5 WHERE project_id=$1 AND (criterion_id=$2 OR description=$2)",
            _uuid(project_id),
            criterion,
            status,
            agent_id,
            evidence,
        )

    async def get_project_dod_status(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            "SELECT * FROM dod_checks WHERE project_id=$1 ORDER BY created_at", _uuid(project_id)
        )
        return [dict(row) for row in rows]

    async def get_checks(self, project_id: str, criterion_ids: list[str]) -> list[dict[str, Any]]:
        if not criterion_ids:
            return []
        rows = await self.db.fetch(
            """
            SELECT * FROM dod_checks
            WHERE project_id=$1 AND criterion_id=ANY($2::text[])
            ORDER BY created_at
            """,
            _uuid(project_id),
            criterion_ids,
        )
        return [dict(row) for row in rows]


class ResourcePlanRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def save(self, project_id: str, generated_by: str, plan: dict[str, Any]) -> str:
        serialized = json.dumps(plan, sort_keys=True, default=str)
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        async with self.db.transaction() as connection:
            await connection.execute(
                "UPDATE resource_plans SET active=false WHERE project_id=$1 AND active",
                _uuid(project_id),
            )
            row = await connection.fetchrow(
                """
                INSERT INTO resource_plans(project_id,generated_by_agent_id,host_snapshot,allocations,config_hash)
                VALUES($1,$2,$3::jsonb,$4::jsonb,$5) RETURNING id
                """,
                _uuid(project_id),
                generated_by,
                json.dumps(plan.get("envelope", {})),
                json.dumps(plan.get("allocations", [])),
                digest,
            )
            assert row is not None
            return str(row["id"])


class RuntimeConfigRepository:
    def __init__(self, db_manager: PostgresClient):
        self.db = db_manager

    async def save(self, public_config: dict[str, Any], project_id: str | None = None) -> str:
        serialized = json.dumps(public_config, sort_keys=True, default=str)
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        row = await self.db.fetchrow(
            "INSERT INTO runtime_config_snapshots(project_id,config_hash,public_config) VALUES($1,$2,$3::jsonb) RETURNING id",
            _uuid(project_id) if project_id else None,
            digest,
            serialized,
        )
        assert row is not None
        return str(row["id"])


class CodebaseMapRepository:
    """Compatibility facade; semantic code chunks now use MemoryService + Milvus."""

    def __init__(self, db_manager: PostgresClient):
        self.memory = MemoryRepository(db_manager)

    async def index_file_chunk(
        self,
        project_id: str,
        file_path: str,
        chunk_identifier: str,
        code_snippet: str,
        embedding: list[float],
    ) -> str:
        del embedding
        return await self.memory.save_memory_item(
            project_id,
            "project_memory",
            "code_indexer",
            "code_chunk",
            f"{file_path}:{chunk_identifier}",
            code_snippet,
        )

    async def clear_file_index(self, project_id: str, file_path: str) -> None:
        await self.memory.db.execute(
            "DELETE FROM memory_items WHERE project_id=$1 AND memory_type='code_chunk' AND title LIKE $2",
            _uuid(project_id),
            f"{file_path}:%",
        )

    async def search_codebase(
        self, project_id: str, query_embedding: list[float], limit: int = 3
    ) -> list[dict[str, Any]]:
        del query_embedding
        rows = await self.memory.db.fetch(
            "SELECT title,content FROM memory_items WHERE project_id=$1 AND memory_type='code_chunk' ORDER BY created_at DESC LIMIT $2",
            _uuid(project_id),
            limit,
        )
        return [
            {
                "file_path": row["title"].split(":", 1)[0],
                "chunk_identifier": row["title"],
                "code_snippet": row["content"],
            }
            for row in rows
        ]
