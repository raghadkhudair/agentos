from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from agentos.config.loader import runtime_tuning
from agentos.config.runtime import RuntimeConfig
from agentos.messaging.events import Event
from agentos.runtime.team_plan import (
    AgentRole,
    AgentSpec,
    EvidenceType,
    InitialTask,
    TeamPlan,
    _patterns_overlap,
)
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

    async def persist_plan_bundle(
        self,
        project_id: str,
        plan: TeamPlan,
        agents: list[AgentSpec],
        runtime_config: RuntimeConfig,
        settings_snapshot: dict[str, Any],
        planning_context: dict[str, Any],
        *,
        created_by: str = "pm_tech_lead-bootstrap",
    ) -> dict[str, Any]:
        """Persist the initial contract, work graph, team, and resources atomically."""

        project_uuid = _uuid(project_id)
        runtime_payload = runtime_config.model_dump(mode="json")
        resource_serialized = json.dumps(runtime_payload, sort_keys=True, default=str)
        resource_hash = hashlib.sha256(resource_serialized.encode("utf-8")).hexdigest()
        snapshot = {
            "settings": settings_snapshot,
            "generated_runtime": runtime_payload,
            "planning_context": planning_context,
        }
        snapshot_serialized = json.dumps(snapshot, sort_keys=True, default=str)
        snapshot_hash = hashlib.sha256(snapshot_serialized.encode("utf-8")).hexdigest()
        allocation_by_agent = {item.agent_id: item for item in runtime_config.allocations}
        task_ids: dict[str, UUID] = {}
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT status,dod_contract_version FROM projects WHERE id=$1 FOR UPDATE",
                project_uuid,
            )
            if project is None:
                raise LookupError(f"project not found: {project_id}")
            if project["status"] != ProjectState.PLANNING.value:
                raise ValueError("initial plan may only be persisted while the project is PLANNING")
            if int(project["dod_contract_version"] or 0) != 0:
                raise ValueError("project already has an authoritative DoD contract")
            await connection.execute(
                """
                INSERT INTO dod_contract_versions(
                  project_id,contract_version,contract_hash,source_revision,
                  planning_context_hash,prompt_version,contract,created_by
                ) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
                """,
                project_uuid,
                plan.contract_version,
                plan.contract_hash,
                plan.source_revision,
                plan.planning_context_hash,
                plan.prompt_version,
                json.dumps(plan.model_dump(mode="json")),
                created_by,
            )
            await connection.execute(
                """
                UPDATE projects SET name=$2,dod=$3::jsonb,architecture=$4,assumptions=$5::jsonb,
                  dod_contract_version=$6,dod_contract_hash=$7,planning_context_hash=$8,
                  planning_prompt_version=$9,source_revision=$10,evidence_generation=0,
                  evaluation_requested_generation=0
                WHERE id=$1
                """,
                project_uuid,
                plan.project_name,
                json.dumps([item.model_dump(mode="json") for item in plan.dod]),
                plan.high_level_architecture,
                json.dumps(plan.assumptions),
                plan.contract_version,
                plan.contract_hash,
                plan.planning_context_hash,
                plan.prompt_version,
                plan.source_revision,
            )
            for criterion in plan.dod:
                await connection.execute(
                    """
                    INSERT INTO dod_checks(
                      project_id,criterion_id,description,verification_type,
                      verification_command,required_artifacts,required_evidence_types,
                      evidence_scopes,contract_version,criterion_hash,source,locked,
                      mandatory,severity,affected_contracts,status
                    ) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,'MISSING')
                    """,
                    project_uuid,
                    criterion.criterion_id,
                    criterion.description,
                    criterion.verification_type.value,
                    json.dumps(criterion.verification_command),
                    json.dumps(criterion.required_artifacts),
                    [item.value for item in criterion.required_evidence_types],
                    json.dumps(
                        {key.value: value.value for key, value in criterion.evidence_scopes.items()}
                    ),
                    plan.contract_version,
                    criterion.contract_hash,
                    criterion.source.value,
                    criterion.locked,
                    criterion.mandatory,
                    criterion.severity.value,
                    criterion.affected_contracts,
                )
            for index, task in enumerate(plan.initial_backlog, start=1):
                task_id = uuid4()
                task_ids[task.title] = task_id
                await connection.execute(
                    """
                    INSERT INTO tasks(
                      id,project_id,external_key,title,description,owner_role,priority,complexity,
                      acceptance_criteria,allowed_paths,blocked_paths,expected_outputs,
                      required_reviewers,dod_criteria,affected_contracts,risk_level,dod_contract_version
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16,$17)
                    """,
                    task_id,
                    project_uuid,
                    f"contract-{plan.contract_version}-bootstrap-{index}",
                    task.title,
                    task.description,
                    task.owner_role.value,
                    task.priority,
                    task.complexity,
                    json.dumps(task.acceptance_criteria),
                    task.allowed_paths,
                    task.blocked_paths,
                    task.expected_outputs,
                    task.required_reviewers,
                    task.dod_criteria,
                    task.affected_contracts,
                    task.risk_level,
                    plan.contract_version,
                )
            for task in plan.initial_backlog:
                for dependency_title in task.depends_on:
                    await connection.execute(
                        "INSERT INTO task_dependencies(task_id,depends_on_task_id) VALUES($1,$2)",
                        task_ids[task.title],
                        task_ids[dependency_title],
                    )
            resource_id = await connection.fetchval(
                """
                INSERT INTO resource_plans(
                  project_id,generated_by_agent_id,host_snapshot,allocations,config_hash
                ) VALUES($1,'infrastructure_agent-1',$2::jsonb,$3::jsonb,$4) RETURNING id
                """,
                project_uuid,
                json.dumps(runtime_payload.get("envelope", {})),
                json.dumps(runtime_payload.get("allocations", [])),
                resource_hash,
            )
            snapshot_id = await connection.fetchval(
                """
                INSERT INTO runtime_config_snapshots(project_id,config_hash,public_config)
                VALUES($1,$2,$3::jsonb) RETURNING id
                """,
                project_uuid,
                snapshot_hash,
                snapshot_serialized,
            )
            for spec in agents:
                for index in range(1, spec.count + 1):
                    agent_id = f"{spec.role.value}-{index}"
                    allocation = allocation_by_agent[agent_id]
                    permissions = {
                        "allowed_actions": spec.allowed_action_categories,
                        "ownership_domains": spec.ownership_domains,
                        "event_subscriptions": spec.event_subscriptions,
                    }
                    await connection.execute(
                        """
                        INSERT INTO agents(
                          project_id,id,role,squad,status,permissions,memory_scopes,
                          provider_assignment,resource_allocation,last_heartbeat_at
                        ) VALUES($1,$2,$3,$4,'PLANNED',$5::jsonb,$6,$7::jsonb,$8::jsonb,now())
                        """,
                        project_uuid,
                        agent_id,
                        spec.role.value,
                        (
                            "platform"
                            if spec.role == AgentRole.INFRASTRUCTURE_AGENT
                            else spec.role.value.split("_", 1)[0]
                        ),
                        json.dumps(permissions),
                        spec.memory_scopes,
                        json.dumps(
                            {
                                "provider": allocation.provider,
                                "model": allocation.model,
                                "model_routes": {
                                    key.value: value
                                    for key, value in allocation.model_routes.items()
                                },
                            }
                        ),
                        json.dumps(allocation.model_dump(mode="json")),
                    )
        return {
            "resource_plan_id": str(resource_id),
            "runtime_snapshot_id": str(snapshot_id),
            "task_ids": {key: str(value) for key, value in task_ids.items()},
            "contract_version": plan.contract_version,
            "contract_hash": plan.contract_hash,
        }

    async def request_dod_approval(
        self,
        project_id: str,
        gate: str,
        payload: dict[str, Any],
        requested_by: str,
    ) -> str:
        if gate not in {"DOD_AMENDMENT", "DOD_WAIVER"}:
            raise ValueError("unsupported DoD approval gate")
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        integrity = hashlib.sha256(f"{project_id}:{gate}:{serialized}".encode()).hexdigest()
        row = await self.db.fetchrow(
            """
            INSERT INTO approval_requests(
              project_id,action_integrity_hash,requested_by_agent_id,required_gate,
              request_payload,expires_at
            ) VALUES($1,$2,$3,$4,$5::jsonb,$6)
            ON CONFLICT(action_integrity_hash) DO UPDATE SET
              expires_at=CASE WHEN approval_requests.status='PENDING'
                THEN EXCLUDED.expires_at ELSE approval_requests.expires_at END
            RETURNING id
            """,
            _uuid(project_id),
            integrity,
            requested_by,
            gate,
            serialized,
            datetime.now(UTC) + timedelta(hours=24),
        )
        assert row is not None
        return str(row["id"])

    async def amend_dod_contract(
        self,
        project_id: str,
        plan: TeamPlan,
        approval_id: str,
        reason: str,
        amended_by: str,
    ) -> dict[str, Any]:
        if not reason.strip():
            raise ValueError("DoD amendment requires an auditable reason")
        project_uuid = _uuid(project_id)
        task_ids: dict[str, UUID] = {}
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT * FROM projects WHERE id=$1 FOR UPDATE", project_uuid
            )
            approval = await connection.fetchrow(
                """
                SELECT * FROM approval_requests WHERE id=$1 AND project_id=$2 FOR UPDATE
                """,
                _uuid(approval_id),
                project_uuid,
            )
            if project is None or approval is None:
                raise ValueError("project or approval was not found")
            if project["status"] in {item.value for item in self.TERMINAL_STATES}:
                raise ValueError("a terminal project contract cannot be amended")
            if approval["status"] != "APPROVED" or approval["required_gate"] != "DOD_AMENDMENT":
                raise ValueError("DoD amendment requires an approved DOD_AMENDMENT decision")
            approval_payload = dict(approval["request_payload"] or {})
            if (
                approval_payload.get("contract_hash") != plan.contract_hash
                or approval_payload.get("reason") != reason
            ):
                raise ValueError("approval is not bound to this exact amended contract")
            next_version = int(project["dod_contract_version"]) + 1
            if plan.contract_version != next_version:
                raise ValueError(f"amended contract version must be {next_version}")
            expected_revision = project["integration_head"] or project["source_revision"]
            if plan.source_revision != expected_revision:
                raise ValueError("amendment must be grounded in the current integrated revision")
            roles = {
                row["role"]
                for row in await connection.fetch(
                    "SELECT DISTINCT role FROM agents WHERE project_id=$1", project_uuid
                )
            }
            missing_roles = {task.owner_role.value for task in plan.initial_backlog} - roles
            if missing_roles:
                raise ValueError(f"amendment uses unavailable owner roles: {sorted(missing_roles)}")
            await connection.execute(
                """
                INSERT INTO dod_contract_versions(
                  project_id,contract_version,contract_hash,source_revision,
                  planning_context_hash,prompt_version,contract,created_by,
                  amendment_reason,approval_id
                ) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10)
                """,
                project_uuid,
                plan.contract_version,
                plan.contract_hash,
                plan.source_revision,
                plan.planning_context_hash,
                plan.prompt_version,
                json.dumps(plan.model_dump(mode="json")),
                amended_by,
                reason,
                _uuid(approval_id),
            )
            await connection.execute(
                "UPDATE dod_checks SET active=false WHERE project_id=$1", project_uuid
            )
            for criterion in plan.dod:
                await connection.execute(
                    """
                    INSERT INTO dod_checks(
                      project_id,criterion_id,description,verification_type,verification_command,
                      required_artifacts,required_evidence_types,evidence_scopes,contract_version,
                      criterion_hash,source,locked,mandatory,severity,affected_contracts,status,active
                    ) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,'MISSING',true)
                    ON CONFLICT(project_id,criterion_id) DO UPDATE SET
                      description=EXCLUDED.description,verification_type=EXCLUDED.verification_type,
                      verification_command=EXCLUDED.verification_command,
                      required_artifacts=EXCLUDED.required_artifacts,
                      required_evidence_types=EXCLUDED.required_evidence_types,
                      evidence_scopes=EXCLUDED.evidence_scopes,
                      contract_version=EXCLUDED.contract_version,criterion_hash=EXCLUDED.criterion_hash,
                      source=EXCLUDED.source,locked=EXCLUDED.locked,mandatory=EXCLUDED.mandatory,
                      severity=EXCLUDED.severity,affected_contracts=EXCLUDED.affected_contracts,
                      status='MISSING',waiver_approval_id=NULL,verified_by_agent_id=NULL,
                      evidence_summary=NULL,active=true
                    """,
                    project_uuid,
                    criterion.criterion_id,
                    criterion.description,
                    criterion.verification_type.value,
                    json.dumps(criterion.verification_command),
                    json.dumps(criterion.required_artifacts),
                    [item.value for item in criterion.required_evidence_types],
                    json.dumps(
                        {key.value: value.value for key, value in criterion.evidence_scopes.items()}
                    ),
                    plan.contract_version,
                    criterion.contract_hash,
                    criterion.source.value,
                    criterion.locked,
                    criterion.mandatory,
                    criterion.severity.value,
                    criterion.affected_contracts,
                )
            await connection.execute(
                """
                UPDATE tasks SET status='CANCELLED',lease_expires_at=NULL
                WHERE project_id=$1 AND status NOT IN ('COMPLETED','CANCELLED')
                """,
                project_uuid,
            )
            for index, task in enumerate(plan.initial_backlog, start=1):
                task_id = uuid4()
                task_ids[task.title] = task_id
                await connection.execute(
                    """
                    INSERT INTO tasks(
                      id,project_id,external_key,title,description,owner_role,priority,complexity,
                      acceptance_criteria,allowed_paths,blocked_paths,expected_outputs,
                      required_reviewers,dod_criteria,affected_contracts,risk_level,dod_contract_version
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16,$17)
                    """,
                    task_id,
                    project_uuid,
                    f"contract-{plan.contract_version}-amendment-{index}",
                    task.title,
                    task.description,
                    task.owner_role.value,
                    task.priority,
                    task.complexity,
                    json.dumps(task.acceptance_criteria),
                    task.allowed_paths,
                    task.blocked_paths,
                    task.expected_outputs,
                    task.required_reviewers,
                    task.dod_criteria,
                    task.affected_contracts,
                    task.risk_level,
                    plan.contract_version,
                )
            for task in plan.initial_backlog:
                for dependency in task.depends_on:
                    await connection.execute(
                        "INSERT INTO task_dependencies(task_id,depends_on_task_id) VALUES($1,$2)",
                        task_ids[task.title],
                        task_ids[dependency],
                    )
            await connection.execute(
                """
                UPDATE projects SET name=$2,dod=$3::jsonb,architecture=$4,assumptions=$5::jsonb,
                  dod_contract_version=$6,dod_contract_hash=$7,planning_context_hash=$8,
                  planning_prompt_version=$9,status='REPLANNING',evidence_generation=evidence_generation+1,
                  evaluation_requested_generation=evidence_generation+1,replan_attempts=0,next_replan_at=NULL
                WHERE id=$1
                """,
                project_uuid,
                plan.project_name,
                json.dumps([item.model_dump(mode="json") for item in plan.dod]),
                plan.high_level_architecture,
                json.dumps(plan.assumptions),
                plan.contract_version,
                plan.contract_hash,
                plan.planning_context_hash,
                plan.prompt_version,
            )
        return {
            "project_id": project_id,
            "contract_version": plan.contract_version,
            "contract_hash": plan.contract_hash,
            "task_ids": {title: str(task_id) for title, task_id in task_ids.items()},
        }

    async def waive_dod_criterion(
        self,
        project_id: str,
        criterion_id: str,
        approval_id: str,
        reason: str,
    ) -> None:
        if not reason.strip():
            raise ValueError("waiver requires an auditable reason")
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT status FROM projects WHERE id=$1 FOR UPDATE", _uuid(project_id)
            )
            criterion = await connection.fetchrow(
                """
                SELECT * FROM dod_checks WHERE project_id=$1 AND criterion_id=$2 AND active FOR UPDATE
                """,
                _uuid(project_id),
                criterion_id,
            )
            approval = await connection.fetchrow(
                "SELECT * FROM approval_requests WHERE id=$1 AND project_id=$2 FOR UPDATE",
                _uuid(approval_id),
                _uuid(project_id),
            )
            if project is None or criterion is None or approval is None:
                raise ValueError("project, criterion, or approval was not found")
            if project["status"] in {item.value for item in self.TERMINAL_STATES}:
                raise ValueError("terminal project criteria cannot be waived")
            payload = dict(approval["request_payload"] or {})
            if (
                approval["status"] != "APPROVED"
                or approval["required_gate"] != "DOD_WAIVER"
                or payload.get("criterion_id") != criterion_id
                or payload.get("criterion_hash") != criterion["criterion_hash"]
                or payload.get("reason") != reason
            ):
                raise ValueError("waiver approval is missing, stale, or bound to another criterion")
            await connection.execute(
                """
                UPDATE dod_checks SET status='WAIVED_BY_HUMAN',waiver_approval_id=$3,
                  verified_by_agent_id=$4,evidence_summary=$5
                WHERE project_id=$1 AND criterion_id=$2 AND active
                """,
                _uuid(project_id),
                criterion_id,
                _uuid(approval_id),
                approval["decided_by"],
                reason,
            )
            await connection.execute(
                """
                UPDATE projects SET evidence_generation=evidence_generation+1,
                  evaluation_requested_generation=evidence_generation+1 WHERE id=$1
                """,
                _uuid(project_id),
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
    STATUS_TRANSITIONS: dict[str, set[str]] = {
        "PENDING": {"CLAIMED", "CANCELLED"},
        "CLAIMED": {"IN_PROGRESS", "PENDING", "CANCELLED"},
        "IN_PROGRESS": {"UNDER_REVIEW", "PENDING", "BLOCKED", "FAILED_VERIFICATION", "CANCELLED"},
        "UNDER_REVIEW": {"COMPLETED", "PENDING", "BLOCKED", "FAILED_VERIFICATION", "CANCELLED"},
        "BLOCKED": {"PENDING", "CANCELLED"},
        "FAILED_VERIFICATION": {"PENDING", "CANCELLED"},
        "COMPLETED": set(),
        "CANCELLED": set(),
    }

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
        dod_contract_version: int | None = None,
        dependency_titles: list[str] | None = None,
        embedding: list[float] | None = None,
    ) -> str:
        del embedding  # semantic task records are indexed by MemoryService in Milvus
        criteria = list(dict.fromkeys(dod_criteria or []))
        reviewers = list(dict.fromkeys(required_reviewers or []))
        if not owner_role or not acceptance_criteria or not allowed_paths or not expected_outputs:
            raise ValueError(
                "task contract requires owner, acceptance criteria, paths, and outputs"
            )
        if not criteria:
            raise ValueError("task must map to at least one current DoD criterion")
        if AgentRole.CODE_REVIEWER.value not in reviewers:
            raise ValueError("task requires an independent code reviewer")
        project_uuid = _uuid(project_id)
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                """
                SELECT status,dod_contract_version FROM projects WHERE id=$1 FOR UPDATE
                """,
                project_uuid,
            )
            if project is None:
                raise LookupError(f"project not found: {project_id}")
            if project["status"] in {item.value for item in ProjectRepository.TERMINAL_STATES}:
                raise ValueError("tasks cannot be added to a terminal project")
            version = dod_contract_version or int(project["dod_contract_version"])
            if version != int(project["dod_contract_version"]):
                raise ValueError("task targets a stale DoD contract version")
            check_rows = await connection.fetch(
                """
                SELECT criterion_id,required_evidence_types FROM dod_checks
                WHERE project_id=$1 AND active AND contract_version=$2 AND criterion_id=ANY($3::text[])
                """,
                project_uuid,
                version,
                criteria,
            )
            if {row["criterion_id"] for row in check_rows} != set(criteria):
                raise ValueError("task references a missing or stale DoD criterion")
            requires_security = risk_level in {"HIGH", "CRITICAL"} or any(
                EvidenceType.SECURITY_REVIEW.value in (row["required_evidence_types"] or [])
                for row in check_rows
            )
            if requires_security and AgentRole.SECURITY_REVIEWER.value not in reviewers:
                raise ValueError("task risk or criterion requires an independent security reviewer")
            dependency_rows = await connection.fetch(
                "SELECT id,title FROM tasks WHERE project_id=$1 AND title=ANY($2::text[])",
                project_uuid,
                dependency_titles or [],
            )
            dependency_ids = {row["title"]: row["id"] for row in dependency_rows}
            if set(dependency_titles or []) != set(dependency_ids):
                raise ValueError("task references an unknown dependency title")
            if external_key:
                existing = await connection.fetchrow(
                    "SELECT * FROM tasks WHERE project_id=$1 AND external_key=$2",
                    project_uuid,
                    external_key,
                )
                if existing is not None:
                    expected = {
                        "title": title,
                        "description": description,
                        "owner_role": owner_role,
                        "dod_contract_version": version,
                        "dod_criteria": criteria,
                    }
                    if any(existing[key] != value for key, value in expected.items()):
                        raise ValueError(
                            "idempotency key already exists with a different task contract"
                        )
                    return str(existing["id"])
            row = await connection.fetchrow(
                """
                INSERT INTO tasks(
                    project_id,parent_task_id,external_key,title,description,owner_agent_id,owner_role,
                    priority,complexity,acceptance_criteria,allowed_paths,blocked_paths,
                    expected_outputs,required_reviewers,dod_criteria,affected_contracts,risk_level,
                    dod_contract_version
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,$18)
                RETURNING id
                """,
                project_uuid,
                _uuid(parent_task_id) if parent_task_id else None,
                external_key,
                title,
                description,
                owner_agent_id,
                owner_role,
                priority,
                complexity,
                json.dumps(acceptance_criteria),
                allowed_paths,
                blocked_paths or [],
                expected_outputs,
                reviewers,
                criteria,
                affected_contracts or [],
                risk_level,
                version,
            )
            assert row is not None
            for dependency in dependency_titles or []:
                await connection.execute(
                    """
                    INSERT INTO task_dependencies(task_id,depends_on_task_id)
                    VALUES($1,$2) ON CONFLICT DO NOTHING
                    """,
                    row["id"],
                    dependency_ids[dependency],
                )
            await connection.execute(
                """
                UPDATE projects SET evidence_generation=evidence_generation+1,
                  evaluation_requested_generation=evidence_generation+1 WHERE id=$1
                """,
                project_uuid,
            )
            return str(row["id"])

    async def create_replan_batch(
        self,
        project_id: str,
        evaluation_run_id: str,
        gap_criteria: list[str],
        proposals: list[InitialTask],
    ) -> list[str]:
        """Validate and persist a complete gap-replanning graph in one transaction."""

        if not proposals:
            raise ValueError("replanning must produce at least one validated task")
        gap_set = set(gap_criteria)
        mapped = {criterion for task in proposals for criterion in task.dod_criteria}
        if not gap_set or mapped != gap_set:
            raise ValueError("replanned task graph must cover exactly the current gap criteria")
        titles = [task.title for task in proposals]
        if len(titles) != len(set(titles)):
            raise ValueError("replanned task titles must be unique")
        proposal_titles = set(titles)
        dependency_graph = {
            task.title: set(task.depends_on).intersection(proposal_titles) for task in proposals
        }
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(title: str) -> None:
            if title in visited:
                return
            if title in visiting:
                raise ValueError("replanned task graph contains a dependency cycle")
            visiting.add(title)
            for dependency in dependency_graph[title]:
                visit(dependency)
            visiting.remove(title)
            visited.add(title)

        for title in titles:
            visit(title)
        project_uuid = _uuid(project_id)
        task_ids: dict[str, UUID] = {}
        created_any = False
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT * FROM projects WHERE id=$1 FOR UPDATE", project_uuid
            )
            run = await connection.fetchrow(
                """
                SELECT * FROM dod_evaluation_runs WHERE id=$1 AND project_id=$2
                """,
                _uuid(evaluation_run_id),
                project_uuid,
            )
            if (
                project is None
                or run is None
                or run["status"]
                not in {
                    "UNSATISFIED",
                    "INCONCLUSIVE",
                }
            ):
                raise ValueError("replanning requires a current unsatisfied evaluation run")
            if (
                int(project["dod_contract_version"]) != int(run["contract_version"])
                or project["dod_contract_hash"] != run["contract_hash"]
            ):
                raise ValueError("replanning evaluation targets a stale DoD contract")
            evaluated_items = await connection.fetch(
                """
                SELECT criterion_id,status FROM dod_evaluation_items
                WHERE evaluation_run_id=$1
                """,
                _uuid(evaluation_run_id),
            )
            evaluated_gaps = {
                str(row["criterion_id"])
                for row in evaluated_items
                if row["status"] not in {"SATISFIED", "WAIVED_BY_HUMAN"}
            }
            if gap_set != evaluated_gaps:
                raise ValueError(
                    "replanning criteria must exactly match the durable evaluation gaps"
                )
            checks = await connection.fetch(
                """
                SELECT criterion_id,required_evidence_types,required_artifacts,affected_contracts
                FROM dod_checks
                WHERE project_id=$1 AND active AND contract_version=$2 AND criterion_id=ANY($3::text[])
                """,
                project_uuid,
                run["contract_version"],
                list(gap_set),
            )
            if {row["criterion_id"] for row in checks} != gap_set:
                raise ValueError("replanning references an unknown criterion")
            roles = {
                row["role"]
                for row in await connection.fetch(
                    "SELECT DISTINCT role FROM agents WHERE project_id=$1", project_uuid
                )
            }
            existing_titles = {
                row["title"]: row["id"]
                for row in await connection.fetch(
                    "SELECT id,title FROM tasks WHERE project_id=$1", project_uuid
                )
            }
            batch_prefix = f"replan-{evaluation_run_id}-"
            existing_batch = await connection.fetch(
                """
                SELECT * FROM tasks WHERE project_id=$1 AND external_key LIKE $2
                ORDER BY external_key
                """,
                project_uuid,
                f"{batch_prefix}%",
            )
            canonical_proposals = sorted(proposals, key=lambda item: item.title)
            if existing_batch and len(existing_batch) != len(canonical_proposals):
                raise ValueError("replanning generation already has a different task batch")
            for task in proposals:
                if task.owner_role.value not in roles:
                    raise ValueError(f"replanned task owner role is not present: {task.owner_role}")
                unknown_dependencies = set(task.depends_on) - set(titles) - set(existing_titles)
                if unknown_dependencies:
                    raise ValueError(
                        f"replanned task has unknown dependencies: {sorted(unknown_dependencies)}"
                    )
                relevant = [row for row in checks if row["criterion_id"] in task.dod_criteria]
                security_required = task.risk_level in {"HIGH", "CRITICAL"} or any(
                    EvidenceType.SECURITY_REVIEW.value in (row["required_evidence_types"] or [])
                    for row in relevant
                )
                if (
                    security_required
                    and AgentRole.SECURITY_REVIEWER.value not in task.required_reviewers
                ):
                    raise ValueError("replanned task omits its required security reviewer")
            for check in checks:
                criterion_id = str(check["criterion_id"])
                mapped_proposals = [task for task in proposals if criterion_id in task.dod_criteria]
                mapped_outputs = [
                    output for task in mapped_proposals for output in task.expected_outputs
                ]
                for required_artifact in check["required_artifacts"] or []:
                    if not any(
                        _patterns_overlap(str(required_artifact), output)
                        for output in mapped_outputs
                    ):
                        raise ValueError(
                            f"replanned criterion {criterion_id!r} does not cover required "
                            f"artifact {required_artifact!r}"
                        )
                mapped_contracts = {
                    contract for task in mapped_proposals for contract in task.affected_contracts
                }
                missing_contracts = set(check["affected_contracts"] or []) - mapped_contracts
                if missing_contracts:
                    raise ValueError(
                        f"replanned criterion {criterion_id!r} omits affected contracts: "
                        f"{sorted(missing_contracts)}"
                    )
            for index, task in enumerate(canonical_proposals, start=1):
                external_key = f"{batch_prefix}{index:03d}"
                existing = await connection.fetchrow(
                    "SELECT * FROM tasks WHERE project_id=$1 AND external_key=$2",
                    project_uuid,
                    external_key,
                )
                if existing:
                    expected_contract = {
                        "title": task.title,
                        "description": task.description,
                        "owner_role": task.owner_role.value,
                        "priority": task.priority,
                        "complexity": task.complexity,
                        "acceptance_criteria": task.acceptance_criteria,
                        "allowed_paths": task.allowed_paths,
                        "blocked_paths": task.blocked_paths,
                        "expected_outputs": task.expected_outputs,
                        "required_reviewers": task.required_reviewers,
                        "dod_criteria": task.dod_criteria,
                        "affected_contracts": task.affected_contracts,
                        "risk_level": task.risk_level,
                        "dod_contract_version": int(run["contract_version"]),
                    }
                    if any(existing[key] != value for key, value in expected_contract.items()):
                        raise ValueError(
                            "replanning idempotency key conflicts with another contract"
                        )
                    task_ids[task.title] = existing["id"]
                    continue
                if task.title in existing_titles:
                    raise ValueError(f"replanned task title already exists: {task.title!r}")
                task_id = uuid4()
                created_any = True
                task_ids[task.title] = task_id
                await connection.execute(
                    """
                    INSERT INTO tasks(
                      id,project_id,external_key,title,description,owner_role,priority,complexity,
                      acceptance_criteria,allowed_paths,blocked_paths,expected_outputs,
                      required_reviewers,dod_criteria,affected_contracts,risk_level,dod_contract_version
                    ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,$15,$16,$17)
                    """,
                    task_id,
                    project_uuid,
                    external_key,
                    task.title,
                    task.description,
                    task.owner_role.value,
                    task.priority,
                    task.complexity,
                    json.dumps(task.acceptance_criteria),
                    task.allowed_paths,
                    task.blocked_paths,
                    task.expected_outputs,
                    task.required_reviewers,
                    task.dod_criteria,
                    task.affected_contracts,
                    task.risk_level,
                    run["contract_version"],
                )
            for task in proposals:
                for dependency in task.depends_on:
                    dependency_id = task_ids.get(dependency) or existing_titles[dependency]
                    await connection.execute(
                        """
                        INSERT INTO task_dependencies(task_id,depends_on_task_id)
                        VALUES($1,$2) ON CONFLICT DO NOTHING
                        """,
                        task_ids[task.title],
                        dependency_id,
                    )
            if created_any:
                await connection.execute(
                    """
                    UPDATE projects SET status='RUNNING',next_replan_at=NULL,
                      evidence_generation=evidence_generation+1,
                      evaluation_requested_generation=evidence_generation+1 WHERE id=$1
                    """,
                    project_uuid,
                )
        return [str(task_ids[title]) for title in titles]

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

    async def get_runnable_tasks(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            """
            SELECT t.* FROM tasks t
            WHERE t.project_id=$1 AND t.status='PENDING'
              AND NOT EXISTS(
                SELECT 1 FROM task_dependencies td
                JOIN tasks dependency ON dependency.id=td.depends_on_task_id
                WHERE td.task_id=t.id AND dependency.status<>'COMPLETED'
              )
            ORDER BY t.priority DESC,t.created_at
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
                SELECT t.* FROM tasks t JOIN projects p ON p.id=t.project_id
                WHERE t.project_id=$1 AND t.status='PENDING'
                  AND p.status IN ('RUNNING','REPLANNING')
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
            await connection.execute(
                """
                UPDATE projects SET evidence_generation=evidence_generation+1,
                  evaluation_requested_generation=evidence_generation+1 WHERE id=$1
                """,
                _uuid(project_id),
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
        async with self.db.transaction() as connection:
            task = await connection.fetchrow(
                """
                SELECT t.project_id,t.status,t.dod_contract_version,
                  p.status project_status,p.dod_contract_version project_contract_version
                FROM tasks t
                JOIN projects p ON p.id=t.project_id WHERE t.id=$1 FOR UPDATE OF t,p
                """,
                _uuid(task_id),
            )
            if task is None:
                raise LookupError(f"task not found: {task_id}")
            if task["project_status"] in {item.value for item in ProjectRepository.TERMINAL_STATES}:
                raise ValueError("task state is immutable after project finalization")
            if int(task["dod_contract_version"]) != int(task["project_contract_version"]):
                raise ValueError("task belongs to a stale DoD contract version")
            current_status = str(task["status"])
            if status != current_status and status not in self.STATUS_TRANSITIONS[current_status]:
                raise ValueError(f"invalid task status transition: {current_status} -> {status}")
            await connection.execute(
                "UPDATE tasks SET status=$2,completed_at=COALESCE($3,completed_at) WHERE id=$1",
                _uuid(task_id),
                status,
                completed,
            )
            await connection.execute(
                """
                UPDATE projects SET evidence_generation=evidence_generation+1,
                  evaluation_requested_generation=evidence_generation+1 WHERE id=$1
                """,
                task["project_id"],
            )

    update_status = update_task_status

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
        project_uuid = _uuid(project_id)
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT status,evidence_generation FROM projects WHERE id=$1 FOR UPDATE",
                project_uuid,
            )
            if project is None:
                raise LookupError(f"project not found: {project_id}")
            if project["status"] in {item.value for item in ProjectRepository.TERMINAL_STATES}:
                raise ValueError("artifacts are immutable after project finalization")
            row = await connection.fetchrow(
                """
                INSERT INTO artifacts(
                    project_id,task_id,artifact_type,title,object_uri,object_version_id,
                    checksum_sha256,content_length,content_type,summary,metadata
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) RETURNING id
                """,
                project_uuid,
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
            generation = int(project["evidence_generation"]) + 1
            await connection.execute(
                """
                UPDATE projects SET evidence_generation=$2,evaluation_requested_generation=$2
                WHERE id=$1
                """,
                project_uuid,
                generation,
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
        task_id: str | None = None,
        source_role: str | None = None,
        contract_version: int | None = None,
        criterion_hash: str | None = None,
        subject_commit: str | None = None,
        integration_commit: str | None = None,
        command_digest: str | None = None,
        sandbox_digest: str | None = None,
        watched_paths: list[str] | None = None,
        affected_contracts: list[str] | None = None,
        run_status: str = "OK",
    ) -> str:
        try:
            evidence = EvidenceType(evidence_type)
        except ValueError as error:
            raise ValueError(f"unknown evidence type: {evidence_type}") from error
        if not summary.strip():
            raise ValueError("evidence requires a concrete nonempty summary")
        if run_status not in {"OK", "INCONCLUSIVE"}:
            raise ValueError("evidence run_status must be OK or INCONCLUSIVE")
        if run_status == "INCONCLUSIVE" and passed:
            raise ValueError("inconclusive evidence cannot pass")
        project_uuid = _uuid(project_id)
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                """
                SELECT status,dod_contract_version,dod_contract_hash,evidence_generation
                FROM projects WHERE id=$1 FOR UPDATE
                """,
                project_uuid,
            )
            if project is None:
                raise LookupError(f"project not found: {project_id}")
            if project["status"] in {item.value for item in ProjectRepository.TERMINAL_STATES}:
                raise ValueError("evidence is immutable after project finalization")
            criterion = await connection.fetchrow(
                """
                SELECT * FROM dod_checks WHERE project_id=$1 AND active AND criterion_id=$2
                """,
                project_uuid,
                criterion_id,
            )
            if criterion is None:
                raise ValueError("evidence references an unknown criterion")
            current_version = int(project["dod_contract_version"])
            expected_version = contract_version or current_version
            expected_hash = criterion_hash or str(criterion["criterion_hash"])
            if expected_version != current_version or expected_version != int(
                criterion["contract_version"]
            ):
                raise ValueError("evidence targets a stale DoD contract version")
            if expected_hash != criterion["criterion_hash"]:
                raise ValueError("evidence targets a stale criterion hash")
            if evidence.value not in (criterion["required_evidence_types"] or []):
                raise ValueError("evidence type is not authorized by the criterion contract")
            task = None
            if task_id:
                task = await connection.fetchrow(
                    "SELECT * FROM tasks WHERE id=$1 AND project_id=$2",
                    _uuid(task_id),
                    project_uuid,
                )
                if task is None or criterion_id not in (task["dod_criteria"] or []):
                    raise ValueError("evidence task is missing or not mapped to the criterion")
                if int(task["dod_contract_version"]) != current_version:
                    raise ValueError("evidence task uses a stale DoD contract")
            if (
                evidence
                in {
                    EvidenceType.ARTIFACT,
                    EvidenceType.REVIEW,
                    EvidenceType.SECURITY_REVIEW,
                    EvidenceType.INTEGRATION,
                }
                and task is None
            ):
                raise ValueError(f"{evidence.value} evidence requires a first-class task reference")
            artifact = None
            if artifact_id:
                artifact = await connection.fetchrow(
                    "SELECT * FROM artifacts WHERE id=$1 AND project_id=$2",
                    _uuid(artifact_id),
                    project_uuid,
                )
                if artifact is None or task is None or artifact["task_id"] != task["id"]:
                    raise ValueError("evidence artifact must belong to the referenced task")
            if (
                evidence
                in {EvidenceType.ARTIFACT, EvidenceType.REVIEW, EvidenceType.SECURITY_REVIEW}
                and artifact is None
            ):
                raise ValueError(f"{evidence.value} evidence requires a task-bound artifact")
            if (
                evidence
                in {
                    EvidenceType.ARTIFACT,
                    EvidenceType.REVIEW,
                    EvidenceType.SECURITY_REVIEW,
                }
                and not subject_commit
            ):
                raise ValueError(f"{evidence.value} evidence requires a subject commit")
            internal_roles = {"integration_supervisor": "integration_supervisor"}
            registered_role = await connection.fetchval(
                "SELECT role FROM agents WHERE project_id=$1 AND id=$2",
                project_uuid,
                source_agent_id,
            )
            authenticated_role = internal_roles.get(source_agent_id) or registered_role
            if not authenticated_role:
                raise ValueError("evidence producer is not an authenticated project identity")
            if source_role and source_role != authenticated_role:
                raise ValueError("evidence source role does not match the authenticated identity")
            resolved_role = str(authenticated_role)
            if (
                evidence
                in {
                    EvidenceType.ARTIFACT,
                    EvidenceType.TEST,
                    EvidenceType.COMMAND,
                }
                and task is not None
                and source_agent_id != task["owner_agent_id"]
            ):
                raise ValueError("task evidence must be produced by the assigned task owner")
            if evidence in {EvidenceType.REVIEW, EvidenceType.SECURITY_REVIEW}:
                if task is not None and source_agent_id == task["owner_agent_id"]:
                    raise ValueError("an artifact author cannot approve their own work")
                expected_role = (
                    AgentRole.CODE_REVIEWER.value
                    if evidence == EvidenceType.REVIEW
                    else AgentRole.SECURITY_REVIEWER.value
                )
                if resolved_role != expected_role:
                    raise ValueError(
                        f"{evidence.value} evidence requires source role {expected_role}"
                    )
            if evidence in {EvidenceType.TEST, EvidenceType.COMMAND}:
                if command is None or exit_code is None or not subject_commit:
                    raise ValueError(
                        "test and command evidence require command, exit code, and subject commit"
                    )
                if passed != (exit_code == 0 and run_status == "OK"):
                    raise ValueError(
                        "test/command pass state must match the recorded execution result"
                    )
            if evidence == EvidenceType.ARTIFACT:
                assert artifact is not None
                if not checksum_sha256 or checksum_sha256 != artifact["checksum_sha256"]:
                    raise ValueError("artifact evidence checksum must match the durable artifact")
            if evidence == EvidenceType.INTEGRATION:
                if not integration_commit:
                    raise ValueError(
                        "integration evidence requires the resulting integration commit"
                    )
                if source_agent_id != "integration_supervisor":
                    raise ValueError("integration evidence requires the integration supervisor")
            digest = command_digest
            if command and not digest:
                digest = hashlib.sha256(command.encode("utf-8")).hexdigest()
            generation = int(project["evidence_generation"]) + 1
            row = await connection.fetchrow(
                """
                INSERT INTO dod_evidence(
                  project_id,criterion_id,evidence_type,source_agent_id,source_role,task_id,
                  artifact_id,command,exit_code,checksum_sha256,summary,passed,run_status,
                  contract_version,criterion_hash,subject_commit,integration_commit,
                  command_digest,sandbox_digest,watched_paths,affected_contracts,
                  evidence_generation,metadata
                ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23::jsonb)
                RETURNING id
                """,
                project_uuid,
                criterion_id,
                evidence.value,
                source_agent_id,
                resolved_role,
                _uuid(task_id) if task_id else None,
                _uuid(artifact_id) if artifact_id else None,
                command,
                exit_code,
                checksum_sha256,
                summary,
                passed,
                run_status,
                current_version,
                expected_hash,
                subject_commit,
                integration_commit,
                digest,
                sandbox_digest,
                watched_paths or [],
                affected_contracts or [],
                generation,
                json.dumps(metadata or {}),
            )
            await connection.execute(
                """
                UPDATE projects SET evidence_generation=$2,evaluation_requested_generation=$2
                WHERE id=$1
                """,
                project_uuid,
                generation,
            )
            assert row is not None
            return str(row["id"])

    async def start_evaluation(self, project_id: str, requested_by: str) -> dict[str, Any]:
        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                """
                SELECT * FROM projects WHERE id=$1 FOR UPDATE
                """,
                _uuid(project_id),
            )
            if project is None or int(project["dod_contract_version"] or 0) < 1:
                raise ValueError("project has no authoritative DoD contract")
            await connection.execute(
                """
                UPDATE dod_evaluation_runs SET status='STALE',completed_at=now(),
                  failure_summary='[{"code":"SUPERSEDED_SNAPSHOT"}]'::jsonb
                WHERE project_id=$1 AND status='RUNNING' AND NOT (
                  contract_version=$2 AND contract_hash=$3
                  AND integration_head IS NOT DISTINCT FROM $4 AND evidence_generation=$5
                )
                """,
                _uuid(project_id),
                project["dod_contract_version"],
                project["dod_contract_hash"],
                project["integration_head"],
                project["evidence_generation"],
            )
            running = await connection.fetchrow(
                """
                SELECT * FROM dod_evaluation_runs
                WHERE project_id=$1 AND contract_version=$2 AND contract_hash=$3
                  AND integration_head IS NOT DISTINCT FROM $4 AND evidence_generation=$5
                  AND status='RUNNING' ORDER BY created_at DESC LIMIT 1
                """,
                _uuid(project_id),
                project["dod_contract_version"],
                project["dod_contract_hash"],
                project["integration_head"],
                project["evidence_generation"],
            )
            if running is not None:
                maximum_age = int(runtime_tuning()["dod"]["recovery_scan_seconds"])
                if running["requested_by"] == requested_by:
                    await connection.execute(
                        """
                        UPDATE dod_evaluation_runs SET status='ERROR',completed_at=now(),
                          failure_summary='[{"code":"ABANDONED_SAME_EVALUATOR_RUN"}]'::jsonb
                        WHERE id=$1
                        """,
                        running["id"],
                    )
                elif datetime.now(UTC) - running["created_at"] <= timedelta(seconds=maximum_age):
                    return {**dict(running), "reused": False, "reused_running": True}
                else:
                    await connection.execute(
                        """
                        UPDATE dod_evaluation_runs SET status='ERROR',completed_at=now(),
                          failure_summary='[{"code":"ABANDONED_RUNNING_EVALUATION"}]'::jsonb
                        WHERE id=$1
                        """,
                        running["id"],
                    )
            existing = await connection.fetchrow(
                """
                SELECT * FROM dod_evaluation_runs
                WHERE project_id=$1 AND contract_version=$2 AND contract_hash=$3
                  AND integration_head IS NOT DISTINCT FROM $4 AND evidence_generation=$5
                  AND status IN ('SATISFIED','UNSATISFIED','INCONCLUSIVE')
                ORDER BY created_at DESC LIMIT 1
                """,
                _uuid(project_id),
                project["dod_contract_version"],
                project["dod_contract_hash"],
                project["integration_head"],
                project["evidence_generation"],
            )
            if existing is not None:
                return {**dict(existing), "reused": True}
            row = await connection.fetchrow(
                """
                INSERT INTO dod_evaluation_runs(
                  project_id,contract_version,contract_hash,integration_head,
                  evidence_generation,requested_by
                ) VALUES($1,$2,$3,$4,$5,$6) RETURNING *
                """,
                _uuid(project_id),
                project["dod_contract_version"],
                project["dod_contract_hash"],
                project["integration_head"],
                project["evidence_generation"],
                requested_by,
            )
            return {**dict(row), "reused": False}

    async def persist_evaluation(
        self,
        run_id: str,
        items: list[dict[str, Any]],
        status: str,
        failure_summary: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if status not in {"SATISFIED", "UNSATISFIED", "INCONCLUSIVE", "ERROR"}:
            raise ValueError("invalid evaluation terminal status")
        async with self.db.transaction() as connection:
            run = await connection.fetchrow(
                "SELECT * FROM dod_evaluation_runs WHERE id=$1 FOR UPDATE", _uuid(run_id)
            )
            if run is None or run["status"] != "RUNNING":
                raise ValueError("evaluation run is missing or no longer writable")
            checks = await connection.fetch(
                """
                SELECT criterion_id,criterion_hash,mandatory FROM dod_checks
                WHERE project_id=$1 AND active AND contract_version=$2
                """,
                run["project_id"],
                run["contract_version"],
            )
            expected = {str(row["criterion_id"]): row for row in checks}
            supplied = {str(item["criterion_id"]): item for item in items}
            if len(supplied) != len(items) or set(supplied) != set(expected):
                raise ValueError("evaluation must persist exactly one item per active criterion")
            for criterion_id, item in supplied.items():
                if item["criterion_hash"] != expected[criterion_id]["criterion_hash"]:
                    raise ValueError("evaluation item targets a stale criterion hash")
                if item["status"] not in {
                    "MISSING",
                    "FAILED",
                    "INCONCLUSIVE",
                    "STALE",
                    "SATISFIED",
                    "WAIVED_BY_HUMAN",
                }:
                    raise ValueError("evaluation item has an invalid status")
            mandatory_statuses = {
                supplied[criterion_id]["status"]
                for criterion_id, row in expected.items()
                if row["mandatory"]
            }
            computed_satisfied = bool(mandatory_statuses) and mandatory_statuses <= {
                "SATISFIED",
                "WAIVED_BY_HUMAN",
            }
            if (status == "SATISFIED") != computed_satisfied:
                raise ValueError("evaluation summary conflicts with mandatory criterion items")
            project = await connection.fetchrow(
                "SELECT * FROM projects WHERE id=$1 FOR UPDATE", run["project_id"]
            )
            stale = (
                int(project["dod_contract_version"]) != int(run["contract_version"])
                or project["dod_contract_hash"] != run["contract_hash"]
                or project["integration_head"] != run["integration_head"]
                or int(project["evidence_generation"]) != int(run["evidence_generation"])
            )
            terminal = "STALE" if stale else status
            persisted_failure_summary = list(failure_summary)
            if stale:
                persisted_failure_summary.append(
                    {
                        "criterion_id": None,
                        "code": "EVALUATION_SNAPSHOT_STALE",
                        "message": "project changed during evaluation",
                        "retryable": True,
                        "suggested_owner_role": "platform_engineer",
                    }
                )
            for item in items:
                persisted_reasons = list(item.get("reasons", []))
                if stale:
                    persisted_reasons.append(
                        {
                            "criterion_id": item["criterion_id"],
                            "code": "EVALUATION_SNAPSHOT_STALE",
                            "message": "project changed during evaluation",
                            "retryable": True,
                            "suggested_owner_role": "platform_engineer",
                        }
                    )
                await connection.execute(
                    """
                    INSERT INTO dod_evaluation_items(
                      evaluation_run_id,project_id,criterion_id,criterion_hash,status,reasons,evidence_ids
                    ) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::uuid[])
                    """,
                    run["id"],
                    run["project_id"],
                    item["criterion_id"],
                    item["criterion_hash"],
                    "STALE" if stale else item["status"],
                    json.dumps(persisted_reasons),
                    [_uuid(value) for value in item.get("evidence_ids", [])],
                )
                if not stale:
                    await connection.execute(
                        """
                        UPDATE dod_checks SET status=$3,evidence_summary=$4,
                          verified_by_agent_id='dod_evaluator'
                        WHERE project_id=$1 AND criterion_id=$2 AND criterion_hash=$5
                        """,
                        run["project_id"],
                        item["criterion_id"],
                        item["status"],
                        json.dumps(item.get("reasons", [])),
                        item["criterion_hash"],
                    )
            await connection.execute(
                """
                UPDATE dod_evaluation_runs SET status=$2,failure_summary=$3::jsonb,
                  completed_at=now() WHERE id=$1
                """,
                run["id"],
                terminal,
                json.dumps(persisted_failure_summary),
            )
            return {"evaluation_run_id": str(run["id"]), "status": terminal, "stale": stale}

    async def finalize_project(self, project_id: str, run_id: str) -> bool:
        """Atomically fence the exact satisfied snapshot and make the project terminal."""

        async with self.db.transaction() as connection:
            project = await connection.fetchrow(
                "SELECT * FROM projects WHERE id=$1 FOR UPDATE", _uuid(project_id)
            )
            run = await connection.fetchrow(
                "SELECT * FROM dod_evaluation_runs WHERE id=$1 AND project_id=$2 FOR UPDATE",
                _uuid(run_id),
                _uuid(project_id),
            )
            if (
                project is None
                or run is None
                or run["status"] != "SATISFIED"
                or not run["integration_head"]
            ):
                return False
            if (
                int(project["dod_contract_version"]) != int(run["contract_version"])
                or project["dod_contract_hash"] != run["contract_hash"]
                or project["integration_head"] != run["integration_head"]
                or int(project["evidence_generation"]) != int(run["evidence_generation"])
            ):
                return False
            unsatisfied = await connection.fetchval(
                """
                SELECT count(*) FROM dod_checks WHERE project_id=$1 AND active AND mandatory
                  AND status NOT IN ('SATISFIED','WAIVED_BY_HUMAN')
                """,
                _uuid(project_id),
            )
            incomplete = await connection.fetchval(
                """
                SELECT count(*) FROM tasks WHERE project_id=$1
                  AND dod_contract_version=$2 AND status<>'COMPLETED'
                  AND EXISTS(
                    SELECT 1 FROM dod_checks c
                    WHERE c.project_id=tasks.project_id AND c.active AND c.mandatory
                      AND c.status<>'WAIVED_BY_HUMAN' AND c.criterion_id=ANY(tasks.dod_criteria)
                  )
                """,
                _uuid(project_id),
                run["contract_version"],
            )
            if int(unsatisfied or 0) or int(incomplete or 0):
                return False
            changed = await connection.execute(
                """
                UPDATE projects SET status='DOD_SATISFIED'
                WHERE id=$1 AND status IN ('RUNNING','VERIFYING','REPLANNING')
                """,
                _uuid(project_id),
            )
            return str(changed).endswith(" 1")

    async def get_project_dod_status(self, project_id: str) -> list[dict[str, Any]]:
        rows = await self.db.fetch(
            "SELECT * FROM dod_checks WHERE project_id=$1 AND active ORDER BY created_at",
            _uuid(project_id),
        )
        return [dict(row) for row in rows]

    async def get_checks(self, project_id: str, criterion_ids: list[str]) -> list[dict[str, Any]]:
        if not criterion_ids:
            return []
        rows = await self.db.fetch(
            """
            SELECT * FROM dod_checks
            WHERE project_id=$1 AND active AND criterion_id=ANY($2::text[])
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
