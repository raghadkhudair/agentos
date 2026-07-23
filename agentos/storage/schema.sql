CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION agentos_touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION agentos_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'append-only table % cannot be updated or deleted', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    request TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'INITIALIZING' CHECK (status IN (
        'INITIALIZING', 'PLANNING', 'TEAM_FORMING', 'RUNNING', 'REPLANNING',
        'INTEGRATING', 'VERIFYING', 'PAUSED', 'BLOCKED_REQUIRES_APPROVAL',
        'BLOCKED_REQUIRES_INPUT', 'DOD_SATISFIED', 'FAILED_BY_POLICY', 'STOPPED_BY_USER'
    )),
    architecture TEXT NOT NULL DEFAULT '',
    assumptions JSONB NOT NULL DEFAULT '[]'::jsonb,
    dod JSONB NOT NULL DEFAULT '[]'::jsonb,
    dod_contract_version INTEGER NOT NULL DEFAULT 0 CHECK (dod_contract_version >= 0),
    dod_contract_hash TEXT,
    planning_context_hash TEXT,
    planning_prompt_version TEXT,
    source_revision TEXT,
    integration_head TEXT,
    evidence_generation BIGINT NOT NULL DEFAULT 0 CHECK (evidence_generation >= 0),
    evaluation_requested_generation BIGINT NOT NULL DEFAULT 0 CHECK (evaluation_requested_generation >= 0),
    evaluation_failure_count INTEGER NOT NULL DEFAULT 0 CHECK (evaluation_failure_count >= 0),
    replan_attempts INTEGER NOT NULL DEFAULT 0 CHECK (replan_attempts >= 0),
    next_replan_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE projects
    ADD COLUMN IF NOT EXISTS dod_contract_version INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dod_contract_hash TEXT,
    ADD COLUMN IF NOT EXISTS planning_context_hash TEXT,
    ADD COLUMN IF NOT EXISTS planning_prompt_version TEXT,
    ADD COLUMN IF NOT EXISTS source_revision TEXT,
    ADD COLUMN IF NOT EXISTS integration_head TEXT,
    ADD COLUMN IF NOT EXISTS evidence_generation BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS evaluation_requested_generation BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS evaluation_failure_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS replan_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_replan_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS agents (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    id TEXT NOT NULL,
    role TEXT NOT NULL,
    squad TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'STARTING',
    permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_scopes TEXT[] NOT NULL DEFAULT '{}',
    provider_assignment JSONB NOT NULL DEFAULT '{}'::jsonb,
    resource_allocation JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, id)
);

CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    topic TEXT NOT NULL,
    producer_agent_id TEXT,
    target_agent_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload_object_uri TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    schema_version INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS events_project_id_id_uidx
ON events(project_id, id);

CREATE TABLE IF NOT EXISTS event_outbox (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE REFERENCES events(id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS event_receipts (
    project_id UUID NOT NULL,
    event_id UUID NOT NULL,
    agent_id TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'DELIVERED' CHECK (status IN (
        'DELIVERED', 'PROCESSING', 'PROCESSED', 'FAILED'
    )),
    consumer_name TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, event_id, agent_id),
    FOREIGN KEY (project_id, event_id)
        REFERENCES events(project_id, id) ON DELETE CASCADE,
    FOREIGN KEY (project_id, agent_id)
        REFERENCES agents(project_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    parent_task_id UUID REFERENCES tasks(id),
    external_key TEXT,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN (
        'PENDING', 'CLAIMED', 'IN_PROGRESS', 'BLOCKED', 'UNDER_REVIEW',
        'FAILED_VERIFICATION', 'COMPLETED', 'CANCELLED'
    )),
    owner_agent_id TEXT,
    owner_role TEXT,
    priority INTEGER NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
    complexity TEXT NOT NULL DEFAULT 'standard' CHECK (complexity IN ('low', 'standard', 'high', 'critical')),
    acceptance_criteria JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_paths TEXT[] NOT NULL DEFAULT '{}',
    blocked_paths TEXT[] NOT NULL DEFAULT '{}',
    expected_outputs TEXT[] NOT NULL DEFAULT '{}',
    required_reviewers TEXT[] NOT NULL DEFAULT '{}',
    dod_criteria TEXT[] NOT NULL DEFAULT '{}',
    affected_contracts TEXT[] NOT NULL DEFAULT '{}',
    risk_level TEXT NOT NULL DEFAULT 'LOW' CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    dod_contract_version INTEGER NOT NULL DEFAULT 1 CHECK (dod_contract_version >= 1),
    lease_expires_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, external_key)
);

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS dod_contract_version INTEGER NOT NULL DEFAULT 1;

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id),
    CHECK (task_id <> depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id),
    artifact_type TEXT NOT NULL,
    title TEXT NOT NULL,
    object_uri TEXT NOT NULL,
    object_version_id TEXT,
    checksum_sha256 TEXT NOT NULL CHECK (length(checksum_sha256) = 64),
    content_length BIGINT NOT NULL CHECK (content_length >= 0),
    content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
    summary TEXT NOT NULL DEFAULT '',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='artifacts_version_binding_check'
          AND conrelid='artifacts'::regclass
    ) THEN
        -- NOT VALID preserves readable legacy rows, while PostgreSQL enforces the exact-version
        -- contract for every new artifact written after this migration.
        ALTER TABLE artifacts ADD CONSTRAINT artifacts_version_binding_check CHECK (
          object_version_id IS NOT NULL AND object_version_id <> ''
          AND object_uri LIKE '%versionId=%'
        ) NOT VALID;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS checkpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    task_id UUID REFERENCES tasks(id),
    achievement TEXT NOT NULL,
    summary TEXT NOT NULL,
    state_pointer JSONB NOT NULL DEFAULT '{}'::jsonb,
    artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS summaries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    owner_id TEXT,
    summary TEXT NOT NULL,
    source_checkpoint_id UUID REFERENCES checkpoints(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memory_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    visibility_roles TEXT[] NOT NULL DEFAULT '{}',
    visibility_agents TEXT[] NOT NULL DEFAULT '{}',
    owner_agent_id TEXT NOT NULL,
    memory_type TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    content_object_uri TEXT,
    content_object_version_id TEXT,
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64),
    content_length BIGINT NOT NULL DEFAULT 0 CHECK (content_length >= 0),
    storage_status TEXT NOT NULL DEFAULT 'READY' CHECK (
        storage_status IN ('PENDING_OBJECT', 'READY', 'OBJECT_FAILED')
    ),
    source_event_id UUID REFERENCES events(id),
    source_artifact_id UUID REFERENCES artifacts(id),
    milvus_record_id TEXT UNIQUE,
    embedding_model TEXT,
    importance INTEGER NOT NULL DEFAULT 3 CHECK (importance BETWEEN 1 AND 5),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (project_id IS NOT NULL OR scope = 'global_patterns')
);

ALTER TABLE memory_items
    ADD COLUMN IF NOT EXISTS content_object_version_id TEXT,
    ADD COLUMN IF NOT EXISTS content_length BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS storage_status TEXT NOT NULL DEFAULT 'READY';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'memory_items_content_length_check'
          AND conrelid = 'memory_items'::regclass
    ) THEN
        ALTER TABLE memory_items
            ADD CONSTRAINT memory_items_content_length_check
            CHECK (content_length >= 0) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'memory_items_storage_status_check'
          AND conrelid = 'memory_items'::regclass
    ) THEN
        ALTER TABLE memory_items
            ADD CONSTRAINT memory_items_storage_status_check
            CHECK (storage_status IN ('PENDING_OBJECT', 'READY', 'OBJECT_FAILED')) NOT VALID;
    END IF;
END;
$$;

ALTER TABLE memory_items VALIDATE CONSTRAINT memory_items_content_length_check;
ALTER TABLE memory_items VALIDATE CONSTRAINT memory_items_storage_status_check;

CREATE TABLE IF NOT EXISTS provider_call_intents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT,
    purpose TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL CHECK (length(prompt_hash) = 64),
    redaction_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS provider_calls (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    intent_id UUID REFERENCES provider_call_intents(id),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT,
    purpose TEXT NOT NULL,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    response_hash TEXT,
    redaction_status TEXT NOT NULL,
    token_usage JSONB NOT NULL DEFAULT '{}'::jsonb,
    cost_usd NUMERIC(14, 8) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
    latency_ms INTEGER CHECK (latency_ms >= 0),
    status TEXT NOT NULL,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE provider_calls ADD COLUMN IF NOT EXISTS intent_id UUID;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'provider_calls_intent_id_fkey'
          AND conrelid = 'provider_calls'::regclass
    ) THEN
        ALTER TABLE provider_calls
            ADD CONSTRAINT provider_calls_intent_id_fkey
            FOREIGN KEY (intent_id) REFERENCES provider_call_intents(id);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT,
    event_type TEXT NOT NULL,
    risk_level TEXT,
    decision TEXT,
    integrity_hash TEXT NOT NULL,
    previous_hash TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS approval_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    action_integrity_hash TEXT NOT NULL UNIQUE,
    requested_by_agent_id TEXT NOT NULL,
    required_gate TEXT NOT NULL,
    request_payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED')),
    decided_by TEXT,
    decision_reason TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dod_contract_versions (
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    contract_version INTEGER NOT NULL CHECK (contract_version >= 1),
    contract_hash TEXT NOT NULL CHECK (length(contract_hash) = 64),
    source_revision TEXT NOT NULL,
    planning_context_hash TEXT NOT NULL CHECK (length(planning_context_hash) = 64),
    prompt_version TEXT NOT NULL,
    contract JSONB NOT NULL,
    created_by TEXT NOT NULL,
    amendment_reason TEXT,
    approval_id UUID REFERENCES approval_requests(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (project_id, contract_version),
    UNIQUE (project_id, contract_hash),
    CHECK (
        (contract_version = 1 AND amendment_reason IS NULL)
        OR (contract_version > 1 AND amendment_reason IS NOT NULL AND approval_id IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS dod_checks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    criterion_id TEXT NOT NULL,
    description TEXT NOT NULL,
    verification_type TEXT NOT NULL,
    verification_command JSONB NOT NULL DEFAULT '[]'::jsonb,
    required_artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    required_evidence_types TEXT[] NOT NULL DEFAULT '{}',
    evidence_scopes JSONB NOT NULL DEFAULT '{}'::jsonb,
    contract_version INTEGER NOT NULL DEFAULT 1,
    criterion_hash TEXT NOT NULL DEFAULT repeat('0', 64),
    source TEXT NOT NULL DEFAULT 'system' CHECK (source IN ('user', 'system', 'inferred')),
    locked BOOLEAN NOT NULL DEFAULT TRUE,
    mandatory BOOLEAN NOT NULL DEFAULT TRUE,
    severity TEXT NOT NULL DEFAULT 'required' CHECK (severity IN ('advisory', 'required', 'critical')),
    affected_contracts TEXT[] NOT NULL DEFAULT '{}',
    waiver_approval_id UUID REFERENCES approval_requests(id),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    status TEXT NOT NULL DEFAULT 'MISSING' CHECK (status IN (
        'MISSING', 'FAILED', 'INCONCLUSIVE', 'STALE', 'SATISFIED', 'WAIVED_BY_HUMAN'
    )),
    verified_by_agent_id TEXT,
    evidence_summary TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, criterion_id)
);

ALTER TABLE dod_checks
    ADD COLUMN IF NOT EXISTS evidence_scopes JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS contract_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS criterion_hash TEXT NOT NULL DEFAULT repeat('0', 64),
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'system',
    ADD COLUMN IF NOT EXISTS locked BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS mandatory BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS severity TEXT NOT NULL DEFAULT 'required',
    ADD COLUMN IF NOT EXISTS affected_contracts TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS waiver_approval_id UUID REFERENCES approval_requests(id),
    ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE dod_checks DROP CONSTRAINT IF EXISTS dod_checks_status_check;
ALTER TABLE dod_checks ADD CONSTRAINT dod_checks_status_check CHECK (status IN (
    'NOT_STARTED', 'IN_PROGRESS', 'IMPLEMENTED', 'UNDER_REVIEW', 'FAILED_VERIFICATION',
    'MISSING', 'FAILED', 'INCONCLUSIVE', 'STALE', 'SATISFIED', 'WAIVED_BY_HUMAN'
)) NOT VALID;
ALTER TABLE dod_checks VALIDATE CONSTRAINT dod_checks_status_check;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='dod_checks_source_check'
          AND conrelid='dod_checks'::regclass
    ) THEN
        ALTER TABLE dod_checks ADD CONSTRAINT dod_checks_source_check
          CHECK (source IN ('user','system','inferred')) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='dod_checks_severity_check'
          AND conrelid='dod_checks'::regclass
    ) THEN
        ALTER TABLE dod_checks ADD CONSTRAINT dod_checks_severity_check
          CHECK (severity IN ('advisory','required','critical')) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='dod_checks_revision_check'
          AND conrelid='dod_checks'::regclass
    ) THEN
        ALTER TABLE dod_checks ADD CONSTRAINT dod_checks_revision_check
          CHECK (contract_version >= 1 AND length(criterion_hash) = 64) NOT VALID;
    END IF;
END;
$$;

ALTER TABLE dod_checks VALIDATE CONSTRAINT dod_checks_source_check;
ALTER TABLE dod_checks VALIDATE CONSTRAINT dod_checks_severity_check;
ALTER TABLE dod_checks VALIDATE CONSTRAINT dod_checks_revision_check;

CREATE TABLE IF NOT EXISTS dod_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    criterion_id TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    source_agent_id TEXT NOT NULL,
    source_role TEXT NOT NULL DEFAULT 'unknown',
    task_id UUID REFERENCES tasks(id),
    artifact_id UUID REFERENCES artifacts(id),
    command TEXT,
    exit_code INTEGER,
    checksum_sha256 TEXT,
    summary TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    run_status TEXT NOT NULL DEFAULT 'OK' CHECK (run_status IN ('OK', 'INCONCLUSIVE')),
    contract_version INTEGER NOT NULL DEFAULT 1,
    criterion_hash TEXT NOT NULL DEFAULT repeat('0', 64),
    subject_commit TEXT,
    integration_commit TEXT,
    command_digest TEXT,
    sandbox_digest TEXT,
    watched_paths TEXT[] NOT NULL DEFAULT '{}',
    affected_contracts TEXT[] NOT NULL DEFAULT '{}',
    evidence_generation BIGINT NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (project_id, criterion_id) REFERENCES dod_checks(project_id, criterion_id) ON DELETE CASCADE
);

ALTER TABLE dod_evidence
    ADD COLUMN IF NOT EXISTS source_role TEXT NOT NULL DEFAULT 'unknown',
    ADD COLUMN IF NOT EXISTS task_id UUID REFERENCES tasks(id),
    ADD COLUMN IF NOT EXISTS run_status TEXT NOT NULL DEFAULT 'OK',
    ADD COLUMN IF NOT EXISTS contract_version INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS criterion_hash TEXT NOT NULL DEFAULT repeat('0', 64),
    ADD COLUMN IF NOT EXISTS subject_commit TEXT,
    ADD COLUMN IF NOT EXISTS integration_commit TEXT,
    ADD COLUMN IF NOT EXISTS command_digest TEXT,
    ADD COLUMN IF NOT EXISTS sandbox_digest TEXT,
    ADD COLUMN IF NOT EXISTS watched_paths TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS affected_contracts TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN IF NOT EXISTS evidence_generation BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='dod_evidence_type_check'
          AND conrelid='dod_evidence'::regclass
    ) THEN
        ALTER TABLE dod_evidence ADD CONSTRAINT dod_evidence_type_check CHECK (
            evidence_type IN ('artifact','test','command','review','security_review','integration')
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname='dod_evidence_run_status_check'
          AND conrelid='dod_evidence'::regclass
    ) THEN
        ALTER TABLE dod_evidence ADD CONSTRAINT dod_evidence_run_status_check CHECK (
            run_status IN ('OK','INCONCLUSIVE')
        ) NOT VALID;
    END IF;
END;
$$;

ALTER TABLE dod_evidence VALIDATE CONSTRAINT dod_evidence_type_check;
ALTER TABLE dod_evidence VALIDATE CONSTRAINT dod_evidence_run_status_check;

CREATE OR REPLACE FUNCTION agentos_validate_dod_evidence() RETURNS trigger AS $$
DECLARE
    criterion_record dod_checks%ROWTYPE;
    task_record tasks%ROWTYPE;
    artifact_record artifacts%ROWTYPE;
    authenticated_role TEXT;
BEGIN
    SELECT * INTO criterion_record FROM dod_checks
    WHERE project_id=NEW.project_id AND criterion_id=NEW.criterion_id AND active;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'evidence criterion must be active in the same project';
    END IF;
    IF NEW.contract_version IS DISTINCT FROM criterion_record.contract_version
       OR NEW.criterion_hash IS DISTINCT FROM criterion_record.criterion_hash THEN
        RAISE EXCEPTION 'evidence must target the active criterion revision';
    END IF;
    IF NOT (NEW.evidence_type = ANY(criterion_record.required_evidence_types)) THEN
        RAISE EXCEPTION 'evidence type is not authorized by the criterion contract';
    END IF;

    IF NEW.source_agent_id = 'integration_supervisor' THEN
        authenticated_role := 'integration_supervisor';
    ELSE
        SELECT role INTO authenticated_role FROM agents
        WHERE project_id=NEW.project_id AND id=NEW.source_agent_id;
    END IF;
    IF authenticated_role IS NULL OR NEW.source_role IS DISTINCT FROM authenticated_role THEN
        RAISE EXCEPTION 'evidence producer role must match an authenticated project identity';
    END IF;

    IF NEW.task_id IS NOT NULL THEN
        SELECT * INTO task_record FROM tasks WHERE id=NEW.task_id AND project_id=NEW.project_id;
        IF NOT FOUND OR NOT (NEW.criterion_id = ANY(task_record.dod_criteria))
           OR task_record.dod_contract_version IS DISTINCT FROM NEW.contract_version THEN
            RAISE EXCEPTION 'evidence task must map to the active criterion revision';
        END IF;
    END IF;
    IF NEW.evidence_type IN ('artifact','review','security_review','integration')
       AND NEW.task_id IS NULL THEN
        RAISE EXCEPTION '% evidence requires a task reference', NEW.evidence_type;
    END IF;
    IF NEW.evidence_type IN ('artifact','test','command') AND NEW.task_id IS NOT NULL
       AND NEW.source_agent_id IS DISTINCT FROM task_record.owner_agent_id THEN
        RAISE EXCEPTION 'task evidence must be produced by the assigned task owner';
    END IF;

    IF NEW.artifact_id IS NOT NULL THEN
        SELECT * INTO artifact_record FROM artifacts
        WHERE id=NEW.artifact_id AND project_id=NEW.project_id;
        IF NOT FOUND OR NEW.task_id IS NULL OR artifact_record.task_id IS DISTINCT FROM NEW.task_id THEN
            RAISE EXCEPTION 'evidence artifact must belong to its referenced task';
        END IF;
    END IF;
    IF NEW.evidence_type IN ('artifact','review','security_review')
       AND NEW.artifact_id IS NULL THEN
        RAISE EXCEPTION '% evidence requires an artifact reference', NEW.evidence_type;
    END IF;
    IF NEW.evidence_type IN ('artifact','review','security_review')
       AND NEW.subject_commit IS NULL THEN
        RAISE EXCEPTION '% evidence requires a subject commit', NEW.evidence_type;
    END IF;
    IF NEW.evidence_type = 'artifact'
       AND NEW.checksum_sha256 IS DISTINCT FROM artifact_record.checksum_sha256 THEN
        RAISE EXCEPTION 'artifact evidence checksum must match the durable artifact';
    END IF;
    IF NEW.evidence_type IN ('artifact','review','security_review')
       AND NEW.subject_commit IS DISTINCT FROM artifact_record.metadata->>'git_commit' THEN
        RAISE EXCEPTION 'artifact evidence must target the artifact Git revision';
    END IF;
    IF NEW.evidence_type IN ('review','security_review') AND (
       artifact_record.metadata->>'review_diff_sha256' IS NULL
       OR length(artifact_record.metadata->>'review_diff_sha256') <> 64
       OR artifact_record.metadata->>'review_diff_characters' IS NULL
    ) THEN
        RAISE EXCEPTION 'review evidence requires committed diff provenance';
    END IF;

    IF NEW.evidence_type = 'review' THEN
        IF authenticated_role <> 'code_reviewer' OR NEW.source_agent_id = task_record.owner_agent_id THEN
            RAISE EXCEPTION 'review evidence requires an independent code reviewer';
        END IF;
    ELSIF NEW.evidence_type = 'security_review' THEN
        IF authenticated_role <> 'security_reviewer'
           OR NEW.source_agent_id = task_record.owner_agent_id THEN
            RAISE EXCEPTION 'security evidence requires an independent security reviewer';
        END IF;
    ELSIF NEW.evidence_type IN ('test','command') THEN
        IF NEW.command IS NULL OR NEW.exit_code IS NULL OR NEW.subject_commit IS NULL
           OR NEW.passed IS DISTINCT FROM (NEW.exit_code = 0 AND NEW.run_status = 'OK') THEN
            RAISE EXCEPTION 'test and command evidence must match its execution result';
        END IF;
        IF NEW.task_id IS NULL AND NEW.source_agent_id <> 'integration_supervisor' THEN
            RAISE EXCEPTION 'criterion-global command evidence requires the integration supervisor';
        END IF;
        IF NEW.command_digest IS NULL
           OR NEW.command_digest <> encode(digest(NEW.command, 'sha256'), 'hex') THEN
            RAISE EXCEPTION 'command evidence digest must match its canonical token array';
        END IF;
        IF NEW.run_status = 'OK' AND (
           NEW.sandbox_digest IS NULL OR length(NEW.sandbox_digest) <> 64
        ) THEN
            RAISE EXCEPTION 'executed command evidence requires a sandbox digest';
        END IF;
    ELSIF NEW.evidence_type = 'integration' THEN
        IF NEW.source_agent_id <> 'integration_supervisor' OR NEW.subject_commit IS NULL
           OR NEW.integration_commit IS NULL THEN
            RAISE EXCEPTION 'integration evidence requires the integration supervisor and revisions';
        END IF;
    END IF;
    IF NEW.run_status = 'INCONCLUSIVE' AND NEW.passed THEN
        RAISE EXCEPTION 'inconclusive evidence cannot pass';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dod_evidence_contract_guard ON dod_evidence;
CREATE TRIGGER dod_evidence_contract_guard BEFORE INSERT ON dod_evidence
FOR EACH ROW EXECUTE FUNCTION agentos_validate_dod_evidence();

CREATE TABLE IF NOT EXISTS dod_evaluation_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    contract_version INTEGER NOT NULL,
    contract_hash TEXT NOT NULL CHECK (length(contract_hash) = 64),
    integration_head TEXT,
    evidence_generation BIGINT NOT NULL,
    evidence_cutoff TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN (
        'RUNNING', 'SATISFIED', 'UNSATISFIED', 'INCONCLUSIVE', 'STALE', 'ERROR'
    )),
    requested_by TEXT NOT NULL,
    failure_summary JSONB NOT NULL DEFAULT '[]'::jsonb,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dod_evaluation_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    evaluation_run_id UUID NOT NULL REFERENCES dod_evaluation_runs(id) ON DELETE CASCADE,
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    criterion_id TEXT NOT NULL,
    criterion_hash TEXT NOT NULL CHECK (length(criterion_hash) = 64),
    status TEXT NOT NULL CHECK (status IN (
        'MISSING', 'FAILED', 'INCONCLUSIVE', 'STALE', 'SATISFIED', 'WAIVED_BY_HUMAN'
    )),
    reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (evaluation_run_id, criterion_id)
);

CREATE TABLE IF NOT EXISTS integration_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    pre_head TEXT NOT NULL,
    branch_head TEXT NOT NULL,
    result_head TEXT,
    status TEXT NOT NULL DEFAULT 'PREPARED' CHECK (status IN ('PREPARED','COMMITTED','ABORTED')),
    failure_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS one_prepared_integration_per_project
ON integration_attempts(project_id) WHERE status='PREPARED';

CREATE TABLE IF NOT EXISTS resource_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    generated_by_agent_id TEXT NOT NULL,
    host_snapshot JSONB NOT NULL,
    allocations JSONB NOT NULL,
    config_hash TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_resource_plan_per_project
ON resource_plans(project_id) WHERE active;

CREATE TABLE IF NOT EXISTS runtime_config_snapshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    config_hash TEXT NOT NULL,
    public_config JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_project_time_idx ON events(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS events_type_idx ON events(project_id, event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS event_outbox_pending_idx ON event_outbox(available_at, id) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS event_receipts_recovery_idx
ON event_receipts(project_id, agent_id, status, lease_expires_at);
CREATE INDEX IF NOT EXISTS tasks_project_status_idx ON tasks(project_id, status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS tasks_owner_role_idx ON tasks(project_id, owner_role, status);
CREATE INDEX IF NOT EXISTS checkpoints_project_agent_idx ON checkpoints(project_id, agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS memory_items_project_scope_idx ON memory_items(project_id, scope, memory_type, created_at DESC);
CREATE INDEX IF NOT EXISTS memory_items_lexical_idx ON memory_items USING gin(to_tsvector('english', title || ' ' || content));
CREATE INDEX IF NOT EXISTS provider_calls_project_time_idx ON provider_calls(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS provider_call_intents_project_time_idx
ON provider_call_intents(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS audit_events_project_time_idx ON audit_events(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS dod_checks_project_status_idx ON dod_checks(project_id, status);
CREATE INDEX IF NOT EXISTS dod_evidence_criterion_idx ON dod_evidence(project_id, criterion_id, passed, evidence_type);
CREATE INDEX IF NOT EXISTS dod_evidence_snapshot_idx
ON dod_evidence(project_id, contract_version, evidence_generation, created_at DESC);
CREATE INDEX IF NOT EXISTS dod_evaluation_runs_project_idx
ON dod_evaluation_runs(project_id, created_at DESC);
WITH duplicate_running AS (
    SELECT id,row_number() OVER (PARTITION BY project_id ORDER BY created_at DESC,id DESC) rank
    FROM dod_evaluation_runs WHERE status='RUNNING'
)
UPDATE dod_evaluation_runs SET status='ERROR',completed_at=now(),
  failure_summary='[{"code":"DUPLICATE_RUNNING_EVALUATION_MIGRATED"}]'::jsonb
WHERE id IN (SELECT id FROM duplicate_running WHERE rank > 1);
CREATE UNIQUE INDEX IF NOT EXISTS one_running_dod_evaluation_per_project
ON dod_evaluation_runs(project_id) WHERE status='RUNNING';

CREATE OR REPLACE FUNCTION agentos_enforce_project_isolation() RETURNS trigger AS $$
DECLARE
    referenced_project UUID;
    dependency_project UUID;
BEGIN
    IF TG_TABLE_NAME = 'tasks' THEN
        IF NEW.parent_task_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM tasks WHERE id=NEW.parent_task_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'parent task must belong to the same project';
            END IF;
        END IF;
        IF NEW.owner_agent_id IS NOT NULL AND NOT EXISTS(
            SELECT 1 FROM agents WHERE project_id=NEW.project_id AND id=NEW.owner_agent_id
        ) THEN
            RAISE EXCEPTION 'task owner must be a registered agent in the same project';
        END IF;
    ELSIF TG_TABLE_NAME = 'task_dependencies' THEN
        SELECT project_id INTO referenced_project FROM tasks WHERE id=NEW.task_id;
        SELECT project_id INTO dependency_project FROM tasks WHERE id=NEW.depends_on_task_id;
        IF referenced_project IS NULL OR dependency_project IS NULL
           OR referenced_project IS DISTINCT FROM dependency_project THEN
            RAISE EXCEPTION 'task dependency must stay within one project';
        END IF;
    ELSIF TG_TABLE_NAME = 'artifacts' THEN
        IF NEW.task_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM tasks WHERE id=NEW.task_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'artifact task must belong to the same project';
            END IF;
        END IF;
    ELSIF TG_TABLE_NAME = 'checkpoints' THEN
        IF NEW.task_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM tasks WHERE id=NEW.task_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'checkpoint task must belong to the same project';
            END IF;
        END IF;
        IF NOT EXISTS(
            SELECT 1 FROM agents WHERE project_id=NEW.project_id AND id=NEW.agent_id
        ) THEN
            RAISE EXCEPTION 'checkpoint agent must be registered in the same project';
        END IF;
    ELSIF TG_TABLE_NAME = 'summaries' THEN
        IF NEW.source_checkpoint_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM checkpoints WHERE id=NEW.source_checkpoint_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'summary checkpoint must belong to the same project';
            END IF;
        END IF;
    ELSIF TG_TABLE_NAME = 'dod_evidence' THEN
        IF NEW.task_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM tasks WHERE id=NEW.task_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'evidence task must belong to the same project';
            END IF;
        END IF;
        IF NEW.artifact_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM artifacts WHERE id=NEW.artifact_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'evidence artifact must belong to the same project';
            END IF;
        END IF;
    ELSIF TG_TABLE_NAME = 'event_outbox' THEN
        SELECT project_id INTO referenced_project FROM events WHERE id=NEW.event_id;
        IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
            RAISE EXCEPTION 'outbox event must belong to the same project';
        END IF;
    ELSIF TG_TABLE_NAME = 'memory_items' THEN
        IF NEW.source_event_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM events WHERE id=NEW.source_event_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'memory event must belong to the same project';
            END IF;
        END IF;
        IF NEW.source_artifact_id IS NOT NULL THEN
            SELECT project_id INTO referenced_project FROM artifacts WHERE id=NEW.source_artifact_id;
            IF referenced_project IS NULL OR referenced_project IS DISTINCT FROM NEW.project_id THEN
                RAISE EXCEPTION 'memory artifact must belong to the same project';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS projects_isolated_tasks ON tasks;
CREATE TRIGGER projects_isolated_tasks BEFORE INSERT OR UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_task_dependencies ON task_dependencies;
CREATE TRIGGER projects_isolated_task_dependencies BEFORE INSERT OR UPDATE ON task_dependencies
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_artifacts ON artifacts;
CREATE TRIGGER projects_isolated_artifacts BEFORE INSERT OR UPDATE ON artifacts
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_checkpoints ON checkpoints;
CREATE TRIGGER projects_isolated_checkpoints BEFORE INSERT OR UPDATE ON checkpoints
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_summaries ON summaries;
CREATE TRIGGER projects_isolated_summaries BEFORE INSERT OR UPDATE ON summaries
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_dod_evidence ON dod_evidence;
CREATE TRIGGER projects_isolated_dod_evidence BEFORE INSERT OR UPDATE ON dod_evidence
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_event_outbox ON event_outbox;
CREATE TRIGGER projects_isolated_event_outbox BEFORE INSERT OR UPDATE ON event_outbox
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();
DROP TRIGGER IF EXISTS projects_isolated_memory_items ON memory_items;
CREATE TRIGGER projects_isolated_memory_items BEFORE INSERT OR UPDATE ON memory_items
FOR EACH ROW EXECUTE FUNCTION agentos_enforce_project_isolation();

DROP TRIGGER IF EXISTS projects_touch_updated_at ON projects;
CREATE TRIGGER projects_touch_updated_at BEFORE UPDATE ON projects
FOR EACH ROW EXECUTE FUNCTION agentos_touch_updated_at();
DROP TRIGGER IF EXISTS agents_touch_updated_at ON agents;
CREATE TRIGGER agents_touch_updated_at BEFORE UPDATE ON agents
FOR EACH ROW EXECUTE FUNCTION agentos_touch_updated_at();
DROP TRIGGER IF EXISTS tasks_touch_updated_at ON tasks;
CREATE TRIGGER tasks_touch_updated_at BEFORE UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION agentos_touch_updated_at();
DROP TRIGGER IF EXISTS event_receipts_touch_updated_at ON event_receipts;
CREATE TRIGGER event_receipts_touch_updated_at BEFORE UPDATE ON event_receipts
FOR EACH ROW EXECUTE FUNCTION agentos_touch_updated_at();
DROP TRIGGER IF EXISTS dod_checks_touch_updated_at ON dod_checks;
CREATE TRIGGER dod_checks_touch_updated_at BEFORE UPDATE ON dod_checks
FOR EACH ROW EXECUTE FUNCTION agentos_touch_updated_at();

DROP TRIGGER IF EXISTS audit_events_append_only ON audit_events;
CREATE TRIGGER audit_events_append_only BEFORE UPDATE OR DELETE ON audit_events
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();
DROP TRIGGER IF EXISTS provider_calls_append_only ON provider_calls;
CREATE TRIGGER provider_calls_append_only BEFORE UPDATE OR DELETE ON provider_calls
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();

DROP TRIGGER IF EXISTS provider_call_intents_append_only ON provider_call_intents;
CREATE TRIGGER provider_call_intents_append_only BEFORE UPDATE OR DELETE ON provider_call_intents
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();

DROP TRIGGER IF EXISTS dod_evidence_append_only ON dod_evidence;
CREATE TRIGGER dod_evidence_append_only BEFORE UPDATE OR DELETE ON dod_evidence
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();

DROP TRIGGER IF EXISTS artifacts_append_only ON artifacts;
CREATE TRIGGER artifacts_append_only BEFORE UPDATE OR DELETE ON artifacts
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();

DROP TRIGGER IF EXISTS dod_contract_versions_append_only ON dod_contract_versions;
CREATE TRIGGER dod_contract_versions_append_only BEFORE UPDATE OR DELETE ON dod_contract_versions
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();

DROP TRIGGER IF EXISTS dod_evaluation_items_append_only ON dod_evaluation_items;
CREATE TRIGGER dod_evaluation_items_append_only BEFORE UPDATE OR DELETE ON dod_evaluation_items
FOR EACH ROW EXECUTE FUNCTION agentos_reject_mutation();

INSERT INTO schema_migrations(version, description)
VALUES (2, 'Milvus-MongoDB-MinIO production architecture')
ON CONFLICT (version) DO NOTHING;

INSERT INTO schema_migrations(version, description)
VALUES (3, 'Reliable messaging, lossless memory, provider intents, and project isolation')
ON CONFLICT (version) DO NOTHING;

INSERT INTO schema_migrations(version, description)
VALUES (4, 'Versioned DoD contracts, append-only provenance, and fenced evaluations')
ON CONFLICT (version) DO NOTHING;
