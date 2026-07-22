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
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

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
    lease_expires_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, external_key)
);

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

CREATE TABLE IF NOT EXISTS dod_checks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    criterion_id TEXT NOT NULL,
    description TEXT NOT NULL,
    verification_type TEXT NOT NULL,
    verification_command JSONB NOT NULL DEFAULT '[]'::jsonb,
    required_artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    required_evidence_types TEXT[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'NOT_STARTED' CHECK (status IN (
        'NOT_STARTED', 'IN_PROGRESS', 'IMPLEMENTED', 'UNDER_REVIEW',
        'FAILED_VERIFICATION', 'SATISFIED', 'WAIVED_BY_HUMAN'
    )),
    verified_by_agent_id TEXT,
    evidence_summary TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, criterion_id)
);

CREATE TABLE IF NOT EXISTS dod_evidence (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    criterion_id TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    source_agent_id TEXT NOT NULL,
    artifact_id UUID REFERENCES artifacts(id),
    command TEXT,
    exit_code INTEGER,
    checksum_sha256 TEXT,
    summary TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (project_id, criterion_id) REFERENCES dod_checks(project_id, criterion_id) ON DELETE CASCADE
);

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

INSERT INTO schema_migrations(version, description)
VALUES (2, 'Milvus-MongoDB-MinIO production architecture')
ON CONFLICT (version) DO NOTHING;

INSERT INTO schema_migrations(version, description)
VALUES (3, 'Reliable messaging, lossless memory, provider intents, and project isolation')
ON CONFLICT (version) DO NOTHING;
