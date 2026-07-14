CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL UNIQUE,
    request TEXT,
    status TEXT NOT NULL DEFAULT 'INITIALIZING',
    dod JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    squad TEXT,
    status TEXT NOT NULL DEFAULT 'STARTING',
    permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_scopes TEXT[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    topic TEXT NOT NULL,
    producer_agent_id TEXT,
    target_agent_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    correlation_id TEXT,
    causation_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_project_time_idx ON events(project_id, created_at DESC);
CREATE INDEX IF NOT EXISTS events_type_idx ON events(event_type);

CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    parent_task_id UUID REFERENCES tasks(id),
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING',
    owner_agent_id TEXT,
    priority INTEGER NOT NULL DEFAULT 3,
    acceptance_criteria JSONB NOT NULL DEFAULT '[]'::jsonb,
    allowed_paths TEXT[] NOT NULL DEFAULT '{}',
    blocked_paths TEXT[] NOT NULL DEFAULT '{}',
    required_reviewers TEXT[] NOT NULL DEFAULT '{}',
    affected_contracts TEXT[] NOT NULL DEFAULT '{}',
    risk_level TEXT NOT NULL DEFAULT 'LOW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id UUID REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS artifacts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    task_id UUID REFERENCES tasks(id),
    artifact_type TEXT NOT NULL,
    title TEXT NOT NULL,
    uri TEXT,
    checksum TEXT,
    summary TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT NOT NULL,
    task_id UUID REFERENCES tasks(id),
    achievement TEXT NOT NULL,
    summary TEXT NOT NULL,
    state_pointer JSONB NOT NULL DEFAULT '{}'::jsonb,
    artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS summaries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    owner_id TEXT,
    summary TEXT NOT NULL,
    source_checkpoint_id UUID REFERENCES checkpoints(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memory_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    visibility_roles TEXT[] NOT NULL DEFAULT '{}',
    visibility_agents TEXT[] NOT NULL DEFAULT '{}',
    owner_agent_id TEXT,
    memory_type TEXT NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    source_event_id UUID REFERENCES events(id),
    source_artifact_id UUID REFERENCES artifacts(id),
    importance INTEGER NOT NULL DEFAULT 3,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS memory_embeddings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    memory_item_id UUID REFERENCES memory_items(id) ON DELETE CASCADE,
    embedding_model TEXT NOT NULL,
    embedding_dimension INTEGER NOT NULL,
    embedding vector(768),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS memory_items_project_scope_idx ON memory_items(project_id, scope, memory_type);
CREATE INDEX IF NOT EXISTS memory_items_metadata_idx ON memory_items USING gin(metadata);
CREATE INDEX IF NOT EXISTS memory_embeddings_vector_idx ON memory_embeddings USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS provider_calls (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT,
    purpose TEXT NOT NULL,
    provider TEXT,
    model TEXT,
    prompt_hash TEXT,
    response_hash TEXT,
    redaction_status TEXT,
    token_usage JSONB NOT NULL DEFAULT '{}'::jsonb,
    cost_usd NUMERIC(12, 6),
    latency_ms INTEGER,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    agent_id TEXT,
    event_type TEXT NOT NULL,
    risk_level TEXT,
    decision TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
