from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agentos.config.runtime import ResourcePlanner, TaskComplexity
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest
from agentos.messaging.events import Event, EventType, validate_event
from agentos.provider.gateway import ProviderRegistry, ProviderRequest
from agentos.runtime.team_plan import (
    AgentRole,
    AgentSpec,
    DoDCriterion,
    InitialTask,
    TeamPlan,
)


def test_production_settings_fail_on_placeholders_and_redact_snapshots() -> None:
    unsafe = Settings(
        environment="production",
        minio_access_key="CHANGE_ME",
        minio_secret_key="CHANGE_ME",
        sandbox_database_url=None,
    )
    with pytest.raises(ValueError, match="unsafe production configuration"):
        unsafe.validate_production_secrets()
    snapshot = unsafe.safe_snapshot()
    assert snapshot["database_url"] == "[REDACTED]"
    assert "minio_secret_key" not in snapshot


def test_resource_planner_leaves_cpu_and_memory_headroom(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("agentos.config.runtime.os.cpu_count", lambda: 16)
    monkeypatch.setattr(
        "agentos.config.runtime.psutil.virtual_memory",
        lambda: SimpleNamespace(total=32 * 1024**3),
    )
    settings = Settings(
        environment="test",
        max_cpu_cores=10,
        max_memory_bytes=16 * 1024**3,
        max_agents_total=10,
        max_active_agents=8,
    )
    agents = [(f"backend_developer-{index}", "backend_developer") for index in range(1, 7)]
    runtime = ResourcePlanner(settings).build_runtime_config(agents)
    assert runtime.envelope.allocated_cpu_cores < runtime.envelope.detected_cpu_cores
    assert runtime.allocated_agent_cpu <= runtime.envelope.allocated_cpu_cores
    assert (
        sum(item.memory_bytes for item in runtime.allocations)
        + runtime.envelope.object_store_memory_bytes
        <= runtime.envelope.allocated_memory_bytes
    )
    assert all(value == "1" for value in runtime.thread_environment.values())


def test_every_requested_provider_routes_when_credential_is_present() -> None:
    registry = ProviderRegistry()
    assert set(registry.profiles) == {
        "openai",
        "anthropic",
        "gemini",
        "deepseek",
        "moonshot",
        "alibaba",
        "zai",
        "minimax",
        "ollama",
    }
    for provider_id, profile in registry.profiles.items():
        environment = {profile.api_base_env or "OLLAMA_API_BASE": "http://localhost:11434"}
        if profile.api_key_env:
            environment[profile.api_key_env] = "test-key"
        request = ProviderRequest(
            purpose="decide_next_action",
            messages=[{"role": "user", "content": "work"}],
            budget_key=str(uuid4()),
            preferred_provider=provider_id,
            complexity=TaskComplexity.HIGH,
        )
        candidates = registry.candidates(request, environment=environment)
        assert candidates[0][0].provider_id == provider_id
        assert candidates[0][1].startswith(f"{profile.litellm_prefix}/")


def test_team_plan_rejects_unknown_owner_role_and_unknown_dod() -> None:
    criterion = DoDCriterion(
        criterion_id="verified",
        description="A verified result",
        verification_command=["pytest", "-q"],
        required_artifacts=["src/result.py"],
        required_evidence_types=["artifact", "test", "review", "integration"],
    )
    agent = AgentSpec(
        role=AgentRole.BACKEND_DEVELOPER,
        count=1,
        description="Implement backend",
    )
    with pytest.raises(ValidationError, match="unplanned role"):
        TeamPlan(
            project_name="demo-project",
            user_request="Build the demo",
            high_level_architecture="A bounded backend and frontend delivery.",
            dod=[criterion],
            agents=[agent],
            initial_backlog=[
                InitialTask(
                    title="Build UI",
                    description="Create the UI",
                    owner_role=AgentRole.FRONTEND_DEVELOPER,
                    acceptance_criteria=["The result is verified"],
                    allowed_paths=["src"],
                    expected_outputs=["src/result.py"],
                    required_reviewers=["code_reviewer"],
                    dod_criteria=["verified"],
                )
            ],
            max_requested_agents=1,
            source_revision="abcdef1234567890",
            planning_context_hash="a" * 64,
            prompt_version="test-v1",
        )


def test_event_topics_are_project_scoped_and_identity_checked() -> None:
    project_id = str(uuid4())
    event = Event(
        project_id=project_id,
        event_type=EventType.TASK_CREATED,
        producer_agent_id="pm-1",
        payload={"task_id": str(uuid4())},
    )
    assert event.topic == f"project.{project_id}.tasks"
    assert validate_event(event, "pm-1") == (True, "")
    assert validate_event(event, "spoofed")[0] is False


def test_action_request_seals_exact_execution_fields() -> None:
    project_id = str(uuid4())
    with pytest.raises(ValidationError, match="exactly match target_paths"):
        ActionRequest(
            project_id=project_id,
            agent_id="backend-developer-1",
            task_id=str(uuid4()),
            action_type="write_file",
            description="mismatched path",
            target_paths=["src/safe.py"],
            payload={"file_path": "src/other.py", "content": "pass\n"},
        )
    with pytest.raises(ValidationError, match="exactly match command"):
        ActionRequest(
            project_id=project_id,
            agent_id="qa-engineer-1",
            task_id=str(uuid4()),
            action_type="shell_command",
            description="mismatched command",
            command=["pytest", "-q"],
            payload={"command": ["python", "-V"]},
        )
