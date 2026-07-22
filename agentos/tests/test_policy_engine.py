from __future__ import annotations

from uuid import uuid4

import pytest

from agentos.config.settings import Settings
from agentos.governance.models import (
    ActionRequest,
    AgentIdentity,
    PolicyDecision,
)
from agentos.governance.policy_engine import PolicyEngine


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, int] = {}
        self.sets: dict[str, set[str]] = {}

    async def incr(self, key: str) -> int:
        self.values[key] = self.values.get(key, 0) + 1
        return self.values[key]

    async def expire(self, key: str, seconds: int) -> bool:
        return key in self.values and seconds > 0

    async def sadd(self, key: str, value: str) -> int:
        before = len(self.sets.setdefault(key, set()))
        self.sets[key].add(value)
        return len(self.sets[key]) - before

    async def srem(self, key: str, value: str) -> int:
        self.sets.setdefault(key, set()).discard(value)
        return 1

    async def sismember(self, key: str, value: str) -> bool:
        return value in self.sets.get(key, set())

    async def delete(self, *keys: str) -> int:
        for key in keys:
            self.values.pop(key, None)
            self.sets.pop(key, None)
        return len(keys)


class FakeDragonfly:
    def __init__(self) -> None:
        self.redis = FakeRedis()

    @staticmethod
    def key(*parts: object) -> str:
        return ":".join(str(part) for part in parts)


def identity(project_id: str, agent_id: str = "backend-1") -> AgentIdentity:
    return AgentIdentity(
        project_id=project_id,
        agent_id=agent_id,
        role="backend_developer",
        allowed_actions=["read_file", "write_file", "shell_command"],
        allowed_paths=["src"],
    )


@pytest.mark.asyncio
async def test_policy_denies_destructive_command() -> None:
    project_id = str(uuid4())
    engine = PolicyEngine(
        Settings(environment="test", allow_destructive_actions=False),
        FakeDragonfly(),
    )
    request = ActionRequest(
        project_id=project_id,
        agent_id="backend-1",
        action_type="shell_command",
        description="attempt cleanup",
        command=["rm", "-rf", "/"],
        payload={"command": ["rm", "-rf", "/"]},
    )
    result = await engine.evaluate_action(request, identity(project_id))
    assert result.decision == PolicyDecision.DENY
    assert "destructive operation denied" in result.reasons[0]


@pytest.mark.asyncio
async def test_policy_allows_read_and_constrains_write() -> None:
    project_id = str(uuid4())
    engine = PolicyEngine(Settings(environment="test"), FakeDragonfly())
    actor = identity(project_id)
    read = ActionRequest(
        project_id=project_id,
        agent_id=actor.agent_id,
        action_type="read_file",
        description="read source",
        target_paths=["src/main.py"],
        payload={"file_path": "src/main.py"},
    )
    write = ActionRequest(
        project_id=project_id,
        agent_id=actor.agent_id,
        action_type="write_file",
        description="write source",
        target_paths=["src/main.py"],
        payload={"file_path": "src/main.py", "content": "value = 1\n"},
    )
    assert (await engine.evaluate_action(read, actor)).decision == PolicyDecision.ALLOW
    assert (
        await engine.evaluate_action(write, actor)
    ).decision == PolicyDecision.ALLOW_WITH_CONSTRAINTS


@pytest.mark.asyncio
async def test_policy_rejects_tampering_cross_project_and_path_escape() -> None:
    project_id = str(uuid4())
    engine = PolicyEngine(Settings(environment="test"), FakeDragonfly())
    actor = identity(project_id)
    sealed = ActionRequest(
        project_id=project_id,
        agent_id=actor.agent_id,
        action_type="read_file",
        description="read source",
        target_paths=["src/main.py"],
        payload={"file_path": "src/main.py"},
    )
    tampered = sealed.model_copy(update={"description": "tampered"})
    assert (await engine.evaluate_action(tampered, actor)).decision == PolicyDecision.DENY

    cross_project = ActionRequest(
        project_id=str(uuid4()),
        agent_id=actor.agent_id,
        action_type="read_file",
        description="cross project",
        target_paths=["src/main.py"],
        payload={"file_path": "src/main.py"},
    )
    assert (await engine.evaluate_action(cross_project, actor)).decision == PolicyDecision.DENY

    outside = ActionRequest(
        project_id=project_id,
        agent_id=actor.agent_id,
        action_type="read_file",
        description="outside ownership",
        target_paths=["docs/readme.md"],
        payload={"file_path": "docs/readme.md"},
    )
    assert (await engine.evaluate_action(outside, actor)).decision == PolicyDecision.DENY
