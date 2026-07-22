from __future__ import annotations

from uuid import uuid4

import pytest

from agentos.config.settings import Settings
from agentos.execution.supervisor import ExecutionService


@pytest.mark.asyncio
async def test_execution_creates_a_valid_isolated_git_worktree(tmp_path) -> None:
    settings = Settings(
        environment="test",
        workspace=tmp_path,
        minio_access_key="test-access",
        minio_secret_key="test-secret",
    )
    service = ExecutionService(settings)
    project_id = str(uuid4())
    task_id = str(uuid4())
    worktree = await service._ensure_worktree(project_id, task_id)
    assert worktree.is_dir()
    assert (worktree / ".git").is_file()
    code, branch, _ = await service._run_host("git", "branch", "--show-current", cwd=worktree)
    assert code == 0
    assert branch.strip() == f"agentos/task-{task_id}"


@pytest.mark.asyncio
async def test_execution_clones_configured_source_repository(tmp_path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    setup_settings = Settings(
        environment="test",
        workspace=tmp_path / "setup-workspaces",
        minio_access_key="test-access",
        minio_secret_key="test-secret",
    )
    setup_service = ExecutionService(setup_settings)
    code, _, error = await setup_service._run_host("git", "init", "-b", "main", cwd=source)
    assert code == 0, error
    (source / "existing.py").write_text("value = 1\n", encoding="utf-8")
    code, _, error = await setup_service._run_host("git", "add", "existing.py", cwd=source)
    assert code == 0, error
    code, _, error = await setup_service._run_host(
        "git",
        "-c",
        "user.name=Source",
        "-c",
        "user.email=source@example.invalid",
        "commit",
        "-m",
        "Initial source",
        cwd=source,
    )
    assert code == 0, error

    service = ExecutionService(
        Settings(
            environment="test",
            workspace=tmp_path / "managed",
            source_repository=source,
            minio_access_key="test-access",
            minio_secret_key="test-secret",
        )
    )
    repository = await service._ensure_repository(str(uuid4()))
    assert (repository / "existing.py").read_text(encoding="utf-8") == "value = 1\n"
    assert repository.resolve() != source.resolve()
