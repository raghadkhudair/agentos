from __future__ import annotations

import os
import subprocess
import sys
import tomllib
import venv
from email.parser import Parser
from pathlib import Path
from zipfile import ZipFile

from packaging.requirements import Requirement

from agentos.storage.clients.postgres import PostgresClient

ROOT = Path(__file__).resolve().parents[2]


def _requirement_names(path: Path) -> set[str]:
    return {
        Requirement(line).name.lower().replace("_", "-")
        for raw_line in path.read_text(encoding="utf-8").splitlines()
        if (line := raw_line.strip()) and not line.startswith("#")
    }


def test_project_metadata_uses_requirements_files_as_single_source() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["project"]["dynamic"] == ["dependencies"]
    dynamic = project["tool"]["setuptools"]["dynamic"]
    assert dynamic["dependencies"]["file"] == ["requirements.txt"]
    assert project["project"]["optional-dependencies"]["test"]
    development_requirements = [
        line.strip()
        for line in (ROOT / "requirements-dev.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert development_requirements == [".[test]"]
    assert set(project["tool"]["setuptools"]["package-data"]["agentos.config"]) == {
        "*.yaml",
        "*.yml",
    }


def test_built_wheel_metadata_and_installed_runtime_assets(tmp_path: Path) -> None:
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-build-isolation",
            "--no-deps",
            "--wheel-dir",
            str(wheelhouse),
            str(ROOT),
        ],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(wheelhouse.glob("agentos_local-*.whl"))
    with ZipFile(wheel) as archive:
        metadata_name = next(
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        )
        metadata = Parser().parsestr(archive.read(metadata_name).decode("utf-8"))
        packaged_files = set(archive.namelist())

    wheel_requirements = {
        Requirement(value).name.lower().replace("_", "-")
        for value in metadata.get_all("Requires-Dist", [])
        if 'extra == "test"' not in value
    }
    assert wheel_requirements == _requirement_names(ROOT / "requirements.txt")
    assert "agentos/config/actor_team.yml" in packaged_files
    assert "agentos/config/providers.yaml" in packaged_files
    assert "agentos/config/guardrail_policies.yaml" in packaged_files
    assert "agentos/config/runtime_tuning.yaml" in packaged_files
    assert "agentos/storage/schema.sql" in packaged_files

    environment = tmp_path / "venv"
    venv.EnvBuilder(with_pip=True, system_site_packages=True).create(environment)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run(
        [str(python), "-m", "pip", "install", "--no-deps", "--force-reinstall", str(wheel)],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    )
    smoke = """
from pathlib import Path
import agentos
from agentos.cli.main import app
from agentos.config.loader import guardrail_policies, provider_registry, runtime_tuning, team_roles
from agentos.storage.clients.postgres import SCHEMA_PATH

assert Path(agentos.__file__).resolve().is_relative_to(Path(__import__('sys').prefix).resolve())
assert team_roles()['roles']
assert guardrail_policies()['destructive_patterns']
assert runtime_tuning()['execution']
assert provider_registry()['providers']
assert SCHEMA_PATH.is_file()
assert app.info.help == 'AgentOS production CLI'
"""
    clean_environment = os.environ.copy()
    clean_environment.pop("PYTHONPATH", None)
    subprocess.run(
        [str(python), "-I", "-c", smoke],
        cwd=tmp_path,
        env=clean_environment,
        check=True,
        capture_output=True,
        text=True,
    )


def test_postgres_pool_initializes_native_json_codecs() -> None:
    class Connection:
        def __init__(self) -> None:
            self.codecs: dict[str, dict[str, object]] = {}

        async def set_type_codec(self, type_name: str, **kwargs: object) -> None:
            self.codecs[type_name] = kwargs

    connection = Connection()
    __import__("asyncio").run(PostgresClient._initialize_connection(connection))
    assert set(connection.codecs) == {"json", "jsonb"}
    for codec in connection.codecs.values():
        assert codec["schema"] == "pg_catalog"
        assert codec["format"] == "text"
        assert codec["decoder"]('{"native":true}') == {"native": True}
        assert codec["encoder"]({"native": True}) == '{"native":true}'
        assert codec["encoder"]('{"native":true}') == '{"native":true}'
