from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from agentos.config.loader import provider_registry
from agentos.storage.clients.postgres import SCHEMA_PATH

ROOT = Path(__file__).resolve().parents[2]


def test_schema_uses_postgres_as_truth_without_pgvector() -> None:
    schema = (ROOT / "agentos" / "storage" / "schema.sql").read_text(encoding="utf-8")
    lowered = schema.lower()
    assert "create extension if not exists vector" not in lowered
    for table in (
        "projects",
        "tasks",
        "event_outbox",
        "memory_items",
        "dod_evidence",
        "resource_plans",
    ):
        assert f"create table if not exists {table}" in lowered
    assert "owner_role text" in lowered


def test_compose_contains_every_required_store_and_resource_ceiling() -> None:
    compose_path = ROOT / "docker-compose.yml"
    if not compose_path.is_file():
        pytest.skip("source deployment manifest is not part of the installed runtime wheel")
    compose = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    services = compose["services"]
    assert {"postgres", "dragonfly", "mongodb", "minio", "milvus"}.issubset(services)
    assert services["agentos"]["cpus"]
    assert services["agentos"]["mem_limit"]
    assert services["agentos"]["networks"] == ["agentos_internal", "agentos_egress"]
    assert compose["networks"]["agentos_internal"]["internal"] is True


def test_storage_clients_are_real_runtime_dependencies() -> None:
    runtime_files = "\n".join(
        (ROOT / path).read_text(encoding="utf-8")
        for path in (
            "agentos/runtime/supervisor.py",
            "agentos/memory/broker.py",
            "agentos/execution/supervisor.py",
            "agentos/messaging/dragonfly_bus.py",
        )
    )
    for client in (
        "PostgresClient",
        "DragonflyClient",
        "MongoDocumentClient",
        "MinioObjectClient",
        "MilvusVectorClient",
    ):
        assert client in runtime_files


def test_runtime_package_data_is_available() -> None:
    assert provider_registry()["providers"]
    assert SCHEMA_PATH.is_file()
