from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agentos.config.settings import Settings
from agentos.provider.gateway import (
    BudgetExceededError,
    GenerationOptions,
    ProviderGateway,
    ProviderProfile,
    ProviderRegistry,
    ProviderRequest,
)


def _registry_data() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "providers": {
            "openai": {
                "litellm_prefix": "openai",
                "api_key_env": "OPENAI_API_KEY",
                "api_base_env": "OPENAI_API_BASE",
                "allowed_hosts": ["api.openai.com"],
                "models": {
                    "low": "openai/low",
                    "standard": "openai/standard",
                    "high": "openai/high",
                    "critical": "openai/critical",
                },
                "capabilities": ["chat", "json", "tools"],
            }
        },
        "routing": {
            "default_order": ["openai"],
            "role_preferences": {"backend_developer": ["openai"]},
            "purpose_complexity": {"decide_next_action": "standard"},
            "circuit_breaker": {"failure_threshold": 2, "recovery_seconds": 30},
        },
    }


def test_custom_registry_path_is_honored_and_schema_is_strict(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_path = tmp_path / "providers.yaml"
    registry_path.write_text(
        """schema_version: 1
providers:
  custom:
    litellm_prefix: custom
    api_key_env: CUSTOM_API_KEY
    api_base_env: CUSTOM_API_BASE
    allowed_hosts: [api.custom.example]
    models:
      low: custom/low
      standard: custom/standard
      high: custom/high
      critical: custom/critical
    capabilities: [chat]
routing:
  default_order: [custom]
  role_preferences: {}
  purpose_complexity: {}
  circuit_breaker:
    failure_threshold: 2
    recovery_seconds: 30
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("CUSTOM_API_KEY", "real-custom-key")
    registry = ProviderRegistry(
        settings=Settings(environment="test", provider_registry_path=registry_path)
    )
    assert set(registry.profiles) == {"custom"}

    registry_path.write_text(
        registry_path.read_text().replace("schema_version: 1", "schema_version: 2")
    )
    from agentos.config.loader import clear_config_cache

    clear_config_cache()
    with pytest.raises(ValidationError, match="schema_version"):
        ProviderRegistry(
            settings=Settings(environment="test", provider_registry_path=registry_path)
        )


def test_provider_availability_rejects_placeholder_values() -> None:
    profile = ProviderProfile(
        provider_id="openai",
        litellm_prefix="openai",
        api_key_env="OPENAI_API_KEY",
        api_base_env="OPENAI_API_BASE",
        allowed_hosts=["api.openai.com"],
        models={
            "low": "openai/low",
            "standard": "openai/standard",
            "high": "openai/high",
            "critical": "openai/critical",
        },
        capabilities={"chat"},
    )
    assert profile.available({"OPENAI_API_KEY": "CHANGE_ME"}) is False
    assert profile.available({"OPENAI_API_KEY": "test"}) is False
    assert profile.available({"OPENAI_API_KEY": "  "}) is False
    assert profile.available({"OPENAI_API_KEY": "real-provider-key"}) is True


def test_generation_options_forbid_egress_and_auth_overrides() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        GenerationOptions.model_validate({"api_base": "https://attacker.invalid"})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        GenerationOptions.model_validate({"api_key": "stolen"})
    with pytest.raises(ValidationError, match="budget_key"):
        ProviderRequest(
            purpose="decide_next_action",
            messages=[{"role": "user", "content": "work"}],
            budget_key="not-a-project-uuid",
        )


def test_production_validation_requires_tls_safe_gates_and_provider_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "CHANGE_ME_PROVIDER_KEY")
    monkeypatch.setenv("OPENAI_API_BASE", "http://api.openai.com")
    settings = Settings(
        environment="production",
        database_url="postgresql://user:strong-postgres@db.external.invalid/agentos",
        mongodb_url="mongodb://user:strong-mongo@mongo.external.invalid/agentos",
        dragonfly_url="redis://:strong-dragonfly@cache.external.invalid/0",
        minio_endpoint="minio.external.invalid:9000",
        minio_access_key="real-access-key",
        minio_secret_key="strong-minio-secret",
        minio_secure=True,
        milvus_uri="https://milvus.external.invalid:19530",
        milvus_token="strong-milvus-token",
        sandbox_database_url=(
            "postgresql://sandbox:strong-sandbox@sandbox.external.invalid/agentos_sandbox"
        ),
        dependency_health_fail_closed=False,
        require_review=False,
        require_tests=False,
    )
    with pytest.raises(ValueError) as captured:
        settings.validate_production_secrets()
    message = str(captured.value)
    assert "TLS sslmode required" in message
    assert "TLS required for external MongoDB" in message
    assert "rediss TLS required" in message
    assert "AGENTOS_DEPENDENCY_HEALTH_FAIL_CLOSED must be true" in message
    assert "AGENTOS_REQUIRE_REVIEW must be true" in message
    assert "AGENTOS_REQUIRE_TESTS must be true" in message
    assert "placeholder credential" in message
    assert "external provider base URLs must use HTTPS" in message


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, float | str] = {}

    async def eval(self, script: str, _key_count: int, *args: Any) -> int:
        daily_key, monthly_key = str(args[0]), str(args[1])
        daily = float(self.values.get(daily_key, 0))
        monthly = float(self.values.get(monthly_key, 0))
        if "local delta" not in script:
            reservation, daily_cap, monthly_cap = map(float, args[2:5])
            if daily + reservation > daily_cap:
                return -1
            if monthly + reservation > monthly_cap:
                return -2
            self.values[daily_key] = daily + reservation
            self.values[monthly_key] = monthly + reservation
            return 1
        reserved, charge, daily_cap, monthly_cap = map(float, args[2:6])
        delta = charge - reserved
        if delta > 0 and daily + delta > daily_cap:
            return -1
        if delta > 0 and monthly + delta > monthly_cap:
            return -2
        self.values[daily_key] = daily + delta
        self.values[monthly_key] = monthly + delta
        return 1

    async def exists(self, key: str) -> int:
        return int(key in self.values)

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.values.pop(key, None)

    async def incr(self, key: str) -> int:
        value = int(float(self.values.get(key, 0))) + 1
        self.values[key] = value
        return value

    async def set(self, key: str, value: str, **_: Any) -> None:
        self.values[key] = value


class _FakeDragonfly:
    def __init__(self) -> None:
        self.redis = _FakeRedis()

    @staticmethod
    def key(*parts: object) -> str:
        return ":".join(str(part) for part in parts)


class _FakeCallRepository:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.intents: list[dict[str, Any]] = []

    async def log_intent(self, **kwargs: Any) -> str:
        self.intents.append(kwargs)
        return str(uuid4())

    async def log_call(self, **kwargs: Any) -> str:
        self.calls.append(kwargs)
        return str(uuid4())


def _gateway(
    monkeypatch: pytest.MonkeyPatch,
    *,
    completion: Any = None,
    embedding: Any = None,
) -> tuple[ProviderGateway, _FakeDragonfly, _FakeCallRepository]:
    monkeypatch.setenv("OPENAI_API_KEY", "real-provider-key")
    settings = Settings(
        environment="test",
        embedding_model="openai/embed",
        embedding_dimension=8,
        daily_budget_usd=10,
        monthly_budget_usd=20,
        provider_call_reservation_usd=2,
        provider_unknown_cost_charge_usd=1,
        provider_max_attempts=1,
    )
    dragonfly = _FakeDragonfly()
    gateway = ProviderGateway(
        settings,
        db=object(),  # type: ignore[arg-type]
        dragonfly=dragonfly,  # type: ignore[arg-type]
        completion=completion,
        embedding=embedding,
        registry=ProviderRegistry(_registry_data(), settings=settings),
    )
    repository = _FakeCallRepository()
    gateway.call_repo = repository  # type: ignore[assignment]
    return gateway, dragonfly, repository


@pytest.mark.asyncio
async def test_unknown_completion_cost_is_charged_and_options_are_allowlisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    async def completion(**kwargs: Any) -> Any:
        observed.update(kwargs)
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
            usage=None,
        )

    gateway, dragonfly, repository = _gateway(monkeypatch, completion=completion)
    project_id = str(uuid4())
    response = await gateway.get_completion(
        ProviderRequest(
            purpose="decide_next_action",
            messages=[{"role": "user", "content": "work"}],
            budget_key=project_id,
        ),
        generation_options={"temperature": 0.2, "max_tokens": 100},
    )
    assert response["estimated_cost_usd"] == 1
    assert observed["temperature"] == 0.2
    assert observed["max_tokens"] == 100
    assert "api_base" not in observed and "api_key" not in observed
    assert repository.calls[-1].get("status", "COMPLETED") == "COMPLETED"
    usage_values = [
        float(value) for key, value in dragonfly.redis.values.items() if "budget" in key
    ]
    assert usage_values and all(value == 1 for value in usage_values)


@pytest.mark.asyncio
async def test_allowlisted_configured_api_base_is_forwarded_by_gateway_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    async def completion(**kwargs: Any) -> Any:
        observed.update(kwargs)
        return SimpleNamespace(
            choices=[
                SimpleNamespace(message=SimpleNamespace(content="ok")),
            ],
            usage=None,
        )

    monkeypatch.setenv("OPENAI_API_BASE", "https://api.openai.com/v1")
    gateway, _, _ = _gateway(monkeypatch, completion=completion)
    await gateway.get_completion(
        ProviderRequest(
            purpose="decide_next_action",
            messages=[{"role": "user", "content": "work"}],
            budget_key=uuid4(),
        )
    )

    assert observed["api_base"] == "https://api.openai.com/v1"
    assert "api_key" not in observed


@pytest.mark.asyncio
async def test_atomic_settlement_keeps_reservation_when_actual_cost_exceeds_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway, dragonfly, _ = _gateway(monkeypatch)
    project_id = str(uuid4())
    await gateway._reserve_budget(project_id, 2)
    with pytest.raises(BudgetExceededError):
        await gateway._settle_budget(project_id, 2, 12)
    usage_values = [
        float(value) for key, value in dragonfly.redis.values.items() if "budget" in key
    ]
    assert usage_values and all(value == 2 for value in usage_values)


@pytest.mark.asyncio
async def test_each_retry_has_an_independent_conservative_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def completion(**_: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("first provider failed")
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="recovered"))],
            usage=None,
        )

    async def no_sleep(_: float) -> None:
        return None

    gateway, dragonfly, repository = _gateway(monkeypatch, completion=completion)
    gateway.settings.provider_max_attempts = 2
    registry_data = _registry_data()
    registry_data["providers"]["backup"] = {
        "litellm_prefix": "backup",
        "api_key_env": "BACKUP_API_KEY",
        "api_base_env": "BACKUP_API_BASE",
        "allowed_hosts": ["api.backup.example"],
        "models": {
            "low": "backup/low",
            "standard": "backup/standard",
            "high": "backup/high",
            "critical": "backup/critical",
        },
        "capabilities": ["chat", "json", "tools"],
    }
    registry_data["routing"]["default_order"] = ["openai", "backup"]
    monkeypatch.setenv("BACKUP_API_KEY", "real-backup-key")
    monkeypatch.setattr("agentos.provider.gateway.asyncio.sleep", no_sleep)
    gateway.registry = ProviderRegistry(registry_data, settings=gateway.settings)

    result = await gateway.get_completion(
        ProviderRequest(
            purpose="decide_next_action",
            messages=[{"role": "user", "content": "work"}],
            budget_key=uuid4(),
        )
    )

    assert result["attempts"] == 2
    assert [call.get("status", "COMPLETED") for call in repository.calls] == [
        "FAILED",
        "COMPLETED",
    ]
    usage_values = [
        float(value) for key, value in dragonfly.redis.values.items() if "budget" in key
    ]
    assert usage_values and all(value == 2 for value in usage_values)


@pytest.mark.asyncio
async def test_embedding_failure_opens_failure_counter_audits_and_charges_unknown_cost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def embedding(**_: Any) -> Any:
        raise RuntimeError("provider offline")

    gateway, dragonfly, repository = _gateway(monkeypatch, embedding=embedding)
    project_id = str(uuid4())
    with pytest.raises(RuntimeError, match="provider offline"):
        await gateway.get_embedding("remember this", project_id)
    assert repository.calls[-1]["status"] == "FAILED"
    assert repository.calls[-1]["error_code"] == "RuntimeError"
    assert any("failures" in key for key in dragonfly.redis.values)
    usage_values = [
        float(value) for key, value in dragonfly.redis.values.items() if "budget" in key
    ]
    assert usage_values and all(value == 1 for value in usage_values)
