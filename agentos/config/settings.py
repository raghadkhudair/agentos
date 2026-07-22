from __future__ import annotations

import os
from pathlib import Path
from typing import Self, cast
from urllib.parse import parse_qs, urlparse

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed process configuration.

    Secrets are intentionally represented as ``SecretStr`` and are never included in
    runtime snapshots. Production validation is explicit so tooling can still render an
    example configuration without inventing credentials.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
        case_sensitive=False,
    )

    project_name: str = Field(default="agentos-project", alias="AGENTOS_PROJECT_NAME")
    workspace: Path = Field(
        default_factory=lambda: Path.home() / ".agentos" / "workspaces",
        alias="AGENTOS_WORKSPACE",
    )
    source_repository: Path | None = Field(
        default=None,
        alias="AGENTOS_SOURCE_REPOSITORY",
        description="Optional local Git repository copied into the isolated delivery workspace.",
    )
    environment: str = Field(default="production", alias="AGENTOS_ENV")
    log_level: str = Field(default="INFO", alias="AGENTOS_LOG_LEVEL")

    database_url: SecretStr = Field(
        default=SecretStr("postgresql://agentos:agentos@localhost:5432/agentos"),
        alias="DATABASE_URL",
    )
    postgres_pool_min_size: int = Field(default=1, ge=1, alias="POSTGRES_POOL_MIN_SIZE")
    postgres_pool_max_size: int = Field(default=2, ge=2, alias="POSTGRES_POOL_MAX_SIZE")
    postgres_connection_budget: int = Field(default=100, ge=32, alias="POSTGRES_CONNECTION_BUDGET")
    postgres_command_timeout_seconds: float = Field(
        default=30.0, gt=0, le=300, alias="POSTGRES_COMMAND_TIMEOUT_SECONDS"
    )

    dragonfly_url: SecretStr = Field(
        default=SecretStr("redis://localhost:6380/0"), alias="DRAGONFLY_URL"
    )
    dragonfly_key_prefix: str = Field(default="agentos", alias="DRAGONFLY_KEY_PREFIX")

    mongodb_url: SecretStr = Field(
        default=SecretStr("mongodb://localhost:27017"), alias="MONGODB_URL"
    )
    mongodb_database: str = Field(default="agentos", alias="MONGODB_DATABASE")
    midterm_memory_ttl_seconds: int = Field(
        default=604_800, ge=3600, alias="AGENTOS_MIDTERM_MEMORY_TTL_SECONDS"
    )

    milvus_uri: str = Field(default="http://localhost:19530", alias="MILVUS_URI")
    milvus_token: SecretStr | None = Field(default=None, alias="MILVUS_TOKEN")
    milvus_database: str = Field(default="default", alias="MILVUS_DATABASE")
    milvus_collection_prefix: str = Field(default="agentos", alias="MILVUS_COLLECTION_PREFIX")
    embedding_dimension: int = Field(default=1536, ge=8, alias="AGENTOS_EMBEDDING_DIMENSION")

    minio_endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    minio_access_key: SecretStr = Field(default=SecretStr(""), alias="MINIO_ACCESS_KEY")
    minio_secret_key: SecretStr = Field(default=SecretStr(""), alias="MINIO_SECRET_KEY")
    minio_secure: bool = Field(default=False, alias="MINIO_SECURE")
    minio_region: str = Field(default="us-east-1", alias="MINIO_REGION")
    minio_artifacts_bucket: str = Field(default="agentos-artifacts", alias="MINIO_ARTIFACTS_BUCKET")
    minio_memory_bucket: str = Field(default="agentos-memory", alias="MINIO_MEMORY_BUCKET")

    ray_address: str | None = Field(default=None, alias="RAY_ADDRESS")
    cpu_usage_fraction: float = Field(
        default=0.70, gt=0.05, le=0.90, alias="AGENTOS_CPU_USAGE_FRACTION"
    )
    reserved_cpu_cores: int = Field(default=1, ge=1, alias="AGENTOS_RESERVED_CPU_CORES")
    max_cpu_cores: int | None = Field(default=None, ge=1, alias="AGENTOS_MAX_CPU_CORES")
    system_actor_cpu: float = Field(default=0.20, gt=0, le=1, alias="AGENTOS_SYSTEM_ACTOR_CPU")
    worker_cpu: float = Field(default=0.50, gt=0, le=4, alias="AGENTOS_WORKER_CPU")
    max_memory_bytes: int | None = Field(
        default=None, ge=536_870_912, alias="AGENTOS_MAX_MEMORY_BYTES"
    )
    memory_usage_fraction: float = Field(
        default=0.70, gt=0.10, le=0.90, alias="AGENTOS_MEMORY_USAGE_FRACTION"
    )
    reserved_memory_bytes: int = Field(
        default=1_073_741_824, ge=268_435_456, alias="AGENTOS_RESERVED_MEMORY_BYTES"
    )
    worker_memory_bytes: int = Field(
        default=536_870_912, ge=134_217_728, alias="AGENTOS_WORKER_MEMORY_BYTES"
    )
    object_store_memory_bytes: int = Field(
        default=268_435_456, ge=78_643_200, alias="AGENTOS_OBJECT_STORE_MEMORY_BYTES"
    )
    max_agents_total: int = Field(default=20, ge=5, le=100, alias="AGENTOS_MAX_AGENTS_TOTAL")
    max_active_agents: int = Field(default=8, ge=1, le=64, alias="AGENTOS_MAX_ACTIVE_AGENTS")
    max_parallel_code_tasks: int = Field(
        default=4, ge=1, le=32, alias="AGENTOS_MAX_PARALLEL_CODE_TASKS"
    )
    max_threads_per_agent: int = Field(default=2, ge=1, le=8, alias="AGENTOS_MAX_THREADS_PER_AGENT")
    collaboration_interval_seconds: int = Field(
        default=30, ge=5, le=600, alias="AGENTOS_COLLABORATION_INTERVAL_SECONDS"
    )

    provider_registry_path: Path = Field(
        default_factory=lambda: Path(__file__).with_name("providers.yaml"),
        alias="AGENTOS_PROVIDER_REGISTRY_PATH",
    )
    provider_default: str = Field(default="openai", alias="AGENTOS_PROVIDER_DEFAULT")
    provider_max_attempts: int = Field(default=3, ge=1, le=9, alias="AGENTOS_PROVIDER_MAX_ATTEMPTS")
    provider_timeout_seconds: float = Field(
        default=120.0, gt=1, le=600, alias="AGENTOS_PROVIDER_TIMEOUT_SECONDS"
    )
    provider_max_concurrency: int = Field(
        default=8, ge=1, le=64, alias="AGENTOS_PROVIDER_MAX_CONCURRENCY"
    )
    provider_call_reservation_usd: float = Field(
        default=1.0, gt=0, alias="AGENTOS_PROVIDER_CALL_RESERVATION_USD"
    )
    provider_unknown_cost_charge_usd: float = Field(
        default=1.0, gt=0, alias="AGENTOS_PROVIDER_UNKNOWN_COST_CHARGE_USD"
    )
    embedding_model: str = Field(
        default="openai/text-embedding-3-small", alias="AGENTOS_EMBEDDING_MODEL"
    )
    daily_budget_usd: float = Field(default=100.0, gt=0, alias="AGENTOS_DAILY_BUDGET_USD")
    monthly_budget_usd: float = Field(default=1000.0, gt=0, alias="AGENTOS_MONTHLY_BUDGET_USD")

    require_review: bool = Field(default=True, alias="AGENTOS_REQUIRE_REVIEW")
    require_tests: bool = Field(default=True, alias="AGENTOS_REQUIRE_TESTS")
    require_human_approval_for_critical: bool = Field(
        default=True, alias="AGENTOS_REQUIRE_HUMAN_APPROVAL_FOR_CRITICAL"
    )
    allow_destructive_actions: bool = Field(
        default=False, alias="AGENTOS_ALLOW_DESTRUCTIVE_ACTIONS"
    )
    dependency_health_fail_closed: bool = Field(
        default=True, alias="AGENTOS_DEPENDENCY_HEALTH_FAIL_CLOSED"
    )

    sandbox_image: str = Field(
        default="python:3.12.11-slim-bookworm", alias="AGENTOS_SANDBOX_IMAGE"
    )
    sandbox_workspace_volume: str | None = Field(
        default=None, alias="AGENTOS_SANDBOX_WORKSPACE_VOLUME"
    )
    sandbox_cpu_limit: float = Field(default=1.0, gt=0, le=4, alias="AGENTOS_SANDBOX_CPU_LIMIT")
    sandbox_memory_bytes: int = Field(
        default=1_073_741_824, ge=134_217_728, alias="AGENTOS_SANDBOX_MEMORY_BYTES"
    )
    sandbox_pids_limit: int = Field(default=256, ge=16, le=4096, alias="AGENTOS_SANDBOX_PIDS_LIMIT")
    sandbox_database_url: SecretStr | None = Field(default=None, alias="SANDBOX_DATABASE_URL")
    docker_host: str | None = Field(default=None, alias="DOCKER_HOST")

    @field_validator("workspace", "provider_registry_path", mode="before")
    @classmethod
    def _expand_paths(cls, value: object) -> Path:
        return Path(os.path.expandvars(os.path.expanduser(str(value)))).resolve()

    @field_validator("source_repository", mode="before")
    @classmethod
    def _expand_optional_path(cls, value: object) -> Path | None:
        if value is None or not str(value).strip():
            return None
        return Path(os.path.expandvars(os.path.expanduser(str(value)))).resolve()

    @field_validator("environment")
    @classmethod
    def _validate_environment(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"production", "development", "test"}:
            raise ValueError("AGENTOS_ENV must be production, development, or test")
        return normalized

    @field_validator("dragonfly_key_prefix", "milvus_collection_prefix")
    @classmethod
    def _safe_identifier(cls, value: str) -> str:
        normalized = value.strip().lower().replace("-", "_")
        if not normalized or not normalized.replace("_", "").isalnum():
            raise ValueError(
                "identifier must contain only letters, numbers, hyphens, or underscores"
            )
        return normalized

    @model_validator(mode="after")
    def _validate_limits(self) -> Self:
        if self.postgres_pool_min_size > self.postgres_pool_max_size:
            raise ValueError("POSTGRES_POOL_MIN_SIZE cannot exceed POSTGRES_POOL_MAX_SIZE")
        if self.max_active_agents > self.max_agents_total:
            raise ValueError("AGENTOS_MAX_ACTIVE_AGENTS cannot exceed AGENTOS_MAX_AGENTS_TOTAL")
        estimated_connections = (self.max_agents_total + 15) * self.postgres_pool_max_size
        if estimated_connections > self.postgres_connection_budget:
            raise ValueError(
                "configured actor pools exceed POSTGRES_CONNECTION_BUDGET; reduce "
                "POSTGRES_POOL_MAX_SIZE or AGENTOS_MAX_AGENTS_TOTAL"
            )
        if self.monthly_budget_usd < self.daily_budget_usd:
            raise ValueError("monthly provider budget cannot be lower than daily provider budget")
        if self.provider_call_reservation_usd > self.daily_budget_usd:
            raise ValueError("provider call reservation cannot exceed the daily provider budget")
        if self.provider_unknown_cost_charge_usd > self.provider_call_reservation_usd:
            raise ValueError("unknown provider cost charge cannot exceed the reserved call budget")
        return self

    @property
    def postgres_dsn(self) -> str:
        return cast(str, self.database_url.get_secret_value())

    @property
    def dragonfly_dsn(self) -> str:
        return cast(str, self.dragonfly_url.get_secret_value())

    @property
    def mongodb_dsn(self) -> str:
        return cast(str, self.mongodb_url.get_secret_value())

    def validate_production_secrets(self) -> None:
        """Fail closed before a production runtime starts."""

        if self.environment != "production":
            return
        errors: list[str] = []
        placeholder_markers = (
            "change_me",
            "changeme",
            "replace_me",
            "placeholder",
            "example",
            "dummy",
        )
        placeholder_values = {"agentos", "admin", "password", "secret", "root"}

        def unsafe_secret(value: str | None) -> bool:
            normalized = (value or "").strip().lower()
            return (
                not normalized
                or normalized in placeholder_values
                or any(marker in normalized for marker in placeholder_markers)
            )

        internal_hosts = {
            "localhost",
            "127.0.0.1",
            "::1",
            "postgres",
            "sandbox-postgres",
            "dragonfly",
            "mongodb",
            "minio",
            "milvus",
        }

        def is_external(parsed: object) -> bool:
            host = (getattr(parsed, "hostname", None) or "").lower()
            return bool(host) and host not in internal_hosts

        def query_flag(parsed: object, *names: str) -> str:
            query = parse_qs(getattr(parsed, "query", ""), keep_blank_values=True)
            for name in names:
                values = query.get(name) or query.get(name.lower())
                if values:
                    return str(values[-1]).strip().lower()
            return ""

        if unsafe_secret(self.minio_access_key.get_secret_value()):
            errors.append("MINIO_ACCESS_KEY")
        if unsafe_secret(self.minio_secret_key.get_secret_value()):
            errors.append("MINIO_SECRET_KEY")
        postgres = urlparse(self.postgres_dsn)
        if (
            postgres.scheme not in {"postgres", "postgresql"}
            or not postgres.hostname
            or unsafe_secret(postgres.password)
            or postgres.username is None
        ):
            errors.append("DATABASE_URL (authenticated non-placeholder credentials required)")
        if is_external(postgres) and query_flag(postgres, "sslmode") not in {
            "require",
            "verify-ca",
            "verify-full",
        }:
            errors.append("DATABASE_URL (TLS sslmode required for external PostgreSQL)")
        mongo = urlparse(self.mongodb_dsn)
        if (
            mongo.scheme not in {"mongodb", "mongodb+srv"}
            or not mongo.hostname
            or unsafe_secret(mongo.password)
            or mongo.username is None
        ):
            errors.append("MONGODB_URL (authenticated non-placeholder credentials required)")
        mongo_tls = query_flag(mongo, "tls", "ssl")
        if is_external(mongo) and mongo.scheme != "mongodb+srv" and mongo_tls != "true":
            errors.append("MONGODB_URL (TLS required for external MongoDB)")
        dragonfly = urlparse(self.dragonfly_dsn)
        if (
            dragonfly.scheme not in {"redis", "rediss"}
            or not dragonfly.hostname
            or unsafe_secret(dragonfly.password)
        ):
            errors.append("DRAGONFLY_URL (authenticated non-placeholder credentials required)")
        if is_external(dragonfly) and dragonfly.scheme != "rediss":
            errors.append("DRAGONFLY_URL (rediss TLS required for external DragonflyDB)")
        if self.sandbox_database_url is None:
            errors.append("SANDBOX_DATABASE_URL")
        else:
            sandbox = urlparse(self.sandbox_database_url.get_secret_value())
            if unsafe_secret(sandbox.password):
                errors.append("SANDBOX_DATABASE_URL (non-placeholder password required)")
        minio_host = self.minio_endpoint.rsplit(":", 1)[0].strip("[]").lower()
        if minio_host not in internal_hosts and not self.minio_secure:
            errors.append("MINIO_SECURE (TLS required for non-local endpoints)")
        milvus_host = (urlparse(self.milvus_uri).hostname or "").lower()
        if milvus_host not in internal_hosts and not (
            self.milvus_token and self.milvus_token.get_secret_value()
        ):
            errors.append("MILVUS_TOKEN (authentication required for non-local endpoints)")
        if not self.dependency_health_fail_closed:
            errors.append("AGENTOS_DEPENDENCY_HEALTH_FAIL_CLOSED must be true")
        if not self.require_review:
            errors.append("AGENTOS_REQUIRE_REVIEW must be true")
        if not self.require_tests:
            errors.append("AGENTOS_REQUIRE_TESTS must be true")
        if self.source_repository is not None and not (
            self.source_repository.is_dir() and (self.source_repository / ".git").exists()
        ):
            errors.append("AGENTOS_SOURCE_REPOSITORY (local Git repository required)")

        try:
            from agentos.provider.gateway import ProviderRegistry

            registry = ProviderRegistry(settings=self)
            if self.provider_default not in registry.profiles:
                errors.append("AGENTOS_PROVIDER_DEFAULT must name a registered provider")
            elif not registry.profiles[self.provider_default].available():
                errors.append(
                    "AGENTOS_PROVIDER_DEFAULT requires a non-placeholder credential or local base URL"
                )
            errors.extend(registry.production_configuration_errors())
        except (FileNotFoundError, OSError, ValueError) as error:
            errors.append(f"AGENTOS_PROVIDER_REGISTRY_PATH ({type(error).__name__})")
        if errors:
            raise ValueError("unsafe production configuration: " + ", ".join(errors))

    def safe_snapshot(self) -> dict[str, object]:
        snapshot = self.model_dump(
            mode="json",
            exclude={
                "database_url",
                "dragonfly_url",
                "mongodb_url",
                "milvus_token",
                "minio_access_key",
                "minio_secret_key",
                "sandbox_database_url",
            },
        )
        snapshot["database_url"] = "[REDACTED]"
        snapshot["dragonfly_url"] = "[REDACTED]"
        snapshot["mongodb_url"] = "[REDACTED]"
        return cast(dict[str, object], snapshot)


def load_settings() -> Settings:
    return Settings()
