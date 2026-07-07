from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True)

    project_name: str = Field(default="agentos-project", alias="AGENTOS_PROJECT_NAME")
    workspace: str = Field(default="/workspace", alias="AGENTOS_WORKSPACE")
    environment: str = Field(default="local", alias="AGENTOS_ENV")
    log_level: str = Field(default="INFO", alias="AGENTOS_LOG_LEVEL")

    ray_address: str | None = Field(default=None, alias="RAY_ADDRESS")
    max_agents_total: int = Field(default=20, alias="AGENTOS_MAX_AGENTS_TOTAL")
    max_active_agents: int = Field(default=12, alias="AGENTOS_MAX_ACTIVE_AGENTS")
    max_parallel_code_tasks: int = Field(default=8, alias="AGENTOS_MAX_PARALLEL_CODE_TASKS")

    database_url: str = Field(
        default="postgresql://agentos:agentos@localhost:5432/agentos", alias="DATABASE_URL"
    )
    dragonfly_url: str = Field(default="redis://localhost:6379/0", alias="DRAGONFLY_URL")

    provider_default_model: str = Field(
        default="openai/gpt-4.1", alias="AGENTOS_PROVIDER_DEFAULT_MODEL"
    )
    provider_fallback_model: str = Field(
        default="openai/gpt-4.1-mini", alias="AGENTOS_PROVIDER_FALLBACK_MODEL"
    )
    embedding_model: str = Field(
        default="openai/text-embedding-3-small", alias="AGENTOS_EMBEDDING_MODEL"
    )
    embedding_dimension: int = Field(default=1536, alias="AGENTOS_EMBEDDING_DIMENSION")

    daily_budget_usd: float = Field(default=100.0, alias="AGENTOS_DAILY_BUDGET_USD")
    monthly_budget_usd: float = Field(default=1000.0, alias="AGENTOS_MONTHLY_BUDGET_USD")

    require_review: bool = Field(default=True, alias="AGENTOS_REQUIRE_REVIEW")
    require_tests: bool = Field(default=True, alias="AGENTOS_REQUIRE_TESTS")
    require_human_approval_for_critical: bool = Field(
        default=True, alias="AGENTOS_REQUIRE_HUMAN_APPROVAL_FOR_CRITICAL"
    )
    allow_destructive_actions: bool = Field(default=False, alias="AGENTOS_ALLOW_DESTRUCTIVE_ACTIONS")


def load_settings() -> Settings:
    return Settings()
