from __future__ import annotations
import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", populate_by_name=True)

    project_name: str = Field(default="agentos-project", alias="AGENTOS_PROJECT_NAME")
    
    workspace: str = Field(
        default_factory=lambda: os.path.abspath(os.path.join(os.path.expanduser("~"), ".agentos_sandbox")),
        alias="AGENTOS_WORKSPACE"
    )
    
    environment: str = Field(default="local", alias="AGENTOS_ENV")
    log_level: str = Field(default="INFO", alias="AGENTOS_LOG_LEVEL")

    ray_address: str | None = Field(default=None, alias="RAY_ADDRESS")
    
    database_url: str = Field(
        default="postgresql://agentos:agentos@localhost:5432/agentos", alias="DATABASE_URL"
    )
    dragonfly_url: str = Field(default="redis://dragonfly:6379/0", alias="DRAGONFLY_URL")

    provider_default_model: str = Field(
        default="gemini/gemini-2.5-pro", alias="AGENTOS_PROVIDER_DEFAULT_MODEL"
    )
    provider_fallback_model: str = Field(
        default="gemini/gemini-2.5-flash", alias="AGENTOS_PROVIDER_FALLBACK_MODEL"
    )

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