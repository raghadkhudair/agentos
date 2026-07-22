from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal, cast
from urllib.parse import urlparse
from uuid import UUID

import litellm
import ray
import structlog
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from agentos.config.loader import provider_registry
from agentos.config.runtime import TaskComplexity
from agentos.config.settings import Settings
from agentos.storage.clients.dragonfly import DragonflyClient
from agentos.storage.clients.postgres import PostgresClient
from agentos.storage.repositories import ProviderCallRepository

logger = structlog.get_logger()


class ProviderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    purpose: str = Field(min_length=2, max_length=200)
    messages: list[dict[str, Any]] = Field(min_length=1)
    budget_key: UUID
    agent_id: str | None = None
    agent_role: str | None = None
    complexity: TaskComplexity | None = None
    preferred_provider: str | None = None
    preferred_model: str | None = None
    required_capabilities: set[str] = Field(default_factory=lambda: {"chat"})
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _messages_are_chat_messages(self) -> ProviderRequest:
        for message in self.messages:
            if message.get("role") not in {"system", "developer", "user", "assistant", "tool"}:
                raise ValueError("unsupported chat message role")
            if not isinstance(message.get("content"), (str, list)):
                raise ValueError("message content must be text or a multimodal content list")
        return self


class GenerationOptions(BaseModel):
    """Allowlisted caller-controlled completion options.

    Authentication, base URLs, proxy settings, custom provider selection, and
    timeout controls intentionally have no representation here.
    """

    model_config = ConfigDict(extra="forbid")

    temperature: float | None = Field(default=None, ge=0, le=2)
    top_p: float | None = Field(default=None, gt=0, le=1)
    max_tokens: int | None = Field(default=None, ge=1, le=200_000)
    seed: int | None = None
    stop: str | list[str] | None = None
    presence_penalty: float | None = Field(default=None, ge=-2, le=2)
    frequency_penalty: float | None = Field(default=None, ge=-2, le=2)
    tools: list[dict[str, Any]] | None = None
    tool_choice: str | dict[str, Any] | None = None

    @field_validator("stop")
    @classmethod
    def _bounded_stop_sequences(cls, value: str | list[str] | None) -> str | list[str] | None:
        values = [value] if isinstance(value, str) else value
        if values is not None:
            if len(values) > 8 or any(not item or len(item) > 500 for item in values):
                raise ValueError("stop sequences must contain 1-8 bounded nonempty strings")
        return value


class ProviderResponse(BaseModel):
    content: str
    model: str
    provider: str
    estimated_cost_usd: float = 0
    token_usage: dict[str, Any] = Field(default_factory=dict)
    latency_ms: int = 0
    attempts: int = 1


class ProviderProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_id: str
    litellm_prefix: str
    api_key_env: str | None = None
    api_base_env: str | None = None
    allowed_hosts: list[str] = Field(default_factory=list)
    local: bool = False
    models: dict[TaskComplexity, str]
    capabilities: set[str] = Field(default_factory=set)

    _PLACEHOLDER_MARKERS: ClassVar[tuple[str, ...]] = (
        "change_me",
        "changeme",
        "replace_me",
        "placeholder",
        "example",
        "dummy",
    )
    _PLACEHOLDER_VALUES: ClassVar[frozenset[str]] = frozenset(
        {"api_key", "key", "none", "null", "password", "secret", "test", "testing", "token"}
    )

    @staticmethod
    def _configured_value(value: str | None) -> bool:
        normalized = (value or "").strip().lower()
        return (
            bool(normalized)
            and normalized not in ProviderProfile._PLACEHOLDER_VALUES
            and not any(marker in normalized for marker in ProviderProfile._PLACEHOLDER_MARKERS)
        )

    def available(self, environment: Mapping[str, str] | None = None) -> bool:
        selected_environment = os.environ if environment is None else environment
        if self.local:
            return self._configured_value(
                selected_environment.get(self.api_base_env or "OLLAMA_API_BASE")
            )
        return bool(
            self.api_key_env and self._configured_value(selected_environment.get(self.api_key_env))
        )

    def validate_base_url(
        self,
        *,
        production: bool,
        environment: Mapping[str, str] | None = None,
    ) -> str | None:
        selected_environment = os.environ if environment is None else environment
        configured = selected_environment.get(self.api_base_env) if self.api_base_env else None
        if not configured or not configured.strip():
            return None
        parsed = urlparse(configured.strip())
        host = (parsed.hostname or "").lower()
        if parsed.scheme not in {"http", "https"} or not host:
            raise ValueError(f"provider {self.provider_id} base URL must be an absolute HTTP URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError(
                f"provider {self.provider_id} base URL cannot contain credentials, query, or fragment"
            )
        if host not in {item.lower() for item in self.allowed_hosts}:
            raise ValueError(f"provider base URL host {host!r} is outside the egress allowlist")
        if production and not self.local and parsed.scheme != "https":
            raise ValueError("external provider base URLs must use HTTPS in production")
        return configured.strip()


class ProviderProfileConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    litellm_prefix: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    api_key_env: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]*$")
    api_base_env: str | None = Field(default=None, pattern=r"^[A-Z][A-Z0-9_]*$")
    allowed_hosts: list[str] = Field(min_length=1)
    local: bool = False
    models: dict[TaskComplexity, str]
    capabilities: set[str] = Field(min_length=1)

    @model_validator(mode="after")
    def _complete_profile(self) -> ProviderProfileConfig:
        if set(self.models) != set(TaskComplexity):
            raise ValueError("provider models must define low, standard, high, and critical")
        if not self.local and not self.api_key_env:
            raise ValueError("remote providers require api_key_env")
        if self.local and not self.api_base_env:
            raise ValueError("local providers require api_base_env")
        if "chat" not in self.capabilities:
            raise ValueError("provider capabilities must include chat")
        prefix = f"{self.litellm_prefix}/"
        if any(not model.startswith(prefix) for model in self.models.values()):
            raise ValueError(f"all models must use the {self.litellm_prefix!r} prefix")
        normalized_hosts = [host.strip().lower() for host in self.allowed_hosts]
        if any(not host or "://" in host or "/" in host for host in normalized_hosts):
            raise ValueError("allowed_hosts must contain hostnames only")
        if len(normalized_hosts) != len(set(normalized_hosts)):
            raise ValueError("allowed_hosts must be unique")
        self.allowed_hosts = normalized_hosts
        return self


class CircuitBreakerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_threshold: int = Field(ge=1, le=100)
    recovery_seconds: int = Field(ge=1, le=86_400)


class ProviderRoutingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    default_order: list[str] = Field(min_length=1)
    role_preferences: dict[str, list[str]] = Field(default_factory=dict)
    purpose_complexity: dict[str, TaskComplexity] = Field(default_factory=dict)
    circuit_breaker: CircuitBreakerConfig


class ProviderRegistryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    providers: dict[str, ProviderProfileConfig] = Field(min_length=1)
    routing: ProviderRoutingConfig

    @model_validator(mode="after")
    def _routing_references_registered_providers(self) -> ProviderRegistryConfig:
        registered = set(self.providers)
        if any(not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", item) for item in registered):
            raise ValueError("provider IDs must be safe lowercase identifiers")
        if set(self.routing.default_order) != registered:
            raise ValueError("routing.default_order must contain every provider exactly once")
        if len(self.routing.default_order) != len(set(self.routing.default_order)):
            raise ValueError("routing.default_order cannot contain duplicates")
        for role, preferences in self.routing.role_preferences.items():
            unknown = set(preferences) - registered
            if unknown:
                raise ValueError(f"role {role!r} references unknown providers: {sorted(unknown)}")
        return self


class ProviderRegistry:
    def __init__(
        self,
        raw: dict[str, Any] | None = None,
        *,
        settings: Settings | None = None,
    ):
        self.settings = settings or Settings()
        source = provider_registry(self.settings.provider_registry_path) if raw is None else raw
        config = ProviderRegistryConfig.model_validate(source)
        self.routing = config.routing.model_dump(mode="json")
        self.profiles: dict[str, ProviderProfile] = {}
        for provider_id, data in config.providers.items():
            self.profiles[provider_id] = ProviderProfile(
                provider_id=provider_id,
                **data.model_dump(),
            )

    def production_configuration_errors(
        self, environment: Mapping[str, str] | None = None
    ) -> list[str]:
        selected_environment = os.environ if environment is None else environment
        errors: list[str] = []
        for profile in self.profiles.values():
            credential = (
                selected_environment.get(profile.api_key_env) if profile.api_key_env else None
            )
            if credential and not profile._configured_value(credential):
                errors.append(f"{profile.api_key_env} contains a placeholder credential")
            try:
                profile.validate_base_url(
                    production=True,
                    environment=selected_environment,
                )
            except ValueError as error:
                errors.append(str(error))
        return errors

    def complexity_for(self, request: ProviderRequest) -> TaskComplexity:
        if request.complexity:
            return request.complexity
        configured = self.routing.get("purpose_complexity", {}).get(request.purpose)
        return TaskComplexity(configured or TaskComplexity.STANDARD)

    def candidates(
        self,
        request: ProviderRequest,
        *,
        environment: Mapping[str, str] | None = None,
    ) -> list[tuple[ProviderProfile, str]]:
        complexity = self.complexity_for(request)
        role_order = self.routing.get("role_preferences", {}).get(request.agent_role or "", [])
        default_order = self.routing.get("default_order", list(self.profiles))
        requested = [request.preferred_provider] if request.preferred_provider else []
        order = list(dict.fromkeys([*requested, *role_order, *default_order]))
        result: list[tuple[ProviderProfile, str]] = []
        for provider_id in order:
            profile = self.profiles.get(provider_id)
            if profile is None or not profile.available(environment):
                continue
            if not request.required_capabilities.issubset(profile.capabilities):
                continue
            model = (
                request.preferred_model
                if request.preferred_provider == provider_id and request.preferred_model
                else profile.models[complexity]
            )
            if not model.startswith(f"{profile.litellm_prefix}/"):
                raise ValueError(
                    f"model {model!r} does not use provider prefix {profile.litellm_prefix!r}"
                )
            result.append((profile, model))
        return result


class ProviderUnavailableError(RuntimeError):
    pass


class BudgetExceededError(RuntimeError):
    pass


CompletionCallable = Callable[..., Awaitable[Any]]


class ProviderGateway:
    """Provider-neutral gateway with routing, budgets, redaction, and failover."""

    _CREDENTIAL_PATTERNS = (
        re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
        re.compile(r"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
        re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~-]{16,}"),
        re.compile(r"(?i)\b(password|api[_-]?key|secret|token)\s*[:=]\s*['\"]?[^\s'\"]{8,}"),
        re.compile(
            r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----[\s\S]*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
        ),
    )

    _RESERVE_BUDGET = """
    local daily = tonumber(redis.call('get', KEYS[1]) or '0')
    local monthly = tonumber(redis.call('get', KEYS[2]) or '0')
    local reservation = tonumber(ARGV[1])
    if daily + reservation > tonumber(ARGV[2]) then return -1 end
    if monthly + reservation > tonumber(ARGV[3]) then return -2 end
    redis.call('incrbyfloat', KEYS[1], reservation)
    redis.call('expire', KEYS[1], 172800)
    redis.call('incrbyfloat', KEYS[2], reservation)
    redis.call('expire', KEYS[2], 2764800)
    return 1
    """

    _SETTLE_BUDGET = """
    local daily = tonumber(redis.call('get', KEYS[1]) or '0')
    local monthly = tonumber(redis.call('get', KEYS[2]) or '0')
    local reserved = tonumber(ARGV[1])
    local charge = tonumber(ARGV[2])
    local delta = charge - reserved
    if charge < 0 then return -3 end
    if delta > 0 and daily + delta > tonumber(ARGV[3]) then return -1 end
    if delta > 0 and monthly + delta > tonumber(ARGV[4]) then return -2 end
    redis.call('incrbyfloat', KEYS[1], delta)
    redis.call('expire', KEYS[1], 172800)
    redis.call('incrbyfloat', KEYS[2], delta)
    redis.call('expire', KEYS[2], 2764800)
    return 1
    """

    def __init__(
        self,
        settings: Settings,
        *,
        db: PostgresClient | None = None,
        dragonfly: DragonflyClient | None = None,
        completion: CompletionCallable | None = None,
        embedding: CompletionCallable | None = None,
        registry: ProviderRegistry | None = None,
    ):
        self.settings = settings
        self.db = db or PostgresClient(settings)
        self.dragonfly = dragonfly or DragonflyClient(settings)
        self.call_repo = ProviderCallRepository(self.db)
        self.completion = completion or litellm.acompletion
        self.embedding = embedding or litellm.aembedding
        self.registry = registry or ProviderRegistry(settings=settings)
        self._semaphore = asyncio.Semaphore(settings.provider_max_concurrency)

    @staticmethod
    def _extract_content(response: Any) -> str:
        try:
            content = response.choices[0].message.content
        except (AttributeError, IndexError, KeyError, TypeError) as error:
            raise ValueError("provider response did not contain completion content") from error
        if not isinstance(content, str) or not content.strip():
            raise ValueError("provider returned empty completion content")
        return content

    def _sanitize_messages(
        self, messages: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        sanitized: list[dict[str, Any]] = []
        redactions = 0
        for message in messages:
            copied = dict(message)
            content = copied.get("content")
            if isinstance(content, str):
                for pattern in self._CREDENTIAL_PATTERNS:
                    content, count = pattern.subn("[REDACTED_CREDENTIAL]", content)
                    redactions += count
                copied["content"] = content
            sanitized.append(copied)
        return sanitized, redactions

    @staticmethod
    def _validate_response(content: str, response_format: dict[str, Any] | None) -> None:
        if not response_format or response_format.get("type") != "json_object":
            return
        clean = content.strip()
        if clean.startswith("```"):
            clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", clean, flags=re.IGNORECASE)
        parsed = json.loads(clean)
        if not isinstance(parsed, dict):
            raise ValueError("provider response must be a JSON object")

    def _validate_egress(self, profile: ProviderProfile) -> str | None:
        return profile.validate_base_url(production=self.settings.environment == "production")

    @staticmethod
    def _project_id(value: UUID | str) -> str:
        return str(UUID(str(value)))

    async def _reserve_budget(self, project_id: str, amount: float) -> None:
        project_id = self._project_id(project_id)
        now = datetime.now(UTC)
        daily_key = self.dragonfly.key("budget", project_id, "day", now.strftime("%Y-%m-%d"))
        monthly_key = self.dragonfly.key("budget", project_id, "month", now.strftime("%Y-%m"))
        result = await self.dragonfly.redis.eval(
            self._RESERVE_BUDGET,
            2,
            daily_key,
            monthly_key,
            amount,
            self.settings.daily_budget_usd,
            self.settings.monthly_budget_usd,
        )
        if int(result) < 0:
            raise BudgetExceededError("provider budget cap would be exceeded")

    async def _settle_budget(self, project_id: str, reserved: float, actual: float) -> None:
        project_id = self._project_id(project_id)
        now = datetime.now(UTC)
        daily_key = self.dragonfly.key("budget", project_id, "day", now.strftime("%Y-%m-%d"))
        monthly_key = self.dragonfly.key("budget", project_id, "month", now.strftime("%Y-%m"))
        result = await self.dragonfly.redis.eval(
            self._SETTLE_BUDGET,
            2,
            daily_key,
            monthly_key,
            reserved,
            actual,
            self.settings.daily_budget_usd,
            self.settings.monthly_budget_usd,
        )
        if int(result) < 0:
            raise BudgetExceededError(
                "actual provider cost exceeds the remaining budget; reserved charge retained"
            )

    def _charge(self, response: Any) -> float:
        try:
            cost = float(litellm.completion_cost(completion_response=response) or 0)
        except Exception:
            cost = 0.0
        return cost if cost > 0 else self.settings.provider_unknown_cost_charge_usd

    async def _circuit_is_open(self, provider: str) -> bool:
        return bool(
            await self.dragonfly.redis.exists(self.dragonfly.key("provider", provider, "open"))
        )

    async def _record_provider_result(self, provider: str, success: bool) -> None:
        failures_key = self.dragonfly.key("provider", provider, "failures")
        if success:
            await self.dragonfly.redis.delete(
                failures_key, self.dragonfly.key("provider", provider, "open")
            )
            return
        failures = await self.dragonfly.redis.incr(failures_key)
        threshold = int(
            self.registry.routing.get("circuit_breaker", {}).get("failure_threshold", 3)
        )
        if failures >= threshold:
            recovery = int(
                self.registry.routing.get("circuit_breaker", {}).get("recovery_seconds", 120)
            )
            await self.dragonfly.redis.set(
                self.dragonfly.key("provider", provider, "open"), "1", ex=recovery
            )

    async def get_completion(
        self,
        request: ProviderRequest,
        response_format: dict[str, Any] | None = None,
        generation_options: GenerationOptions | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        project_id = self._project_id(request.budget_key)
        options = (
            generation_options
            if isinstance(generation_options, GenerationOptions)
            else GenerationOptions.model_validate(generation_options or {})
        )
        if options.tools and "tools" not in request.required_capabilities:
            raise ValueError("tool definitions require the tools provider capability")
        completion_kwargs = options.model_dump(exclude_none=True)
        candidates = self.registry.candidates(request)
        if not candidates:
            raise ProviderUnavailableError(
                "no configured AI provider satisfies this request; add a real provider credential or configure Ollama"
            )
        sanitized_messages, redactions = self._sanitize_messages(request.messages)
        prompt_material = json.dumps(sanitized_messages, sort_keys=True, default=str)
        prompt_hash = hashlib.sha256(prompt_material.encode("utf-8")).hexdigest()
        last_error: Exception | None = None
        attempted = 0
        reservation = self.settings.provider_call_reservation_usd

        async with self._semaphore:
            for profile, model in candidates:
                if attempted >= self.settings.provider_max_attempts:
                    break
                if await self._circuit_is_open(profile.provider_id):
                    continue
                api_base = self._validate_egress(profile)
                await self._reserve_budget(project_id, reservation)
                attempted += 1
                started = time.monotonic()
                intent_id = await self.call_repo.log_intent(
                    project_id=project_id,
                    agent_id=request.agent_id,
                    purpose=request.purpose,
                    provider=profile.provider_id,
                    model=model,
                    prompt_hash=prompt_hash,
                    redaction_status=f"APPLIED:{redactions}",
                )
                try:
                    provider_kwargs = dict(completion_kwargs)
                    if api_base:
                        provider_kwargs["api_base"] = api_base
                    response = await asyncio.wait_for(
                        self.completion(
                            model=model,
                            messages=sanitized_messages,
                            response_format=response_format,
                            **provider_kwargs,
                        ),
                        timeout=self.settings.provider_timeout_seconds,
                    )
                    content = self._extract_content(response)
                    self._validate_response(content, response_format)
                    cost = self._charge(response)
                    usage = getattr(response, "usage", None)
                    if usage is None:
                        token_usage = {}
                    elif hasattr(usage, "model_dump"):
                        token_usage = usage.model_dump()
                    else:
                        token_usage = dict(usage)
                    latency_ms = int((time.monotonic() - started) * 1000)
                    response_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
                    await self.call_repo.log_call(
                        project_id=project_id,
                        agent_id=request.agent_id,
                        purpose=request.purpose,
                        provider=profile.provider_id,
                        model=model,
                        cost_usd=cost,
                        prompt_hash=prompt_hash,
                        response_hash=response_hash,
                        redaction_status=f"APPLIED:{redactions}",
                        token_usage=token_usage,
                        latency_ms=latency_ms,
                        intent_id=intent_id,
                    )
                    await self._record_provider_result(profile.provider_id, True)
                    await self._settle_budget(project_id, reservation, cost)
                    return cast(
                        dict[str, Any],
                        ProviderResponse(
                            content=content,
                            model=model,
                            provider=profile.provider_id,
                            estimated_cost_usd=cost,
                            token_usage=token_usage,
                            latency_ms=latency_ms,
                            attempts=attempted,
                        ).model_dump(),
                    )
                except BudgetExceededError:
                    raise
                except Exception as error:
                    last_error = error
                    unknown_charge = self.settings.provider_unknown_cost_charge_usd
                    await self._settle_budget(project_id, reservation, unknown_charge)
                    await self._record_provider_result(profile.provider_id, False)
                    await self.call_repo.log_call(
                        project_id=project_id,
                        agent_id=request.agent_id,
                        purpose=request.purpose,
                        provider=profile.provider_id,
                        model=model,
                        cost_usd=unknown_charge,
                        prompt_hash=prompt_hash,
                        redaction_status=f"APPLIED:{redactions}",
                        latency_ms=int((time.monotonic() - started) * 1000),
                        status="FAILED",
                        error_code=type(error).__name__,
                        intent_id=intent_id,
                    )
                    if attempted < self.settings.provider_max_attempts:
                        jitter = secrets.randbelow(1000) / 1000
                        await asyncio.sleep(min(4.0, (2 ** (attempted - 1)) + jitter))
        raise ProviderUnavailableError(
            f"all eligible provider attempts failed; last error type: {type(last_error).__name__ if last_error else 'none'}"
        ) from last_error

    async def get_embedding(self, text: str, project_id: str) -> list[float]:
        project_id = self._project_id(project_id)
        prefix = self.settings.embedding_model.split("/", 1)[0]
        profile = next(
            (item for item in self.registry.profiles.values() if item.litellm_prefix == prefix),
            None,
        )
        if profile is None or not profile.available():
            raise ProviderUnavailableError(f"embedding provider {prefix!r} is not configured")
        api_base = self._validate_egress(profile)
        if await self._circuit_is_open(profile.provider_id):
            raise ProviderUnavailableError(f"embedding provider {prefix!r} circuit is open")
        sanitized, redactions = self._sanitize_messages([{"role": "user", "content": text}])
        safe_text = str(sanitized[0]["content"])
        prompt_hash = hashlib.sha256(safe_text.encode("utf-8")).hexdigest()
        reservation = self.settings.provider_call_reservation_usd
        await self._reserve_budget(project_id, reservation)
        started = time.monotonic()
        intent_id = await self.call_repo.log_intent(
            project_id=project_id,
            purpose="semantic_memory_embedding",
            provider=profile.provider_id,
            model=self.settings.embedding_model,
            prompt_hash=prompt_hash,
            redaction_status=f"APPLIED:{redactions}",
        )
        budget_finalized = False
        try:
            async with self._semaphore:
                embedding_kwargs: dict[str, Any] = {
                    "model": self.settings.embedding_model,
                    "input": [safe_text],
                }
                if api_base:
                    embedding_kwargs["api_base"] = api_base
                response = await asyncio.wait_for(
                    self.embedding(**embedding_kwargs),
                    timeout=self.settings.provider_timeout_seconds,
                )
            vector = list(
                response.data[0]["embedding"]
                if hasattr(response, "data")
                else response["data"][0]["embedding"]
            )
            if len(vector) != self.settings.embedding_dimension:
                raise ValueError(
                    f"embedding model returned {len(vector)} values, expected {self.settings.embedding_dimension}"
                )
            cost = self._charge(response)
            await self.call_repo.log_call(
                project_id=project_id,
                purpose="semantic_memory_embedding",
                provider=self.settings.embedding_model.split("/", 1)[0],
                model=self.settings.embedding_model,
                cost_usd=cost,
                prompt_hash=prompt_hash,
                response_hash=hashlib.sha256(
                    json.dumps(vector, separators=(",", ":")).encode("utf-8")
                ).hexdigest(),
                redaction_status=f"APPLIED:{redactions}",
                latency_ms=int((time.monotonic() - started) * 1000),
                intent_id=intent_id,
            )
            await self._record_provider_result(profile.provider_id, True)
            await self._settle_budget(project_id, reservation, cost)
            budget_finalized = True
            return [float(value) for value in vector]
        except BudgetExceededError:
            budget_finalized = True
            raise
        except Exception as error:
            await self._record_provider_result(profile.provider_id, False)
            await self.call_repo.log_call(
                project_id=project_id,
                purpose="semantic_memory_embedding",
                provider=profile.provider_id,
                model=self.settings.embedding_model,
                cost_usd=self.settings.provider_unknown_cost_charge_usd,
                prompt_hash=prompt_hash,
                redaction_status=f"APPLIED:{redactions}",
                latency_ms=int((time.monotonic() - started) * 1000),
                status="FAILED",
                error_code=type(error).__name__,
                intent_id=intent_id,
            )
            if not budget_finalized:
                await self._settle_budget(
                    project_id,
                    reservation,
                    self.settings.provider_unknown_cost_charge_usd,
                )
            raise


@ray.remote(num_cpus=0.2, max_concurrency=32)  # type: ignore[call-overload]
class ProviderGatewayActor:
    def __init__(self, settings_payload: dict[str, Any]):
        settings = Settings(**settings_payload)
        self.gateway = ProviderGateway(settings)

    async def get_completion(
        self,
        request: ProviderRequest | dict[str, Any],
        response_format: dict[str, Any] | None = None,
        generation_options: GenerationOptions | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        parsed = (
            request
            if isinstance(request, ProviderRequest)
            else ProviderRequest.model_validate(request)
        )
        return await self.gateway.get_completion(
            parsed,
            response_format=response_format,
            generation_options=generation_options,
        )

    async def get_embedding(self, text: str, project_id: str) -> list[float]:
        return await self.gateway.get_embedding(text, project_id)


__all__ = [
    "BudgetExceededError",
    "GenerationOptions",
    "ProviderGateway",
    "ProviderGatewayActor",
    "ProviderProfile",
    "ProviderRegistry",
    "ProviderRequest",
    "ProviderResponse",
    "ProviderUnavailableError",
]
