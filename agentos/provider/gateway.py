from __future__ import annotations

import re
import uuid
import json
import ray
import litellm
import structlog
from dataclasses import dataclass
from typing import Any, Dict, List

from agentos.config.settings import Settings
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProviderCallRepository
from agentos.config.loader import guardrail_policies
from agentos.config.loader import runtime_tuning

logger = structlog.get_logger()

@dataclass(frozen=True)
class ProviderRequest:
    purpose: str
    messages: list[dict[str, str]]
    budget_key: str  # Maps directly to project_id UUID
    metadata: dict[str, Any] = None  # Optional additional metadata for logging or auditing

@dataclass(frozen=True)
class ProviderResponse:
    content: str
    model: str
    provider: str
    estimated_cost_usd: float | None = None


@ray.remote(namespace="agentos")
class ProviderGatewayActor:
    """Isolates external AI provider access, enforces zero-trust budgets, and validates output structures."""

    def __init__(self, settings_payload: dict):
        self.settings = Settings(**settings_payload) if settings_payload else Settings()
        self.db_manager = DatabaseManager(self.settings)
        self.call_repo = ProviderCallRepository(self.db_manager)
        
        tuning = runtime_tuning()
        model_cfg = tuning.get("models", {})
        
        self.default_model = model_cfg.get("primary", "gemini/gemini-2.5-pro")
        self.fallback_model = model_cfg.get("fallback", "gemini/gemini-2.5-flash")
        self.embedding_model = model_cfg.get("embedding", "gemini/text-embedding-004")
        self._connected = False

    async def _ensure_connected(self):
        """Ensures the database manager is initialized inside this Ray process."""
        if not self._connected:
            await self.db_manager.connect()
            self._connected = True

    def _sanitize_prompt_input(self, text: str) -> str:
        """Sanitizes user prompt inputs against configured safety patterns."""
        patterns = guardrail_policies()["prompt_sanitization_patterns"]
        sanitized = text
        for pattern in patterns:
            sanitized = re.sub(pattern, "[REDACTED_SECURITY_VIOLATION]", sanitized, flags=re.IGNORECASE)
        return sanitized

    async def _check_budget_allowance(self, project_id: str) -> bool:
        """Strictly enforces project budget caps before calling external models."""
        await self._ensure_connected()
        if not self.db_manager or not self.db_manager.pool:
            return False 
        
        try:
            query = "SELECT COALESCE(SUM(cost_usd), 0.0) FROM provider_calls WHERE project_id = $1"
            total_spent = await self.db_manager.pool.fetchval(query, uuid.UUID(project_id))
            
            max_budget = getattr(self.settings, "daily_budget_usd", 10.0)
            return float(total_spent) < float(max_budget)
        except Exception as e:
            logger.error("budget_lookup_failed_failing_closed", error=str(e))
            return False 

    def _validate_response_format(self, content: str, response_format: dict | None) -> bool:
        """Validates if the generated content conforms to expected structure formatting."""
        if not response_format:
            return True
            
        fmt_type = response_format.get("type")
        if fmt_type == "json_object":
            try:
                # Strip markdown code blocks if present before validating
                clean_txt = content.strip()
                if clean_txt.startswith("```"):
                    clean_txt = re.sub(r"^```json\s*|^```\s*", "", clean_txt, flags=re.MULTILINE)
                    clean_txt = re.sub(r"\s*```$", "", clean_txt, flags=re.MULTILINE).strip()
                json.loads(clean_txt)
                return True
            except (ValueError, TypeError):
                return False
        return True

    async def get_completion(self, request: ProviderRequest, response_format: dict | None = None, **kwargs) -> dict:
        """Gets AI completion, handles fallbacks, enforces budgets, and validates output structures."""
        await self._ensure_connected()

   
        if not await self._check_budget_allowance(request.budget_key):
            logger.error("budget_breach_detected", project_id=request.budget_key)
            raise RuntimeError("API Request blocked: Project budget cap has been exceeded.")

        
        sanitized_messages = []
        for msg in request.messages:
            sanitized_messages.append({
                "role": msg["role"],
                "content": self._sanitize_prompt_input(msg["content"])
            })

        used_model = self.default_model
        response_content = ""
        cost = 0.0
        
        
        try:
            response = await litellm.acompletion(
                model=self.default_model,
                messages=sanitized_messages,
                **kwargs
            )
            response_content = response.choices[0].message.content
            cost = litellm.completion_cost(completion_response=response) or 0.0
        except Exception as primary_error:
            logger.warning(
                "primary_model_failure_triggering_fallback", 
                model=self.default_model, 
                error=str(primary_error)
            )
            used_model = self.fallback_model
            try:
                response = await litellm.acompletion(
                    model=used_model,
                    messages=sanitized_messages,
                    **kwargs
                )
                response_content = response.choices[0].message.content
                cost = litellm.completion_cost(completion_response=response) or 0.0
            except Exception as fallback_error:
                logger.critical("all_available_provider_models_failed", error=str(fallback_error))
                raise fallback_error

        
        if not self._validate_response_format(response_content, response_format):
            logger.error("invalid_output_structure_detected", model=used_model)
            raise ValueError("Provider generated invalid output structure. JSON validation failed.")

        
        try:
            await self.call_repo.log_call(
                project_id=request.budget_key,
                purpose=request.purpose,
                provider="litellm",
                model=used_model,
                cost_usd=float(cost)
            )
        except Exception as log_error:
            logger.error("failed_to_log_call_metrics", error=str(log_error))

        return {
            "content": response_content,
            "model": used_model,
            "provider": "litellm",
            "estimated_cost_usd": float(cost)
        }
    
    async def get_embedding(self, text: str) -> list[float]:
        """Generates a semantic vector embedding using LiteLLM."""
        try:
            response = await litellm.aembedding(model=self.embedding_model, input=[text])
            return response['data'][0]['embedding']
        except Exception as e:
            logger.error("failed_to_fetch_embedding", error=str(e))
            return [0.0] * runtime_tuning()["embedding"]["dimension"]