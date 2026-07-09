from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List
import uuid
import re

import litellm
from agentos.config.settings import Settings
from agentos.storage.database import DatabaseManager
from agentos.storage.repositories import ProviderCallRepository

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

class ProviderGateway:
    def __init__(self, settings: Settings, db_manager: DatabaseManager | None = None):
        self.settings = settings
        self.default_model = self.settings.provider_default_model
        self.fallback_model = self.settings.provider_fallback_model
        self.db_manager = db_manager
        self.call_repo = ProviderCallRepository(db_manager) if db_manager else None

    def _sanitize_prompt_input(self, text: str) -> str:
        """
        Scans and sanitizes inputs to block malicious structural instructions, 
        system policy modifications, or environment file access attempts.
        """
        # Block attempts to target core configuration or environment profiles
        malicious_patterns = [
            r"\.env", 
            r"ignore\s+previous\s+instructions", 
            r"disable\s+guardrails",
            r"system_policies"
        ]
        sanitized = text
        for pattern in malicious_patterns:
            sanitized = re.sub(pattern, "[REDACTED_SECURITY_VIOLATION]", sanitized, flags=re.IGNORECASE)
        return sanitized

    async def _check_budget_allowance(self, project_id: str) -> bool:
        """Enforces daily/monthly guardrail caps before spending API credits."""
        if not self.db_manager or not self.db_manager.pool:
            return True
        
        try:
            query = "SELECT COALESCE(SUM(cost_usd), 0.0) FROM provider_calls WHERE project_id = $1"
            total_spent = await self.db_manager.pool.fetchval(query, uuid.UUID(project_id))
            
            max_budget = getattr(self.settings, "daily_budget_usd", 10.0)
            return float(total_spent) < float(max_budget)
        except Exception:
            return True  # Fail open to prevent blocking if metadata querying breaks

    async def get_completion(self, request: ProviderRequest, **kwargs) -> ProviderResponse:
        """
        Enforces budget metrics, sanitizes inputs, and routes requests via LiteLLM.
        """
        if not await self._check_budget_allowance(request.budget_key):
            raise RuntimeError("API Request blocked: Project budget cap has been exceeded.")

        # Sanitize incoming prompt streams for prompt-injection safety
        sanitized_messages = []
        for msg in request.messages:
            sanitized_messages.append({
                "role": msg["role"],
                "content": self._sanitize_prompt_input(msg["content"])
            })

        used_model = self.default_model
        
        try:
            response = await litellm.acompletion(
                model=self.default_model,
                messages=sanitized_messages,
                **kwargs
            )
        except Exception as e:
            print(f"Primary model ({self.default_model}) failed. Fallback triggered... Error: {e}")
            
            fallback = self.fallback_model
            if fallback == "gemini/gemini-2.5-flash" or fallback.endswith("gemini-2.5-flash"):
                used_model = "gemini/gemini-2.5-flash"
            else:
                used_model = fallback
                
            response = await litellm.acompletion(
                model=used_model,
                messages=sanitized_messages,
                **kwargs
            )

        content = response.choices[0].message.content
        
        try:
            cost = litellm.completion_cost(completion_response=response)
        except Exception:
            cost = 0.0

        if self.call_repo:
            try:
                await self.call_repo.log_call(
                    project_id=request.budget_key,
                    purpose=request.purpose,
                    provider="litellm",
                    model=used_model,
                    cost_usd=float(cost or 0.0)
                )
            except Exception as log_error:
                print(f"Failed to save provider call log: {log_error}")

        return ProviderResponse(
            content=content,
            model=used_model,
            provider="litellm",
            estimated_cost_usd=cost
        )
    
    async def get_embedding(self, text: str) -> list[float]:
        """Generates a semantic vector embedding using LiteLLM."""
        try:
            response = await litellm.aembedding(model=self.settings.embedding_model, input=[text])
            return response['data'][0]['embedding']
        except Exception as e:
            print(f"Failed to fetch embedding: {e}")
            return [0.0] * self.settings.embedding_dimension