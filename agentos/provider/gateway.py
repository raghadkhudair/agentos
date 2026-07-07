from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agentos.config.settings import Settings
import os
import litellm
from typing import Dict, Any, List

@dataclass(frozen=True)
class ProviderRequest:
    purpose: str
    messages: list[dict[str, str]]
    budget_key: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ProviderResponse:
    content: str
    model: str
    provider: str
    estimated_cost_usd: float | None = None


class ProviderGateway:
    def __init__(self):
        # LiteLLM automatically looks for GEMINI_API_KEY in the environment variables
        self.default_model = os.getenv("AGENTOS_PROVIDER_DEFAULT_MODEL", "gemini/gemini-1.5-flash")
        self.fallback_model = os.getenv("AGENTOS_PROVIDER_FALLBACK_MODEL", "gemini/gemini-1.5-pro")

    async def get_completion(self, messages: List[Dict[str, str]], **kwargs) -> str:
        try:
            # LiteLLM universal completion call
            response = await litellm.acompletion(
                model=self.default_model,
                messages=messages,
                **kwargs
            )
            # Standardized extraction format for all models
            return response.choices[0].message.content
            
        except Exception as e:
            print(f"Primary model failed, attempting fallback... Error: {e}")
            # Dynamic fallback handling
            response = await litellm.acompletion(
                model=self.fallback_model,
                messages=messages,
                **kwargs
            )
            return response.choices[0].message.content
