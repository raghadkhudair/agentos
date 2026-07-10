from __future__ import annotations
import json
import ray
import structlog
from agentos.config.settings import Settings
from agentos.provider.gateway import ProviderGateway, ProviderRequest

logger = structlog.get_logger()

@ray.remote
class ReviewerAgentActor:
    """A specialized Ray actor for reviewing code patches and enforcing security guardrails."""
    def __init__(self, settings_payload: dict):
        self.settings = Settings(**settings_payload)
        self.provider = ProviderGateway(self.settings)

    async def review_code_patch(self, file_path: str, code_content: str) -> dict:
        """Evaluates structural patches against security boundaries and prompt injection indicators."""
        logger.info("review_started", target_file=file_path)

        system_prompt = (
            "You are a Senior Security Reviewer Agent.\n"
            "Inspect the provided source code for vulnerabilities, structural syntax flaws, or malicious patterns.\n"
            "Respond with a single raw JSON object matching this schema shape:\n"
            "{\n"
            "  \"approved\": true | false,\n"
            "  \"score\": 0-100,\n"
            "  \"vulnerabilities_found\": [\"strings\"]\n"
            "}"
        )

        user_prompt = f"File: {file_path}\nSource Code Content:\n{code_content}"
        
        request = ProviderRequest(
            purpose="review_code_patch",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            budget_key="global-governance"
        )

        response = await self.provider.get_completion(request, response_format={"type": "json_object"})
        
        try:
            result = json.loads(response.content)
            logger.info("review_completed", file=file_path, approved=result.get("approved"))
            return result
        except Exception:
            return {"approved": False, "score": 0, "vulnerabilities_found": ["Failed to extract valid review schema."]}