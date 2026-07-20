from __future__ import annotations
import json
import ray
import structlog
from agentos.config.settings import Settings
from agentos.provider.gateway import ProviderRequest

logger = structlog.get_logger()

@ray.remote(namespace="agentos")
class SafetyReviewerAgentActor:
    def __init__(self, settings_payload: dict):
        self.settings = Settings(**settings_payload)

    async def review_agent_behavior(self, action_type: str, description: str, recent_violation_count: int, provider_gateway) -> dict:
        system_prompt = (
            "You are the Safety Reviewer for AgentOS. You do not review code quality — "
            "you review whether an agent's PROPOSED BEHAVIOR looks unsafe, deceptive, or "
            "like it's trying to bypass guardrails, regardless of whether the code itself is well-written.\n"
            "Respond with raw JSON only: {\"safe\": true|false, \"reason\": \"...\"}"
        )
        user_prompt = f"Action type: {action_type}\nDescription: {description}\nRecent policy violations by this agent: {recent_violation_count}"
        request = ProviderRequest(
            purpose="safety_behavior_review",
            messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
            budget_key="global-governance"
        )
        try:
            response = await provider_gateway.get_completion.remote(request, response_format={"type": "json_object"})
            return json.loads(response["content"])
        except Exception as e:
            logger.error("safety_review_failed_failing_closed", error=str(e))
            return {"safe": False, "reason": "Safety reviewer failure — failing closed."}