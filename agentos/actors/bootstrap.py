from __future__ import annotations

import json
import re
import ray

from agentos.runtime.team_plan import AgentRole, AgentSpec, TeamPlan
from agentos.provider.gateway import ProviderGateway, ProviderRequest
from agentos.config.settings import load_settings


@ray.remote(max_restarts=-1, max_task_retries=3)
class BootstrapAgentActor:
    """The first-run PM/Tech Lead agent.

    Evaluates the incoming user request via LLM reasoning to dynamically form a tailored team plan,
    define explicit assumptions, and determine project-specific Definition of Done (DoD) milestones.
    """

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.settings = load_settings()
        self.provider = ProviderGateway(self.settings)

    async def create_team_plan(self, user_request: str, max_agents_total: int) -> dict:
        """
        Queries the provider gateway with the raw user request to synthesize 
        a dynamic, custom-fitted team plan and task graph.
        """
        system_prompt = (
            "You are the Lead Project Manager and Principal Architect for AgentOS.\n"
            "Your task is to analyze a user's IT/software request and output a structured project blueprint.\n\n"
            "Rules for team sizing and composition:\n"
            "1. Only request specialized roles that are absolutely necessary for the task.\n"
            "2. If the request is backend-only or CLI-only, DO NOT include FRONTEND_DEVELOPER.\n"
            "3. Ensure the sum of agent counts does not exceed the provided max_agents limit.\n\n"
            "Available Agent Roles:\n"
            "- PM_TECH_LEAD: Owns planning, coordination, and overall delivery gating.\n"
            "- SOLUTION_ARCHITECT: Designs API contracts, system components, and architectural records.\n"
            "- BACKEND_DEVELOPER: Writes backend logic, code modules, databases, and structural asserts.\n"
            "- FRONTEND_DEVELOPER: Crafts client application elements, layout systems, and views.\n"
            "- PLATFORM_ENGINEER: Builds Docker wrappers, sandboxes, and integration lifecycles.\n"
            "- QA_ENGINEER: Asserts acceptance criteria and collects delivery execution proof.\n"
            "- SECURITY_REVIEWER: Inspects logic paths for vulnerabilities or safety boundary violations.\n\n"
            "You MUST respond with a single un-wrapped valid JSON object matching this schema exactly:\n"
            "{\n"
            "  \"project_name\": \"string-identifier\",\n"
            "  \"dod\": [\"clear, measurable completion milestones matching requirement scopes\"],\n"
            "  \"assumptions\": [\"explicit boundary assumptions matching context limits\"],\n"
            "  \"agents\": [\n"
            "     {\n"
            "       \"role\": \"PM_TECH_LEAD\" | \"SOLUTION_ARCHITECT\" | \"BACKEND_DEVELOPER\" | \"FRONTEND_DEVELOPER\" | \"PLATFORM_ENGINEER\" | \"QA_ENGINEER\" | \"SECURITY_REVIEWER\",\n"
            "       \"count\": 1,\n"
            "       \"description\": \"custom focus explanation for this project\"\n"
            "     }\n"
            "  ]\n"
            "}"
        )

        user_prompt = (
            f"USER SOFTWARE REQUEST: \"{user_request}\"\n"
            f"MAXIMUM TOTAL ALLOWED AGENTS BOUNDARY: {max_agents_total}\n\n"
            "Generate the optimized JSON team plan configuration."
        )

        request = ProviderRequest(
            purpose="bootstrap_team_planning",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            budget_key=self.project_id,
            metadata={}
        )

        # Execute structured generation through the provider gateway
        response = await self.provider.get_completion(request, response_format={"type": "json_object"})
        
        clean_content = response.content.strip()
        if clean_content.startswith("```"):
            clean_content = re.sub(r"^```json\s*|^```\s*", "", clean_content, flags=re.MULTILINE)
            clean_content = re.sub(r"\s*```$", "", clean_content, flags=re.MULTILINE).strip()

        try:
            plan_data = json.loads(clean_content)
        except Exception as e:
            print(f"Failed to parse dynamically generated team plan, falling back to basic skeleton setup: {e}")
            # Dynamic safety fallback structure to avoid breaking the runtime initialization sequence
            plan_data = {
                "project_name": "emergency-fallback-project",
                "dod": ["Code base executes", "Verify output standards"],
                "assumptions": ["Fallback mode active"],
                "agents": [{"role": "PM_TECH_LEAD", "count": 1, "description": "Fallback coordinator"}]
            }

        # Format and validate agent payload specifications array
        validated_agents = []
        running_count = 0
        
        for a in plan_data.get("agents", []):
            role_str = a.get("role", "PM_TECH_LEAD")
            count = int(a.get("count", 1))
            
            # Prevent overflow past our system's max worker ceiling thresholds
            if running_count + count > max_agents_total:
                count = max(1, max_agents_total - running_count)
                if running_count >= max_agents_total:
                    continue
            
            running_count += count
            
            # Match metadata profiles dynamically
            validated_agents.append(
                AgentSpec(
                    role=AgentRole[role_str],
                    count=count,
                    description=a.get("description", "Assigned software engineer worker instance."),
                    memory_scopes=["project", role_str.lower(), "decision"],
                    allowed_action_categories=["implement", "test", "review"],
                    ownership_domains=[role_str.lower()]
                )
            )

        # Enforce clean schema building
        final_plan = TeamPlan(
            project_name=plan_data.get("project_name", "agentos-autonomous-project"),
            user_request=user_request,
            dod=plan_data.get("dod", ["Verify code delivery output standards."]),
            assumptions=plan_data.get("assumptions", ["Isolated local sandbox driver active."]),
            agents=validated_agents,
            max_requested_agents=max_agents_total
        )

        return final_plan.model_dump()