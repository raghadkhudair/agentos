from __future__ import annotations

import json
import re
import ray

from agentos.runtime.team_plan import AgentRole, AgentSpec, TeamPlan
from agentos.provider.gateway import ProviderGateway, ProviderRequest
from agentos.config.settings import load_settings
from agentos.config.loader import team_roles
from agentos.messaging.events import EventType


@ray.remote(max_restarts=-1, max_task_retries=3)
class BootstrapAgentActor:

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.settings = load_settings()
        self.provider = ProviderGateway(self.settings)

    async def create_team_plan(self, user_request: str, max_agents_total: int) -> dict:
        roles_cfg = team_roles()
        
        role_profiles = []
        role_names_list = []
        for r in roles_cfg.get("roles", []):
            role_name = r["role"].upper()
            role_names_list.append(f'"{role_name}"')
            
            profile = (
                f"- Role: {role_name}\n"
                f"  Description: {r.get('description', '')}\n"
                f"  Default Memory Scopes: {r.get('default_memory_scopes', [])}\n"
                f"  Allowed Action Categories: {r.get('allowed_action_categories', [])}\n"
                f"  Default Event Subscriptions: {r.get('default_event_subscriptions', [])}\n"
            )
            role_profiles.append(profile)

        role_bullets = "\n".join(role_profiles)
        role_enum_string = " | ".join(role_names_list)

        system_prompt = (
            "You are the Lead Project Manager and Principal Architect for AgentOS.\n"
            "Your task is to analyze a user's IT/software request and choose the optimal team roster "
            "using ONLY the predefined configuration templates listed below.\n\n"
            "Rules for team selection:\n"
            "1. Only request specialized roles that are absolutely necessary for the task.\n"
            "2. If the request is backend-only or CLI-only, DO NOT include FRONTEND_DEVELOPER.\n"
            "3. Ensure the sum of agent counts does not exceed the provided max_agents limit.\n"
            "4. Do not invent variables or scopes. Simply choose the role names.\n\n"
            f"Predefined Agent Registry Templates:\n{role_bullets}\n\n"
            "You MUST respond with a single un-wrapped valid JSON object matching this schema exactly:\n"
            "{\n"
            "  \"project_name\": \"string-identifier\",\n"
            "  \"high_level_architecture\": \"Summary of technical approach and system architecture\",\n"
            "  \"dod\": [\"clear, measurable completion milestones matching requirement scopes\"],\n"
            "  \"assumptions\": [\"explicit boundary assumptions matching context limits\"],\n"
            "  \"agents\": [\n"
            "     {\n"
            f"       \"role\": {role_enum_string},\n"
            "       \"count\": 1,\n"
            "       \"description\": \"Specific purpose/assignment for this role on this project\"\n"
            "     }\n"
            "  ]\n"
            "}"
        )

        user_prompt = (
            f"USER SOFTWARE REQUEST: \"{user_request}\"\n"
            f"MAXIMUM TOTAL ALLOWED AGENTS BOUNDARY: {max_agents_total}\n\n"
            "Select the ideal team configuration matching the allowed registry profiles."
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

        response = await self.provider.get_completion(request, response_format={"type": "json_object"})
        
        clean_content = response.content.strip()
        if clean_content.startswith("```"):
            clean_content = re.sub(r"^```json\s*|^```\s*", "", clean_content, flags=re.MULTILINE)
            clean_content = re.sub(r"\s*```$", "", clean_content, flags=re.MULTILINE).strip()

        try:
            plan_data = json.loads(clean_content)
        except Exception as e:
            print(f"Failed to parse dynamically generated team plan, falling back to basic skeleton setup: {e}")
            plan_data = roles_cfg.get("fallback_team", {})

        validated_agents = []
        running_count = 0
        
        for a in plan_data.get("agents", []):
            role_str = a.get("role", "PM_TECH_LEAD").upper()
            count = int(a.get("count", 1))
            
            if running_count + count > max_agents_total:
                count = max(1, max_agents_total - running_count)
                if running_count >= max_agents_total:
                    continue
            
            running_count += count
            
            role_template = next((r for r in roles_cfg.get("roles", []) if r["role"].upper() == role_str), {})
            
            validated_agents.append(
                AgentSpec(
                    role=AgentRole[role_str] if hasattr(AgentRole, role_str) else AgentRole.PM_TECH_LEAD,
                    count=count,
                    description=a.get("description", role_template.get("description", "Assigned worker worker instance.")),
                    memory_scopes=role_template.get("default_memory_scopes", ["project"]),
                    allowed_action_categories=role_template.get("allowed_action_categories", ["implement"]),
                    ownership_domains=[role_str.lower()],
                    event_subscriptions=role_template.get("default_event_subscriptions", ["PROJECT_CREATED", "TASK_CREATED"])
                )
            )

        final_plan = TeamPlan(
            project_name=plan_data.get("project_name", "agentos-autonomous-project"),
            user_request=user_request,
            dod=plan_data.get("dod", ["Verify code delivery output standards."]),
            assumptions=plan_data.get("assumptions", ["Isolated local sandbox driver active."]),
            agents=validated_agents,
            max_requested_agents=max_agents_total
        )

        return final_plan.model_dump()