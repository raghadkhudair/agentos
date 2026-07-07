from __future__ import annotations

import ray

from agentos.runtime.team_plan import AgentRole, AgentSpec, TeamPlan


@ray.remote(max_restarts=-1, max_task_retries=3)
class BootstrapAgentActor:
    """First-run PM/Tech Lead actor.

    This actor creates the first TeamPlan. The runtime validates and may reduce the plan.
    """

    def __init__(self, project_id: str):
        self.project_id = project_id

    async def create_team_plan(self, user_request: str, max_agents_total: int) -> dict:
        # Starter deterministic team plan. Replace with provider-backed reasoning later.
        agents = [
            AgentSpec(
                role=AgentRole.PM_TECH_LEAD,
                count=1,
                description="Owns DoD, planning, gap analysis, and final readiness proposal.",
                memory_scopes=["project", "decision", "dod"],
                allowed_action_categories=["plan", "summarize", "assign", "evaluate_dod"],
                ownership_domains=["project_plan", "dod"],
            ),
            AgentSpec(
                role=AgentRole.SOLUTION_ARCHITECT,
                count=2,
                description="Owns architecture, contracts, boundaries, and ADRs.",
                memory_scopes=["project", "decision", "contract"],
                allowed_action_categories=["design", "review", "publish_contract"],
                ownership_domains=["architecture", "contracts"],
            ),
            AgentSpec(
                role=AgentRole.BACKEND_DEVELOPER,
                count=4,
                description="Builds backend services, database migrations, and API tests.",
                memory_scopes=["project", "backend", "contract", "decision"],
                allowed_action_categories=["implement", "test", "publish_artifact"],
                ownership_domains=["backend"],
            ),
            AgentSpec(
                role=AgentRole.FRONTEND_DEVELOPER,
                count=3,
                description="Builds frontend application, components, state, and integration tests.",
                memory_scopes=["project", "frontend", "contract", "decision"],
                allowed_action_categories=["implement", "test", "publish_artifact"],
                ownership_domains=["frontend"],
            ),
            AgentSpec(
                role=AgentRole.PLATFORM_ENGINEER,
                count=2,
                description="Owns Docker, local deployment, CI design, and developer experience.",
                memory_scopes=["project", "platform", "decision"],
                allowed_action_categories=["configure", "test", "publish_artifact"],
                ownership_domains=["platform", "docker", "ci"],
            ),
            AgentSpec(
                role=AgentRole.QA_ENGINEER,
                count=2,
                description="Owns acceptance, integration, regression, and DoD evidence testing.",
                memory_scopes=["project", "qa", "contract", "dod"],
                allowed_action_categories=["test", "verify", "publish_report"],
                ownership_domains=["qa"],
            ),
            AgentSpec(
                role=AgentRole.SECURITY_REVIEWER,
                count=1,
                description="Reviews security-sensitive changes and unsafe agent behavior.",
                memory_scopes=["project", "security", "decision"],
                allowed_action_categories=["review", "block", "quarantine_recommendation"],
                ownership_domains=["security"],
            ),
        ]
        plan = TeamPlan(
            project_name="agentos-generated-project",
            user_request=user_request,
            dod=[
                "Application runs locally through Docker.",
                "Required backend functionality is implemented and tested.",
                "Required frontend functionality is implemented and tested.",
                "QA acceptance evidence exists for all mandatory features.",
                "Security review is completed for high-risk areas.",
                "README and operational documentation are complete.",
            ],
            assumptions=[
                "The system is dedicated to IT and software development activities only.",
                "External AI calls are routed through the provider gateway.",
                "All execution happens through guarded supervisors, not direct agent shell access.",
            ],
            agents=agents,
            max_requested_agents=max_agents_total,
        )
        return plan.model_dump()
