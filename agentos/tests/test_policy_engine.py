import pytest
from agentos.config.settings import Settings
from agentos.governance.models import ActionRequest, PolicyDecision
from agentos.governance.policy_engine import PolicyEngine


def test_policy_engine_blocks_destructive_commands():
    settings = Settings(allow_destructive_actions=False)
    engine = PolicyEngine(settings)
    
    # Simulate an injection attempt or malicious command action
    request = ActionRequest(
        project_id="test-env",
        agent_id="attacker-agent",
        action_type="shell_command",
        description="Attempting a clean break out",
        command="rm -rf /var/log/audit.log"
    )
    
    result = engine.evaluate_action(request)
    
    # Assert that the policy system actively intercepts and blocks the request
    assert result.decision == PolicyDecision.DENY or result.decision == PolicyDecision.QUARANTINE_AGENT
    assert any("Blocked destructive pattern" in reason for reason in result.reasons)


def test_policy_engine_allows_safe_file_reads():
    settings = Settings()
    engine = PolicyEngine(settings)
    
    request = ActionRequest(
        project_id="test-env",
        agent_id="dev-1",
        action_type="read_file",
        description="Reading localized code asset parameters",
        payload={"file_path": "src/main.py"}
    )
    
    result = engine.evaluate_action(request)
    assert result.decision == PolicyDecision.ALLOW