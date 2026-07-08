"""Registers the reach Lambda as AgentCore Gateway tools and wires a managed agent.

Run: ./.venv/bin/python -m agent.deploy_agent

The 2026 AgentCore control plane is CLI-first (`agentcore` from
bedrock-agentcore-starter-toolkit). This script resolves the Lambda ARN from
Terraform and prints the exact commands, so the wiring is reproducible even as
the AgentCore API surface evolves.
"""

import json
import subprocess

from agent.agent_config import SYSTEM_PROMPT, TOOL_SCHEMA


def _tf_output(name):
    return (
        subprocess.check_output(["terraform", "output", "-raw", name], cwd="terraform")
        .decode()
        .strip()
    )


def main():
    lambda_arn = _tf_output("reach_tools_lambda_arn")
    print("Lambda:", lambda_arn)
    print("System prompt:\n", SYSTEM_PROMPT)
    print("Tools:", json.dumps(TOOL_SCHEMA, indent=2))
    print(
        "\nAgentCore setup (via the `agentcore` CLI — the 2026 recommended path):\n"
        "  pip install bedrock-agentcore-starter-toolkit\n"
        "  1) agentcore gateway create --name convergence-reach-gw\n"
        f"  2) agentcore gateway add-target --gateway convergence-reach-gw \\\n"
        f"       --lambda {lambda_arn} --tool-schema agent/agent_config.py\n"
        "  3) agentcore agent create --name convergence-reach \\\n"
        "       --model anthropic.claude --system-prompt-file - \\\n"
        "       --gateway convergence-reach-gw\n"
        "  4) agentcore agent invoke --name convergence-reach \\\n"
        "       --prompt 'cumulative reach of camp_finals for sports 2026-07-01..05'\n"
    )


if __name__ == "__main__":
    main()
