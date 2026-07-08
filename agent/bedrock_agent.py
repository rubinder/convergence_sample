"""Claude-on-Bedrock reach agent (Converse API tool-use loop).

This is the agentic layer: a Bedrock-hosted Claude model whose tools are the
Convergence reach semantic-layer functions. The AgentCore Gateway/Runtime path
(agent/deploy_agent.py) is the production hosting story; this in-process loop is
the demoable version that runs behind the dashboard's /api/chat.
"""

import json
import os

import boto3

from agent.agent_config import SYSTEM_PROMPT
from semantic import reach
from semantic.validate import InvalidInput

_REGION = os.getenv("AWS_REGION", "us-east-1")

# Preference order for the Bedrock model behind the agent. Claude is preferred;
# Amazon Nova is the always-available backstop so the agent works even before
# the account's Anthropic model-access / use-case form is approved. Set
# BEDROCK_MODEL to pin one explicitly (e.g. us.anthropic.claude-opus-4-8 once
# Opus access is granted).
_MODEL_CANDIDATES = (
    [os.environ["BEDROCK_MODEL"]]
    if os.getenv("BEDROCK_MODEL")
    else [
        "us.anthropic.claude-opus-4-8",
        "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        "us.amazon.nova-pro-v1:0",
        "us.amazon.nova-lite-v1:0",
    ]
)
_picked_model = None

_TOOLS = [
    {
        "toolSpec": {
            "name": "get_daily_reach",
            "description": "Exact daily reach (unique individuals) for a campaign on a single day. Optionally filter by segment.",
            "inputSchema": {"json": {
                "type": "object",
                "properties": {
                    "campaign": {"type": "string"},
                    "segment": {"type": "string"},
                    "day": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["campaign", "day"],
            }},
        }
    },
    {
        "toolSpec": {
            "name": "get_cumulative_reach",
            "description": "HLL-merged cumulative reach (deduped unique individuals) for a campaign across a date window. Optionally filter by segment.",
            "inputSchema": {"json": {
                "type": "object",
                "properties": {
                    "campaign": {"type": "string"},
                    "segment": {"type": "string"},
                    "start": {"type": "string", "description": "YYYY-MM-DD"},
                    "end": {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["campaign", "start", "end"],
            }},
        }
    },
    {
        "toolSpec": {
            "name": "merge_segment_reach",
            "description": "HLL-merged reach across multiple audience segments (deduped unique individuals) for a campaign over a window.",
            "inputSchema": {"json": {
                "type": "object",
                "properties": {
                    "campaign": {"type": "string"},
                    "segments": {"type": "array", "items": {"type": "string"}},
                    "start": {"type": "string"},
                    "end": {"type": "string"},
                },
                "required": ["campaign", "segments", "start", "end"],
            }},
        }
    },
    {
        "toolSpec": {
            "name": "list_campaigns",
            "description": "List available campaign ids.",
            "inputSchema": {"json": {"type": "object", "properties": {}}},
        }
    },
    {
        "toolSpec": {
            "name": "list_segments",
            "description": "List available audience segments.",
            "inputSchema": {"json": {"type": "object", "properties": {}}},
        }
    },
]

_DISPATCH = {
    "get_daily_reach": lambda p: reach.get_daily_reach(p["campaign"], p.get("segment"), p["day"]),
    "get_cumulative_reach": lambda p: reach.get_cumulative_reach(p["campaign"], p.get("segment"), p["start"], p["end"]),
    "merge_segment_reach": lambda p: reach.merge_segment_reach(p["campaign"], p["segments"], p["start"], p["end"]),
    "list_campaigns": lambda p: {"campaigns": reach.list_campaigns()},
    "list_segments": lambda p: {"segments": reach.list_segments()},
}


def _run_tool(name: str, params: dict) -> dict:
    try:
        return _DISPATCH[name](params)
    except InvalidInput:
        return {"error": "invalid parameters"}
    except Exception as e:  # surfaced to the model, scrubbed of internals
        return {"error": f"{name} failed"}


def _pick_model(client) -> str:
    """Return the first candidate model this account can actually invoke."""
    global _picked_model
    if _picked_model:
        return _picked_model
    from botocore.exceptions import ClientError

    last_err = None
    for m in _MODEL_CANDIDATES:
        try:
            client.converse(
                modelId=m,
                messages=[{"role": "user", "content": [{"text": "ok"}]}],
                inferenceConfig={"maxTokens": 5},
            )
            _picked_model = m
            return m
        except ClientError as e:
            last_err = e
            continue
    raise last_err or RuntimeError("no invokable Bedrock model")


def chat(prompt: str, max_turns: int = 6) -> str:
    client = boto3.client("bedrock-runtime", region_name=_REGION)
    model_id = _pick_model(client)
    messages = [{"role": "user", "content": [{"text": prompt}]}]
    for _ in range(max_turns):
        resp = client.converse(
            modelId=model_id,
            system=[{"text": SYSTEM_PROMPT}],
            messages=messages,
            toolConfig={"tools": _TOOLS},
            inferenceConfig={"maxTokens": 1024, "temperature": 0},
        )
        out = resp["output"]["message"]
        messages.append(out)
        if resp.get("stopReason") != "tool_use":
            return "".join(b.get("text", "") for b in out["content"]).strip()
        results = []
        for block in out["content"]:
            if "toolUse" in block:
                tu = block["toolUse"]
                result = _run_tool(tu["name"], tu.get("input", {}))
                results.append({
                    "toolResult": {
                        "toolUseId": tu["toolUseId"],
                        "content": [{"json": result}],
                    }
                })
        messages.append({"role": "user", "content": results})
    return "(reached max reasoning turns without a final answer)"
