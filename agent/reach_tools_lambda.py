import logging

from semantic import reach
from semantic.validate import InvalidInput

log = logging.getLogger("convergence.reach_tools")

TOOLS = {
    "get_daily_reach": lambda p: reach.get_daily_reach(
        p["campaign"], p.get("segment"), p["day"], p.get("delivery")
    ),
    "get_cumulative_reach": lambda p: reach.get_cumulative_reach(
        p["campaign"], p.get("segment"), p["start"], p["end"], p.get("delivery")
    ),
    "merge_segment_reach": lambda p: reach.merge_segment_reach(
        p["campaign"], p["segments"], p["start"], p["end"]
    ),
    "get_convergence_reach": lambda p: reach.get_convergence_reach(
        p["campaign"], p.get("segment"), p["start"], p["end"]
    ),
    "list_campaigns": lambda p: {"campaigns": reach.list_campaigns()},
    "list_segments": lambda p: {"segments": reach.list_segments()},
}


def handler(event, context):
    tool = event.get("tool")
    params = event.get("params", {})
    if tool not in TOOLS:
        return {"error": f"unknown tool: {tool}", "available": list(TOOLS)}
    try:
        return TOOLS[tool](params)
    except InvalidInput:
        # Reject prompt-injected / malformed identifiers without echoing them back.
        return {"error": "invalid parameters"}
    except KeyError as e:
        return {"error": f"missing parameter: {e.args[0]}"}
    except Exception:
        log.exception("reach tool failed: %s", tool)
        return {"error": "reach tool failed"}
