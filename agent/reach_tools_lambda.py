from semantic import reach

TOOLS = {
    "get_daily_reach": lambda p: reach.get_daily_reach(
        p["campaign"], p.get("segment"), p["day"]
    ),
    "get_cumulative_reach": lambda p: reach.get_cumulative_reach(
        p["campaign"], p.get("segment"), p["start"], p["end"]
    ),
    "merge_segment_reach": lambda p: reach.merge_segment_reach(
        p["campaign"], p["segments"], p["start"], p["end"]
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
    except Exception as e:  # surface to the agent
        return {"error": str(e)}
