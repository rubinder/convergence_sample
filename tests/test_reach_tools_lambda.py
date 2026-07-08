from unittest.mock import patch

from agent import reach_tools_lambda as L


def test_dispatch_daily():
    with patch(
        "agent.reach_tools_lambda.reach.get_daily_reach", return_value={"reach": 10}
    ) as m:
        out = L.handler(
            {
                "tool": "get_daily_reach",
                "params": {
                    "campaign": "camp_finals",
                    "segment": "sports",
                    "day": "2026-07-03",
                },
            },
            None,
        )
    assert out["reach"] == 10
    m.assert_called_once()


def test_unknown_tool_errors():
    out = L.handler({"tool": "nope", "params": {}}, None)
    assert "error" in out
