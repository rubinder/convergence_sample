from unittest.mock import patch

import semantic.reach as reach


def test_get_daily_reach_returns_value_and_sql():
    with patch(
        "semantic.reach.run_query",
        return_value=[{"reach": "1234", "impressions": "5000"}],
    ):
        out = reach.get_daily_reach("camp_finals", "sports", "2026-07-03")
    assert out["reach"] == 1234
    assert "daily_reach_snapshot" in out["sql"]


def test_get_cumulative_reach_uses_hll_merge():
    with patch("semantic.reach.run_query", return_value=[{"reach": "9000"}]):
        out = reach.get_cumulative_reach(
            "camp_finals", "sports", "2026-07-01", "2026-07-05"
        )
    assert out["reach"] == 9000
    assert "merge(cast(hll_sketch as HyperLogLog))" in out["sql"]


def test_merge_segment_reach_over_segments():
    with patch("semantic.reach.run_query", return_value=[{"reach": "12000"}]):
        out = reach.merge_segment_reach(
            "camp_finals", ["sports", "news"], "2026-07-01", "2026-07-05"
        )
    assert out["reach"] == 12000
