import pytest

from semantic.reach import daily_reach_sql, cumulative_reach_sql, segment_merge_sql
from semantic.validate import InvalidInput, valid_name, valid_date, valid_segments


def test_valid_name_accepts_expected():
    assert valid_name("camp_finals", "campaign") == "camp_finals"


@pytest.mark.parametrize(
    "bad",
    ["camp'; drop table x--", "a b", "seg;delete", "' or '1'='1", "", "x" * 65],
)
def test_valid_name_rejects_injection(bad):
    with pytest.raises(InvalidInput):
        valid_name(bad, "campaign")


def test_valid_date_canonicalizes_and_rejects():
    assert valid_date("2026-07-03", "day") == "2026-07-03"
    with pytest.raises(InvalidInput):
        valid_date("2026-07-03' or '1'='1", "day")


def test_valid_segments_rejects_bad_element():
    assert valid_segments(["sports", "news"]) == ["sports", "news"]
    with pytest.raises(InvalidInput):
        valid_segments(["sports", "news'; drop"])


def test_sql_builders_reject_injection():
    with pytest.raises(InvalidInput):
        daily_reach_sql("camp'; drop table t--", "sports", "2026-07-03")
    with pytest.raises(InvalidInput):
        cumulative_reach_sql("camp_finals", "sports", "bad-date", "2026-07-05")
    with pytest.raises(InvalidInput):
        segment_merge_sql("camp_finals", ["sports", "x'; drop"], "2026-07-01", "2026-07-05")
