from semantic.reach import daily_reach_sql, cumulative_reach_sql, segment_merge_sql


def test_daily_reach_reads_snapshot_exact():
    sql = daily_reach_sql("camp_finals", "sports", "2026-07-03")
    assert "daily_reach_snapshot" in sql
    assert "exact_reach" in sql
    assert "camp_finals" in sql and "sports" in sql and "2026-07-03" in sql


def test_cumulative_merges_hll_over_window():
    sql = cumulative_reach_sql("camp_finals", "sports", "2026-07-01", "2026-07-05")
    assert "cardinality(merge(cast(hll_sketch as HyperLogLog)))" in sql
    assert "day between date '2026-07-01' and date '2026-07-05'" in sql


def test_segment_merge_unions_segments():
    sql = segment_merge_sql(
        "camp_finals", ["sports", "news"], "2026-07-01", "2026-07-05"
    )
    assert "segment in ('sports', 'news')" in sql
    assert "cardinality(merge(cast(hll_sketch as HyperLogLog)))" in sql


def test_no_segment_filter_when_none():
    sql = daily_reach_sql("camp_finals", None, "2026-07-03")
    assert "segment =" not in sql
