import time

DB = "convergence"
_HLL = "cardinality(merge(cast(hll_sketch as HyperLogLog)))"


def _seg_clause(segment):
    return f" and segment = '{segment}'" if segment else ""


def daily_reach_sql(campaign: str, segment, day: str) -> str:
    return (
        f"select sum(exact_reach) as reach, sum(impressions) as impressions "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' and day = date '{day}'"
        f"{_seg_clause(segment)}"
    )


def cumulative_reach_sql(campaign: str, segment, start: str, end: str) -> str:
    return (
        f"select {_HLL} as reach "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' "
        f"and day between date '{start}' and date '{end}'"
        f"{_seg_clause(segment)}"
    )


def segment_merge_sql(campaign: str, segments: list, start: str, end: str) -> str:
    seg_list = ", ".join(f"'{s}'" for s in segments)
    return (
        f"select {_HLL} as reach "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' "
        f"and day between date '{start}' and date '{end}' "
        f"and segment in ({seg_list})"
    )


# ---- live-query wrappers (imported lazily so SQL-builder tests need no boto3) ----


def _run(sql: str):
    from semantic.athena_client import run_query

    return run_query(sql)


# module-level alias so tests can patch `semantic.reach.run_query`
def run_query(sql: str):
    return _run(sql)


def _one(sql: str, key: str = "reach") -> dict:
    t0 = time.time()
    rows = run_query(sql)
    val = rows[0].get(key) if rows and rows[0].get(key) is not None else 0
    return {
        "reach": int(val),
        "sql": sql,
        "latency_ms": int((time.time() - t0) * 1000),
        "rows": rows,
    }


def get_daily_reach(campaign, segment, day):
    return _one(daily_reach_sql(campaign, segment, day))


def get_cumulative_reach(campaign, segment, start, end):
    return _one(cumulative_reach_sql(campaign, segment, start, end))


def merge_segment_reach(campaign, segments, start, end):
    out = _one(segment_merge_sql(campaign, segments, start, end))
    out["segments"] = segments
    return out


def list_campaigns():
    return [
        r["campaign_id"]
        for r in run_query(
            f"select distinct campaign_id from {DB}.daily_reach_snapshot order by 1"
        )
    ]


def list_segments():
    return [
        r["segment"]
        for r in run_query(
            f"select distinct segment from {DB}.daily_reach_snapshot order by 1"
        )
    ]
