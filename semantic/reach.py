import time

from semantic.validate import valid_name, valid_date, valid_segments

DB = "convergence"
_HLL = "cardinality(merge(cast(hll_sketch as HyperLogLog)))"
DELIVERY_TYPES = ("digital", "linear")


def _norm(value, field):
    # Identifiers in the lake are lowercase; normalize so callers (and the
    # NL agent) match the data regardless of the casing they used.
    return valid_name(value, field).lower()


def _seg_clause(segment):
    return f" and segment = '{_norm(segment, 'segment')}'" if segment else ""


def _delivery_clause(delivery):
    if not delivery:
        return ""
    d = _norm(delivery, "delivery")
    if d not in DELIVERY_TYPES:
        from semantic.validate import InvalidInput

        raise InvalidInput("delivery must be 'digital' or 'linear'")
    return f" and delivery_type = '{d}'"


def daily_reach_sql(campaign, segment, day, delivery=None):
    campaign = _norm(campaign, "campaign")
    day = valid_date(day, "day")
    return (
        f"select sum(exact_reach) as reach, sum(impressions) as impressions "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' and day = date '{day}'"
        f"{_seg_clause(segment)}{_delivery_clause(delivery)}"
    )


def cumulative_reach_sql(campaign, segment, start, end, delivery=None):
    campaign = _norm(campaign, "campaign")
    start = valid_date(start, "start")
    end = valid_date(end, "end")
    return (
        f"select {_HLL} as reach "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' "
        f"and day between date '{start}' and date '{end}'"
        f"{_seg_clause(segment)}{_delivery_clause(delivery)}"
    )


def segment_merge_sql(campaign, segments, start, end):
    campaign = _norm(campaign, "campaign")
    start = valid_date(start, "start")
    end = valid_date(end, "end")
    segments = [s.lower() for s in valid_segments(segments)]
    seg_list = ", ".join(f"'{s}'" for s in segments)
    return (
        f"select {_HLL} as reach "
        f"from {DB}.daily_reach_snapshot "
        f"where campaign_id = '{campaign}' "
        f"and day between date '{start}' and date '{end}' "
        f"and segment in ({seg_list})"
    )


# ---- live-query wrappers ----


def run_query(sql: str):
    from semantic.athena_client import run_query as _rq

    return _rq(sql)


def _one(sql: str, key: str = "reach") -> dict:
    t0 = time.time()
    rows = run_query(sql)
    raw = rows[0].get(key) if rows and rows[0].get(key) is not None else None
    # `found` distinguishes a genuine 0 from "no rows matched" (e.g. an unknown
    # campaign/segment) so callers never mistake a miss for zero reach.
    return {
        "reach": int(raw) if raw is not None else 0,
        "found": raw is not None,
        "sql": sql,
        "latency_ms": int((time.time() - t0) * 1000),
    }


def get_daily_reach(campaign, segment, day, delivery=None):
    return _one(daily_reach_sql(campaign, segment, day, delivery))


def get_cumulative_reach(campaign, segment, start, end, delivery=None):
    return _one(cumulative_reach_sql(campaign, segment, start, end, delivery))


def merge_segment_reach(campaign, segments, start, end):
    out = _one(segment_merge_sql(campaign, segments, start, end))
    out["segments"] = segments
    return out


def get_convergence_reach(campaign, segment, start, end):
    """The convergence selling view: reach across linear + digital as one.

    Returns digital-only, linear-only, and the HLL-deduped combined reach — the
    combined figure counts a person once even if reached on both delivery
    methods, which is the whole point of selling the two as a single audience.
    """
    digital = get_cumulative_reach(campaign, segment, start, end, "digital")["reach"]
    linear = get_cumulative_reach(campaign, segment, start, end, "linear")["reach"]
    out = get_cumulative_reach(campaign, segment, start, end)  # both, deduped
    combined = out["reach"]
    return {
        "campaign": campaign,
        "segment": segment,
        "window": [start, end],
        "digital": digital,
        "linear": linear,
        "combined": combined,
        "overlap": max(0, digital + linear - combined),
        "found": out["found"],
        "sql": out["sql"],
    }


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
