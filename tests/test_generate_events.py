from data_generator.generate_events import build_events, SEGMENTS, DELIVERY_TYPES


def test_event_schema_and_values():
    events = build_events(days=2, events_per_day=100, seed=1)
    assert len(events) == 200
    e = events[0]
    assert set(e) >= {
        "event_id",
        "event_ts",
        "delivery_type",
        "individual_id",
        "household_id",
        "campaign_id",
        "segment",
        "geo",
        "device",
        "network",
    }
    assert e["delivery_type"] in DELIVERY_TYPES
    assert e["segment"] in SEGMENTS


def test_events_are_deterministic():
    assert build_events(2, 50, 7) == build_events(2, 50, 7)


def test_audience_overlaps_across_days():
    all_days = build_events(days=3, events_per_day=500, seed=3)
    first_day = _first_day(all_days)
    day0 = [e for e in all_days if e["event_ts"].startswith(first_day)]
    uniq_all = len({e["individual_id"] for e in all_days})
    uniq_day0 = len({e["individual_id"] for e in day0})
    assert uniq_all < 3 * uniq_day0


def _first_day(events):
    return min(e["event_ts"] for e in events)[:10]
