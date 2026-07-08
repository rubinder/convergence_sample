import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone

from data_generator.identity import resolve_individual, resolve_household

SEGMENTS = ["sports", "news", "drama", "kids", "lifestyle"]
DELIVERY_TYPES = ["digital", "linear"]
CAMPAIGNS = ["camp_finals", "camp_launch", "camp_holiday"]
NETWORKS = {"digital": ["Max", "DiscoveryPlus"], "linear": ["CNN", "TNT", "TBS"]}
GEOS = ["NY", "LA", "CHI", "DAL", "ATL"]
DEVICES = {"digital": ["mobile", "desktop", "ctv"], "linear": ["set-top"]}
START_DAY = date(2026, 7, 1)


def _pick(seq, n):
    return seq[n % len(seq)]


def build_events(days: int, events_per_day: int, seed: int) -> list[dict]:
    events = []
    for d in range(days):
        day = START_DAY + timedelta(days=d)
        for i in range(events_per_day):
            n = seed * 1_000_003 + d * 7919 + i
            delivery = _pick(DELIVERY_TYPES, n)
            # audience skews by segment but reuses a shared id pool -> cross-day/segment overlap
            seg = _pick(SEGMENTS, n // 3)
            user_seed = f"{seg}-{n % 1500}"
            ind = resolve_individual(user_seed)
            ts = datetime(
                day.year, day.month, day.day, (i % 24), (i % 60), tzinfo=timezone.utc
            ).isoformat()
            events.append(
                {
                    "event_id": f"evt_{seed}_{d}_{i}",
                    "event_ts": ts,
                    "delivery_type": delivery,
                    "individual_id": ind,
                    "household_id": resolve_household(ind),
                    "campaign_id": _pick(CAMPAIGNS, n // 11),
                    "creative_id": f"cr_{n % 20}",
                    "segment": seg,
                    "network": _pick(NETWORKS[delivery], n),
                    "geo": _pick(GEOS, n // 13),
                    "device": _pick(DEVICES[delivery], n),
                }
            )
    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=5)
    ap.add_argument("--events-per-day", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", default=None, help="local dir")
    ap.add_argument("--bucket", default=None, help="s3 bucket for landing/")
    args = ap.parse_args()
    events = build_events(args.days, args.events_per_day, args.seed)
    by_day: dict[str, list[dict]] = {}
    for e in events:
        by_day.setdefault(e["event_ts"][:10], []).append(e)
    for day, rows in by_day.items():
        body = "\n".join(json.dumps(r) for r in rows)
        key = f"landing/ingest_date={day}/events.json"
        if args.bucket:
            import boto3

            boto3.client("s3").put_object(
                Bucket=args.bucket, Key=key, Body=body.encode()
            )
            print(f"uploaded s3://{args.bucket}/{key} ({len(rows)} rows)")
        else:
            path = os.path.join(args.out or "landing_local", key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(body)
            print(f"wrote {path} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
