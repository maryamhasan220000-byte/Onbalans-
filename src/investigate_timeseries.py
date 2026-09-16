"""
Investigate responses containing more than one TimeSeries entry.

Throwaway diagnostic, not part of the pipeline. Answers three questions:

  1. WHEN do multi-TimeSeries responses occur?  (clustered, or scattered?)
  2. HOW do the entries differ?                 (unit, mRID, point count)
  3. WHAT time ranges do they cover?            (adjacent, or overlapping?)

Read-only. Reuses the inspector's file helpers rather than duplicating them.

Usage:
    python src/investigate_timeseries.py
    python src/investigate_timeseries.py --examples 5
"""

import argparse
import json
from collections import Counter
from pathlib import Path

from inspect_bronze import iter_bronze_files, iter_records, parse_utc

RAW_DIR = Path("data/raw")
DUTCH_SUMMER_OFFSET_HOURS = 2      # UTC+2 in August (CEST)


def multi_series_responses(root: Path):
    """Yield (fetched_at, series_list) for every response with != 1 TimeSeries."""
    for path in iter_bronze_files(root):
        for record in iter_records(path):
            if record.get("status_code") != 200:
                continue

            body = record.get("body")
            if not body:
                continue

            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                continue

            if not isinstance(payload, dict):
                continue

            series_list = payload.get("Response", {}).get("TimeSeries", [])
            if not isinstance(series_list, list) or len(series_list) == 1:
                continue

            fetched_at = parse_utc(record.get("fetched_at", ""))
            if fetched_at:
                yield fetched_at, series_list


def describe_series(series: dict) -> dict:
    """Summarise one TimeSeries entry without dumping every point."""
    periods = series.get("Period", [])
    points = []
    for period in periods:
        if isinstance(period, dict):
            raw = period.get("points", [])
            if isinstance(raw, list):
                points.extend(p for p in raw if isinstance(p, dict))

    starts = [p.get("timeInterval_start")
              for p in points if p.get("timeInterval_start")]
    sequences = [p.get("sequence")
                 for p in points if p.get("sequence") is not None]

    return {
        "mRID": series.get("mRID"),
        "unit": series.get("quantity_Measurement_Unit_name"),
        "currency": series.get("currency_Unit_name"),
        "periods": len(periods),
        "points": len(points),
        "first_start": min(starts) if starts else None,
        "last_start": max(starts) if starts else None,
        "first_seq": sequences[0] if sequences else None,
        "last_seq": sequences[-1] if sequences else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--examples", type=int, default=3,
                        help="How many full examples to print")
    args = parser.parse_args()

    by_utc_hour = Counter()
    by_local_hour = Counter()
    shapes = Counter()
    examples = []
    total = 0

    for fetched_at, series_list in multi_series_responses(args.raw_dir):
        total += 1
        by_utc_hour[fetched_at.hour] += 1
        local_hour = (fetched_at.hour + DUTCH_SUMMER_OFFSET_HOURS) % 24
        by_local_hour[local_hour] += 1
        shapes[len(series_list)] += 1

        if len(examples) < args.examples:
            examples.append((fetched_at, series_list))

    print("=" * 68)
    print("MULTI-TIMESERIES INVESTIGATION")
    print("=" * 68)

    if total == 0:
        print("\nNo multi-TimeSeries responses found.")
        return

    print(f"\nResponses with more than one TimeSeries: {total:,}")
    print("\nEntry counts seen:")
    for count, n in sorted(shapes.items()):
        print(f"  {count} entries : {n:,}")

    print("\n-- WHEN, by Dutch local hour (UTC+2) --")
    print("   Clustering near hour 0 would mean these straddle local midnight.\n")
    for hour in range(24):
        n = by_local_hour.get(hour, 0)
        bar = "#" * min(60, n // max(1, total // 60) if total > 60 else n)
        marker = "  <-- local midnight" if hour == 0 and n else ""
        print(f"  {hour:02d}:00  {n:>6,}  {bar}{marker}")

    print("\n-- WHEN, by UTC hour --")
    busiest = by_utc_hour.most_common(5)
    for hour, n in busiest:
        print(f"  {hour:02d}:00 UTC  {n:>6,}  ({n / total:.1%} of all anomalies)")

    print("\n" + "=" * 68)
    print("EXAMPLES")
    print("=" * 68)

    for fetched_at, series_list in examples:
        local = (fetched_at.hour + DUTCH_SUMMER_OFFSET_HOURS) % 24
        print(f"\nFetched {fetched_at:%Y-%m-%d %H:%M:%S} UTC "
              f"({local:02d}:{fetched_at.minute:02d} Dutch local)")
        print(f"  {len(series_list)} TimeSeries entries:")

        for i, series in enumerate(series_list):
            if not isinstance(series, dict):
                print(f"    [{i}] not an object: {type(series).__name__}")
                continue
            d = describe_series(series)
            print(f"    [{i}] mRID={d['mRID']}  unit={d['unit']}  "
                  f"currency={d['currency']}")
            print(f"        periods={d['periods']}  points={d['points']}")
            print(f"        covers {d['first_start']} -> {d['last_start']}")
            print(f"        sequence {d['first_seq']} -> {d['last_seq']}")


if __name__ == "__main__":
    main()
