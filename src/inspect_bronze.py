"""
Onbalans bronze inspector.

Reads captured JSONL (plain or gzipped) and reports capture health: poll
cadence, failure modes, response latency, publication lag, and gaps in grid
interval coverage — distinguishing gaps caused by this pipeline being down
from gaps where TenneT published nothing.

Read-only by design. Bronze is immutable; this tool never writes to it.

Memory: unique interval timestamps are held in a set for gap detection,
roughly 100 bytes each. A month of capture is ~20 MB; a year is ~300 MB.
Beyond that, inspection belongs in a database rather than a Python script.

Exit codes:
    0  healthy
    1  problems detected (gaps, failures, or a stalled feed)
    2  could not run (missing path, no matching files)
"""

import argparse
import gzip
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, TextIO

from logging_setup import configure_logging

# ---------------------------------------------------------------- config

RAW_DIR = Path("data/raw")

INTERVAL_SECONDS = 12          # TenneT grid measurement cadence
POLL_INTERVAL_SECONDS = 12     # tailer poll cadence
FETCH_GAP_TOLERANCE = 2.0      # multiple of poll interval before calling it a gap
MAX_GAPS_SHOWN = 10

logger = configure_logging("inspect_bronze")

# ---------------------------------------------------------------- helpers


def parse_utc(value: str) -> datetime | None:
    """Parse a TenneT timestamp, returning None rather than raising."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def percentile(values: list[float], pct: float) -> float:
    """Linear-interpolated percentile. Returns NaN for empty input."""
    if not values:
        return float("nan")
    ordered = sorted(values)
    position = (len(ordered) - 1) * pct / 100
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight

# ---------------------------------------------------------------- reading


def iter_bronze_files(root: Path, date: str | None = None) -> Iterator[Path]:
    """Yield bronze files in chronological order, plain or gzipped."""
    pattern = f"date={date}/hour=*/*.jsonl*" if date else "date=*/hour=*/*.jsonl*"
    yield from sorted(root.glob(pattern))


def open_bronze(path: Path) -> TextIO:
    """Open a bronze file for reading, transparently handling gzip."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def iter_records(path: Path) -> Iterator[dict]:
    """
    Yield one parsed envelope per line.

    Skips unparseable lines and unreadable files rather than aborting: this
    tool inspects possibly-damaged data, so it must survive damage.
    """
    try:
        handle = open_bronze(path)
    except (OSError, gzip.BadGzipFile, EOFError) as exc:
        logger.error("Cannot open %s: %s", path.name, exc)
        return

    with handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                logger.warning("Skipped malformed line %s:%d",
                               path.name, line_no)
            except (OSError, EOFError) as exc:
                logger.error(
                    "Read failed in %s at line %d: %s", path.name, line_no, exc
                )
                return

# ---------------------------------------------------------------- parsing


def extract_points(body: str | None) -> tuple[list[dict], list[str]]:
    """
    Pull grid measurements out of a TenneT response body.

    Returns (points, anomalies). Anomalies record deviations from the
    single-TimeSeries, single-Period structure observed so far, so a violated
    assumption surfaces in the report instead of being silently indexed past.
    """
    if not body:
        return [], ["empty body"]

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return [], ["body is not valid JSON"]

    if not isinstance(payload, dict) or "Response" not in payload:
        return [], ["unexpected top-level structure"]

    anomalies: list[str] = []
    points: list[dict] = []

    series_list = payload["Response"].get("TimeSeries", [])
    if not isinstance(series_list, list):
        return [], ["TimeSeries is not a list"]
    if len(series_list) != 1:
        anomalies.append(f"TimeSeries count = {len(series_list)}")

    for series in series_list:
        if not isinstance(series, dict):
            anomalies.append("TimeSeries entry is not an object")
            continue

        periods = series.get("Period", [])
        if not isinstance(periods, list):
            anomalies.append("Period is not a list")
            continue
        if len(periods) != 1:
            anomalies.append(f"Period count = {len(periods)}")

        for period in periods:
            if not isinstance(period, dict):
                anomalies.append("Period entry is not an object")
                continue

            raw_points = period.get("points", [])
            if not isinstance(raw_points, list):
                anomalies.append("points is not a list")
                continue

            for point in raw_points:
                if isinstance(point, dict):
                    points.append(point)
                else:
                    anomalies.append("point is not an object")

    return points, anomalies

# ---------------------------------------------------------------- analysis


@dataclass
class CaptureStats:
    """Accumulated health metrics across the inspected bronze files."""

    files: int = 0
    responses: int = 0
    unreadable_records: int = 0
    total_points: int = 0
    write_failures: int = 0
    identical_consecutive_bodies: int = 0

    status_counts: Counter = field(default_factory=Counter)
    error_counts: Counter = field(default_factory=Counter)
    anomaly_counts: Counter = field(default_factory=Counter)

    durations_ms: list[int] = field(default_factory=list)
    lag_seconds: list[float] = field(default_factory=list)

    fetch_times: list[datetime] = field(default_factory=list)
    interval_starts: set[str] = field(default_factory=set)


def analyse(paths: list[Path]) -> CaptureStats:
    """Walk every record once, accumulating all metrics in a single pass."""
    stats = CaptureStats()
    previous_body: str | None = None

    for path in paths:
        stats.files += 1
        logger.info("Reading %s", path)

        for record in iter_records(path):
            if not isinstance(record, dict) or "fetched_at" not in record:
                stats.unreadable_records += 1
                continue

            try:
                fetched_at = datetime.fromisoformat(record["fetched_at"])
            except (TypeError, ValueError):
                stats.unreadable_records += 1
                continue

            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)

            stats.responses += 1
            stats.fetch_times.append(fetched_at)

            status = record.get("status_code")
            stats.status_counts[status if status is not None else "no response"] += 1

            error = record.get("error")
            if error:
                stats.error_counts[str(error).split(":")[0]] += 1

            if record.get("write_failed"):
                stats.write_failures += 1

            duration = record.get("duration_ms")
            if isinstance(duration, (int, float)):
                stats.durations_ms.append(int(duration))

            if status != 200:
                continue

            body = record.get("body")
            if body is not None and body == previous_body:
                stats.identical_consecutive_bodies += 1
            previous_body = body

            points, anomalies = extract_points(body)
            stats.total_points += len(points)
            for anomaly in anomalies:
                stats.anomaly_counts[anomaly] += 1

            newest_end: datetime | None = None
            for point in points:
                start = point.get("timeInterval_start")
                if start:
                    stats.interval_starts.add(start)

                end = point.get("timeInterval_end")
                if end:
                    parsed = parse_utc(end)
                    if parsed and (newest_end is None or parsed > newest_end):
                        newest_end = parsed

            if newest_end is not None:
                stats.lag_seconds.append(
                    (fetched_at - newest_end).total_seconds())

    return stats

# ---------------------------------------------------------------- gaps


def find_interval_gaps(
    interval_starts: set[str],
) -> list[tuple[datetime, datetime, int]]:
    """
    Find breaks in grid interval coverage.

    Compares observed timestamps to each other rather than to an expected
    count, so DST transitions (23- and 25-hour Dutch days) need no handling.
    """
    times = []
    for value in interval_starts:
        parsed = parse_utc(value)
        if parsed:
            times.append(parsed)
    times.sort()

    if len(times) < 2:
        return []

    expected = timedelta(seconds=INTERVAL_SECONDS)
    gaps: list[tuple[datetime, datetime, int]] = []

    for earlier, later in zip(times, times[1:]):
        delta = later - earlier
        if delta > expected:
            missing = int(delta.total_seconds() // INTERVAL_SECONDS) - 1
            gaps.append((earlier, later, missing))

    return gaps


def find_fetch_gaps(
    fetch_times: list[datetime],
) -> list[tuple[datetime, datetime, float]]:
    """
    Find periods where this pipeline was not polling.

    Distinguishes 'we were down' from 'TenneT published nothing', which
    determines whether an interval gap is worth spending recovery quota on.
    """
    if len(fetch_times) < 2:
        return []

    times = sorted(fetch_times)
    threshold = POLL_INTERVAL_SECONDS * FETCH_GAP_TOLERANCE
    gaps: list[tuple[datetime, datetime, float]] = []

    for earlier, later in zip(times, times[1:]):
        seconds = (later - earlier).total_seconds()
        if seconds > threshold:
            gaps.append((earlier, later, seconds))

    return gaps


def gap_is_explained(
    gap_start: datetime,
    gap_end: datetime,
    fetch_gaps: list[tuple[datetime, datetime, float]],
) -> bool:
    """True if a downtime window overlaps this interval gap."""
    return any(
        fetch_start < gap_end and fetch_end > gap_start
        for fetch_start, fetch_end, _ in fetch_gaps
    )

# ---------------------------------------------------------------- report


def report(stats: CaptureStats) -> int:
    """Write a human-readable summary to stdout. Returns an exit code."""
    problems = 0

    print("=" * 64)
    print("ONBALANS BRONZE INSPECTION")
    print("=" * 64)

    if stats.responses == 0:
        print("\nNo readable records found.")
        return 2

    first, last = min(stats.fetch_times), max(stats.fetch_times)
    span = last - first
    span_seconds = span.total_seconds()
    expected_polls = span_seconds / POLL_INTERVAL_SECONDS if span_seconds else 0

    print(f"\nFiles read            : {stats.files}")
    print(f"Responses             : {stats.responses:,}")
    print(f"First fetch (UTC)     : {first:%Y-%m-%d %H:%M:%S}")
    print(f"Last fetch  (UTC)     : {last:%Y-%m-%d %H:%M:%S}")
    print(f"Elapsed               : {span}")
    if expected_polls:
        completeness = stats.responses / expected_polls
        print(f"Poll completeness     : {completeness:.1%}")
        if completeness < 0.95:
            problems += 1

    if stats.unreadable_records:
        print(f"Unreadable records    : {stats.unreadable_records:,}")
        problems += 1

    print("\n-- Outcomes --")
    for status, count in sorted(
        stats.status_counts.items(), key=lambda kv: str(kv[0])
    ):
        print(f"  {str(status):<14} {count:>9,}  ({count / stats.responses:.1%})")

    if stats.error_counts:
        problems += 1
        print("\n-- Errors --")
        for name, count in stats.error_counts.most_common():
            print(f"  {name:<30} {count:>7,}")

    if stats.write_failures:
        problems += 1
        print(f"\n  !! Disk write failures: {stats.write_failures:,}")

    if stats.anomaly_counts:
        problems += 1
        print("\n-- Structure anomalies --")
        for name, count in stats.anomaly_counts.most_common():
            print(f"  {name:<42} {count:>7,}")

    if stats.durations_ms:
        print("\n-- Response time (ms) --")
        for label, pct in (("p50", 50), ("p95", 95), ("p99", 99)):
            print(f"  {label}   {percentile(stats.durations_ms, pct):>9.0f}")
        print(f"  max   {max(stats.durations_ms):>9,}")

    if stats.lag_seconds:
        print("\n-- Publication lag (seconds behind real time) --")
        for label, pct in (("p50", 50), ("p95", 95)):
            print(f"  {label}   {percentile(stats.lag_seconds, pct):>9.1f}")
        print(f"  min   {min(stats.lag_seconds):>9.1f}")
        print(f"  max   {max(stats.lag_seconds):>9.1f}")

    fetch_gaps = find_fetch_gaps(stats.fetch_times)
    print("\n-- Polling continuity --")
    if not fetch_gaps:
        print("  No polling gaps detected.")
    else:
        problems += 1
        downtime = sum(seconds for _, _, seconds in fetch_gaps)
        print(
            f"  Polling gaps: {len(fetch_gaps)} "
            f"({timedelta(seconds=int(downtime))} total downtime)"
        )
        for start, end, seconds in fetch_gaps[:MAX_GAPS_SHOWN]:
            print(
                f"    {start:%Y-%m-%d %H:%M:%S} -> {end:%H:%M:%S}  "
                f"({timedelta(seconds=int(seconds))})"
            )
        if len(fetch_gaps) > MAX_GAPS_SHOWN:
            print(f"    ... and {len(fetch_gaps) - MAX_GAPS_SHOWN} more")

    print("\n-- Grid interval coverage --")
    print(f"  Unique intervals      : {len(stats.interval_starts):,}")
    print(f"  Total points seen     : {stats.total_points:,}")
    if stats.total_points:
        ratio = len(stats.interval_starts) / stats.total_points
        print(f"  Unique / total points : {ratio:.4f}")
        if ratio > 0.5:
            problems += 1
            print("  !! Expected ~0.007. A high ratio means responses are not")
            print("     overlapping, so the outage tolerance this design")
            print("     relies on does not exist.")

    interval_gaps = find_interval_gaps(stats.interval_starts)
    if not interval_gaps:
        print("  No interval gaps detected.")
    else:
        problems += 1
        recoverable = 0
        unexplained = 0
        for earlier, later, missing in interval_gaps:
            if gap_is_explained(earlier, later, fetch_gaps):
                recoverable += missing
            else:
                unexplained += missing

        total = recoverable + unexplained
        print(f"  Gaps: {len(interval_gaps)} ({total:,} intervals missing)")
        print(
            f"    caused by pipeline downtime : {recoverable:,}  (recoverable)")
        print(
            f"    no downtime explanation     : {unexplained:,}  "
            f"(TenneT published nothing — not worth recovery quota)"
        )

        for earlier, later, missing in interval_gaps[:MAX_GAPS_SHOWN]:
            tag = "ours" if gap_is_explained(
                earlier, later, fetch_gaps) else "theirs"
            print(
                f"    [{tag:<6}] {earlier:%Y-%m-%d %H:%M:%S} -> "
                f"{later:%H:%M:%S}  ({missing:,} missing)"
            )
        if len(interval_gaps) > MAX_GAPS_SHOWN:
            print(f"    ... and {len(interval_gaps) - MAX_GAPS_SHOWN} more")

    if stats.identical_consecutive_bodies:
        problems += 1
        share = stats.identical_consecutive_bodies / stats.responses
        print(
            f"\n  !! Identical consecutive bodies: "
            f"{stats.identical_consecutive_bodies:,} ({share:.1%})"
        )
        print(
            "     Possible caching or a stalled feed — investigate before "
            "trusting this data."
        )

    print()
    if problems:
        print(f"{problems} problem area(s) detected.")
    else:
        print("No problems detected.")
    print()

    return 1 if problems else 0

# ---------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inspect the Onbalans bronze layer."
    )
    parser.add_argument(
        "--date", help="Inspect a single UTC date, e.g. 2026-08-23")
    parser.add_argument(
        "--raw-dir", type=Path, default=RAW_DIR, help="Bronze root directory"
    )
    args = parser.parse_args()

    if not args.raw_dir.exists():
        logger.error("%s does not exist. Has the tailer run?", args.raw_dir)
        return 2

    paths = list(iter_bronze_files(args.raw_dir, args.date))
    if not paths:
        logger.error("No bronze files matched.")
        return 2

    return report(analyse(paths))


if __name__ == "__main__":
    sys.exit(main())
