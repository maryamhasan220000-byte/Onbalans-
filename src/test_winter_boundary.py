"""
One-off test: does the sequence counter reset at Dutch LOCAL midnight
(which shifts with daylight saving) or at a fixed UTC+2 offset?

August data cannot distinguish these, because the Netherlands IS UTC+2 then.
January can: the Netherlands is UTC+1, so the two hypotheses predict
boundaries one hour apart.

    reset at 23:00 UTC -> true local time (DST-aware)
    reset at 22:00 UTC -> fixed +2 offset

COSTS ONE of the 8 daily historical requests. The raw response is saved to
disk so re-analysis never costs a second request.
"""

import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("TENNET_API_KEY")
if not API_KEY:
    sys.exit("TENNET_API_KEY not found.")

URL = "https://api.tennet.eu/publications/v1/balance-delta-high-res"
SAVED = Path("notes/winter-boundary-response.json")

# Spans both candidate boundaries: 22:00 UTC and 23:00 UTC.
PARAMS = {
    "date_from": "2026-01-14T21:30:00Z",
    "date_to": "2026-01-14T23:30:00Z",
}


def fetch_or_load() -> dict | None:
    """Return the response, from disk if already fetched, else from the API."""
    if SAVED.exists():
        print(f"Using saved response from {SAVED} (no request spent).")
        return json.loads(SAVED.read_text(encoding="utf-8"))

    print("Spending ONE historical request...")
    response = requests.get(
        URL,
        headers={"apikey": API_KEY, "Accept": "application/json"},
        params=PARAMS,
        timeout=30,
    )
    print(f"Status: {response.status_code}")

    if response.status_code != 200:
        print(response.text[:500])
        return None

    SAVED.parent.mkdir(parents=True, exist_ok=True)
    SAVED.write_text(response.text, encoding="utf-8")
    print(f"Saved to {SAVED}")
    return response.json()


def collect_points(payload: dict) -> list[dict]:
    """Flatten every point from every TimeSeries entry."""
    points = []
    for series in payload.get("Response", {}).get("TimeSeries", []):
        if not isinstance(series, dict):
            continue
        for period in series.get("Period", []):
            if not isinstance(period, dict):
                continue
            for point in period.get("points", []):
                if isinstance(point, dict):
                    points.append(point)
    return points


def main() -> None:
    payload = fetch_or_load()
    if payload is None:
        print("\nNo data. January may be outside the retention window.")
        return

    series_count = len(payload.get("Response", {}).get("TimeSeries", []))
    points = collect_points(payload)
    points.sort(key=lambda p: p.get("timeInterval_start", ""))

    print(f"\nTimeSeries entries: {series_count}")
    print(f"Points returned   : {len(points)}")

    if not points:
        print("No points in the response.")
        return

    print(f"Range             : {points[0]['timeInterval_start']} "
          f"-> {points[-1]['timeInterval_start']}")

    print("\n-- Looking for the sequence reset --")
    reset_at = None
    previous = None

    for point in points:
        try:
            seq = int(point.get("sequence", -1))
        except (TypeError, ValueError):
            continue

        if previous is not None and seq < previous:
            reset_at = point["timeInterval_start"]
            print(f"  Reset: {previous} -> {seq} at {reset_at}")
            break
        previous = seq

    print("\n-- VERDICT --")
    if reset_at is None:
        print("  No reset found in this window. Both hypotheses questionable.")
        print(f"  Sequence ran {points[0].get('sequence')} "
              f"-> {points[-1].get('sequence')}")
    elif "T23:00" in reset_at:
        print("  Reset at 23:00 UTC -> TRUE LOCAL TIME (DST-aware).")
        print("  The boundary moves with Dutch clocks. DST handling required.")
    elif "T22:00" in reset_at:
        print("  Reset at 22:00 UTC -> FIXED +2 OFFSET.")
        print("  The boundary never moves. DST is irrelevant here.")
    else:
        print(f"  Reset at an unexpected time: {reset_at}")
        print("  Neither hypothesis holds. Investigate what happens then.")


if __name__ == "__main__":
    main()
