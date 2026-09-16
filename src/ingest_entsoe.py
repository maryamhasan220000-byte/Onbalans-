"""
Onbalans ENTSO-E day-ahead price ingester.

Fetches the day-ahead price document for one or more Dutch market days and
writes the raw XML to the bronze layer. Parsing happens later, in the loader.

Unlike the TenneT tailer, this data is not perishable: day-ahead prices are
decided at auction, published once, and archived permanently. So there is no
continuous process, no uptime requirement and no daily quota. One batch job,
run daily, able to backfill any range on demand.

Bronze layout mirrors the TenneT side:

    data/raw_entsoe/date=2026-09-06/day_ahead_2026-09-06.xml

The security token travels in the URL rather than a header, so every logged
URL is redacted before it reaches the log file.

Usage:
    python src/ingest_entsoe.py                          # tomorrow
    python src/ingest_entsoe.py --date 2026-09-06        # one Dutch day
    python src/ingest_entsoe.py --start 2026-08-01 --end 2026-09-06
    python src/ingest_entsoe.py --date 2026-09-06 --force   # refetch
"""

import argparse
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

from logging_setup import configure_logging

# ---------------------------------------------------------------- config

load_dotenv()

TOKEN = os.getenv("ENTSOE_TOKEN")
if not TOKEN:
    sys.exit("ENTSOE_TOKEN not found. Check .env at the project root.")

URL = "https://web-api.tp.entsoe.eu/api"

# EIC code for the Netherlands bidding zone. The dashes are literal.
NL_DOMAIN = "10YNL----------L"

# A44 = Price Document (day-ahead), per the ENTSO-E document type list.
DOCUMENT_TYPE = "A44"

DUTCH_TZ = ZoneInfo("Europe/Amsterdam")
RAW_DIR = Path("data/raw_entsoe")

REQUEST_TIMEOUT_SECONDS = 60      # XML documents are larger than TenneT's JSON
POLITE_DELAY_SECONDS = 1.0        # between requests when backfilling
MAX_ATTEMPTS = 3
BACKOFF_INITIAL_SECONDS = 5

logger = configure_logging("ingest_entsoe")

TOKEN_PATTERN = re.compile(r"(securityToken=)[^&]*")

# ---------------------------------------------------------------- helpers


def redact(url: str) -> str:
    """
    Remove the security token from a URL before logging it.

    ENTSO-E takes the token as a query parameter, so an unredacted URL in a
    log file is a working credential. Log files are far easier to share by
    accident than .env is.
    """
    return TOKEN_PATTERN.sub(r"\1REDACTED", url)


def dutch_day_bounds(day: date) -> tuple[datetime, datetime]:
    """
    Return the UTC start and end of one Dutch market day.

    The day-ahead market trades Dutch calendar days, so the document for
    6 September runs from Dutch midnight to Dutch midnight. In summer that
    is 22:00Z to 22:00Z; in winter 23:00Z to 23:00Z. Using a named timezone
    means daylight saving is handled by the standard library rather than a
    hardcoded offset, and the 23- and 25-hour DST days come out correct.
    """
    start_local = datetime(day.year, day.month, day.day, tzinfo=DUTCH_TZ)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def raw_path_for(day: date, root: Path = RAW_DIR) -> Path:
    """Bronze path for one Dutch market day."""
    return root / f"date={day:%Y-%m-%d}" / f"day_ahead_{day:%Y-%m-%d}.xml"


def looks_like_price_document(text: str) -> bool:
    """
    Cheap sanity check before writing to bronze.

    ENTSO-E returns HTTP 200 with an Acknowledgement_MarketDocument when it
    has no data for a period, so status code alone is not enough to know a
    fetch succeeded.
    """
    return "Publication_MarketDocument" in text and "<Point>" in text

# ---------------------------------------------------------------- fetching


def fetch_day(session: requests.Session, day: date) -> str | None:
    """
    Fetch one Dutch market day. Returns the XML, or None if unavailable.

    Retries on transient failures. Unlike the tailer, retrying here is worth
    the complexity: there is no overlapping window to cover a miss, and no
    daily quota that a retry would consume.
    """
    period_start, period_end = dutch_day_bounds(day)

    params = {
        "securityToken": TOKEN,
        "documentType": DOCUMENT_TYPE,
        "in_Domain": NL_DOMAIN,
        "out_Domain": NL_DOMAIN,
        "periodStart": period_start.strftime("%Y%m%d%H%M"),
        "periodEnd": period_end.strftime("%Y%m%d%H%M"),
    }

    backoff = BACKOFF_INITIAL_SECONDS

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.get(URL, params=params,
                                   timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.exceptions.RequestException as exc:
            logger.warning("%s attempt %d/%d failed: %s",
                           day, attempt, MAX_ATTEMPTS, exc)
            if attempt == MAX_ATTEMPTS:
                return None
            time.sleep(backoff)
            backoff *= 2
            continue

        if response.status_code == 200:
            if looks_like_price_document(response.text):
                logger.info("%s fetched (%d bytes) from %s",
                            day, len(response.text), redact(response.url))
                return response.text

            # 200 with an Acknowledgement document: no data published.
            logger.warning("%s returned 200 but no price data (not yet "
                           "published, or no data for this day)", day)
            return None

        if response.status_code == 401:
            # A bad token will not fix itself; stop rather than retry.
            logger.error("%s rejected: 401 Unauthorized. Check ENTSOE_TOKEN.", day)
            return None

        if response.status_code == 429 or response.status_code >= 500:
            logger.warning("%s got %d, attempt %d/%d, backing off %ss",
                           day, response.status_code, attempt,
                           MAX_ATTEMPTS, backoff)
            if attempt == MAX_ATTEMPTS:
                return None
            time.sleep(backoff)
            backoff *= 2
            continue

        logger.error("%s got %d: %s", day, response.status_code,
                     response.text[:300])
        return None

    return None


def write_bronze(day: date, xml_text: str, root: Path) -> Path:
    """
    Write the raw XML to bronze atomically.

    Writing to a temporary file and renaming means a crash mid-write cannot
    leave a half-written document that a later run would treat as complete.
    """
    path = raw_path_for(day, root)
    path.parent.mkdir(parents=True, exist_ok=True)

    temp = path.with_suffix(".xml.tmp")
    temp.write_text(xml_text, encoding="utf-8")
    temp.replace(path)

    return path

# ----------------------------------------------------------------- dates


def parse_day(value: str) -> date:
    """Parse a YYYY-MM-DD argument, with a clear error if it is malformed."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"{value!r} is not a date in YYYY-MM-DD form"
        )


def days_to_fetch(args: argparse.Namespace) -> list[date]:
    """Work out which Dutch market days this run should cover."""
    if args.date:
        return [args.date]

    if args.start:
        end = args.end or args.start
        if end < args.start:
            sys.exit("--end is before --start")
        span = (end - args.start).days
        return [args.start + timedelta(days=n) for n in range(span + 1)]

    # Default: tomorrow, which is what a daily scheduled run wants once
    # prices are published (around 13:00 CET for the following day).
    today_dutch = datetime.now(DUTCH_TZ).date()
    return [today_dutch + timedelta(days=1)]

# ------------------------------------------------------------------ main


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch ENTSO-E day-ahead prices for Dutch market days."
    )
    parser.add_argument("--date", type=parse_day,
                        help="A single Dutch market day, YYYY-MM-DD")
    parser.add_argument("--start", type=parse_day, help="Backfill from this day")
    parser.add_argument("--end", type=parse_day, help="Backfill to this day")
    parser.add_argument("--force", action="store_true",
                        help="Refetch days already present in bronze")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    args = parser.parse_args()

    if args.date and (args.start or args.end):
        sys.exit("Use --date or --start/--end, not both")

    days = days_to_fetch(args)
    logger.info("Fetching %d day(s): %s to %s", len(days), days[0], days[-1])

    fetched = skipped = failed = 0
    session = requests.Session()

    try:
        for index, day in enumerate(days):
            path = raw_path_for(day, args.raw_dir)

            if path.exists() and not args.force:
                logger.info("%s already in bronze, skipping", day)
                skipped += 1
                continue

            xml_text = fetch_day(session, day)
            if xml_text is None:
                failed += 1
            else:
                written = write_bronze(day, xml_text, args.raw_dir)
                logger.info("%s written to %s", day, written)
                fetched += 1

            # Be a polite client when backfilling. Not required by a
            # documented limit, but a burst of hundreds of requests is
            # the kind of thing that gets tokens throttled.
            if index < len(days) - 1:
                time.sleep(POLITE_DELAY_SECONDS)
    finally:
        session.close()

    print("=" * 56)
    print("ENTSO-E INGEST")
    print("=" * 56)
    print(f"\nDays requested : {len(days)}")
    print(f"  fetched      : {fetched}")
    print(f"  skipped      : {skipped}")
    if failed:
        print(f"  failed       : {failed}")
    print()

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())