"""
Onbalans ENTSO-E loader: bronze XML -> raw.day_ahead_prices.

Parses the day-ahead price documents written by ingest_entsoe.py and upserts
one row per market time unit. Prices are stored as text, exactly as ENTSO-E
sent them; casting happens in dbt, where a mistake costs a `dbt run` rather
than a full reload.

Three properties, matching the TenneT loader:

  idempotent  Running twice produces the same result as running once.
  resumable   Completed files are recorded, so a crash mid-run continues.
  atomic      Each file is one transaction: either all its rows land and the
              file is marked done, or neither happens.

Two things about the source format drive the parsing:

  NAMESPACE   Every tag carries an XML namespace, so find('Point') returns
              nothing. The namespace is read from the root tag rather than
              hardcoded, because ENTSO-E versions it (…:7:3 today).

  SPARSE      curveType A03 means "variable sized block": a price stays in
  POSITIONS   force until the next position appears. Missing positions are
              not missing data, they are unchanged prices, and must be
              forward-filled. A real document for 2026-09-06 held 92 points
              covering 96 quarter-hours.

Usage:
    python src/load_entsoe.py
    python src/load_entsoe.py --date 2026-09-06
    python src/load_entsoe.py --reload
"""

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from logging_setup import configure_logging

# ---------------------------------------------------------------- config

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("DATABASE_URL not found. Check .env at the project root.")

RAW_DIR = Path("data/raw_entsoe")
BATCH_SIZE = 1000
MAX_ANOMALY_WARNINGS = 20

logger = configure_logging("load_entsoe")

NAMESPACE_PATTERN = re.compile(r"\{(.+?)\}")
DURATION_PATTERN = re.compile(r"PT(?:(\d+)H)?(?:(\d+)M)?")

COLUMNS = [
    "period_start",
    "period_end",
    "position",
    "price_text",
    "resolution",
    "curve_type",
    "currency",
    "price_unit",
    "was_forward_filled",
    "document_mrid",
    "created_at_source",
    "source_file",
]

# Columns compared to decide whether a conflicting row genuinely changed.
# Excludes the key and all lineage: a file re-read later is not a change.
VALUE_COLUMNS = [
    "period_end", "position", "price_text", "resolution",
    "curve_type", "currency", "price_unit", "was_forward_filled",
]


def build_upsert_sql() -> str:
    """
    INSERT ... ON CONFLICT, generated from COLUMNS so the three lists that
    must agree cannot drift apart.

    DO UPDATE rather than DO NOTHING so a republished document is captured
    rather than silently discarded. The WHERE means an identical row is not
    rewritten, so rowcount reflects real changes only.

    IS DISTINCT FROM rather than <> because NULL <> NULL is NULL in SQL.
    """
    cols = ", ".join(COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in VALUE_COLUMNS + ["document_mrid",
                                                        "created_at_source",
                                                        "source_file"]
    )
    updates += ", loaded_at = now()"
    differs = "\n           OR ".join(
        f"raw.day_ahead_prices.{c} IS DISTINCT FROM EXCLUDED.{c}"
        for c in VALUE_COLUMNS
    )
    return (
        f"INSERT INTO raw.day_ahead_prices ({cols})\n"
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT (period_start) DO UPDATE SET {updates}\n"
        f"WHERE {differs}"
    )


UPSERT_SQL = build_upsert_sql()

# ----------------------------------------------------------------- stats


@dataclass
class LoadStats:
    files_seen: int = 0
    files_skipped: int = 0
    files_loaded: int = 0
    files_failed: int = 0
    documents_parsed: int = 0
    points_in_source: int = 0
    rows_built: int = 0
    rows_forward_filled: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    anomalies: dict[str, int] = field(default_factory=dict)

    def note(self, message: str) -> None:
        self.anomalies[message] = self.anomalies.get(message, 0) + 1

# ---------------------------------------------------------------- parsing


def namespace_of(root: ET.Element) -> dict[str, str]:
    """
    Read the XML namespace from the root tag.

    ElementTree stores it as {uri}tagname, so find('Point') silently returns
    nothing without it. Read rather than hardcoded because ENTSO-E versions
    the URI, and a version bump would otherwise break parsing with no error.
    """
    match = NAMESPACE_PATTERN.match(root.tag)
    return {"n": match.group(1)} if match else {}


def parse_iso_duration(text: str) -> timedelta | None:
    """Turn an ISO 8601 duration such as PT15M or PT60M into a timedelta."""
    if not text:
        return None
    match = DURATION_PATTERN.fullmatch(text.strip())
    if not match or not any(match.groups()):
        return None
    hours, minutes = match.group(1), match.group(2)
    return timedelta(hours=int(hours or 0), minutes=int(minutes or 0))


def parse_utc(text: str | None) -> datetime | None:
    """
    Parse an ENTSO-E timestamp such as 2026-09-05T22:00Z.

    The format omits seconds, which datetime.fromisoformat rejects on older
    Python versions, so the seconds are supplied when missing.
    """
    if not text:
        return None
    value = text.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        pass
    try:
        # 2026-09-05T22:00+00:00 -> add seconds
        return datetime.strptime(value, "%Y-%m-%dT%H:%M%z")
    except ValueError:
        return None


def text_of(element: ET.Element | None) -> str | None:
    """Return an element's text, or None if the element is absent or empty."""
    if element is None or element.text is None:
        return None
    stripped = element.text.strip()
    return stripped or None


def parse_document(xml_text: str, source: str,
                   stats: LoadStats) -> list[tuple]:
    """
    Turn one price document into a list of database rows.

    Returns one row per market time unit across the document's interval,
    forward-filling positions that curveType A03 omits. Never raises: a
    malformed document is recorded as an anomaly and yields no rows.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        stats.note(f"XML parse error: {type(exc).__name__}")
        logger.error("%s is not valid XML: %s", source, exc)
        return []

    ns = namespace_of(root)
    if not ns:
        stats.note("no XML namespace on root element")

    local_root = root.tag.split("}")[-1]
    if local_root != "Publication_MarketDocument":
        stats.note(f"unexpected root element: {local_root}")
        logger.warning("%s root is %s, not a price document", source, local_root)
        return []

    document_mrid = text_of(root.find("n:mRID", ns))
    created_at = parse_utc(text_of(root.find("n:createdDateTime", ns)))

    series_list = root.findall("n:TimeSeries", ns)
    if len(series_list) != 1:
        stats.note(f"TimeSeries count = {len(series_list)}")

    rows: list[tuple] = []

    for series in series_list:
        currency = text_of(series.find("n:currency_Unit.name", ns))
        price_unit = text_of(series.find("n:price_Measure_Unit.name", ns))
        curve_type = text_of(series.find("n:curveType", ns))

        if curve_type not in (None, "A01", "A03"):
            stats.note(f"unhandled curveType: {curve_type}")

        periods = series.findall("n:Period", ns)
        if len(periods) != 1:
            stats.note(f"Period count = {len(periods)}")

        for period in periods:
            rows.extend(
                parse_period(period, ns, source, stats,
                             currency, price_unit, curve_type,
                             document_mrid, created_at)
            )

    return rows


def parse_period(period: ET.Element, ns: dict[str, str], source: str,
                 stats: LoadStats, currency: str | None,
                 price_unit: str | None, curve_type: str | None,
                 document_mrid: str | None,
                 created_at: datetime | None) -> list[tuple]:
    """
    Expand one Period into one row per market time unit.

    Positions are sparse under curveType A03: a price holds until the next
    position appears. The expected count is derived from the interval and
    the resolution rather than assumed, so PT60M and PT15M both work and a
    23- or 25-hour DST day comes out with the right number of slots.
    """
    interval = period.find("n:timeInterval", ns)
    start = parse_utc(text_of(interval.find("n:start", ns))) if interval is not None else None
    end = parse_utc(text_of(interval.find("n:end", ns))) if interval is not None else None

    resolution_text = text_of(period.find("n:resolution", ns))
    resolution = parse_iso_duration(resolution_text or "")

    if start is None or end is None:
        stats.note("Period has no usable timeInterval")
        return []
    if resolution is None or resolution <= timedelta(0):
        stats.note(f"unusable resolution: {resolution_text!r}")
        return []

    span_seconds = (end - start).total_seconds()
    expected_slots = int(span_seconds // resolution.total_seconds())
    if expected_slots <= 0:
        stats.note("Period interval is zero or negative")
        return []

    # Collect the sparse points first.
    prices_by_position: dict[int, str] = {}
    for point in period.findall("n:Point", ns):
        position_text = text_of(point.find("n:position", ns))
        price_text = text_of(point.find("n:price.amount", ns))
        if position_text is None or price_text is None:
            stats.note("Point missing position or price.amount")
            continue
        try:
            position = int(position_text)
        except ValueError:
            stats.note(f"non-numeric position: {position_text!r}")
            continue
        if position in prices_by_position:
            stats.note(f"duplicate position {position}")
        prices_by_position[position] = price_text

    stats.points_in_source += len(prices_by_position)

    if not prices_by_position:
        stats.note("Period contains no usable points")
        return []

    highest = max(prices_by_position)
    if highest > expected_slots:
        stats.note(f"position {highest} exceeds expected {expected_slots} slots")
        expected_slots = highest

    if 1 not in prices_by_position:
        # Nothing to carry forward into slot 1; the series is unusable.
        stats.note("position 1 missing, cannot forward-fill from the start")
        return []

    rows: list[tuple] = []
    current_price: str | None = None

    for position in range(1, expected_slots + 1):
        supplied = prices_by_position.get(position)
        if supplied is not None:
            current_price = supplied
            was_filled = False
        else:
            was_filled = True
            stats.rows_forward_filled += 1

        period_start = start + resolution * (position - 1)
        period_end = period_start + resolution

        rows.append((
            period_start,
            period_end,
            position,
            current_price,
            resolution_text,
            curve_type,
            currency,
            price_unit,
            was_filled,
            document_mrid,
            created_at,
            source,
        ))

    return rows

# --------------------------------------------------------------- loading


def load_file(conn: psycopg.Connection, path: Path, stats: LoadStats) -> None:
    """
    Load one bronze document inside a single transaction.

    Either every row lands and the file is recorded, or nothing happens. A
    half-loaded file marked complete would leave a permanent, silent gap.
    """
    try:
        xml_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise OSError(f"cannot read {path}: {exc}") from exc

    rows = parse_document(xml_text, str(path), stats)

    if not rows:
        logger.warning("%s produced no rows; not marking complete.", path.name)
        return

    stats.documents_parsed += 1
    stats.rows_built += len(rows)

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.day_ahead_prices")
            before = cur.fetchone()[0]

            affected = 0
            for offset in range(0, len(rows), BATCH_SIZE):
                cur.executemany(UPSERT_SQL, rows[offset:offset + BATCH_SIZE])
                affected += max(0, cur.rowcount)

            cur.execute("SELECT count(*) FROM raw.day_ahead_prices")
            after = cur.fetchone()[0]

            inserted = after - before
            updated = max(0, affected - inserted)

            cur.execute(
                "INSERT INTO raw.loaded_entsoe_files "
                "(path, row_count, forward_filled_count) VALUES (%s, %s, %s) "
                "ON CONFLICT (path) DO UPDATE SET loaded_at = now(), "
                "row_count = EXCLUDED.row_count, "
                "forward_filled_count = EXCLUDED.forward_filled_count",
                (str(path), len(rows),
                 sum(1 for r in rows if r[COLUMNS.index("was_forward_filled")])),
            )

    stats.rows_inserted += inserted
    stats.rows_updated += updated
    stats.files_loaded += 1

    filled = sum(1 for r in rows if r[COLUMNS.index("was_forward_filled")])
    logger.info("%s: %d rows (%d forward-filled) -> %d new, %d changed",
                path.name, len(rows), filled, inserted, updated)


def already_loaded(conn: psycopg.Connection) -> set[str]:
    """Paths recorded as complete in a previous run."""
    with conn.cursor() as cur:
        cur.execute("SELECT path FROM raw.loaded_entsoe_files")
        return {row[0] for row in cur.fetchall()}


def iter_bronze_files(root: Path, day: str | None = None):
    """Yield bronze XML documents in chronological order."""
    pattern = f"date={day}/*.xml" if day else "date=*/*.xml"
    yield from sorted(root.glob(pattern))

# ------------------------------------------------------------------ main


def report(stats: LoadStats) -> None:
    print("=" * 60)
    print("ENTSO-E LOAD")
    print("=" * 60)
    print(f"\nFiles seen            : {stats.files_seen}")
    print(f"  already loaded      : {stats.files_skipped}")
    print(f"  loaded now          : {stats.files_loaded}")
    if stats.files_failed:
        print(f"  failed              : {stats.files_failed}")

    print(f"\nDocuments parsed      : {stats.documents_parsed}")
    print(f"Points in source XML  : {stats.points_in_source:,}")
    print(f"Rows built            : {stats.rows_built:,}")
    print(f"  forward-filled      : {stats.rows_forward_filled:,}")
    if stats.rows_built:
        share = stats.rows_forward_filled / stats.rows_built
        print(f"  filled share        : {share:.2%}")

    print(f"\nRows inserted (new)   : {stats.rows_inserted:,}")
    print(f"Rows updated (changed): {stats.rows_updated:,}")
    if stats.rows_updated == 0 and stats.rows_built:
        print("\n  No republished prices detected.")

    if stats.anomalies:
        print("\n-- Structure anomalies --")
        for name, count in sorted(stats.anomalies.items(), key=lambda kv: -kv[1]):
            print(f"  {name:<48} {count:>6,}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load ENTSO-E day-ahead price XML into Postgres."
    )
    parser.add_argument("--date", help="Load a single market day, YYYY-MM-DD")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--reload", action="store_true",
                        help="Ignore loaded_entsoe_files and re-read every file")
    args = parser.parse_args()

    if not args.raw_dir.exists():
        logger.error("%s does not exist. Has the ingester run?", args.raw_dir)
        return 2

    paths = list(iter_bronze_files(args.raw_dir, args.date))
    if not paths:
        logger.error("No bronze XML files matched.")
        return 2

    stats = LoadStats()
    stats.files_seen = len(paths)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            done = set() if args.reload else already_loaded(conn)
            logger.info("%d file(s) found, %d already loaded.",
                        len(paths), len(done))

            for path in paths:
                if str(path) in done:
                    stats.files_skipped += 1
                    continue
                try:
                    load_file(conn, path, stats)
                except (psycopg.Error, OSError) as exc:
                    stats.files_failed += 1
                    logger.error("Failed on %s: %s", path.name, exc)

    except psycopg.OperationalError as exc:
        logger.error("Cannot connect to the database: %s", exc)
        return 2

    report(stats)
    return 1 if stats.files_failed else 0


if __name__ == "__main__":
    sys.exit(main())




        




        
    
    










