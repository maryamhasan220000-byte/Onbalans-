"""
Onbalans loader: bronze files -> raw.balance_delta.

Flattens the nested TenneT response into one row per grid measurement. Values
are stored exactly as TenneT sent them (text); casting, null handling and
business logic all happen later in dbt, where a mistake costs a `dbt run`
rather than a full reload.

Three properties this guarantees:

  idempotent  Running twice produces the same result as running once. Every
              write is an upsert keyed on interval_start.
  resumable   Completed files are recorded, so a crash mid-run continues
              rather than restarting.
  atomic      Each file is one transaction: either all its rows land and the
              file is marked done, or neither happens.

Deduplication happens in Python before the database is touched. Each interval
arrives in ~150 overlapping responses, so collapsing them in memory turns
~45,000 inserts per file into ~450.

Usage:
    python src/loader.py
    python src/loader.py --reload          # ignore loaded_files, re-read all
    python src/loader.py --date 2026-08-24 # one UTC date only
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from inspect_bronze import extract_points, iter_bronze_files, iter_records, parse_utc
from logging_setup import configure_logging

# ---------------------------------------------------------------- config

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("DATABASE_URL not found. Check .env at the project root.")

RAW_DIR = Path("data/raw")
BATCH_SIZE = 1000
MAX_RESTATEMENT_WARNINGS = 20

logger = configure_logging("loader")

# Field names exactly as TenneT sends them, paired with our column names.
# Declared once so the INSERT, the UPDATE and the row builder can never
# drift apart.
FIELD_MAP: list[tuple[str, str]] = [
    ("timeInterval_start", "interval_start"),
    ("timeInterval_end", "interval_end"),
    ("sequence", "sequence"),
    ("power_afrr_in", "power_afrr_in"),
    ("power_afrr_out", "power_afrr_out"),
    ("power_igcc_in", "power_igcc_in"),
    ("power_igcc_out", "power_igcc_out"),
    ("power_mfrrda_in", "power_mfrrda_in"),
    ("power_mfrrda_out", "power_mfrrda_out"),
    ("power_picasso_in", "power_picasso_in"),
    ("power_picasso_out", "power_picasso_out"),
    ("power_mari_in", "power_mari_in"),
    ("power_mari_out", "power_mari_out"),
    ("max_upw_regulation_price", "max_upw_regulation_price"),
    ("min_downw_regulation_price", "min_downw_regulation_price"),
    ("mid_price", "mid_price"),
]
assert FIELD_MAP[0][0] == "timeInterval_start",(
    "interval_start must stay first: it is the primary key, the dedup key,"
    "and row[0] throughtout collect_file"
)
VALUE_SLICE = slice(1, len(FIELD_MAP))

DATA_COLUMNS = [column for _, column in FIELD_MAP]
ALL_COLUMNS = DATA_COLUMNS + ["fetched_at", "source_file"]

# Columns compared to decide whether a conflicting row is a genuine
# restatement. Excludes the key itself and all lineage columns: a row
# refetched later is not a change.
VALUE_COLUMNS = [c for c in DATA_COLUMNS if c != "interval_start"]


def build_upsert_sql() -> str:
    """
    Build the INSERT ... ON CONFLICT statement from FIELD_MAP.

    DO UPDATE rather than DO NOTHING, so a restatement by TenneT is captured
    rather than silently discarded. The WHERE clause means an identical row
    (the usual case, ~149 times in 150) is not rewritten, so rowcount
    reflects real changes only.


    """
    columns = ", ".join(ALL_COLUMNS)
    placeholders = ", ".join(["%s"] * len(ALL_COLUMNS))

    updates = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in VALUE_COLUMNS + ["fetched_at", "source_file"]
    )
    updates += ", loaded_at = now()"

    differs = "\n           OR ".join(
        f"raw.balance_delta.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in VALUE_COLUMNS
    )

    return (
        f"INSERT INTO raw.balance_delta ({columns})\n"
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT (interval_start) DO UPDATE SET {updates}\n"
        f"WHERE {differs}"
    )


UPSERT_SQL = build_upsert_sql()

# ----------------------------------------------------------------- stats

def is_current_hour(path: Path) -> bool:
    now = datetime.now(timezone.utc)
    expected = f"responses-{now:%Y-%m-%dT%H}.jsonl"
    return path.name == expected 

@dataclass
class LoadStats:
    """Totals across the whole run."""

    files_seen: int = 0
    files_skipped: int = 0
    files_loaded: int = 0
    files_failed: int = 0
    records_read: int = 0
    points_read: int = 0
    rows_written: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    restatements_in_file: int = 0
    unparseable_points: int = 0
    anomalies: dict[str, int] = field(default_factory=dict)

# -------------------------------------------------------------- reading


def row_from_point(point: dict, fetched_at: datetime, source: str) -> tuple | None:
    """
    Turn one TenneT measurement into a row tuple, or None if unusable.

    Only interval_start is converted to a real type: it is the primary key
    and TimescaleDB partitions on it. Everything else passes through as the
    text TenneT sent.
    """
    interval_start = parse_utc(point.get("timeInterval_start", ""))
    if interval_start is None:
        return None

    values: list = [interval_start]
    for tennet_field, _ in FIELD_MAP[1:]:
        raw_value = point.get(tennet_field)
        values.append(None if raw_value is None else str(raw_value))

    values.append(fetched_at)
    values.append(source)
    return tuple(values)


def collect_file(path: Path, stats: LoadStats) -> tuple[dict, int, int]:
    """
    Read one bronze file into a dict keyed by interval_start.

    Returns (rows, record_count, point_count).

    The dict is what removes duplicates: each interval appears in ~150
    overlapping responses, and a key can only hold one value. The newest
    fetch wins, so a corrected value replaces the earlier one.
    """
    rows: dict[datetime, tuple] = {}
    fetch_times: dict[datetime, datetime] = {}
    record_count = 0
    point_count = 0
    source = str(path)

    for record in iter_records(path):
        if record.get("status_code") != 200:
            continue

        fetched_at = parse_utc(record.get("fetched_at", ""))
        if fetched_at is None:
            continue

        record_count += 1

        points, anomalies = extract_points(record.get("body"))
        point_count += len(points)

        for anomaly in anomalies:
            stats.anomalies[anomaly] = stats.anomalies.get(anomaly, 0) + 1

        for point in points:
            row = row_from_point(point, fetched_at, source)
            if row is None:
                stats.unparseable_points += 1
                continue

            key = row[0]
            existing = rows.get(key)

            if existing is None:
                rows[key] = row
                fetch_times[key] = fetched_at
                continue

            # Same interval seen again. Compare only the values TenneT sent,
            # ignoring lineage: a difference here means a restatement.
            if existing[VALUE_SLICE] != row[VALUE_SLICE]:
                stats.restatements_in_file += 1
                # Cap the logging: a systemic change would otherwise write
                # hundreds of thousands of lines and fill the disk.
                if stats.restatements_in_file <= MAX_RESTATEMENT_WARNINGS:
                    logger.warning(
                        "Restatement at %s in %s: values changed between fetches",
                        key, path.name,
                    )
                elif stats.restatements_in_file == MAX_RESTATEMENT_WARNINGS + 1:
                    logger.warning(
                        "Further restatement warnings suppressed; see the "
                        "final count in the report."
                    )

            # Keep whichever was fetched most recently.
            if fetched_at >= fetch_times[key]:
                rows[key] = row
                fetch_times[key] = fetched_at

    return rows, record_count, point_count

# -------------------------------------------------------------- writing


def load_file(
    conn: psycopg.Connection,
    path: Path,
    stats: LoadStats,
    mark_complete: bool,
) -> None:
    """
    Load one file inside a single transaction.

    Either every row lands and the file is recorded, or nothing happens.
    A half-loaded file that was marked complete would leave a permanent,
    invisible gap.
    """
    rows, record_count, point_count = collect_file(path, stats)

    stats.records_read += record_count
    stats.points_read += point_count

    if not rows:
        logger.warning("%s produced no rows; not marking complete.", path.name)
        return

    ordered = list(rows.values())

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM raw.balance_delta")
            before = cur.fetchone()[0]

            affected = 0
            for start in range(0, len(ordered), BATCH_SIZE):
                batch = ordered[start:start + BATCH_SIZE]
                cur.executemany(UPSERT_SQL, batch)
                affected += max(0, cur.rowcount)

            cur.execute("SELECT count(*) FROM raw.balance_delta")
            after = cur.fetchone()[0]

            inserted = after - before
            updated = max(0, affected - inserted)

            if mark_complete:
                cur.execute(
                    "INSERT INTO raw.loaded_files (path, record_count, point_count) "
                    "VALUES (%s, %s, %s) "
                    "ON CONFLICT (path) DO UPDATE SET "
                    "loaded_at = now(), "
                    "record_count = EXCLUDED.record_count, "
                    "point_count = EXCLUDED.point_count",
                    (str(path), record_count, point_count),
                )

    stats.rows_written += len(ordered)
    stats.rows_inserted += inserted
    stats.rows_updated += updated
    stats.files_loaded += 1

    logger.info(
        "%s: %d responses, %d points -> %d unique (%d new, %d changed)%s",
        path.name, record_count, point_count, len(ordered),
        inserted, updated, "" if mark_complete else "  [not marked: newest file]",
    )


def already_loaded(conn: psycopg.Connection) -> set[str]:
    """Paths recorded as complete in a previous run."""
    with conn.cursor() as cur:
        cur.execute("SELECT path FROM raw.loaded_files")
        return {row[0] for row in cur.fetchall()}

# ----------------------------------------------------------------- main


def report(stats: LoadStats) -> None:
    """Human-readable summary to stdout. Diagnostics go to the log."""
    print("=" * 62)
    print("ONBALANS LOAD")
    print("=" * 62)
    print(f"\nFiles seen            : {stats.files_seen}")
    print(f"  already loaded      : {stats.files_skipped}")
    print(f"  loaded now          : {stats.files_loaded}")
    if stats.files_failed:
        print(f"  failed              : {stats.files_failed}")

    print(f"\nResponses read        : {stats.records_read:,}")
    print(f"Points read           : {stats.points_read:,}")
    print(f"Rows sent to database : {stats.rows_written:,}")
    if stats.points_read:
        saved = 1 - stats.rows_written / stats.points_read
        print(f"  dedup saved         : {saved:.1%} of inserts")

    print(f"\nRows inserted (new)   : {stats.rows_inserted:,}")
    print(f"Rows updated (changed): {stats.rows_updated:,}")

    if stats.restatements_in_file:
        print(f"\n  !! Restatements within a file: {stats.restatements_in_file:,}")
        print("     TenneT published different values for the same interval.")
    elif stats.points_read:
        print("\n  No restatements detected: TenneT values were stable.")

    if stats.unparseable_points:
        print(f"\n  Unparseable points   : {stats.unparseable_points:,}")

    if stats.anomalies:
        print("\n-- Structure anomalies --")
        for name, count in sorted(stats.anomalies.items(), key=lambda kv: -kv[1]):
            print(f"  {name:<42} {count:>7,}")

    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Load bronze JSONL into Postgres.")
    parser.add_argument("--date", help="Load a single UTC date, e.g. 2026-08-24")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--reload", action="store_true",
                        help="Ignore loaded_files and re-read every file")
    args = parser.parse_args()

    if not args.raw_dir.exists():
        logger.error("%s does not exist. Has the tailer run?", args.raw_dir)
        return 2

    paths = list(iter_bronze_files(args.raw_dir, args.date))
    if not paths:
        logger.error("No bronze files matched.")
        return 2

    # The newest file is still being appended to by the tailer, so it is
    # processed but never recorded as complete. Marking it would strand
    # every record the tailer writes for the rest of the hour.
    newest = paths[-1]

    stats = LoadStats()
    stats.files_seen = len(paths)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            done = set() if args.reload else already_loaded(conn)
            logger.info("%d files found, %d already loaded.", len(paths), len(done))

            for path in paths:
                if str(path) in done:
                    stats.files_skipped += 1
                    continue

                try:
                    load_file(conn, path, stats, mark_complete=not is_current_hour(path))
                except (psycopg.Error, OSError) as exc:
                    # The transaction rolled back, so this file is untouched.
                    # Keep going: one bad file should not stop the rest.
                    stats.files_failed += 1
                    logger.error("Failed on %s: %s", path.name, exc)

    except psycopg.OperationalError as exc:
        logger.error("Cannot connect to the database: %s", exc)
        return 2

    report(stats)
    return 1 if stats.files_failed else 0


if __name__ == "__main__":
    sys.exit(main())