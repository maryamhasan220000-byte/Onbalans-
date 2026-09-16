import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

from logging_setup import configure_logging

load_dotenv()

API_KEY = os.getenv("TENNET_API_KEY")
if not API_KEY:
    sys.exit("TENNET_API_KEY not found. Check .env exists at the project root.")
URL = "https://api.tennet.eu/publications/v1/balance-delta-high-res/latest"
POLL_INTERVAL_SECONDS = 12
REQUEST_TIMEOUT_SECONDS = 10
BACKOFF_INITIAL_SECONDS = 60
BACKOFF_MAX_SECONDS = 30 * 60

RAW_DIR = Path("data/raw")
LOG_DIR = Path("logs")
LOCK_FILE = Path("tailer.lock")

HEADERS = {"apikey": API_KEY, "Accept": "application/json"}
logger = configure_logging("tailer")

running = True


def handle_shutdown(signum: int, frame: object) -> None:
    global running
    running = False
    logger.info("Shutdown signal %s received; finishing cuurent cycle.", signum)


signal.signal(signal.SIGINT, handle_shutdown)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, handle_shutdown)


def raw_path_for(moment: datetime) -> Path:
    return (
        RAW_DIR
        / f"date={moment:%Y-%m-%d}"
        / f"hour={moment:%H}"
        / f"responses-{moment:%Y-%m-%dT%H}.jsonl"
    )


def append_record(record: dict, moment: datetime) -> None:
    path = raw_path_for(moment)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False) + "\n"

    with path.open("a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def poll_once(session: requests.Session) -> dict:
    fetched_at = datetime.now(timezone.utc)
    started = time.monotonic()

    record: dict = {
        "fetched_at": fetched_at.isoformat(),
        "url": URL,
        "status_code": None,
        "duration_ms": None,
        "error": None,
        "body": None,
    }
    try:
        response = session.get(URL, headers=HEADERS,
                               timeout=REQUEST_TIMEOUT_SECONDS)
        record["status_code"] = response.status_code
        record["body"] = response.text
    except requests.exceptions.RequestException as exc:
        record["error"] = f"{type(exc).__name__}: {exc}"

    record["duration_ms"] = round((time.monotonic() - started) * 1000)

    try:
        append_record(record, fetched_at)
    except OSError as exc:
        logger.error("WRITE FAILED (%s): %s", type(exc).__name__, exc)
        record["write_failed"] = True

    return record


def acquire_lock() -> None:
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        sys.exit(f"{LOCK_FILE} exists. Another tailer is running, or it crashed"
                 f"- delete the file if you are sure it is not.")
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)


def main() -> None:
    acquire_lock()
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Tailer started; polling every %ss.", POLL_INTERVAL_SECONDS)

    session = requests.Session()
    backoff = BACKOFF_INITIAL_SECONDS
    next_poll = time.monotonic()

    try:
        while running:
            record = poll_once(session)

            if record["status_code"] == 429:
                logger.warning("429 rate limited; backing off %ss.", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX_SECONDS)
                next_poll = time.monotonic()
                continue

            if record["status_code"] == 200:
                backoff = BACKOFF_INITIAL_SECONDS

            logger.info(
                "%s %sms",
                record["status_code"] or record["error"],
                record["duration_ms"]
            )

            next_poll += POLL_INTERVAL_SECONDS
            now = time.monotonic()

            if next_poll <= now:
                missed = int((now - next_poll) // POLL_INTERVAL_SECONDS) + 1
                next_poll += missed * POLL_INTERVAL_SECONDS
                logger.warning("Behind schedule; skipped %s slots(s)", missed)

            time.sleep(max(0, next_poll - now))
    finally:
        session.close()
        LOCK_FILE.unlink(missing_ok=True)
        logger.info("Tailer stopped cleanely")


if __name__ == "__main__":
    main()
