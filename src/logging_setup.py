import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
LOG_DIR = Path("logs")
LOG_FORMAT = "%(asctime)s.%(msecs)03dZ %(levelname)-8s %(name)s %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
MAX_BYTES = 10_000_000
BACKUP_COUNT = 5


def configure_logging(component: str, level: int = logging.INFO) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(component)
    logger.setLevel(level)
    if logger.handlers:
        return logger
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    formatter.converter = time.gmtime
    file_handler = RotatingFileHandler(
        LOG_DIR / f"{component}.log",
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger
