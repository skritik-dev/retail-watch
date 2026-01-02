import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] [%(module)s] %(message)s"

# Resolve project root safely
PROJECT_ROOT = Path(__file__).resolve().parent

def get_logger(
    name: str = "retail-watch",
    log_dir: str = "logs",
    log_file: str = "retail-watch.log",
    level: int = logging.INFO
):
    log_path = PROJECT_ROOT / log_dir
    log_path.mkdir(exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(LOG_FORMAT)

    # File handler
    file_handler = RotatingFileHandler(
        log_path / log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
