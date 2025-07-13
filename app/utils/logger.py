# utils/logger.py

import logging
from pathlib import Path

def setup_logger(log_path: Path, level: int = logging.DEBUG) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("climastation_logger")
    logger.setLevel(level)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s — %(levelname)s — %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
