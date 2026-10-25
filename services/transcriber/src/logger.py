import logging
import os
from pathlib import Path


def setup_logging(name: str = "listener") -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # In dev, also log to .cache/logs/
    if os.getenv("ENVIRONMENT") == "dev":
        log_dir = Path(".cache/logs")
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / f"{name}.log")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    return logger


logger = setup_logging()
