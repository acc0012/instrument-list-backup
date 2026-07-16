import logging
from pathlib import Path

# Ensure log directory exists
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


def get_logger(file_name: str) -> logging.Logger:
    """
    Returns a configured logger instance that outputs to both file and console.
    """
    logger = logging.getLogger(file_name)

    # Avoid adding handlers multiple times if logger is reused
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # File handler
    log_file = LOG_DIR / f"{file_name}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
