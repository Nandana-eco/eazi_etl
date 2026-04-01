import logging
import os
from logging.handlers import TimedRotatingFileHandler

def setup_logger():
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger("etl_incremental")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    log_file = os.path.join(log_dir, "etl_incremental.log")

    # File rotation
    file_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=7)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger