import logging
import os
from pathlib import Path

# Set the log directory
LOG_DIR = "logs"
Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

def setup_logger(name, log_file, level=logging.INFO):
    """Function to set up a logger with a given name and file."""
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create a file handler
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, log_file))
    file_handler.setFormatter(formatter)

    # Create a logger and set level
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)

    return logger
