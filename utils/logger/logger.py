import logging
from logging import config

from utils.filePath.filePath import get_filePath

def setup_logging():
    config_file = get_filePath("logConfig")

    try:
        config.fileConfig(config_file)
    except Exception as e:
        print(f"Error setting up logging: {e}")

def get_logger(logger_name):
    """Retrieve a logger by name."""
    return logging.getLogger(logger_name)

# Setup logging on module import
setup_logging()