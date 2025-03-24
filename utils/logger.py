import logging
import logging.config
import os

# Load logger configuration from loggers.ini
config_file = os.path.join(os.path.dirname(__file__), "../config/loggers.ini")
logging.config.fileConfig(config_file)

def get_logger(name: str) -> logging.Logger:
    """
    Retrieves a logger with the specified name.
    
    Args:
        name (str): The name of the logger.
    
    Returns:
        logging.Logger: The configured logger.
    """
    return logging.getLogger(name)
