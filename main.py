from utils.logger import get_logger
from src.process_config import fetch_and_update_process_config

# Initialize the logger
logger = get_logger("status_logger")

if __name__ == "__main__":
    logger.info("Starting main.py")
        
    fetch_and_update_process_config()

    logger.info("Finished main.py")

