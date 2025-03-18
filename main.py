from actionManipulation
from utils.loggers import get_logger

logger=get_logger("status_logger")

if __name__ == "__main__":
    logger.info("Starting main.py")
    actionManipulation.main()
    logger.info("Finished main.py")