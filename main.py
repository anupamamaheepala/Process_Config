from actionManipulation import main as action_main
from utils.logger import get_logger

# Initialize the logger
logger = get_logger("status_logger")

if __name__ == "__main__":
    try:
        logger.info("Starting main.py")

        # Call the main function from actionManipulation
        action_main()

        logger.info("Finished main.py")

    except Exception as e:
        logger.error(f"An error occurred during the execution of main.py: {e}")
