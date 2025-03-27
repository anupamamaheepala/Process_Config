import configparser
import platform
from pathlib import Path
import logging
 
def get_project_root():
    """
    Returns the project root directory dynamically.
    Assumes this script is inside the project directory.
    """
    return Path(__file__).resolve().parent.parent.parent  # Adjust depth if needed


def get_filePath(key):
    try:
       
        logger = logging.getLogger("System_logger")
        logging.basicConfig(level=logging.INFO)  # Configure logging if not already set

        # Initialize ConfigParser
        config = configparser.ConfigParser()

        # Get the project root dynamically
        project_root = get_project_root()

        # Construct the config file path
        config_file_path = project_root / "Config" / "filePathConfig.ini" 

        if not config_file_path.is_file():
            raise FileNotFoundError(f"Configuration file '{config_file_path}' not found.")

        config.read(str(config_file_path))  # read content of filePathConfig.ini

        # Determine the operating system
        os_type = platform.system().lower()  # 'windows' or 'linux'

        # Map OS type to key suffix
        os_suffix = "WIN" if os_type == 'windows' else "LIN" # change this into preffix

        # Define section mappings
        section_mapping = {
            "logConfig": "LogConfigFile_path",
            "databaseConfig": "DatabaseConfigFile_path",
            "filePathConfig": "FilePathConfigFile_path",
            "apiConfig": "ApiConfigFile_path"  
        }

        # Retrieve section name
        section = section_mapping.get(key)
        if not section:
            raise KeyError(f"Key '{key}' does not have a mapped section.")

        # Retrieve the path
        if section in config:
            requested_key = f"{os_suffix}_{key}"
            path = config[section].get(requested_key, "").strip()

            if not path:
                logger.error(f"Key '{requested_key}' not found in section '{section}'. Configuration file: {config_file_path}")
                raise KeyError(f"Key '{requested_key}' not found in section '{section}'.")

            return Path(path)  # Return as Path object for consistency

        else:
            raise KeyError(f"Section '{section}' is missing in the configuration file.")

    except FileNotFoundError as fnf_error:
        logger.error(f"Error: {fnf_error}")
        return False
    except KeyError as key_error:
        logger.error(f"Error: {key_error}")
        return False
    except Exception as e:
        logger.error(f"Error: Unexpected error occurred - {e}")
        return False
