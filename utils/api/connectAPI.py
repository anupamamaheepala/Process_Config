import configparser
from utils.filePath.filePath import get_filePath

# Retrieve the API config file path
config_path = get_filePath("apiConfig")

def read_api_config(config_path=config_path):
    """
    Read the API URL from the config file.

    Args:
        config_path (Path): Path to the config file.

    Returns:
        str: API URL from the config file.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        KeyError: If the API URL is not found in the configuration.
    """
    if not config_path or not config_path.is_file():
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    config = configparser.ConfigParser()
    config.read(str(config_path))  # Convert Path object to string

    try:
        api_url = config.get('API', 'api_url')
        return api_url
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        raise KeyError(f"API URL not found in {config_path}") from e