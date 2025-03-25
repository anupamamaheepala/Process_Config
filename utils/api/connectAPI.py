import configparser


def read_api_config(config_path='./config/apiConfig.ini'):
    """
    Read the API URL from the config file.
    
    Args:
        config_path (str): Path to the config file.
    
    Returns:
        str: API URL from the config file.
    
    Raises:
        FileNotFoundError: If the configuration file is not found.
        configparser.Error: If there's an issue reading the configuration.
    """
    config = configparser.ConfigParser()
    config.read(config_path)
    
    try:
        api_url = config.get('API', 'api_url')
        return api_url
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"Error reading API configuration: {e}")
        raise