import configparser
from urllib.parse import urlparse
from pathlib import Path
from functools import lru_cache
from utils.filePath.filePath import get_filePath

@lru_cache(maxsize=1)  # Simple caching
def read_api_config() -> str:
    """
    Retrieves and validates the API URL from configuration.
    
    Returns:
        Validated API URL string
        
    Raises:
        FileNotFoundError: If config file is missing
        ValueError: If config is invalid or URL malformed
    """
    config_path = get_filePath("apiConfig")
    
    # Validate file existence
    if not config_path or not config_path.is_file():
        raise FileNotFoundError(f"Config file missing at: {config_path}")

    # Read config
    config = configparser.ConfigParser()
    config.read(str(config_path))
    
    # Get and validate URL
    try:
        url = config.get('API', 'api_url', fallback='').strip()
        if not url:
            raise ValueError("API URL not configured")
        
        # Basic URL validation
        parsed = urlparse(url)
        if not all([parsed.scheme, parsed.netloc]):
            raise ValueError(f"Invalid URL format: {url}")
            
        return url
        
    except configparser.Error as e:
        raise ValueError(f"Invalid API configuration: {e}") from e