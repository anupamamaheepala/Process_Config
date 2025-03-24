import pymysql
import configparser
from utils.logger.logger import get_logger
from utils.filePath.filePath import get_filePath

logger = get_logger("connectSQL")

def get_mysql_connection():
    """
    Establishes a MySQL connection using the configuration from DB_Config.ini.
    :return: A MySQL connection object.
    """
    config = configparser.ConfigParser()
    config_file = get_filePath("databaseConfig")

    try:
        config.read(config_file)
        if 'DATABASE' not in config:
            raise KeyError(f"'DATABASE' section missing in {config_file}")

        connection = pymysql.connect(
            host=config['DATABASE']['MYSQL_HOST'],
            database=config['DATABASE']['MYSQL_DATABASE'],
            user=config['DATABASE']['MYSQL_USER'],
            password=config['DATABASE']['MYSQL_PASSWORD']
        )
        logger.info("Successfully connected to MySQL.")
        return connection
    except KeyError as e:
        logger.error(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")
    return None
