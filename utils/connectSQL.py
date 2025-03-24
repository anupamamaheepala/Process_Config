import pymysql
import configparser
from utils.logger import get_logger

logger = get_logger("connectSQL")

def get_mysql_connection():
    """
    Establishes a MySQL connection using the configuration from DB_Config.ini.
    :return: A MySQL connection object.
    """
    config = configparser.ConfigParser()
    config.read(r'c:\Users\MSii\Downloads\Process_Config\config\DB_Config.ini')

    try:
        connection = pymysql.connect(
            host=config['DATABASE']['MYSQL_HOST'],
            database=config['DATABASE']['MYSQL_DATABASE'],
            user=config['DATABASE']['MYSQL_USER'],
            password=config['DATABASE']['MYSQL_PASSWORD']
        )
        logger.info("Successfully connected to MySQL.")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to MySQL: {e}")
        return None
