import configparser
from pymongo import MongoClient
import os
from utils.logger.logger import get_logger
from utils.custom_exceptions.customize_exceptions import DatabaseConnectionError
from utils.filePath.filePath import get_filePath

# Initialize logger
logger = get_logger("database_logger")

# Read configuration from databaseConfig.ini file
def get_db_connection():
    """
    Establishes a connection to the MongoDB database using the configuration file.

    :return: MongoDB database object.
    :raises DatabaseConnectionError: If the configuration file is missing or connection fails.
    """
    config_path = get_filePath("databaseConfig")
    if not os.path.exists(config_path):
        logger.error(f"Configuration file '{config_path}' not found.")
        raise DatabaseConnectionError(f"Configuration file '{config_path}' not found.")

    config = configparser.ConfigParser()
    config.read(config_path)

    if 'MONGODB' not in config:
        logger.error("'MONGODB' section not found in databaseConfig.ini")
        raise DatabaseConnectionError("'MONGODB' section not found in databaseConfig.ini")

    mongo_uri = config['MONGODB'].get('MONGO_URL', '').strip()
    db_name = config['MONGODB'].get('DATABASE_NAME', '').strip()

    if not mongo_uri or not db_name:
        logger.error("Missing MONGO_URL or DATABASE_NAME in databaseConfig.ini")
        raise DatabaseConnectionError("Missing MONGO_URL or DATABASE_NAME in databaseConfig.ini")

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        return db
    except Exception as db_connection_error:
        logger.error(f"Error connecting to MongoDB: {db_connection_error}")
        raise DatabaseConnectionError(f"Error connecting to MongoDB: {db_connection_error}")

# Get a specific collection
def get_mongo_collection(collection_name):
    """
    Returns a specific collection after establishing a DB connection.

    :param collection_name: Name of the MongoDB collection to fetch.
    :return: MongoDB collection object.
    :raises DatabaseConnectionError: If the database connection fails.
    """
    db = get_db_connection()
    if db is None:
        logger.error("Database connection failed. Exiting...")
        raise DatabaseConnectionError("Database connection failed. Exiting...")

    return db[collection_name]
