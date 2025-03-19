import configparser
from pymongo import MongoClient
import os
from utils.logger import get_logger
from utils.Custom_Exceptions import DatabaseConnectionError

# Collection names
CONFIG_HD_COLLECTION = "Process_Config_hd"
CONFIG_DT_COLLECTION = "Process_Config_dt"
FILTERED_DATA_COLLECTION = "Filtered_Debt_Cust_Detail_data"

# Initialize logger
logger = get_logger("database_logger")

# Read configuration from DB_Config.ini file
config_path = os.path.join(os.path.dirname(__file__), "../config/DB_Config.ini")
if not os.path.exists(config_path):
    logger.error(f"Configuration file '{config_path}' not found.")
    raise DatabaseConnectionError(f"Configuration file '{config_path}' not found.")

config = configparser.ConfigParser()
config.read(config_path)

def handle_database_error(error_message):
    """
    Handles database errors by logging the error and raising a custom exception.
    
    Args:
        error_message (str): The error message to log and raise.
    """
    logger.error(error_message)
    raise DatabaseConnectionError(error_message)

def create_mongo_connection():
    """
    Creates and returns a connection to the MongoDB database.
    
    Returns:
        MongoClient: A MongoDB client object.
    """
    try:
        mongo_uri = config['DATABASE'].get('MONGO_URI', '').strip()
        client = MongoClient(mongo_uri)
        logger.info("Successfully connected to MongoDB")
        return client
    except Exception as e:
        handle_database_error(f"Error while connecting to MongoDB: {e}")

def get_db_connection():
    """
    Establishes and returns a connection to the MongoDB database.
    
    Returns:
        db: A MongoDB database object.
    """
    mongo_uri = config['DATABASE'].get('MONGO_URI', '').strip()
    db_name = config['DATABASE'].get('DB_NAME', '').strip()

    if not mongo_uri or not db_name:
        handle_database_error("Missing MONGO_URI or DB_NAME in DB_Config.ini")

    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        return db
    except Exception as db_connection_error:
        handle_database_error(f"Error connecting to MongoDB: {db_connection_error}")

def get_collection(collection_name):
    """
    Returns a specific collection after establishing a DB connection.
    If the connection fails, logs an error and exits.
    
    Args:
        collection_name (str): The name of the collection to retrieve.
    
    Returns:
        collection: The MongoDB collection object.
    """
    db = get_db_connection()
    if db is None:
        handle_database_error("Database connection failed. Exiting...")

    return db[collection_name]

def establish_mongo_connection():
    """
    Establishes a MongoDB connection and retrieves necessary collections.
    
    Returns:
        tuple: A tuple containing the MongoDB client and the collections.
    """
    mongo_client = create_mongo_connection()
    if not mongo_client:
        handle_database_error("Failed to connect to MongoDB.")
    
    config_hd_collection = get_collection(CONFIG_HD_COLLECTION)
    config_dt_collection = get_collection(CONFIG_DT_COLLECTION)
    filtered_data_collection = get_collection(FILTERED_DATA_COLLECTION)
    
    return mongo_client, config_hd_collection, config_dt_collection, filtered_data_collection
