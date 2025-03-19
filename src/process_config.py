from datetime import datetime, timedelta
from utils.connectDB import establish_mongo_connection
from utils.logger import get_logger

# Initialize logger
logger = get_logger('task_status_logger')

def fetch_and_update_process_config():
    try:
        # Establish MongoDB connection and retrieve collections
        mongo_client, config_hd_collection, config_dt_collection, filtered_data_collection = establish_mongo_connection()

        # Fetch Process_Config_param from Process_Config_dt
        config_dt = config_dt_collection.find_one({"Process_Config_id": 1})
        if not config_dt:
            logger.warning("No process configuration found.")
            return

        process_config_param = config_dt['Process_Config_param']

        # Calculate the time frame
        current_process_time = datetime.now()
        last_process_time = current_process_time - timedelta(minutes=process_config_param)

        # Fetch data from debt_cust_detail
        query = {
            "LOAD DATE": {"$gte": last_process_time}
        }
        debt_cust_details = filtered_data_collection.find(query)

        # Log the fetched data
        logger.info(f"Fetched {debt_cust_details.count()} records from debt_cust_detail.")
        for row in debt_cust_details:
            logger.debug(f"Record: {row}")

        # Update the Process_Config_dt table with the new last_process_time
        config_dt_collection.update_one(
            {"Process_Config_id": 1},
            {"$set": {"end_dat": current_process_time}}
        )

        logger.info("Process configuration updated successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if mongo_client:
            mongo_client.close()

