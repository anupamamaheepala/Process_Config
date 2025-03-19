# from datetime import datetime, timedelta
# from utils.connectDB import establish_mongo_connection
# from utils.logger import get_logger

# # Initialize logger
# logger = get_logger('task_status_logger')

# def fetch_and_update_process_config():
#     try:
#         # Establish MongoDB connection and retrieve collections
#         mongo_client, config_hd_collection, config_dt_collection, filtered_data_collection = establish_mongo_connection()

#         # Fetch Process_Config_param from Process_Config_dt
#         config_dt = config_dt_collection.find_one({"Process_Config_id": 1})
#         if not config_dt:
#             logger.warning("No process configuration found.")
#             return

#         process_config_param = config_dt['Process_Config_param']

#         # Calculate the time frame
#         current_process_time = datetime.now()
#         last_process_time = current_process_time - timedelta(minutes=process_config_param)

#         # Fetch data from debt_cust_detail
#         query = {
#             "LOAD DATE": {"$gte": last_process_time}
#         }
#         debt_cust_details = filtered_data_collection.find(query)

#         # Log the fetched data
#         logger.info(f"Fetched {debt_cust_details.count()} records from debt_cust_detail.")
#         for row in debt_cust_details:
#             logger.debug(f"Record: {row}")

#         # Update the Process_Config_dt table with the new last_process_time
#         config_dt_collection.update_one(
#             {"Process_Config_id": 1},
#             {"$set": {"end_dat": current_process_time}}
#         )

#         logger.info("Process configuration updated successfully.")

#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#     finally:
#         if mongo_client:
#             mongo_client.close()



from datetime import datetime, timedelta
from utils.connectDB import establish_mongo_connection
from utils.logger import get_logger
import json

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
        debt_cust_details = list(filtered_data_collection.find(query))  # Convert cursor to list

        # Log the fetched data
        logger.info(f"Fetched {len(debt_cust_details)} records from debt_cust_detail.")
        for row in debt_cust_details:
            logger.debug(f"Record: {row}")

        # Prepare the JSON data structure
        json_data = []

        # Organize data by Account_Num
        account_map = {}
        for row in debt_cust_details:
            account_num = row.get("Account_Num")
            if account_num:
                if account_num not in account_map:
                    account_map[account_num] = {
                        "Account_Num": account_num,
                        "Details": []
                    }
                account_map[account_num]["Details"].append(row)

        # Populate json_data with the grouped data
        json_data = list(account_map.values())

        # Write to a JSON file
        json_file_path = "process_data.json"
        with open(json_file_path, "w") as json_file:
            json.dump(json_data, json_file, default=str, indent=4)

        logger.info(f"Data written to {json_file_path}.")

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
