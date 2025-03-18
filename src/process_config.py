import mysql.connector
from datetime import datetime, timedelta
from utils.db_connection import create_connection
from utils.logger import get_logger

# Initialize logger
logger = get_logger('task_status_logger')

def fetch_and_update_process_config():
    try:
        # Establish database connection
        conn = create_connection()
        if not conn:
            logger.error("Failed to connect to database.")
            return
        
        cursor = conn.cursor()

        # Step 1: Fetch Process_Config_param from Process_Config_dt
        cursor.execute("SELECT Process_Config_param FROM Process_Config_dt WHERE Process_Config_id = 1")
        process_config_param = cursor.fetchone()

        if process_config_param:
            process_config_param = process_config_param[0]

            # Step 2: Calculate the time frame
            current_process_time = datetime.now()
            last_process_time = current_process_time - timedelta(hours=process_config_param)

            # Step 3: Fetch data from DEBT_CUST_DETAIL
            query = """
            SELECT * FROM DEBT_CUST_DETAIL
            WHERE ASSET_CREATED_DTM >= %s
            """
            cursor.execute(query, (last_process_time,))
            debt_cust_details = cursor.fetchall()

            # Log the fetched data (you can add your logic here)
            logger.info(f"Fetched {len(debt_cust_details)} records from DEBT_CUST_DETAIL.")
            for row in debt_cust_details:
                logger.debug(f"Record: {row}")

            # Step 4: Update the Process_Config_dt table with the new last_process_time
            update_query = """
            UPDATE Process_Config_dt
            SET end_dat = %s
            WHERE Process_Config_id = 1
            """
            cursor.execute(update_query, (current_process_time,))
            conn.commit()

            logger.info("Process configuration updated successfully.")

        else:
            logger.warning("No process configuration found.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

# Run the function
if __name__ == "__main__":
    fetch_and_update_process_config()
