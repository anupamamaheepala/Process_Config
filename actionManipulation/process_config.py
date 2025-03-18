''' 
process config logic 


scenario : the scenario is like this,  get the data collection from [debt_cus_details] by using time frame that i created logic as 
 (current_process_time - last process time) >= Process_Config_param [this is the time that we need to config from the user this is work like a trigger ]  


how do : there is a two tables called process_config_hd and process_config_dt 
table name 1 : Process_Config_hd	
parameters : {Process_Config_id ,created_dtm,Process_Description ,end_dat}

table name 2 : table :Process_Config_dt
parameters : {Process_Config_dt_id , Process_Config_id , Process_Config_param , end_dat}
 

also i need to update that parameters after this process runs 

create a funtion using python 
'''

import pyodbc
from datetime import datetime, timedelta

# Database connection details
server = '127.0.0.1'
database = 'process_config'
username = 'root'
password = ''
connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

def fetch_and_update_process_config():
    try:
        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Step 1: Fetch the Process_Config_param from Process_Config_dt
        cursor.execute("SELECT Process_Config_param FROM Process_Config_dt WHERE Process_Config_id = ?", (1,))
        process_config_param = cursor.fetchone()

        if process_config_param:
            process_config_param = process_config_param[0]

            # Step 2: Calculate the time frame
            current_process_time = datetime.now()
            last_process_time = current_process_time - timedelta(hours=process_config_param)

            # Step 3: Fetch data from DEBT_CUST_DETAIL based on the time frame
            query = """
            SELECT * FROM DEBT_CUST_DETAIL
            WHERE ASSET_CREATED_DTM >= ?
            """
            cursor.execute(query, last_process_time)
            debt_cust_details = cursor.fetchall()

            # Process the fetched data (you can add your logic here)
            for row in debt_cust_details:
                print(row)  # Example: Print each row

            # Step 4: Update the Process_Config_dt table with the new last_process_time
            update_query = """
            UPDATE Process_Config_dt
            SET end_dat = ?
            WHERE Process_Config_id = ?
            """
            cursor.execute(update_query, current_process_time, 1)
            conn.commit()

            print("Process configuration updated successfully.")

        else:
            print("No process configuration found.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the connection
        if conn:
            conn.close()

# Run the function
fetch_and_update_process_config()