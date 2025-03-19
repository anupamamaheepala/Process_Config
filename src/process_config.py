from datetime import datetime
from utils.connectSQL import create_connection
from utils.logger import get_logger
import json

# Initialize logger
logger = get_logger('task_status_logger')

def fetch_data_by_account(account_num):
    """
    Fetch data from MySQL for a specific Account_Num.
    """
    try:
        # Establish MySQL connection
        mysql_connection = create_connection()
        if not mysql_connection:
            logger.error("Failed to connect to MySQL database.")
            return None

        # Fetch data from MySQL for the given Account_Num
        query = """
        SELECT * FROM debt_cust_detail
        WHERE Account_Num = %s
        """
        cursor = mysql_connection.cursor(dictionary=True)
        cursor.execute(query, (account_num,))
        rows = cursor.fetchall()

        return rows

    except Exception as e:
        logger.error(f"An error occurred while fetching data: {e}")
        return None
    finally:
        if mysql_connection and mysql_connection.is_connected():
            cursor.close()
            mysql_connection.close()

def map_to_json(data):
    """
    Map fetched data to the required JSON format.
    """
    if not data:
        return None

    # Initialize the JSON structure
    json_data = {
        "Incident_Id": data[0]["Incident_Id"],
        "Account_Num": data[0]["Account_Num"],
        "Arrears": data[0]["Arrears"],
        "Created_By": data[0]["Created_By"],
        "Created_Dtm": data[0]["Created_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Created_Dtm"] else None,
        "Incident_Status": data[0]["Incident_Status"],
        "Incident_Status_Dtm": data[0]["Incident_Status_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Incident_Status_Dtm"] else None,
        "Status_Description": data[0]["Status_Description"],
        "File_Name_Dump": data[0]["File_Name_Dump"],
        "Batch_Id": data[0]["Batch_Id"],
        "Batch_Id_Tag_Dtm": data[0]["Batch_Id_Tag_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Batch_Id_Tag_Dtm"] else None,
        "External_Data_Update_On": data[0]["External_Data_Update_On"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["External_Data_Update_On"] else None,
        "Filtered_Reason": data[0]["Filtered_Reason"],
        "Export_On": data[0]["Export_On"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Export_On"] else None,
        "File_Name_Rejected": data[0]["File_Name_Rejected"],
        "Rejected_Reason": data[0]["Rejected_Reason"],
        "Incident_Forwarded_By": data[0]["Incident_Forwarded_By"],
        "Incident_Forwarded_On": data[0]["Incident_Forwarded_On"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Incident_Forwarded_On"] else None,
        "Contact_Details": [],
        "Product_Details": [],
        "Customer_Details": {
            "Customer_Name": data[0]["Customer_Name"],
            "Company_Name": data[0]["Company_Name"],
            "Company_Registry_Number": data[0]["Company_Registry_Number"],
            "Full_Address": data[0]["Full_Address"],
            "Zip_Code": data[0]["Zip_Code"],
            "Customer_Type_Name": data[0]["Customer_Type_Name"],
            "Nic": data[0]["Nic"],
            "Customer_Type_Id": data[0]["Customer_Type_Id"]
        },
        "Account_Details": {
            "Account_Status": data[0]["Account_Status"],
            "Acc_Effective_Dtm": data[0]["Acc_Effective_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Acc_Effective_Dtm"] else None,
            "Acc_Activate_Date": data[0]["Acc_Activate_Date"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Acc_Activate_Date"] else None,
            "Credit_Class_Id": data[0]["Credit_Class_Id"],
            "Credit_Class_Name": data[0]["Credit_Class_Name"],
            "Billing_Centre": data[0]["Billing_Centre"],
            "Customer_Segment": data[0]["Customer_Segment"],
            "Mobile_Contact_Tel": data[0]["Mobile_Contact_Tel"],
            "Daytime_Contact_Tel": data[0]["Daytime_Contact_Tel"],
            "Email_Address": data[0]["Email_Address"],
            "Last_Rated_Dtm": data[0]["Last_Rated_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Last_Rated_Dtm"] else None
        },
        "Last_Actions": {
            "Billed_Seq": data[0]["Billed_Seq"],
            "Billed_Created": data[0]["Billed_Created"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Billed_Created"] else None,
            "Payment_Seq": data[0]["Payment_Seq"],
            "Payment_Created": data[0]["Payment_Created"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Payment_Created"] else None
        },
        "Marketing_Details": {
            "ACCOUNT_MANAGER": data[0]["ACCOUNT_MANAGER"],
            "CONSUMER_MARKET": data[0]["CONSUMER_MARKET"],
            "Informed_To": data[0]["Informed_To"],
            "Informed_On": data[0]["Informed_On"].strftime("%Y-%m-%d %H:%M:%S") if data[0]["Informed_On"] else None
        },
        "Action": data[0]["Action"],
        "Validity_period": data[0]["Validity_period"],
        "Remark": data[0]["Remark"],
        "updatedAt": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "Rejected_By": data[0]["Rejected_By"],
        "Rejected_Dtm": data[0]["Rejected_Dtm"].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if data[0]["Rejected_Dtm"] else None,
        "Arrears_Band": data[0]["Arrears_Band"],
        "Source_Type": data[0]["Source_Type"]
    }

    # Add Contact_Details and Product_Details
    for entry in data:
        contact_detail = {
            "Contact_Type": entry["Contact_Type"],
            "Contact": entry["Contact"],
            "Create_Dtm": entry["Create_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if entry["Create_Dtm"] else None,
            "Create_By": entry["Create_By"]
        }
        json_data["Contact_Details"].append(contact_detail)

        product_detail = {
            "Product_Label": entry["Product_Label"],
            "Customer_Ref": entry["Customer_Ref"],
            "Product_Seq": entry["Product_Seq"],
            "Equipment_Ownership": entry["Equipment_Ownership"],
            "Product_Id": entry["Product_Id"],
            "Product_Name": entry["Product_Name"],
            "Product_Status": entry["Product_Status"],
            "Effective_Dtm": entry["Effective_Dtm"].strftime("%Y-%m-%d %H:%M:%S") if entry["Effective_Dtm"] else None,
            "Service_Address": entry["Service_Address"],
            "Cat": entry["Cat"],
            "Db_Cpe_Status": entry["Db_Cpe_Status"],
            "Received_List_Cpe_Status": entry["Received_List_Cpe_Status"],
            "Service_Type": entry["Service_Type"],
            "Region": entry["Region"],
            "Province": entry["Province"]
        }
        
        json_data["Product_Details"].append(product_detail)

    return json_data

def main():
    """
    Main function to fetch data for a specific Account_Num and export it as JSON.
    """
    try:
        # Input: Account_Num
        account_num = "0000003746"


        # Fetch data from MySQL
        data = fetch_data_by_account(account_num)

        if data:
            # Map data to JSON
            json_data = map_to_json(data)

            # Write JSON to a file
            json_file_path = f"{account_num}_data.json"
            with open(json_file_path, "w") as json_file:
                json.dump(json_data, json_file, indent=4)

            logger.info(f"Data for Account_Num {account_num} exported to {json_file_path}.")
        else:
            logger.warning(f"No data found for Account_Num: {account_num}")

    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()