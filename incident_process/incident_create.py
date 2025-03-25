import pymysql  # MySQL connector for interacting with a MySQL database.
from datetime import datetime, date  # Used for handling date and time operations.
from decimal import Decimal  # Provides precise decimal representation for financial and database-related calculations.
import json  # Enables working with JSON data (serialization and deserialization).
import pprint  # Pretty-printing tool for better debugging and readable data structures.
import requests  # Used for making HTTP requests, commonly for APIs or external services.

# Custom utility modules
from utils.database.connectSQL import get_mysql_connection  # Function to establish a connection to the MySQL database.
from utils.logger.logger import get_logger  # Logger utility for handling application logs.
from utils.api.connectAPI import read_api_config


logger = get_logger("incident_logger")

class create_incident:
    account_num = None
    incident_id = None
    mongo_data = None
    api_url = read_api_config()

    def __init__(self, account_num, incident_id):
        """
        Constructor for the create_incident class.
        
        Args:
            account_num (str): The account number associated with the incident.
            incident_id (int): The unique identifier for the incident.

        Initializes the incident details and prepares a MongoDB document structure.
        """
        self.account_num = str(account_num)
        self.incident_id = int(incident_id)
        self.mongo_data = self.initialize_mongo_doc(account_num, incident_id)

    def create_incident(self, payload):
        """
        Creates an incident using the provided payload.

        Args:
            payload (dict): JSON data containing incident details.

        This method currently reads customer details.
        """
        status = self.read_customer_details()
        # if status == "success":
        #     self.read_payment_details()
        # return self.client.post("/incidents", json=payload)

    def initialize_mongo_doc(self, account_num, incident_id):
        """
        Initializes a MongoDB document structure for storing incident-related data.

        Args:
            account_num (str): The account number associated with the incident.
            incident_id (int): The unique identifier for the incident.

        Returns:
            dict: A structured dictionary to store incident-related data.
        """
        self.mongo_data = {
            account_num: {
                "Doc_Version": "1.0",
                "Incident_Id": incident_id,
                "Account_Num": None,
                "Arrears": 0,
                "Created_By": "None",
                "Created_Dtm": None,
                "Incident_Status": "None",
                "Incident_Status_Dtm": None,
                "Status_Description": "None",
                "File_Name_Dump": "None",
                "Batch_Id": "None",
                "Batch_Id_Tag_Dtm": None,
                "External_Data_Update_On": None,
                "Filtered_Reason": "None",
                "Export_On": None,
                "File_Name_Rejected": "None",
                "Rejected_Reason": "None",
                "Incident_Forwarded_By": "None",
                "Incident_Forwarded_On": "None",
                "Contact_Details": [],
                "Product_Details": [],
                "Customer_Details": [],
                "Account_Details": [],
                "Last_Actions": [],
                "Marketing_Details": [],
                "Action": "string",
                "Validity_period": "None",
                "Remark": "string",
                "updatedAt": "None",
                "Rejected_By": "None",
                "Rejected_Dtm": "None",
                "Arrears_Band": "None",
                "Source_Type": "None"
            }
        }
        return self.mongo_data

    def read_customer_details(self):
        """
        Reads customer details from the MySQL database and updates the MongoDB document.

        Returns:
            str: "success" if the details were successfully retrieved and processed, otherwise "error".
        """
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Reading customer details for account number: {self.account_num}")
            # Establish a MySQL connection
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed. Skipping customer details retrieval.")
                return "error"
            # Create a cursor to execute SQL queries
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            # Fetch customer details based on the account number
            cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{self.account_num}'")

            rows = cursor.fetchall()
            
            if not rows:  # No account data found
                logger.error(f"No customer details found for account {self.account_num}")
                return False
            
            # Iterate over each row and process customer data
            for row in rows:
                customer_ref = row["CUSTOMER_REF"]
                account_num = row["ACCOUNT_NUM"]

                # Check if account number already exists in MongoDB data
                Check_Val = self.mongo_data[self.account_num]["Account_Num"]

                # Check if 'Created_By' is None
                if Check_Val is None or Check_Val == 'None':

                    self.mongo_data[self.account_num]["Account_Num"] = account_num
                    self.mongo_data[self.account_num]["incident_id"] = self.incident_id

                    self.mongo_data[self.account_num]["Created_By"] = "drs_admin"

                    contact_details_element = {
                        "Contact_Type": "email",
                        "Contact": row["TECNICAL_CONTACT_EMAIL"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    self.mongo_data[self.account_num]["Contact_Details"].append(contact_details_element)

                    contact_details_element = {
                        "Contact_Type": "mobile",
                        "Contact": row["MOBILE_CONTACT"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    self.mongo_data[self.account_num]["Contact_Details"].append(contact_details_element)

                    contact_details_element = {
                        "Contact_Type": "fix",
                        "Contact": row["WORK_CONTACT"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    self.mongo_data[self.account_num]["Contact_Details"].append(contact_details_element)

                    # Customer details element
                    customer_details_element = {
                        "Customer_Name": row["CONTACT_PERSON"],
                        "Company_Name": row["COMPANY_NAME"],
                        "Company_Registry_Number": None,
                        "Full_Address": row["ASSET_ADDRESS"],
                        "Zip_Code": row["ZIP_CODE"],
                        "Customer_Type_Name": None,
                        "Nic": row["NIC"],
                        "Customer_Type_Id": row["CUSTOMER_TYPE_ID"],
                        "Customer_Type": row["CUSTOMER_TYPE"],
                    }
                    self.mongo_data[self.account_num]["Customer_Details"].append(customer_details_element)

                    # Account details element
                    account_details_element = {
                        "Account_Status": row["ACCOUNT_STATUS_BSS"],
                        "Acc_Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"],
                        "Acc_Activate_Date": None,
                        "Credit_Class_Id": row["CREDIT_CLASS_ID"],
                        "Credit_Class_Name": row["CREDIT_CLASS_NAME"],
                        "Billing_Centre": row["BILLING_CENTER_NAME"],
                        "Customer_Segment": row["CUSTOMER_SEGMENT_ID"],
                        "Mobile_Contact_Tel": row["MOBILE_CONTACT_TEL"],
                        "Daytime_Contact_Tel": row["DAYTIME_CONTACT_TEL"],
                        "Email_Address": row["EMAIL"],
                        "Last_Rated_Dtm": None,
                    }
                    self.mongo_data[self.account_num]["Account_Details"].append(account_details_element)

                    # Marketing details element
                    marketing_details_element = {
                        "ACCOUNT_MANAGER": None,
                        "CONSUMER_MARKET": None,
                        "Informed_To": None,
                        "Informed_On": None,
                    }
                    self.mongo_data[self.account_num]["Marketing_Details"].append(marketing_details_element)

                    # Last actions element
                    last_actions_element = {
                        "Billed_Seq": row["LAST_BILL_SEQ"],
                        "Billed_Created": row["LAST_BILL_DTM"],
                        "Payment_Seq": None,
                        "Payment_Created": row["LAST_PAYMENT_DAT"],
                        "Payment_Money": row["LAST_PAYMENT_MNY"],
                        "Billed_Amount": row["LAST_PAYMENT_MNY"]  # Changed to Billed_Amount
                    }
                    self.mongo_data[self.account_num]["Last_Actions"].append(last_actions_element)

                # Product details element
                product_details_element = {
                    "Product_Label": row["PROMOTION_INTEG_ID"],
                    "Customer_Ref": row["CUSTOMER_REF"],
                    "Product_Seq": row["BSS_PRODUCT_SEQ"],
                    "Equipment_Ownership": "None",
                    "Product_Id": row["ASSET_ID"],
                    "Product_Name": row["PRODUCT_NAME"],
                    "Product_Status": row["ASSET_STATUS"],
                    "Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"],
                    "Service_Address": row["ASSET_ADDRESS"],
                    "Cat": row["CUSTOMER_TYPE_CAT"],
                    "Db_Cpe_Status": "None",
                    "Received_List_Cpe_Status": "None",
                    "Service_Type": row["OSS_SERVICE_ABBREVIATION"],
                    "Region": row["CITY"],
                    "Province": row["PROVINCE"],
                }
                self.mongo_data[self.account_num]["Product_Details"].append(product_details_element)

            logger.info("Successfully read customer details.")
            doc_status = "success"
        except Exception as e:
            logger.error(f"Error: {e}")
            doc_status = "error"
        finally:
            # Ensure database resources are properly closed
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()
        return doc_status # Return the status of the operation

    def get_payment_data(self):
        """
        Retrieves the latest payment data for the given account number from the MySQL database
        and updates the MongoDB document with last payment details.

        Returns:
            str: "success" if the payment data was retrieved and processed, otherwise "failure".
        """
        mysql_conn = None
        cursor = None
        try:
            logger.info(f"Getting payment data for account number: {self.account_num}")
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed. Skipping payment data retrieval.")
                return "failure"
            doc_status = "failure"
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(
                f"SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = '{self.account_num}' ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1")
            payment_rows = cursor.fetchall()

            pay_seq = payment_rows[0]["ACCOUNT_PAYMENT_SEQ"]
            pay_mny = payment_rows[0]["AP_ACCOUNT_PAYMENT_MNY"]
            pay_dat = payment_rows[0]["ACCOUNT_PAYMENT_DAT"]

            last_actions = {
                "Payment_Seq": pay_seq,
                "Payment_Created": pay_dat,
                "Payment_Money": pay_mny,
                "Billed_Seq": None,
                "Billed_Created": None,
                # Changed to Billed_Amount
                "Billed_Amount": pay_mny
            }
            self.mongo_data[self.account_num]["Last_Actions"].append(last_actions)
            logger.info("Successfully retrieved payment data.")
            doc_status = "success"
        except Exception as e:
            logger.error(f"Error: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()
        return doc_status

    def convert_to_serializable(self, data):
        """
        Recursively converts non-serializable data types (datetime, date, Decimal) 
        into JSON-serializable formats.

        Args:
            data (any): The data to be converted.

        Returns:
            any: The converted data with serializable types.
        """
        # Convert datetime, date, and Decimal objects to strings or floats
        if isinstance(data, dict):
            return {key: self.convert_to_serializable(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_to_serializable(item) for item in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, Decimal):
            return float(data)  # Convert Decimal to float
        else:
            return data

    def format_json_object(self):
        """
        Formats the MongoDB data into a JSON-compatible structure by simplifying nested data 
        and converting non-serializable objects like datetime into serializable formats.
        
        Returns:
            str: JSON string representation of the formatted data.
        """
        # Prepare the MongoDB data as a JSON-compatible dictionary with simplified structure
        json_data = [{
            "Doc_Version": self.mongo_data[self.account_num]["Doc_Version"],
            "Incident_Id": self.mongo_data[self.account_num]["Incident_Id"],
            "Account_Num": self.mongo_data[self.account_num]["Account_Num"],
            "Arrears": self.mongo_data[self.account_num]["Arrears"],
            "Created_By": self.mongo_data[self.account_num]["Created_By"],
            "Created_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Created_Dtm"] or datetime.now().isoformat()),
            "Incident_Status": self.mongo_data[self.account_num]["Incident_Status"],
            "Incident_Status_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Incident_Status_Dtm"] or datetime.now().isoformat()),
            "Status_Description": self.mongo_data[self.account_num]["Status_Description"],
            "File_Name_Dump": self.mongo_data[self.account_num]["File_Name_Dump"],
            "Batch_Id": self.mongo_data[self.account_num]["Batch_Id"],
            "Batch_Id_Tag_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Batch_Id_Tag_Dtm"] or datetime.now().isoformat()),
            "External_Data_Update_On": self.convert_to_serializable(self.mongo_data[self.account_num]["External_Data_Update_On"] or datetime.now().isoformat()),
            "Filtered_Reason": self.mongo_data[self.account_num]["Filtered_Reason"],
            "Export_On": self.convert_to_serializable(self.mongo_data[self.account_num]["Export_On"] or datetime.now().isoformat()),
            "File_Name_Rejected": self.mongo_data[self.account_num]["File_Name_Rejected"],
            "Rejected_Reason": self.mongo_data[self.account_num]["Rejected_Reason"],
            "Incident_Forwarded_By": self.mongo_data[self.account_num]["Incident_Forwarded_By"],
            "Incident_Forwarded_On": self.convert_to_serializable(self.mongo_data[self.account_num]["Incident_Forwarded_On"] or datetime.now().isoformat()),

            # Contact details list
            "Contact_Details": [
                {
                    "Contact_Type": contact["Contact_Type"],
                    "Contact": contact["Contact"],
                    "Create_Dtm": self.convert_to_serializable(contact["Create_Dtm"] or datetime.now().isoformat()),
                    "Create_By": contact["Create_By"]
                }
                for contact in self.mongo_data[self.account_num]["Contact_Details"]
            ] if self.mongo_data[self.account_num]["Contact_Details"] else [],

            # Product details list
            "Product_Details": [
                {
                    "Product_Label": product["Product_Label"],
                    "Customer_Ref": product["Customer_Ref"],
                    "Product_Seq": product["Product_Seq"],
                    "Equipment_Ownership": product["Equipment_Ownership"],
                    "Product_Id": product["Product_Id"],
                    "Product_Name": product["Product_Name"],
                    "Product_Status": product["Product_Status"],
                    "Effective_Dtm": self.convert_to_serializable(product["Effective_Dtm"] or datetime.now().isoformat()),
                    "Service_Address": product["Service_Address"],
                    "Cat": product["Cat"],
                    "Db_Cpe_Status": product["Db_Cpe_Status"],
                    "Received_List_Cpe_Status": product["Received_List_Cpe_Status"],
                    "Service_Type": product["Service_Type"],
                    "Region": product["Region"],
                    "Province": product["Province"]
                }
                for product in self.mongo_data[self.account_num]["Product_Details"]
            ] if self.mongo_data[self.account_num]["Product_Details"] else [],

            # Customer details
            "Customer_Details": {
                "Customer_Name": self.mongo_data[self.account_num]["Customer_Details"][0]["Customer_Name"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Company_Name": self.mongo_data[self.account_num]["Customer_Details"][0]["Company_Name"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Company_Registry_Number": self.mongo_data[self.account_num]["Customer_Details"][0].get("Company_Registry_Number", "string") if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Full_Address": self.mongo_data[self.account_num]["Customer_Details"][0]["Full_Address"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Zip_Code": self.mongo_data[self.account_num]["Customer_Details"][0].get("Zip_Code", "string") if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Customer_Type_Name": self.mongo_data[self.account_num]["Customer_Details"][0].get("Customer_Type_Name", "string") if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Nic": self.mongo_data[self.account_num]["Customer_Details"][0]["Nic"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Customer_Type_Id": self.mongo_data[self.account_num]["Customer_Details"][0]["Customer_Type_Id"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Customer_Type": self.mongo_data[self.account_num]["Customer_Details"][0].get("Customer_Type", "string") if self.mongo_data[self.account_num]["Customer_Details"] else "string"
            },

            # Account details
            "Account_Details": {
                "Account_Status": self.mongo_data[self.account_num]["Account_Details"][0]["Account_Status"] if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Acc_Effective_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Account_Details"][0]["Acc_Effective_Dtm"] or datetime.now().isoformat()) if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Acc_Activate_Date": self.convert_to_serializable(self.mongo_data[self.account_num]["Account_Details"][0].get("Acc_Activate_Date", "None")) if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Credit_Class_Id": self.mongo_data[self.account_num]["Account_Details"][0]["Credit_Class_Id"] if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Credit_Class_Name": self.mongo_data[self.account_num]["Account_Details"][0]["Credit_Class_Name"] if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Billing_Centre": self.mongo_data[self.account_num]["Account_Details"][0].get("Billing_Centre", "string") if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Customer_Segment": self.mongo_data[self.account_num]["Account_Details"][0].get("Customer_Segment", "string") if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Mobile_Contact_Tel": self.mongo_data[self.account_num]["Account_Details"][0].get("Mobile_Contact_Tel", "string") if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Daytime_Contact_Tel": self.mongo_data[self.account_num]["Account_Details"][0].get("Daytime_Contact_Tel", "string") if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Email_Address": self.mongo_data[self.account_num]["Account_Details"][0].get("Email_Address", "string") if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Last_Rated_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Account_Details"][0].get("Last_Rated_Dtm", "None")) if self.mongo_data[self.account_num]["Account_Details"] else "string"
            },

            # Last actions
            "Last_Actions": [
                {
                    "Billed_Seq": action["Billed_Seq"],
                    "Billed_Created": self.convert_to_serializable(action["Billed_Created"] or datetime.now().isoformat()),
                    "Payment_Seq": action["Payment_Seq"],
                    "Payment_Created": self.convert_to_serializable(action["Payment_Created"] or datetime.now().isoformat()),
                    "Payment_Money": self.convert_to_serializable(action["Payment_Money"]),
                    # Changed to Billed_Amount
                    "Billed_Amount": self.convert_to_serializable(action.get("Billed_Amount", 0))
                }
                for action in self.mongo_data[self.account_num]["Last_Actions"]
            ] if self.mongo_data[self.account_num]["Last_Actions"] else [],

            # Marketing details
            "Marketing_Details": [
                {
                    "ACCOUNT_MANAGER": marketing["ACCOUNT_MANAGER"],
                    "CONSUMER_MARKET": marketing["CONSUMER_MARKET"],
                    "Informed_To": marketing["Informed_To"],
                    "Informed_On": self.convert_to_serializable(marketing["Informed_On"] or datetime.now().isoformat())
                }
                for marketing in self.mongo_data[self.account_num]["Marketing_Details"]
            ] if self.mongo_data[self.account_num]["Marketing_Details"] else [],

            "Action": self.mongo_data[self.account_num]["Action"],
            "Validity_period": self.mongo_data[self.account_num]["Validity_period"],
            "Remark": self.mongo_data[self.account_num]["Remark"],
            "updatedAt": self.convert_to_serializable(self.mongo_data[self.account_num]["updatedAt"] or datetime.now().isoformat()),
            "Rejected_By": self.mongo_data[self.account_num]["Rejected_By"],
            "Rejected_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Rejected_Dtm"] or datetime.now().isoformat()),
            "Arrears_Band": self.mongo_data[self.account_num]["Arrears_Band"],
            "Source_Type": self.mongo_data[self.account_num]["Source_Type"]
        }]

        # Convert to serializable format and return as JSON string
        serializable_data = self.convert_to_serializable(json_data)
        return json.dumps(serializable_data, indent=4)

    def send_to_api(self, json_output, api_url):
        """
        Sends a POST request to an API with the given JSON data.

        Args:
            json_output (dict): The data to be sent to the API in JSON format.
            api_url (str): The URL of the API to which the request will be sent.

        Returns:
            dict | None: Returns the JSON response from the API if successful, or None if there was an error.
        
        This function is used to send data to an external API and handle any potential errors that may occur during the request.
        It logs the success or failure of the operation based on the outcome.
        """
        
        logger.info(f"Sending data to API: {api_url}")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            response = requests.post(api_url, data=json_output, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            logger.info("Successfully sent data to API.")
            return response.json()  # Return the JSON response from the API
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending data to API: {e}")
            return None


def process_incident(account_num, incident_id):
    """
    Processes an incident by gathering necessary details, formatting the data as JSON,
    and sending it to the API.

    Args:
        account_num (str): The account number associated with the incident.
        incident_id (str): The unique ID of the incident.

    Returns:
        None: This function does not return anything.
    """
    logger.info(f"Processing incident for account: {account_num}, ID: {incident_id}")
    api_url = read_api_config()  # Retrieve the API URL internally
    incident = create_incident(account_num, incident_id)

    # Only proceed if account data exists
    if incident.read_customer_details():
        # Get payment data (will handle its own error cases)
        payment_status = incident.get_payment_data()
        if payment_status == "error":
            logger.error("Failed to retrieve payment data - proceeding without it")
        
        # Format and send data
        json_output = incident.format_json_object()
        print("Formatted JSON Output:", json_output)
        
        api_response = incident.send_to_api(json_output, api_url)
        if api_response:
            logger.info("Incident processed successfully.")
            print("API Response:", api_response)
        else:
            logger.error("Failed to send data to API.")
    else:
        logger.error(f"No account data found for {account_num} - process terminated")
        print(f"No account data found for {account_num} - process terminated")


