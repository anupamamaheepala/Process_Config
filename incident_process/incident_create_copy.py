import pymysql  # Library for interacting with MySQL databases
from datetime import datetime, date  # Modules for working with date and time
from decimal import Decimal  # Module for handling precise decimal arithmetic
import json  # Module for parsing and handling JSON data
import requests  # Library for making HTTP requests

# Import a function to establish a MySQL database connection
from utils.database.connectSQL import get_mysql_connection

# Import a function to configure and retrieve a logger for logging messages
from utils.logger.logger import get_logger

# Import a function to read API config
from utils.api.connectAPI import read_api_config


logger = get_logger("incident_logger")

class create_incident:
    
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
        self.mongo_data = self.initialize_mongo_doc()

    def initialize_mongo_doc(self):
        """
        Initializes a MongoDB document structure for storing incident-related data.

        Args:
            account_num (str): The account number associated with the incident.
            incident_id (int): The unique identifier for the incident.

        Returns:
            dict: A structured dictionary to store incident-related data.
        """
        now = datetime.now().isoformat()
        return {
            "Doc_Version": "1.0",
            "Incident_Id": self.incident_id,
            "Account_Num": self.account_num,
            "Arrears": 0,
            "Created_By": "drs_admin",
            "Created_Dtm": now,
            "Incident_Status": "",
            "Incident_Status_Dtm": now,
            "Status_Description": "",
            "File_Name_Dump": "",
            "Batch_Id": "",
            "Batch_Id_Tag_Dtm": now,
            "External_Data_Update_On": now,
            "Filtered_Reason": "",
            "Export_On": now,
            "File_Name_Rejected": "",
            "Rejected_Reason": "",
            "Incident_Forwarded_By": "",
            "Incident_Forwarded_On": now,
            "Contact_Details": [],
            "Product_Details": [],
            "Customer_Details": {},
            "Account_Details": {},
            "Last_Actions": {
                "Billed_Seq": "",
                "Billed_Created": "",
                "Payment_Seq": "",
                "Payment_Created": "",
                "Payment_Money": "0",
                "Billed_Amount": "0"
            },
            "Marketing_Details": {
                "ACCOUNT_MANAGER": "",
                "CONSUMER_MARKET": "",
                "Informed_To": "",
                "Informed_On": "1900-01-01T00:00:00"
            },
            "Action": "",
            "Validity_period": "0",
            "Remark": "",
            "updatedAt": now,
            "Rejected_By": "",
            "Rejected_Dtm": now,
            "Arrears_Band": "",
            "Source_Type": ""
        }

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
            mysql_conn = get_mysql_connection()
            if not mysql_conn:
                logger.error("MySQL connection failed. Skipping customer details retrieval.")
                return "error"
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{self.account_num}'")
            rows = cursor.fetchall()

            # Track seen product IDs to avoid duplicates
            seen_products = set()

            for row in rows:
                # Only process customer/account details once
                if not self.mongo_data["Customer_Details"]:
                    # Handle contact details - maintaining your exact structure
                    if row.get("TECNICAL_CONTACT_EMAIL"):
                        contact_details_element = {
                            "Contact_Type": "email",
                            "Contact": row["TECNICAL_CONTACT_EMAIL"] if "@" in row["TECNICAL_CONTACT_EMAIL"] else "",
                            "Create_Dtm": row["LOAD_DATE"].isoformat() if row.get("LOAD_DATE") else "",
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("MOBILE_CONTACT"):
                        contact_details_element = {
                            "Contact_Type": "mobile",
                            "Contact": row["MOBILE_CONTACT"],
                            "Create_Dtm": row["LOAD_DATE"].isoformat() if row.get("LOAD_DATE") else "",
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    if row.get("WORK_CONTACT"):
                        contact_details_element = {
                            "Contact_Type": "fix",
                            "Contact": row["WORK_CONTACT"],
                            "Create_Dtm": row["LOAD_DATE"].isoformat() if row.get("LOAD_DATE") else "",
                            "Create_By": "drs_admin"
                        }
                        self.mongo_data["Contact_Details"].append(contact_details_element)

                    # Customer details
                    self.mongo_data["Customer_Details"] = {
                        "Customer_Name": row.get("CONTACT_PERSON", ""),
                        "Company_Name": "",
                        "Company_Registry_Number": "",
                        "Full_Address": row.get("ASSET_ADDRESS", ""),
                        "Zip_Code": row.get("ZIP_CODE", ""),
                        "Customer_Type_Name": "",
                        "Nic": str(row.get("NIC", "")),  # Ensure Nic is always a string
                        "Customer_Type_Id": str(row.get("CUSTOMER_TYPE_ID", "")),
                        "Customer_Type": row.get("CUSTOMER_TYPE", "")
                    }

                    # Account details
                    self.mongo_data["Account_Details"] = {
                        "Account_Status": row.get("ACCOUNT_STATUS_BSS", ""),
                        "Acc_Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"].isoformat() if row.get("ACCOUNT_EFFECTIVE_DTM_BSS") else "",
                        "Acc_Activate_Date": "1900-01-01T00:00:00",
                        "Credit_Class_Id": str(row.get("CREDIT_CLASS_ID", "")),
                        "Credit_Class_Name": row.get("CREDIT_CLASS_NAME", ""),
                        "Billing_Centre": row.get("BILLING_CENTER_NAME", ""),
                        "Customer_Segment": row.get("CUSTOMER_SEGMENT_ID", ""),
                        "Mobile_Contact_Tel": "",
                        "Daytime_Contact_Tel": "",
                        "Email_Address": str(row.get("EMAIL", "")),  # Ensure Email_Address is always a string
                        "Last_Rated_Dtm": "1900-01-01T00:00:00"
                    }

                    # Last actions from customer table
                    if row.get("LAST_PAYMENT_DAT"):
                        self.mongo_data["Last_Actions"] = {
                            "Billed_Seq": str(row.get("LAST_BILL_SEQ", "")),
                            "Billed_Created": row["LAST_BILL_DTM"].isoformat() if row.get("LAST_BILL_DTM") else "",
                            "Payment_Seq": "",
                            "Payment_Created": row["LAST_PAYMENT_DAT"].isoformat() if row.get("LAST_PAYMENT_DAT") else "",
                            "Payment_Money": str(float(row["LAST_PAYMENT_MNY"])) if row.get("LAST_PAYMENT_MNY") else "0",
                            "Billed_Amount": str(float(row["LAST_PAYMENT_MNY"])) if row.get("LAST_PAYMENT_MNY") else "0"
                        }

                # Process product details (avoid duplicates)
                product_id = row.get("ASSET_ID")
                if product_id and product_id not in seen_products:
                    seen_products.add(product_id)
                    self.mongo_data["Product_Details"].append({
                        "Product_Label": row.get("PROMOTION_INTEG_ID", ""),
                        "Customer_Ref": row.get("CUSTOMER_REF", ""),
                        "Product_Seq": str(row.get("BSS_PRODUCT_SEQ", "")),
                        "Equipment_Ownership": "",
                        "Product_Id": product_id,
                        "Product_Name": row.get("PRODUCT_NAME", ""),
                        "Product_Status": row.get("ASSET_STATUS", ""),
                        "Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"].isoformat() if row.get("ACCOUNT_EFFECTIVE_DTM_BSS") else "",
                        "Service_Address": row.get("ASSET_ADDRESS", ""),
                        "Cat": row.get("CUSTOMER_TYPE_CAT", ""),
                        "Db_Cpe_Status": "",
                        "Received_List_Cpe_Status": "",
                        "Service_Type": row.get("OSS_SERVICE_ABBREVIATION", ""),
                        "Region": row.get("CITY", ""),
                        "Province": row.get("PROVINCE", "")
                    })

            logger.info("Successfully read customer details.")
            return "success"

        except Exception as e:
            logger.error(f"Error : {e}")
            return "error"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

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
            
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(
                f"SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = '{self.account_num}' "
                "ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1"
            )
            payment_rows = cursor.fetchall()

            if payment_rows:
                payment = payment_rows[0]
                self.mongo_data["Last_Actions"].update({
                    "Payment_Seq": str(payment.get("ACCOUNT_PAYMENT_SEQ", "")),
                    "Payment_Created": payment["ACCOUNT_PAYMENT_DAT"].isoformat() if payment.get("ACCOUNT_PAYMENT_DAT") else "",
                    "Payment_Money": str(float(payment["AP_ACCOUNT_PAYMENT_MNY"])) if payment.get("AP_ACCOUNT_PAYMENT_MNY") else "0",
                    "Billed_Amount": str(float(payment["AP_ACCOUNT_PAYMENT_MNY"])) if payment.get("AP_ACCOUNT_PAYMENT_MNY") else "0"
                })
                logger.info("Successfully retrieved payment data.")
                return "success"
            return "failure"

        except Exception as e:
            logger.error(f"Error : {e}")
            return "failure"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()

    def format_json_object(self):
        """
        Formats the incident data into a JSON string with proper data types.

        :return: A formatted JSON string with properly serialized values.
        """
        # Create a deep copy to avoid modifying original data
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer))
        
        # Ensure all required fields are present with proper values
        json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
        json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
        
        return json.dumps(json_data, indent=4)

    def json_serializer(self, obj):
        """
        Custom JSON serializer for handling datetime, Decimal, and None values.

        :param obj: The object to serialize.
        :return: Serialized value as a string or float.
        :raises TypeError: If an unsupported type is encountered.
        """
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if obj is None:
            return ""  # Convert None to empty string
        raise TypeError(f"Type {type(obj)} not serializable")

    def send_to_api(self, json_output, api_url):
        """
        Sends the formatted JSON data to a specified API endpoint.

        :param json_output: The JSON string to send.
        :param api_url: The URL of the API endpoint.
        :return: The API response as a dictionary if successful, otherwise None.
        """
        logger.info(f"Sending data to API: {api_url}")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        try:
            response = requests.post(api_url, data=json_output, headers=headers)
            response.raise_for_status()
            logger.info("Successfully sent data to API.")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending data to API: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response content: {e.response.text}")
            return None


def process_incident(account_num, incident_id):
    """
    Processes a debt collection incident by gathering customer/payment data and sending to API.
    
    This function:
    1. Creates a new incident record
    2. Retrieves mandatory customer details from MySQL
    3. Retrieves optional payment data if customer exists
    4. Formats the data as JSON
    5. Sends to the configured API endpoint
    
    Parameters:
        account_num (str): The customer's account number to process.
                           Must be a valid account number present in the database.
                           
        incident_id (int): Unique identifier for the new incident.
                          Must be a positive integer.
    
    Returns:
        bool: 
            - True if incident was successfully created and sent to API
            - False if:
                * No customer details found
                * Database query failed
                * API communication failed
    """
    logger.info(f"Processing incident for account number: {account_num}, incident ID: {incident_id}")
    api_url = read_api_config()
    incident = create_incident(account_num, incident_id)
    
    # 1. FIRST CHECK CUSTOMER DETAILS - EXIT IF NONE FOUND
    customer_status = incident.read_customer_details()
    
    # If customer query failed OR no customer details exist -> exit immediately
    if customer_status != "success" or not incident.mongo_data["Customer_Details"]:
        logger.error(f"No customer details found for account {account_num}. Aborting incident creation.")
        return False
    
    # 2. ONLY CHECK PAYMENTS IF CUSTOMER DETAILS EXIST
    payment_status = incident.get_payment_data()
    
    # Format and send data (we already know customer details exist)
    json_output = incident.format_json_object()
    print("Formatted JSON Output:", json_output)
    
    api_response = incident.send_to_api(json_output, api_url)
    if api_response:
        logger.info("Incident processed successfully.")
        print("API Response:", api_response)
        return True
    
    logger.error("Failed to send incident data to API")
    return False

