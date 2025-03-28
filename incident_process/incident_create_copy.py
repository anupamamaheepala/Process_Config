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
# Import custom exceptions for error handling
from utils.custom_exceptions.customize_exceptions import APIConfigError, IncidentCreationError


logger = get_logger("incident_logger")

class create_incident:
    
    def __init__(self, account_num, incident_id):
        """
        Constructor for the create_incident class.
        
        Args:
            account_num (str): The account number associated with the incident.
            incident_id (int): The unique identifier for the incident.

        Raises:
            ValueError: If either account_num or incident_id fails conversion.
        """
        try: # either one
            if account_num is None:
                raise ValueError("account_num cannot be None")
            self.account_num = str(account_num)

            if incident_id is None:
                raise ValueError("incident_id cannot be None")
            self.incident_id = int(incident_id)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid input: {e}")
        
        self.mongo_data = self.initialize_mongo_doc()

    def initialize_mongo_doc(self):
        """
        Creates and initializes a standardized MongoDB document structure for incident.

        This template document:
        - Establishes all required fields with default values
        - Sets proper initial timestamps
        - Provides empty containers for related data
        - Ensures consistent field types across all incidents

        Structure Overview:
        1. Metadata: Versioning, IDs, timestamps
        2. Core Incident Data: Status, descriptions
        3. Financial Data: Arrears, payment info
        4. Relationship Containers: Contacts, products
        5. System Fields: Audit trails, rejection info

        Returns:
            dict: A complete document structure with these guaranteed characteristics:
                - All fields initialized (no undefined fields)
                - String fields: Empty string defaults ("")
                - Numeric fields: "0" or 0 defaults
                - Dates: Current ISO timestamp or "1900-01-01" placeholder
                - Collections: Empty lists/dicts initialized
                - All timestamps in ISO8601 format
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
        Retrieves and processes customer account data from MySQL, transforming it into MongoDB document structure.

        This method performs three key operations:
        1. Fetches all customer records for the specified account number
        2. Transforms relational MySQL data into document-oriented MongoDB structure
        3. Handles data quality issues and type conversions

        Data Flow:
        MySQL (debt_cust_detail table) → Python Dict → mongo_data structure

        Returns:
            str: 
                - "success" if:
                    * Data was successfully retrieved AND
                    * Customer_Details was populated AND
                    * No unhandled exceptions occurred
                - "error" if:
                    * MySQL connection failed OR
                    * SQL execution failed OR
                    * Any exception occurred during processing

        Database Requirements:
            Requires MySQL table 'debt_cust_detail' with columns:
            - ACCOUNT_NUM (account identifier)
            - ASSET_ID (product identifier)
            - CONTACT_PERSON, NIC, ASSET_ADDRESS (customer info)
            - ACCOUNT_STATUS_BSS, EMAIL (account info)
            - LAST_PAYMENT_DAT, LAST_PAYMENT_MNY (payment info)
            - PROMOTION_INTEG_ID, PRODUCT_NAME (product info)
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
        Retrieves and processes the most recent payment record for the account from MySQL.

        This method:
        1. Establishes a MySQL connection
        2. Executes a query to fetch the latest payment
        3. Updates the in-memory MongoDB document structure
        4. Handles all potential failure scenarios gracefully

        Data Flow:
        MySQL (debt_payment table) → Python dict → mongo_data["Last_Actions"]

        Returns:
            str: 
                - "success" if:
                    * Payment data was found AND
                    * Successfully processed AND
                    * mongo_data was updated
                - "failure" if:
                    * MySQL connection failed OR
                    * No payment records found OR
                    * Any exception occurred

        Error Handling:
            - Catches all exceptions and returns "failure"
            - Logs detailed error messages including:
                * Connection failures
                * Query execution errors
                * Data processing issues
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
        Transforms the incident data into a well-formatted JSON string with type consistency.

        This method performs three key operations:
        1. Creates a safe deep copy of the source data to prevent modification
        2. Enforces type consistency on critical fields (Nic, Email_Address)
        3. Generates human-readable JSON with proper indentation

        Returns:
            str: 
                A prettified JSON string with these guaranteed characteristics:
                - Nic field always exists as string (empty string if missing/null)
                - Email_Address field always exists as string (empty string if missing/null)
                - 4-space indentation for human readability
                - All datetime/Decimal/None values properly converted via json_serializer

        Raises:
            JSONEncodeError: If the data contains unserializable types not handled by json_serializer
            KeyError: If Customer_Details or Account_Details structures are missing entirely
        """
        # Create a deep copy to avoid modifying original data
        json_data = json.loads(json.dumps(self.mongo_data, default=self.json_serializer))
        
        # Ensure all required fields are present with proper values
        json_data["Customer_Details"]["Nic"] = str(json_data["Customer_Details"].get("Nic", ""))
        json_data["Account_Details"]["Email_Address"] = str(json_data["Account_Details"].get("Email_Address", ""))
        
        return json.dumps(json_data, indent=4)

    def json_serializer(self, obj):
        """
        Custom JSON serializer that handles non-native JSON types in Python objects.

        Converts specific Python types to JSON-compatible formats:
        - datetime/date → ISO8601 strings
        - Decimal → float
        - None → empty string

        Parameters:
            obj (any): 
                The Python object to be serialized. Expected types:
                - datetime.datetime or datetime.date objects
                - decimal.Decimal objects
                - None/null values
                - NOTE: Other types will raise TypeError

        Returns:
            str or float:
                - For datetime/date: ISO8601 formatted string (e.g., "2023-01-01T00:00:00")
                - For Decimal: Converted to float (e.g., Decimal("10.5") → 10.5)
                - For None: Returns empty string ("")

        Raises:
            TypeError: 
                When encountering unsupported types. The error message includes:
                - The problematic type encountered
                - Example: "Type <class 'set'> not serializable"

        Notes:
            1. This serializer is designed to work with json.dumps() as its 'default' handler
            2. Native JSON types (int, float, str, list, dict, bool) are automatically
            handled by Python's built-in json module and won't reach this serializer
            3. The empty string conversion for None helps maintain schema consistency
            in APIs that expect strings rather than null values
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
        Sends JSON data to a specified API endpoint via HTTP POST request.

        Handles the entire API communication lifecycle including:
        - Setting proper JSON headers
        - Making the HTTP request
        - Processing successful responses
        - Handling and logging errors

        Parameters:
            json_output (str): 
                The JSON-formatted string to send. 
                Must be valid JSON that conforms to the API's schema requirements.
                Example: '{"key": "value"}' (properly formatted string)

            api_url (str): 
                The complete URL of the API endpoint.

        Returns:
            dict or None: 
                - On success: Returns the API response parsed as a Python dictionary
                - On failure: Returns None and logs detailed error information

        Raises:
            No explicit exceptions raised, but handles and logs these request exceptions:
            - ConnectionError: Network problems (DNS, refused connection, etc.)
            - HTTPError: HTTP 4XX/5XX responses
            - Timeout: Request timeout
            - RequestException: Other request-related exceptions
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


    def process_incident(self):
            """
            Processes a debt collection incident by gathering customer/payment data and sending to API.
            
            This method:
            1. Retrieves customer details from MySQL (already done in initialization)
            2. Retrieves optional payment data if customer exists
            3. Formats the data as JSON
            4. Sends to the configured API endpoint
            
            Returns:
                bool: 
                    - True if incident was successfully created and sent to API
                    - False if:
                        * No customer details found
                        * Database query failed
                        * API communication failed
            """
            try:
                logger.info(f"Processing incident for account: {self.account_num}, ID: {self.incident_id}")
                
                # 1. Retrieve Customer Details (critical failure if missing)
                customer_status = self.read_customer_details()
                if customer_status != "success" or not self.mongo_data["Customer_Details"]:
                    logger.error(f"No customer details found for account {self.account_num}")
                    return False
                
                # 2. Optional: Retrieve Payment Data
                payment_status = self.get_payment_data()  # Failure tolerated
                if payment_status != "success":
                    logger.warning(f"Failed to retrieve payment data for account {self.account_num}")
                    return False
                    
                # 3. Format and Send to API
                json_output = self.format_json_object()
                print(json_output)  # For debugging purposes
                
                api_url = read_api_config()
                if not api_url:
                    raise APIConfigError("Empty API URL in config")
                        
                api_response = self.send_to_api(json_output, api_url)
                if not api_response:
                    raise IncidentCreationError("Empty API response")
                        
                logger.info(f"API Success: {api_response}")
                return True
                
            except IncidentCreationError as e:
                logger.error(f"Incident processing failed: {e}")
                return False
            except APIConfigError as e:
                logger.error(f"Configuration error: {e}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)
                return False

