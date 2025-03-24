import pymysql
from datetime import datetime, date
from decimal import Decimal
import json
import pprint
import requests
from utils.database.connectSQL import get_mysql_connection
from utils.logger.logger import get_logger

logger = get_logger("incident_logger")

class create_incident:
    account_num = None
    incident_id = None
    mongo_data = None

    def __init__(self, account_num, incident_id):
        self.account_num = account_num
        self.incident_id = incident_id
        self.mongo_data = self.initialize_mongo_doc(account_num, incident_id)

    def create_incident(self, payload):
        status = self.read_customer_details()
        # if status == "success":
        #     self.read_payment_details()
        # return self.client.post("/incidents", json=payload)

    def initialize_mongo_doc(self, account_num, incident_id):
        self.mongo_data = {
            account_num: {
                "Doc_Version": "1.0",
                "Incident_Id": incident_id,
                "Account_Num": None,
                "Arrears": 0,
                "Created_By": None,
                "Created_Dtm": None,
                "Incident_Status": None,
                "Incident_Status_Dtm": None,
                "Status_Description": None,
                "File_Name_Dump": None,
                "Batch_Id": None,
                "Batch_Id_Tag_Dtm": None,
                "External_Data_Update_On": None,
                "Filtered_Reason": None,
                "Export_On": None,
                "File_Name_Rejected": None,
                "Rejected_Reason": None,
                "Incident_Forwarded_By": None,
                "Incident_Forwarded_On": None,
                "Contact_Details": [],
                "Product_Details": [],
                "Customer_Details": [],
                "Account_Details": [],
                "Last_Actions": [],
                "Marketing_Details": [],
                "Action": None,
                "Validity_period": None,
                "Remark": None,
                "updatedAt": None,
                "Rejected_By": None,
                "Rejected_Dtm": None,
                "Arrears_Band": None,
                "Source_Type": None
            }
        }
        return self.mongo_data

    def read_customer_details(self):
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
            for row in rows:
                customer_ref = row["CUSTOMER_REF"]
                account_num = row["ACCOUNT_NUM"]

                Check_Val = self.mongo_data[self.account_num]["Account_Num"]

                if Check_Val is None:
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

                    marketing_details_element = {
                        "ACCOUNT_MANAGER": None,
                        "CONSUMER_MARKET": None,
                        "Informed_To": None,
                        "Informed_On": None,
                    }
                    self.mongo_data[self.account_num]["Marketing_Details"].append(marketing_details_element)

                    last_actions_element = {
                        "Billed_Seq": row["LAST_BILL_SEQ"],
                        "Billed_Created": row["LAST_BILL_DTM"],
                        "Payment_Seq": None,
                        "Payment_Created": row["LAST_PAYMENT_DAT"],
                        "Payment_Money": row["LAST_PAYMENT_MNY"],
                        "Billed_Amount": row["LAST_PAYMENT_MNY"]
                    }
                    self.mongo_data[self.account_num]["Last_Actions"].append(last_actions_element)

                product_details_element = {
                    "Product_Label": row["PROMOTION_INTEG_ID"],
                    "Customer_Ref": row["CUSTOMER_REF"],
                    "Product_Seq": row["BSS_PRODUCT_SEQ"],
                    "Equipment_Ownership": None,
                    "Product_Id": row["ASSET_ID"],
                    "Product_Name": row["PRODUCT_NAME"],
                    "Product_Status": row["ASSET_STATUS"],
                    "Effective_Dtm": row["ACCOUNT_EFFECTIVE_DTM_BSS"],
                    "Service_Address": row["ASSET_ADDRESS"],
                    "Cat": row["CUSTOMER_TYPE_CAT"],
                    "Db_Cpe_Status": None,
                    "Received_List_Cpe_Status": None,
                    "Service_Type": row["OSS_SERVICE_ABBREVIATION"],
                    "Region": row["CITY"],
                    "Province": row["PROVINCE"],
                }
                self.mongo_data[self.account_num]["Product_Details"].append(product_details_element)

            logger.info("Successfully read customer details.")
            doc_status = "success"
        except Exception as e:
            logger.error(f"MySQL connection error in reading customer details: {e}")
            doc_status = "error"
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()
        return doc_status

    def get_payment_data(self):
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
                "Billed_Amount": pay_mny
            }
            self.mongo_data[self.account_num]["Last_Actions"].append(last_actions)
            logger.info("Successfully retrieved payment data.")
            doc_status = "success"
        except Exception as e:
            logger.error(f"MySQL connection error in getting payment data: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()
        return doc_status

    def convert_to_serializable(self, data):
        """Convert data to JSON-serializable format, replacing "None" with null."""
        if isinstance(data, dict):
            return {key: self.convert_to_serializable(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self.convert_to_serializable(item) for item in data]
        elif isinstance(data, (datetime, date)):
            return data.isoformat()
        elif isinstance(data, Decimal):
            return float(data)
        elif data == "None" or data is None:
            return None # none
        else:
            return data

    def format_json_object(self):
        """Format the MongoDB data as a JSON-compatible dictionary."""
        json_data = {
            "Doc_Version": self.mongo_data[self.account_num]["Doc_Version"],
            "Incident_Id": self.mongo_data[self.account_num]["Incident_Id"],
            "Account_Num": self.mongo_data[self.account_num]["Account_Num"],
            "Arrears": self.mongo_data[self.account_num]["Arrears"],
            "Created_By": self.mongo_data[self.account_num]["Created_By"],
            "Created_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Created_Dtm"] or datetime.now()),
            "Incident_Status": self.mongo_data[self.account_num]["Incident_Status"],
            "Incident_Status_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Incident_Status_Dtm"] or datetime.now()),
            "Status_Description": self.mongo_data[self.account_num]["Status_Description"],
            "File_Name_Dump": self.mongo_data[self.account_num]["File_Name_Dump"],
            "Batch_Id": self.mongo_data[self.account_num]["Batch_Id"],
            "Batch_Id_Tag_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Batch_Id_Tag_Dtm"] or datetime.now()),
            "External_Data_Update_On": self.convert_to_serializable(self.mongo_data[self.account_num]["External_Data_Update_On"] or datetime.now()),
            "Filtered_Reason": self.mongo_data[self.account_num]["Filtered_Reason"],
            "Export_On": self.convert_to_serializable(self.mongo_data[self.account_num]["Export_On"] or datetime.now()),
            "File_Name_Rejected": self.mongo_data[self.account_num]["File_Name_Rejected"],
            "Rejected_Reason": self.mongo_data[self.account_num]["Rejected_Reason"],
            "Incident_Forwarded_By": self.mongo_data[self.account_num]["Incident_Forwarded_By"],
            "Incident_Forwarded_On": self.convert_to_serializable(self.mongo_data[self.account_num]["Incident_Forwarded_On"] or datetime.now()),
            "Contact_Details": self.convert_to_serializable(self.mongo_data[self.account_num]["Contact_Details"]),
            "Product_Details": self.convert_to_serializable(self.mongo_data[self.account_num]["Product_Details"]),
            "Customer_Details": {
                "Customer_Name": self.mongo_data[self.account_num]["Customer_Details"][0]["Customer_Name"] if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Company_Name": self.mongo_data[self.account_num]["Customer_Details"][0]["Company_Name"] if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Company_Registry_Number": self.mongo_data[self.account_num]["Customer_Details"][0].get("Company_Registry_Number") if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Full_Address": self.mongo_data[self.account_num]["Customer_Details"][0]["Full_Address"] if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Zip_Code": self.mongo_data[self.account_num]["Customer_Details"][0].get("Zip_Code") if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Customer_Type_Name": self.mongo_data[self.account_num]["Customer_Details"][0].get("Customer_Type_Name") if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Nic": self.mongo_data[self.account_num]["Customer_Details"][0]["Nic"] if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Customer_Type_Id": self.mongo_data[self.account_num]["Customer_Details"][0]["Customer_Type_Id"] if self.mongo_data[self.account_num]["Customer_Details"] else None,
                "Customer_Type": self.mongo_data[self.account_num]["Customer_Details"][0].get("Customer_Type") if self.mongo_data[self.account_num]["Customer_Details"] else None
            },
            "Account_Details": {
                "Account_Status": self.mongo_data[self.account_num]["Account_Details"][0]["Account_Status"] if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Acc_Effective_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Account_Details"][0]["Acc_Effective_Dtm"]) if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Acc_Activate_Date": self.convert_to_serializable(self.mongo_data[self.account_num]["Account_Details"][0].get("Acc_Activate_Date")) if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Credit_Class_Id": self.mongo_data[self.account_num]["Account_Details"][0]["Credit_Class_Id"] if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Credit_Class_Name": self.mongo_data[self.account_num]["Account_Details"][0]["Credit_Class_Name"] if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Billing_Centre": self.mongo_data[self.account_num]["Account_Details"][0].get("Billing_Centre") if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Customer_Segment": self.mongo_data[self.account_num]["Account_Details"][0].get("Customer_Segment") if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Mobile_Contact_Tel": self.mongo_data[self.account_num]["Account_Details"][0].get("Mobile_Contact_Tel") if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Daytime_Contact_Tel": self.mongo_data[self.account_num]["Account_Details"][0].get("Daytime_Contact_Tel") if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Email_Address": self.mongo_data[self.account_num]["Account_Details"][0].get("Email_Address") if self.mongo_data[self.account_num]["Account_Details"] else None,
                "Last_Rated_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Account_Details"][0].get("Last_Rated_Dtm")) if self.mongo_data[self.account_num]["Account_Details"] else None
            },
            "Last_Actions": self.convert_to_serializable(self.mongo_data[self.account_num]["Last_Actions"]),
            "Marketing_Details": self.convert_to_serializable(self.mongo_data[self.account_num]["Marketing_Details"]),
            "Action": self.mongo_data[self.account_num]["Action"],
            "Validity_period": self.mongo_data[self.account_num]["Validity_period"],
            "Remark": self.mongo_data[self.account_num]["Remark"],
            "updatedAt": self.convert_to_serializable(self.mongo_data[self.account_num]["updatedAt"] or datetime.now()),
            "Rejected_By": self.mongo_data[self.account_num]["Rejected_By"],
            "Rejected_Dtm": self.convert_to_serializable(self.mongo_data[self.account_num]["Rejected_Dtm"] or datetime.now()),
            "Arrears_Band": self.mongo_data[self.account_num]["Arrears_Band"],
            "Source_Type": self.mongo_data[self.account_num]["Source_Type"]
        }

        serializable_data = self.convert_to_serializable(json_data)
        return json.dumps(serializable_data, indent=4)

    def send_to_api(self, json_output, api_url):
        """Send JSON data to API endpoint."""
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
            # if hasattr(e, 'response') and e.response is not None:
            #     # logger.error(f"Response content: {e.response.text}")
            # return None


def process_incident(account_num, incident_id, api_url):
    """Process incident and send data to API."""
    logger.info(f"Processing incident for account number: {account_num}, incident ID: {incident_id}")
    incident = create_incident(account_num, incident_id)
    incident.read_customer_details()
    incident.get_payment_data()
    json_output = incident.format_json_object()
    print("Formatted JSON Output:", json_output)
    api_response = incident.send_to_api(json_output, api_url)

    if api_response:
        logger.info("Incident processed successfully.")
        print("API Response:", api_response)
    else:
        logger.error("Failed to process incident.")
        print("Failed to send data to the API.")