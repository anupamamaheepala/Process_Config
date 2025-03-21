import pymysql
from datetime import datetime, date
import json
from decimal import Decimal

class create_incident:
    account_num = None
    incident_id = None
    mongo_data = None
    core_config = {
        "mysql_host": "127.0.0.1",
        "mysql_database": "drs",
        "mysql_user": "root",
        "mysql_password": ""
    }
    
    def __init__(self, account_num, incident_id):
        self.account_num = account_num
        self.incident_id = incident_id
        self.mongo_data = self.initialize_mongo_doc(account_num)
        
    def create_incident(self, payload):
        status = self.read_customer_details()
        if status == "success":
            self.get_payment_data()
        json_output = self.format_json_object()
        print("____-------------------------------------------------------------------------------")
        print(json_output)
        return status
    
    def initialize_mongo_doc(self, account_num):
        key = (self.account_num)
        self.mongo_data = {}
        self.mongo_data[key] = {
            "Incident_Id": None,
            "Account_Num": self.account_num,
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
        return self.mongo_data
    
    def read_customer_details(self):
        mysql_conn = None
        cursor = None
        try:
            mysql_conn = pymysql.connect(
                host=self.core_config["mysql_host"],
                database=self.core_config["mysql_database"],
                user=self.core_config["mysql_user"],
                password=self.core_config["mysql_password"]
            )
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{self.account_num}'")  
            rows = cursor.fetchall()
            for row in rows:
                customer_ref = row["CUSTOMER_REF"]
                account_num = row["ACCOUNT_NUM"]
                
                if self.mongo_data[self.account_num]["Account_Num"] != None:
                    self.mongo_data[self.account_num]["Incident_Id"] = self.incident_id
                    
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
                        "Zip_Code": None,
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
                        "Credit_Class_Id" : row["CREDIT_CLASS_ID"],
                        "Credit_Class_Name" : row["CREDIT_CLASS_NAME"],
                        "Billing_Centre" : row["BILLING_CENTER_NAME"],
                        "Customer_Segment" : row["CUSTOMER_SEGMENT_ID"],
                        "Mobile_Contact_Tel" : row["MOBILE_CONTACT_TEL"],
                        "Daytime_Contact_Tel" : row["DAYTIME_CONTACT_TEL"],
                        "Email_Address" : row["EMAIL"],
                        "Last_Rated_Dtm" : None,
                    }
                    self.mongo_data[self.account_num]["Account_Details"].append(account_details_element)
                    
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
                    
                    marketing_details_element = {
                        "ACCOUNT_MANAGER": None,
                        "CONSUMER_MARKET": None,
                        "Informed_To" : None,
                        "Informed_On" : None,
                    }
                    self.mongo_data[self.account_num]["Marketing_Details"].append(marketing_details_element)
                    
                    last_actions_element = {
                        "Billed_Seq": row["LAST_BILL_SEQ"],
                        "Billed_Created": row["LAST_BILL_DTM"],
                        "Payment_Seq": None,
                        "Payment_Created": row["LAST_PAYMENT_DAT"],
                        "Payment_Money": row["LAST_PAYMENT_MNY"],
                    }
                    self.mongo_data[self.account_num]["Last_Actions"].append(last_actions_element)
            
            doc_status = "success"
        except Exception as e:
            print(f"MySQL connection error in reading customer details: {e}")
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
            doc_status = "failure"
            mysql_conn = pymysql.connect(
                host=self.core_config["mysql_host"],
                database=self.core_config["mysql_database"],
                user=self.core_config["mysql_user"],
                password=self.core_config["mysql_password"]
            )
            cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
            cursor.execute(f"SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = '{self.account_num}' ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1")
            payment_rows = cursor.fetchall()
            
            if payment_rows:
                pay_seq = payment_rows[0]["ACCOUNT_PAYMENT_SEQ"]
                pay_mny = payment_rows[0]["AP_ACCOUNT_PAYMENT_MNY"]
                pay_dat = payment_rows[0]["ACCOUNT_PAYMENT_DAT"]
                
                last_actions = {
                    "Payment_Seq": pay_seq,
                    "Payment_Created": pay_dat,
                    "Payment_Money": pay_mny,
                    "Billed_Seq": None,
                    "Billed_Created": None,
                    "Billed_Money": None,
                }
                self.mongo_data[self.account_num]["Last_Actions"].append(last_actions)
                doc_status = "success"
        except Exception as e:
            print(f"MySQL connection error in getting payment data: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()
        return doc_status
    
    def convert_to_serializable(self, obj):
        # Recursively convert non-serializable types (like datetime, Decimal, date) to serializable ones
        if isinstance(obj, dict):
            return {key: self.convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, date):  # Handle date objects
            return obj.isoformat()
        else:
            return obj
    
    def format_json_object(self):
        # Build the JSON structure
        json_data = [{
            "version": 1,
            "Incident_Id": self.mongo_data[self.account_num]["Incident_Id"],
            "Account_Num": self.mongo_data[self.account_num]["Account_Num"],
            "Arrears": self.mongo_data[self.account_num]["Arrears"],
            "Created_By": self.mongo_data[self.account_num]["Created_By"],
            "Created_Dtm": self.mongo_data[self.account_num]["Created_Dtm"] or datetime.now().isoformat(),
            "Incident_Status": self.mongo_data[self.account_num]["Incident_Status"],
            "Incident_Status_Dtm": self.mongo_data[self.account_num]["Incident_Status_Dtm"] or datetime.now().isoformat(),
            "Status_Description": self.mongo_data[self.account_num]["Status_Description"],
            "File_Name_Dump": self.mongo_data[self.account_num]["File_Name_Dump"],
            "Batch_Id": self.mongo_data[self.account_num]["Batch_Id"],
            "Batch_Id_Tag_Dtm": self.mongo_data[self.account_num]["Batch_Id_Tag_Dtm"] or datetime.now().isoformat(),
            "External_Data_Update_On": self.mongo_data[self.account_num]["External_Data_Update_On"] or datetime.now().isoformat(),
            "Filtered_Reason": self.mongo_data[self.account_num]["Filtered_Reason"],
            "Export_On": self.mongo_data[self.account_num]["Export_On"] or datetime.now().isoformat(),
            "File_Name_Rejected": self.mongo_data[self.account_num]["File_Name_Rejected"],
            "Rejected_Reason": self.mongo_data[self.account_num]["Rejected_Reason"],
            "Incident_Forwarded_By": self.mongo_data[self.account_num]["Incident_Forwarded_By"],
            "Incident_Forwarded_On": self.mongo_data[self.account_num]["Incident_Forwarded_On"] or datetime.now().isoformat(),
            "Contact_Details": [
                {
                    "Contact_Type": contact["Contact_Type"],
                    "Contact": contact["Contact"],
                    "Create_Dtm": contact["Create_Dtm"] or datetime.now().isoformat(),
                    "Create_By": contact["Create_By"]
                }
                for contact in self.mongo_data[self.account_num]["Contact_Details"]
            ] if self.mongo_data[self.account_num]["Contact_Details"] else [],
            "Product_Details": [
                {
                    "Product_Label": product["Product_Label"],
                    "Customer_Ref": product["Customer_Ref"],
                    "Product_Seq": product["Product_Seq"],
                    "Equipment_Ownership": product["Equipment_Ownership"],
                    "Product_Id": product["Product_Id"],
                    "Product_Name": product["Product_Name"],
                    "Product_Status": product["Product_Status"],
                    "Effective_Dtm": product["Effective_Dtm"] or datetime.now().isoformat(),
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
            "Customer_Details": {
                "Customer_Name": self.mongo_data[self.account_num]["Customer_Details"][0]["Customer_Name"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Company_Name": self.mongo_data[self.account_num]["Customer_Details"][0]["Company_Name"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Company_Registry_Number": self.mongo_data[self.account_num]["Customer_Details"][0].get("Company_Registry_Number", "string"),
                "Full_Address": self.mongo_data[self.account_num]["Customer_Details"][0]["Full_Address"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Zip_Code": self.mongo_data[self.account_num]["Customer_Details"][0].get("Zip_Code", "string"),
                "Customer_Type_Name": self.mongo_data[self.account_num]["Customer_Details"][0].get("Customer_Type_Name", "string"),
                "Nic": self.mongo_data[self.account_num]["Customer_Details"][0]["Nic"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Customer_Type_Id": self.mongo_data[self.account_num]["Customer_Details"][0]["Customer_Type_Id"] if self.mongo_data[self.account_num]["Customer_Details"] else "string",
                "Customer_Type": self.mongo_data[self.account_num]["Customer_Details"][0].get("Customer_Type", "string")
            },
            "Account_Details": {
                "Account_Status": self.mongo_data[self.account_num]["Account_Details"][0]["Account_Status"] if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Acc_Effective_Dtm": self.mongo_data[self.account_num]["Account_Details"][0]["Acc_Effective_Dtm"] or datetime.now().isoformat(),
                "Acc_Activate_Date": self.mongo_data[self.account_num]["Account_Details"][0].get("Acc_Activate_Date", "None"),
                "Credit_Class_Id": self.mongo_data[self.account_num]["Account_Details"][0]["Credit_Class_Id"] if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Credit_Class_Name": self.mongo_data[self.account_num]["Account_Details"][0]["Credit_Class_Name"] if self.mongo_data[self.account_num]["Account_Details"] else "string",
                "Billing_Centre": self.mongo_data[self.account_num]["Account_Details"][0].get("Billing_Centre", "string"),
                "Customer_Segment": self.mongo_data[self.account_num]["Account_Details"][0].get("Customer_Segment", "string"),
                "Mobile_Contact_Tel": self.mongo_data[self.account_num]["Account_Details"][0].get("Mobile_Contact_Tel", "string"),
                "Daytime_Contact_Tel": self.mongo_data[self.account_num]["Account_Details"][0].get("Daytime_Contact_Tel", "string"),
                "Email_Address": self.mongo_data[self.account_num]["Account_Details"][0].get("Email_Address", "string"),
                "Last_Rated_Dtm": self.mongo_data[self.account_num]["Account_Details"][0].get("Last_Rated_Dtm", "None")
            },
            "Last_Actions": [
                {
                    "Billed_Seq": action["Billed_Seq"],
                    "Billed_Created": action["Billed_Created"] or datetime.now().isoformat(),
                    "Payment_Seq": action["Payment_Seq"],
                    "Payment_Created": action["Payment_Created"] or datetime.now().isoformat(),
                    "Payment_Money": action["Payment_Money"]
                }
                for action in self.mongo_data[self.account_num]["Last_Actions"]
            ] if self.mongo_data[self.account_num]["Last_Actions"] else [],
            "Marketing_Details": [
                {
                    "ACCOUNT_MANAGER": marketing["ACCOUNT_MANAGER"],
                    "CONSUMER_MARKET": marketing["CONSUMER_MARKET"],
                    "Informed_To": marketing["Informed_To"],
                    "Informed_On": marketing["Informed_On"] or datetime.now().isoformat()
                }
                for marketing in self.mongo_data[self.account_num]["Marketing_Details"]
            ] if self.mongo_data[self.account_num]["Marketing_Details"] else [],
            "Action": self.mongo_data[self.account_num]["Action"],
            "Validity_period": self.mongo_data[self.account_num]["Validity_period"],
            "Remark": self.mongo_data[self.account_num]["Remark"],
            "updatedAt": self.mongo_data[self.account_num]["updatedAt"] or datetime.now().isoformat(),
            "Rejected_By": self.mongo_data[self.account_num]["Rejected_By"],
            "Rejected_Dtm": self.mongo_data[self.account_num]["Rejected_Dtm"] or datetime.now().isoformat(),
            "Arrears_Band": self.mongo_data[self.account_num]["Arrears_Band"],
            "Source_Type": self.mongo_data[self.account_num]["Source_Type"]
        }]
        
        # Delegate serialization to convert_to_serializable and then dump to JSON
        return json.dumps(self.convert_to_serializable(json_data), indent=4)

if __name__ == "__main__":
    incident = create_incident(account_num="0000003746", incident_id="67890")
    payload = {}
    incident.create_incident(payload)