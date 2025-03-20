from datetime import datetime
from decimal import Decimal
import json
import pymysql


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
        
        

    # def create_incident(self, payload):
    #     status = self.read_customer_details()
    #         # if status =="success":
    #         #     self.read_payment_details
                
    #         # return self.client.post("/incidents", json=payload)
    def create_incident(self, payload):
        status = self.read_customer_details()
        if status == "success":
            # self.get_payment_data()
            json_output = self.format_json_object()
            print(json_output)
            
        return status   
    
    def initialize_mongo_doc(self):
          # Initialize mongo_data as an empty dictionary
        key = (self.account_num)  # Use a tuple of customer_ref and account_num as key
        self.mongo_data[key] = {
            "case_id": None,
            "incident_id": None,
            "account_num": self.account_num,
            "customer_ref": None,
            "product_details": [],
            "customer_details": [],
            "contact_details": [],
            "account_details": [],
            "marketing_details": [],
            "last_action": [],
            "incident_status": [],
            "settlements": [],
            "last_payment": [],
        }
          
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
                
                if self.mongo_data [self.account_num]("account_num") != None:
                    self.mongo_data[0]["customer_ref"] = customer_ref
                    self.mongo_data[0]["account_num"] = account_num
                    self.mongo_data[0]["incident_id"] = self.incident_id
                    
                    contact_details_element = {
                        "Contact_Type": "email",
                        "Contact": row["TECNICAL_CONTACT_EMAIL"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    self.mongo_data[0]["contact_details"].append(contact_details_element)
                    
                    contact_details_element = {
                        "Contact_Type": "mobile",
                        "Contact": row["MOBILE_CONTACT"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    self.mongo_data[0]["contact_details"].append(contact_details_element)
                    
                    contact_details_element = {
                        "Contact_Type": "fix",
                        "Contact": row["WORK_CONTACT"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    self.mongo_data[0]["contact_details"].append(contact_details_element)
                    
                customer_details = {
                    "customer_name": row["CONTACT_PERSON"],
                    "Company_Name": row["COMPANY_NAME"],
                    "Company_Registry_Number": None,
                    "Full_Address": row["ASSET_ADDRESS"],
                    "Zip_Code": None,
                    "Customer_Type_Name": None,
                    "Nic": row["NIC"],
                    "Customer_Type_Id": row["CUSTOMER_TYPE_ID"],
                }
                self.mongo_data[self.account_num]["customer_details"].append(customer_details)
                
                product_entry = {
                    "service": row["PRODUCT_NAME"],
                    "product_label": row["PROMOTION_INTEG_ID"],
                    "product_status": row["ACCOUNT_STATUS_BSS"],
                }
                self.mongo_data[self.account_num]["product_details"].append(product_entry)

            # Map the data to mongo format
            # mongo_data = map_data_to_mongo_format(rows)
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
            
            pay_seq = payment_rows["ACCOUNT_PAYMENT_SEQ"]
            pay_mny = payment_rows["AP_ACCOUNT_PAYMENT_MNY"]
            pay_dat = payment_rows["ACCOUNT_PAYMENT_DAT"]
            
             
            """
            &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            put here last bill details.....
            ***********************************************************
            """
            
            last_actions = {
                    "Payment_Seq": pay_seq,
                    "Payment_Created": pay_dat,
                    "Payment_Money": pay_mny,
                    "Billed_Seq": None,
                    "Billed_Created": None,
                    "Billed_Money": None,
                }
            self.mongo_data[self.account_num]["last_action"].append(last_actions)
            doc_status = "success"
        except Exception as e:
            print(f"MySQL connection error in getting payment data: {e}")
        finally:
            if cursor:
                cursor.close()
            if mysql_conn:
                mysql_conn.close()
        return doc_status
    
    def format_json_object(self):
        # Prepare the MongoDB data as a JSON-compatible dictionary with simplified structure
        json_data = {
            f"{self.account_num}": {
                # Basic incident data
                "case_id": self.mongo_data[self.account_num]["case_id"],
                "incident_id": self.mongo_data[self.account_num]["incident_id"],
                "account_num": self.mongo_data[self.account_num]["account_num"],
                "customer_ref": self.mongo_data[self.account_num]["customer_ref"],
                
                # Product details list
                "product_details": [
                    {
                        "service": product["service"],
                        "product_label": product["product_label"],
                        "product_status": product["product_status"]
                    }
                    for product in self.mongo_data[self.account_num]["product_details"]
                ] if self.mongo_data[self.account_num]["product_details"] else [],
                
                # Customer details list
                "customer_details": [
                    {
                        "customer_name": details["customer_name"],
                        "Company_Name": details["Company_Name"],
                        "Company_Registry_Number": details["Company_Registry_Number"],
                        "Full_Address": details["Full_Address"],
                        "Zip_Code": details["Zip_Code"],
                        "Customer_Type_Name": details["Customer_Type_Name"],
                        "Nic": details["Nic"],
                        "Customer_Type_Id": details["Customer_Type_Id"]
                    }
                    for details in self.mongo_data[self.account_num]["customer_details"]
                ] if self.mongo_data[self.account_num]["customer_details"] else [],
                
                # Contact details list
                "contact_details": [
                    {
                        "Contact_Type": contact["Contact_Type"],
                        "Contact": contact["Contact"],
                        "Create_Dtm": contact["Create_Dtm"],
                        "Create_By": contact["Create_By"]
                    }
                    for contact in self.mongo_data[self.account_num]["contact_details"]
                ] if self.mongo_data[self.account_num]["contact_details"] else [],
                
                # Last action list
                "last_action": [
                    {
                        "Payment_Seq": action["Payment_Seq"],
                        "Payment_Created": action["Payment_Created"],
                        "Payment_Money": action["Payment_Money"],
                        "Billed_Seq": action["Billed_Seq"],
                        "Billed_Created": action["Billed_Created"],
                        "Billed_Money": action["Billed_Money"]
                    }
                    for action in self.mongo_data[self.account_num]["last_action"]
                ] if self.mongo_data[self.account_num]["last_action"] else [],
                
                "incident_status": self.mongo_data[self.account_num]["incident_status"],
                "settlements": self.mongo_data[self.account_num]["settlements"],
                "last_payment": self.mongo_data[self.account_num]["last_payment"]
            }
        }
        # Convert to serializable format and return as JSON string
        serializable_data = self.convert_to_serializable(json_data)
        return json.dumps(serializable_data, indent=4)

    def convert_to_serializable(self, obj):
        # Recursively convert non-serializable types (like datetime, Decimal) to serializable ones
        if isinstance(obj, dict):
            return {key: self.convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_to_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            # Convert datetime to ISO format string
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            # Convert Decimal to float
            return float(obj)
        else:
            return obj    