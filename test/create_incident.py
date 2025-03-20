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
        
        

    def create_incident(self, payload):
        status = self.read_customer_details()
            # if status =="success":
            #     self.read_payment_details
                
            # return self.client.post("/incidents", json=payload)
    
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
                    mongo_data[0]["customer_details"].append(contact_details_element)
                    
                    contact_details_element = {
                        "Contact_Type": "mobile",
                        "Contact": row["MOBILE_CONTACT"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    mongo_data[0]["customer_details"].append(contact_details_element)
                    
                    contact_details_element = {
                        "Contact_Type": "fix",
                        "Contact": row["WORK_CONTACT"],
                        "Create_Dtm": row["LOAD_DATE"],
                        "Create_By": "drs_admin"
                    }
                    mongo_data[0]["customer_details"].append(contact_details_element)
                    
                

                    # customer_details = {
                    #     "customer_ref": customer_ref,
                    #     "account_no": account_no,
                    # }

                    # mongo_data[key]["customer_details"].append(customer_details)
                
                    

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
        
        