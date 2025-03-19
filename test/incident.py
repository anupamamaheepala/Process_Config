import pymysql
import pymongo
from bson import ObjectId
import json

def get_data_json_format(account_number):
    try:
        # mysql_conn = pymysql.connect(
        #     host=mysql_config["host"],
        #     database=mysql_config["database"],
        #     user=mysql_config["user"],
        #     password=mysql_config["password"]
        # )
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["database"],
            user=core_config["user"],
            password=core_config["password"]
        )

        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{account_number}'")  
        rows = cursor.fetchall()

        mongo_data = {}

        for row in rows:
            customer_ref = row["CUSTOMER_REF"]
            account_no = row["ACCOUNT_NUM"]
            key = (customer_ref, account_no)

            if key not in mongo_data:
                mongo_data[key] = {
                    "_id": ObjectId(),
                    "case_id": 5,
                    "incident_id": 2001,
                    "account_no": account_no,
                    "customer_ref": customer_ref,
                    # "arrears" = "ARREARS",
                    
                    "ref_products": [],
                    "customer_details": [],
                    "incident_status": [],
                    "settlements": [],
                    "last_payment": [],
                }
                
                customer_details = {
                    "customer_ref": customer_ref,
                    "account_no": account_no,
                }
                
                # account_details = {
                #     "account_status": "Account_status",
                    
                # }
                
                # last_acction = {
                #     "billed_seq": "Billed_seq",
                #     "last_action_date": "Last_action_date",
                # }
                
                mongo_data[key]["customer_details"].append(customer_details)

            product_entry = {
                "_id": ObjectId(),
                "service": row["PRODUCT_NAME"],
                "product_label": row["PROMOTION_INTEG_ID"],
                "product_status": row["ACCOUNT_STATUS_BSS"],
            }

            
            mongo_data[key]["ref_products"].append(product_entry)
            

        json_data = {
            f"{key[0]}_{key[1]}": {
                "case_id": value["case_id"],
                "incident_id": value["incident_id"],
                "account_no": value["account_no"],
                "customer_ref": value["customer_ref"],
                "ref_products": [
                    {
                        "service": product["service"],
                        "product_label": product["product_label"],
                        "product_status": product["product_status"]
                    }
                    for product in value["ref_products"]
                ] if value["ref_products"] else [],
                "incident_status": [
                    {
                        "status": status.get("status", ""),
                        "updated_at": status.get("updated_at", "")
                    }
                    for status in value["incident_status"]
                ] if value["incident_status"] else [],
                "settlements": [
                    {
                        "amount": settlement.get("amount", 0),
                        "settled_on": settlement.get("settled_on", "")
                    }
                    for settlement in value["settlements"]
                ] if value["settlements"] else [],
                "customer_details": [
                    {
                        "customer_ref": details.get("customer_ref"),
                        "account_no": details.get("customer_ref"),
                    }
                    for details in value["customer_details"]
                ] if value["customer_details"] else []
            }
            for key, value in mongo_data.items()
            
        }

        json_output = json.dumps(json_data, indent=4)
        print(json_output)

        cursor.close()
        mysql_conn.close()
    except Exception as e:
        print(f"MongoDB connection error: {e}")
        return
    # finally:
    #     mongo_client.close()
        
    

global(core_config) = {
    "mysql_host": "127.0.0.1",
    "mysql_database": "drs",
    "mysql_user": "root",
    "mysql_password": ""
}
account_number = "0000003746"

get_data_json_format(mysql_config, account_number)
