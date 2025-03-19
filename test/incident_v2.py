import pymysql
import pymongo
from bson import ObjectId
import json

def get_mongo_data(account_number):
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{account_number}'")  
        rows = cursor.fetchall()
        
        # Step 1: Map the fetched rows to mongo_data
        mongo_data = map_data_to_mongo_format(rows)
        
        # Step 2: Fetch the payment data and add it to mongo_data
        payment_data = get_payment_data(account_number)
        add_payment_data_to_mongo_data(mongo_data, payment_data)
        
        cursor.close()
        mysql_conn.close()
        
        return mongo_data
    except Exception as e:
        print(f"MySQL connection error: {e}")
        return {}

def map_data_to_mongo_format(rows):
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

            mongo_data[key]["customer_details"].append(customer_details)

        product_entry = {
            "_id": ObjectId(),
            "service": row["PRODUCT_NAME"],
            "product_label": row["PROMOTION_INTEG_ID"],
            "product_status": row["ACCOUNT_STATUS_BSS"],
        }

        mongo_data[key]["ref_products"].append(product_entry)

    return mongo_data

def add_payment_data_to_mongo_data(mongo_data, payment_data):
    for key in mongo_data:
        mongo_data[key]["last_payment"] = payment_data

def prepare_json_output(mongo_data):
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
                    "account_no": details.get("account_ref"),
                }
                for details in value["customer_details"]
            ] if value["customer_details"] else [],
            "last_payment": value.get("last_payment", [])
        }
        for key, value in mongo_data.items()
    }

    json_output = json.dumps(json_data, indent=4)
    print(json_output)

def get_payment_data(account_number):
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"SELECT * FROM payment_table WHERE ACCOUNT_NUM = '{account_number}' ORDER BY PAYMENT_DATE DESC")
        payment_rows = cursor.fetchall()
        cursor.close()
        mysql_conn.close()
        return payment_rows
    except Exception as e:
        print(f"MySQL connection error: {e}")
        return []

core_config = {
    "mysql_host": "127.0.0.1",
    "mysql_database": "drs",
    "mysql_user": "root",
    "mysql_password": ""
}

account_number = "0000003746"

# Calling the functions
mongo_data = get_mongo_data(account_number)
if mongo_data:
    prepare_json_output(mongo_data)
