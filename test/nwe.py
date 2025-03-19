import pymysql
from bson import ObjectId
import json
from datetime import datetime

def fetch_customer_data(account_num):
    """
    Main function to fetch and format customer details into JSON.
    """
    mongo_data = initialize_mongo_doc(account_num)
    
    doc_status, mongo_data = read_customer_details(mongo_data)
    doc_status, mongo_data = read_payment_details(mongo_data)

    doc_status, formatted_json_doc = format_json_object(mongo_data)
    return formatted_json_doc

def initialize_mongo_doc(account_num):
    """
    Initializes the mongo_data dictionary by fetching basic details from MySQL.
    """
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = '{account_num}'")
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
                    "ref_products": [],
                    "customer_details": {
                        "customer_ref": customer_ref,
                        "account_no": account_no,
                        "full_name": row.get("NAME", ""),
                        "email": "",
                        "phone": "",
                        "address": ""
                    },
                    "incident_status": [],
                    "settlements": [],
                    "last_payment": [],
                }

            # Add product details to ref_products
            product_entry = {
                "_id": ObjectId(),
                "service": row["PRODUCT_NAME"],
                "product_label": row["PROMOTION_INTEG_ID"],
                "product_status": row["ACCOUNT_STATUS_BSS"],
                "status_Dtm": datetime.strptime(row["product_status_Dtm"], "%d/%m/%Y %H:%M:%S") if row.get("product_status_Dtm") else None
            }

            mongo_data[key]["ref_products"].append(product_entry)

        cursor.close()
        mysql_conn.close()
        return mongo_data
    except Exception as e:
        print(f"Error initializing MongoDB document: {e}")
        return {}

def read_customer_details(mongo_data):
    """
    Fetches additional customer details from MySQL and updates the mongo_data dictionary.
    """
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

        for key, value in mongo_data.items():
            account_no = value["account_no"]

            # Fetch additional customer details from MySQL
            cursor.execute(f"""
                SELECT * FROM customer_details 
                WHERE ACCOUNT_NUM = '{account_no}'
            """)
            customer_details = cursor.fetchone()

            if customer_details:
                # Update customer details in mongo_data
                value["customer_details"].update({
                    "full_name": customer_details.get("FULL_NAME", ""),
                    "email": customer_details.get("EMAIL", ""),
                    "phone": customer_details.get("PHONE", ""),
                    "address": customer_details.get("ADDRESS", "")
                })

        cursor.close()
        mysql_conn.close()
        return True, mongo_data
    except Exception as e:
        print(f"Error reading customer details: {e}")
        return False, mongo_data

def read_payment_details(mongo_data):
    """
    Fetches payment details from MySQL and updates the mongo_data dictionary.
    """
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)

        for key, value in mongo_data.items():
            account_no = value["account_no"]

            cursor.execute(f"""
                SELECT * FROM debt_payment 
                WHERE AP_ACCOUNT_NUMBER = '{account_no}' 
                ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1
            """)
            payment_details = cursor.fetchall()

            if payment_details:
                value["last_payment"] = []  # Ensure last_payment is updated

                for payment in payment_details:
                    value["last_payment"].append({
                        "amount": payment.get("AMOUNT", 0),
                        "settled_on": payment.get("SETTLED_ON", ""),
                        "payment_method": payment.get("PAYMENT_METHOD", "")
                    })

        cursor.close()
        mysql_conn.close()
        return True, mongo_data
    except Exception as e:
        print(f"Error reading payment details: {e}")
        return False, mongo_data

def format_json_object(mongo_data):
    """
    Formats the mongo_data dictionary into a JSON object.
    """
    if not isinstance(mongo_data, dict):
        print("Error: mongo_data is not a dictionary.")
        return False, None

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
                    "product_status": product["product_status"],
                    "status_Dtm": product["status_Dtm"].isoformat() if product["status_Dtm"] else None
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
                    "settled_on": settlement.get("settled_on", ""),
                    "payment_method": settlement.get("payment_method", "")
                }
                for settlement in value["settlements"]
            ] if value["settlements"] else [],
            "customer_details": value["customer_details"],
            "last_payment": value.get("last_payment", [])  # Ensure last_payment is included
        }
        for key, value in mongo_data.items()
    }

    json_output = json.dumps(json_data, indent=4, default=str)
    print(json_output)
    return True, json_output

# Configuration
core_config = {
    "mysql_host": "127.0.0.1",
    "mysql_database": "drs",
    "mysql_user": "root",
    "mysql_password": ""
}

# Example usage
account_number = "0000003746"
formatted_json_doc = fetch_customer_data(account_number)