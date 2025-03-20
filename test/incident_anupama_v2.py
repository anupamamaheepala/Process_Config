import pymysql
import pymongo
from bson import ObjectId
import json
from decimal import Decimal
from datetime import datetime

def read_customer_detail(account_num):
    mongo_data = initialize_mongo_doc(account_num)
    try:
        doc_status, mongo_data = read_customer_details(mongo_data, account_num)
        doc_status, mongo_data = read_payment_details(mongo_data, account_num)
        doc_status, formatted_json_doc = format_json_object(mongo_data)
    except Exception as e:
        print(f"Error in processing customer details: {e}")
        formatted_json_doc = None
    return formatted_json_doc

def initialize_mongo_doc(account_num):
    """Initialize the Mongo document structure."""
    return {}

def read_customer_details(mongo_data, account_num):
    """Fetch customer details from the database and populate mongo_data."""
    mysql_conn = None
    cursor = None
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

        # Map the data to mongo format
        mongo_data = map_data_to_mongo_format(rows)
        doc_status = "success"
    except Exception as e:
        print(f"MySQL connection error in reading customer details: {e}")
        doc_status = "error"
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()
    return doc_status, mongo_data

def read_payment_details(mongo_data, account_num):
    """Fetch the payment details and add them to the Mongo document."""
    try:
        payment_data = get_payment_data(account_num)
        add_payment_data_to_mongo_data(mongo_data, payment_data)
        doc_status = "success"
    except Exception as e:
        print(f"MySQL connection error in reading payment details: {e}")
        doc_status = "error"
    return doc_status, mongo_data

def get_payment_data(account_number):
    """Fetch the latest payment data."""
    mysql_conn = None
    cursor = None
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f"SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = '{account_number}' ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1")
        payment_rows = cursor.fetchall()
    except Exception as e:
        print(f"MySQL connection error in getting payment data: {e}")
        payment_rows = []
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()
    return payment_rows

def add_payment_data_to_mongo_data(mongo_data, payment_data):
    """Add payment data to the Mongo document."""
    try:
        for key in mongo_data:
            mongo_data[key]["last_payment"] = payment_data
    except Exception as e:
        print(f"Error adding payment data: {e}")

def map_data_to_mongo_format(rows):
    """Map the fetched rows to a MongoDB document format."""
    mongo_data = {}

    try:
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
    except Exception as e:
        print(f"Error mapping data to Mongo format: {e}")

    return mongo_data

def format_json_object(mongo_data):
    """Format the Mongo data into a JSON object."""
    mongo_data = convert_to_serializable(mongo_data)

    try:
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
    except Exception as e:
        print(f"Error formatting the JSON object: {e}")
        json_data = {}

    json_output = json.dumps(json_data, indent=4)
    return json_output

def convert_to_serializable(obj):
    """Recursively converts Decimal and datetime values to serializable types."""
    try:
        if isinstance(obj, dict):
            return {key: convert_to_serializable(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_serializable(item) for item in obj]
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj
    except Exception as e:
        print(f"Error during serialization: {e}")
        return obj

core_config = {
    "mysql_host": "127.0.0.1",
    "mysql_database": "drs",
    "mysql_user": "root",
    "mysql_password": ""
}

# Example usage
account_number = "0000003746"
formatted_json_doc = read_customer_detail(account_number)
print(formatted_json_doc)
