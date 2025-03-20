import pymysql
import pymongo
from bson import ObjectId
import json
from decimal import Decimal
from datetime import datetime
from collections import defaultdict


def read_customer_detail(account_num):
    """Main entry point to read customer details."""
    try:
        mongo_data = initialize_mongo_doc(account_num)
        mongo_data = read_customer_details(mongo_data, account_num)
        mongo_data = read_payment_details(mongo_data, account_num)
        formatted_json_doc = format_json_object(mongo_data)
    except Exception as e:
        print(f"Error in processing customer details: {e}")
        formatted_json_doc = None
    return formatted_json_doc


def initialize_mongo_doc(account_num):
    """Initialize the Mongo document structure."""
    return defaultdict(lambda: {"ref_products": [], "customer_details": [], "incident_status": [], "settlements": [], "last_payment": []})


def fetch_data_from_db(query, params=()):
    """Generalized function to fetch data from the MySQL database."""
    try:
        mysql_conn = pymysql.connect(
            host=core_config["mysql_host"],
            database=core_config["mysql_database"],
            user=core_config["mysql_user"],
            password=core_config["mysql_password"]
        )
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return rows
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if mysql_conn:
            mysql_conn.close()


def read_customer_details(mongo_data, account_num):
    """Fetch customer details and populate mongo_data."""
    query = "SELECT * FROM debt_cust_detail WHERE ACCOUNT_NUM = %s"
    rows = fetch_data_from_db(query, (account_num,))
    return map_data_to_mongo_format(rows, mongo_data)


def read_payment_details(mongo_data, account_num):
    """Fetch payment details and add them to the Mongo document."""
    payment_data = get_payment_data(account_num)
    add_payment_data_to_mongo_data(mongo_data, payment_data)
    return mongo_data


def get_payment_data(account_number):
    """Fetch the latest payment data."""
    query = "SELECT * FROM debt_payment WHERE AP_ACCOUNT_NUMBER = %s ORDER BY ACCOUNT_PAYMENT_DAT DESC LIMIT 1"
    return fetch_data_from_db(query, (account_number,))


def add_payment_data_to_mongo_data(mongo_data, payment_data):
    """Add payment data to the Mongo document."""
    for key in mongo_data:
        mongo_data[key]["last_payment"] = payment_data


def map_data_to_mongo_format(rows, mongo_data):
    """Map the fetched rows to a MongoDB document format."""
    for row in rows:
        customer_ref = row["CUSTOMER_REF"]
        account_no = row["ACCOUNT_NUM"]
        key = (customer_ref, account_no)

        # Adding details to the mongo_data for the key
        customer_details = {"customer_ref": customer_ref, "account_no": account_no}
        mongo_data[key]["customer_details"].append(customer_details)

        # Adding product details
        product_entry = {
            "_id": ObjectId(),
            "service": row["PRODUCT_NAME"],
            "product_label": row["PROMOTION_INTEG_ID"],
            "product_status": row["ACCOUNT_STATUS_BSS"],
        }
        mongo_data[key]["ref_products"].append(product_entry)

    return mongo_data


def format_json_object(mongo_data):
    """Format the Mongo data into a JSON object."""
    mongo_data = convert_to_serializable(mongo_data)
    try:
        json_data = {
            f"{key[0]}_{key[1]}": {
                "case_id": value.get("case_id", 5),
                "incident_id": value.get("incident_id", 2001),
                "account_no": value["account_no"],
                "customer_ref": value["customer_ref"],
                "ref_products": format_ref_products(value["ref_products"]),
                "incident_status": format_incident_status(value["incident_status"]),
                "settlements": format_settlements(value["settlements"]),
                "customer_details": value["customer_details"],
                "last_payment": value.get("last_payment", [])
            }
            for key, value in mongo_data.items()
        }
        return json.dumps(json_data, indent=4)
    except Exception as e:
        print(f"Error formatting JSON: {e}")
        return "{}"


def format_ref_products(ref_products):
    """Format the ref_products data."""
    return [
        {
            "service": product["service"],
            "product_label": product["product_label"],
            "product_status": product["product_status"]
        }
        for product in ref_products
    ] if ref_products else []


def format_incident_status(incident_status):
    """Format the incident status data."""
    return [
        {
            "status": status.get("status", ""),
            "updated_at": status.get("updated_at", "")
        }
        for status in incident_status
    ] if incident_status else []


def format_settlements(settlements):
    """Format the settlements data."""
    return [
        {
            "amount": settlement.get("amount", 0),
            "settled_on": settlement.get("settled_on", "")
        }
        for settlement in settlements
    ] if settlements else []


def convert_to_serializable(obj):
    """Recursively convert Decimal and datetime to serializable types."""
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
