import pymysql
import pymongo
from bson import ObjectId
from datetime import datetime
import json

# MySQL Connection
mysql_conn = pymysql.connect(
    host='127.0.0.1', 
    database='drs',    
    user='root',       
    password=''
)

# MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb+srv://processconfig:pc12345@processcluster.s5cyv.mongodb.net/?retryWrites=true&w=majority&appName=ProcessCluster")
mongo_db = mongo_client["process_config"]
mongo_collection = mongo_db["incident"]

# Fetch data from MySQL
cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
cursor.execute("SELECT * FROM debt_cust_detail where ACCOUNT_NUM = '0000003746'")  # Modify table name
rows = cursor.fetchall()

# Transform Data
mongo_data = {}

for row in rows:
    customer_ref = row["CUSTOMER_REF"]
    account_no = row["ACCOUNT_NUM"]

    key = (customer_ref, account_no)
    
    if key not in mongo_data:
        mongo_data[key] = {
            "_id": ObjectId(),  # Generate a unique ID
            "case_id": 5,  # Customize as needed
            "incident_id": 2001,  # Customize as needed
            "account_no": account_no,
            "customer_ref": customer_ref,
            "ref_products": [],
            "incident_status": [],
        }

    product_entry = {
        "_id": ObjectId(),
        "service": row["PRODUCT_NAME"],
        "product_label": row["PROMOTION_INTEG_ID"],
        "product_status": row["ACCOUNT_STATUS_BSS"],
        # "status_Dtm": {"$date": datetime.strptime(row["ACCOUNT_START_DTM_BSS"], "%d/%m/%Y %H:%M:%S")}
    }

    mongo_data[key]["ref_products"].append(product_entry)
# print("/n/n/n/n/n/n/n/n/n/n")
print(mongo_data)

# Convert this to JSON format here
# json_output = json.dumps(json_data, indent=4)

# print(json_output)
# Convert mongo_data to a JSON-compatible format
json_data = {
    f"{key[0]}_{key[1]}": {  # Convert tuple key to a string
        # "_id": str(value["_id"]),  # Convert ObjectId to string
        "case_id": value["case_id"],
        "incident_id": value["incident_id"],
        "account_no": value["account_no"],
        "customer_ref": value["customer_ref"],
        "ref_products": [
            {
                # "_id": str(product["_id"]),  # Convert ObjectId to string
                "service": product["service"],
                "product_label": product["product_label"],
                "product_status": product["product_status"]
            }
            for product in value["ref_products"]
        ]
    }
    for key, value in mongo_data.items()
}

# Convert to JSON string
json_output = json.dumps(json_data, indent=4)

print("____-------------------------------------------------------------------------------")
print(json_output)

# Insert into MongoDB
mongo_collection.insert_many(mongo_data.values())

# Close connections
cursor.close()
mysql_conn.close()
mongo_client.close()

print("Data migration completed successfully!")