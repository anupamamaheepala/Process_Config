def read_customer_detail(account_num):
    mongo_data = initialize_mongo_doc(account_num)
    doc_status,mongo_data = read_customer_details(mongo_data)
    doc_status, mongo_data = read_payment_details(mongo_data)
    doc_status,formatted_json_doc = format_json_object(mongo_data)
    return formatted_json_doc
    
def initialize_mongo_doc(account_num):
    return mongo_data
    
def read_customer_details(mongo_data):
    return doc_status, mongo_data
    
def read_payment_details(mongo_data):
    return mongo_data
    
def format_json_object(mongo_data):
    return formatted_json_doc
    
def call_incident_create_api(formatted_json_doc):
    url = "https://api.example.com/incident/create"
    headers = {
        "Content-Type": "application/json",
        
# 