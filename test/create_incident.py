class create_incident:
    account_num = None
    def __init__(self, account_num):
        self.account_num = account_num

    def create_incident(self, payload):
        self.initialize_mongo_doc(self.account_num)
        return self.client.post("/incidents", json=payload)
    
    def initialize_mongo_doc(self):
        mongo_data = {}  # Initialize mongo_data as an empty dictionary
        key = (self.account_num)  # Use a tuple of customer_ref and account_num as key
        mongo_data[key] = {
            "case_id": None,
            "incident_id": None,
            "account_no": self.account_num,
            "customer_ref": None,
            "ref_products": [],
            "customer_details": [],
            "incident_status": [],
            "settlements": [],
            "last_payment": [],
        }
        return mongo_data
    
    