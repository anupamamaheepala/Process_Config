import configparser
import sys
from incident_process.incident_create_copy import process_incident
from utils.api.connectAPI import read_api_config

if __name__ == "__main__":
    # Define the account number, incident ID, and API URL
    account_num = "0000003746"
    incident_id = 438911
    api_url = read_api_config()

    # Process the incident
    process_incident(account_num, incident_id, api_url)

