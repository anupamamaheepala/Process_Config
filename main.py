from incident_process.incident_create import process_incident

if __name__ == "__main__":
    # Define the account number, incident ID, and API URL
    account_num = "0000003746"
    incident_id = "3"
    api_url = "http://220.247.224.226:9571/docs"  # Replace with the actual API URL

    # Process the incident
    process_incident(account_num, incident_id, api_url)