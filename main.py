from incident_process.incident_create_copy import process_incident

if __name__ == "__main__":
    # Define the account number, incident ID, and API URL
    account_num = "0000003746"
    incident_id = 4381

    # Process the incident
    process_incident(account_num, incident_id)

