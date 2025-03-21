from test.aaa import create_incident

# Example usage
if __name__ == "__main__":
    incident = create_incident("0000003746", "67890")
    incident.read_customer_details()
    incident.get_payment_data()
    json_output = incident.format_json_object()
    print(json_output)