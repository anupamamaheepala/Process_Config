# import mysql.connector
# from mysql.connector import Error

# def create_connection():
#     """
#     Establishes and returns a connection to the MySQL database.
#     """
#     try:
#         connection = mysql.connector.connect(
#             host='127.0.0.1', 
#             database='process_config',    
#             user='root',       
#             password=''
#         )

#         if connection.is_connected():
#             print("Connected to MySQL database")
#             return connection

#     except Error as e:
#         print(f"Error: {e}")
#         return None

# def fetch_data_from_table():
#     """
#     Fetches data from a table in the MySQL database.
#     """
#     connection = create_connection()

#     if connection:
#         try:
#             # Create a cursor object to execute SQL queries
#             cursor = connection.cursor()

#             # Query to fetch data from the table (example: fetching all rows from `debt_cust_detail`)
#             cursor.execute("SELECT * FROM debt_cust_detail")

#             # Fetch all the rows returned by the query
#             records = cursor.fetchall()

#             # Print each row
#             for row in records:
#                 print(row)

#         except Error as e:
#             print(f"Error: {e}")
#         finally:
#             # Close the cursor and the connection
#             if cursor:
#                 cursor.close()
#             if connection:
#                 connection.close()

# # Call the function to fetch data
# fetch_data_from_table()



import mysql.connector
from mysql.connector import Error

def create_connection():
    """
    Establishes and returns a connection to the MySQL database.
    """
    try:
        connection = mysql.connector.connect(
            host='127.0.0.1', 
            database='drs',    
            user='root',       
            password=''
        )

        if connection.is_connected():
            print("Connected to MySQL database")
            return connection

    except Error as e:
        print(f"Error: {e}")
        return None

def fetch_data_from_table():
    """
    Fetches data from a table in the MySQL database.
    """
    connection = create_connection()

    if connection:
        try:
            # Create a cursor object to execute SQL queries
            cursor = connection.cursor()

            # Query to fetch data from the table (example: fetching all rows from `debt_cust_detail`)
            cursor.execute("SELECT * FROM debt_cust_detail")

            # Fetch all the rows returned by the query
            records = cursor.fetchall()

            # Print each row
            for row in records:
                print(row)

        except Error as e:
            print(f"Error: {e}")
        finally:
            # Close the cursor and the connection
            if cursor:
                cursor.close()
            if connection:
                connection.close()

# Call the function to fetch data
fetch_data_from_table()
