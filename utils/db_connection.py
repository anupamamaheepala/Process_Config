import mysql.connector
from mysql.connector import Error

def create_connection():
    """
    Creates and returns a connection to the MySQL database.
    
    Returns:
        connection: A MySQL database connection object.
    """
    try:
        connection = mysql.connector.connect(
            host='your_host',  # Replace with your MySQL host
            database='your_database',  # Replace with your database name
            user='your_username',  # Replace with your MySQL username
            password='your_password'  # Replace with your MySQL password
        )
        if connection.is_connected():
            print("Successfully connected to the database")
            return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None
