class DatabaseConnectionError(Exception):
    """Exception raised for errors in the database connection."""
    def __init__(self, message="Error connecting to the database"):
        self.message = message
        super().__init__(self.message)
