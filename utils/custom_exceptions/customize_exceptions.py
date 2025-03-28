class APIConfigError(Exception):
    """Raised when API config is missing/invalid."""
    pass

class IncidentCreationError(Exception):
    """Raised when incident creation fails."""
    pass

class PaymentDataError(Exception):
    """Raised when payment data is invalid."""
    pass