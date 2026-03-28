class TradeValidatorError(Exception):
    """Base exception for all validator errors."""
    pass

class ConfigurationError(TradeValidatorError):
    """Invalid configuration or missing required inputs."""
    pass

class DataIngestionError(TradeValidatorError):
    """Errors during CSV loading or schema validation."""
    pass

class OrbAPIError(TradeValidatorError):
    """Orb API communication failures."""
    pass

class ValidationError(TradeValidatorError):
    """Critical validation failures that prevent processing."""
    pass
