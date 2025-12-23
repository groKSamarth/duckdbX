"""Custom exceptions for DuckDBX."""


class DuckDBXError(Exception):
    """Base exception for all DuckDBX errors."""

    pass


class ContainerError(DuckDBXError):
    """Raised when Docker container operations fail."""

    pass


class DuckDBConnectionError(DuckDBXError):
    """Raised when DuckDB connection operations fail."""

    pass


class ConfigurationError(DuckDBXError):
    """Raised when configuration is invalid or missing."""

    pass

