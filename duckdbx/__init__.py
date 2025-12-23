"""
DuckDBX - Ephemeral DuckDB instances in Docker containers.
"""

__version__ = "0.1.0"

from duckdbx.core import DuckDBX
from duckdbx.exceptions import (
    DuckDBXError,
    ContainerError,
    DuckDBConnectionError,
    ConfigurationError,
)

__all__ = [
    "DuckDBX",
    "DuckDBXError",
    "ContainerError",
    "DuckDBConnectionError",
    "ConfigurationError",
]

