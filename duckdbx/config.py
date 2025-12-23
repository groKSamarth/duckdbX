"""Configuration management for DuckDBX."""

import os
from typing import Optional, Dict, Any
from duckdbx.exceptions import ConfigurationError


class Config:
    """Configuration loader with priority: params > env vars."""

    def __init__(
        self,
        container_image: Optional[str] = None,
        container_name: Optional[str] = None,
        port: Optional[int] = None,
    ):
        """
        Initialize configuration.

        Args:
            container_image: Docker image name for DuckDB container
            container_name: Container name prefix
            port: Port number for DuckDB connection (auto-assigned if None)
        """
        # Priority: params > env vars > defaults
        self.container_image = (
            container_image
            or os.getenv("DUCKDBX_CONTAINER_IMAGE")
            or "duckdbx:latest"
        )
        self.container_name = (
            container_name
            or os.getenv("DUCKDBX_CONTAINER_NAME")
            or "duckdbx"
        )
        self.port = port or int(os.getenv("DUCKDBX_PORT", "0"))

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "container_image": self.container_image,
            "container_name": self.container_name,
            "port": self.port,
        }

    def validate(self) -> None:
        """Validate configuration."""
        if not self.container_image:
            raise ConfigurationError("container_image is required")
        if not self.container_name:
            raise ConfigurationError("container_name is required")

