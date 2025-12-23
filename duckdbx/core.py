"""Main DuckDBX class for managing ephemeral DuckDB instances."""

import logging
from typing import Optional, Any
import duckdb

from duckdbx.exceptions import DuckDBConnectionError, ContainerError
from duckdbx.config import Config
from duckdbx.container import ContainerManager

logger = logging.getLogger(__name__)


class DuckDBX:
    """Main class for managing ephemeral DuckDB instances in Docker."""

    def __init__(
        self,
        container_image: Optional[str] = None,
        container_name: Optional[str] = None,
        port: Optional[int] = None,
    ):
        """
        Initialize DuckDBX instance.

        Args:
            container_image: Docker image name for DuckDB container
            container_name: Container name prefix
            port: Port number for DuckDB connection (auto-assigned if None)
        """
        self.config = Config(
            container_image=container_image,
            container_name=container_name,
            port=port,
        )
        self.config.validate()

        self.container_manager = ContainerManager(self.config)
        self.connection: Optional[duckdb.DuckDBPyConnection] = None
        self._started = False

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.stop()
        return False

    def start(self) -> None:
        """Start the DuckDB container and establish connection."""
        if self._started:
            logger.warning("DuckDBX instance already started")
            return

        try:
            # Start container
            connection_string = self.container_manager.start()
            logger.info(f"Container started: {connection_string}")

            # For now, we'll use a local DuckDB connection
            # In the future, this will connect to the containerized instance
            # For bare bones setup, we'll create a local connection
            # that will be replaced with container connection later
            self.connection = duckdb.connect(":memory:")
            self._started = True
            logger.info("DuckDB connection established")

        except ContainerError as e:
            logger.error(f"Failed to start container: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to establish connection: {e}")
            self.stop()  # Cleanup on error
            raise DuckDBConnectionError(f"Failed to start DuckDBX: {e}") from e

    def stop(self) -> None:
        """Stop the DuckDB container and close connection."""
        if not self._started:
            return

        try:
            if self.connection:
                self.connection.close()
                self.connection = None

            self.container_manager.stop()
            self._started = False
            logger.info("DuckDBX instance stopped")

        except Exception as e:
            logger.warning(f"Error during stop: {e}")

    def is_running(self) -> bool:
        """Check if the instance is running."""
        return self._started and self.container_manager.is_running()

    def execute(self, sql: str, parameters: Optional[list] = None) -> Any:
        """
        Execute SQL statement.

        Args:
            sql: SQL statement to execute
            parameters: Optional parameters for parameterized queries

        Returns:
            Cursor object
        """
        if not self._started or not self.connection:
            raise DuckDBConnectionError("DuckDBX instance not started")

        try:
            if parameters:
                return self.connection.execute(sql, parameters)
            else:
                return self.connection.execute(sql)
        except Exception as e:
            raise DuckDBConnectionError(f"Query execution failed: {e}") from e

    def query(self, sql: str, parameters: Optional[list] = None) -> Any:
        """
        Execute SQL query and return results.

        Args:
            sql: SQL query to execute
            parameters: Optional parameters for parameterized queries

        Returns:
            Query results (can be converted to DataFrame)
        """
        if not self._started or not self.connection:
            raise DuckDBConnectionError("DuckDBX instance not started")

        try:
            if parameters:
                return self.connection.execute(sql, parameters).fetchall()
            else:
                return self.connection.execute(sql).fetchall()
        except Exception as e:
            raise DuckDBConnectionError(f"Query execution failed: {e}") from e

