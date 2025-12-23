"""Docker container management for DuckDBX."""

import socket
import time
import logging
from typing import Optional
import docker
from docker.errors import DockerException

from duckdbx.exceptions import ContainerError
from duckdbx.config import Config

logger = logging.getLogger(__name__)


class ContainerManager:
    """Manages Docker container lifecycle for DuckDB instances."""

    def __init__(self, config: Config):
        """
        Initialize container manager.

        Args:
            config: Configuration object
        """
        self.config = config
        self.client = None
        self.container = None
        self.port = None
        self._docker_client = None

    def _get_docker_client(self):
        """Get or create Docker client."""
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
            except DockerException as e:
                raise ContainerError(f"Failed to connect to Docker: {e}") from e
        return self._docker_client

    def _find_available_port(self) -> int:
        """Find an available port on the host."""
        if self.config.port and self.config.port > 0:
            # Check if specified port is available
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("", self.config.port))
                sock.close()
                return self.config.port
            except OSError:
                raise ContainerError(
                    f"Port {self.config.port} is already in use"
                )
        else:
            # Find any available port
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("", 0))
            port = sock.getsockname()[1]
            sock.close()
            return port

    def start(self) -> str:
        """
        Start Docker container with DuckDB.

        Returns:
            Connection string for DuckDB instance
        """
        if self.is_running():
            logger.warning("Container is already running")
            return self.get_connection_string()

        try:
            client = self._get_docker_client()
            self.port = self._find_available_port()

            # Generate unique container name
            container_name = f"{self.config.container_name}-{int(time.time())}"

            # Start container
            logger.info(f"Starting container {container_name} on port {self.port}")
            self.container = client.containers.run(
                self.config.container_image,
                name=container_name,
                ports={"3141/tcp": self.port},  # DuckDB default HTTP port
                detach=True,
                remove=False,  # Keep container for inspection
            )

            # Wait for container to be ready
            self._wait_for_ready()

            logger.info(f"Container {container_name} started successfully")
            return self.get_connection_string()

        except DockerException as e:
            raise ContainerError(f"Failed to start container: {e}") from e

    def _wait_for_ready(self, timeout: int = 30) -> None:
        """Wait for container to be ready."""
        if not self.container:
            raise ContainerError("Container not initialized")

        start_time = time.time()
        while time.time() - start_time < timeout:
            self.container.reload()
            if self.container.status == "running":
                # Simple health check - container is running
                # TODO: Add actual DuckDB health check
                time.sleep(1)  # Give it a moment to fully start
                return
            time.sleep(0.5)

        raise ContainerError(
            f"Container did not become ready within {timeout} seconds"
        )

    def stop(self) -> None:
        """Stop and remove container."""
        if not self.container:
            return

        try:
            container_id = self.container.id
            logger.info(f"Stopping container {container_id}")
            self.container.stop()
            self.container.remove()
            logger.info(f"Container {container_id} stopped and removed")
        except DockerException as e:
            logger.warning(f"Error stopping container: {e}")
            raise ContainerError(f"Failed to stop container: {e}") from e
        finally:
            self.container = None
            self.port = None

    def is_running(self) -> bool:
        """Check if container is running."""
        if not self.container:
            return False

        try:
            self.container.reload()
            return self.container.status == "running"
        except DockerException:
            return False

    def get_connection_string(self) -> str:
        """
        Get connection string for DuckDB instance.

        Returns:
            Connection string (format: duckdb://localhost:PORT)
        """
        if not self.port:
            raise ContainerError("Container not started or port not assigned")
        return f"duckdb://localhost:{self.port}"

