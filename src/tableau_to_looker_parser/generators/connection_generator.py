"""
Connection LookML generator.
"""

from typing import Dict, List
import logging

from .base_generator import BaseGenerator

logger = logging.getLogger(__name__)


class ConnectionGenerator(BaseGenerator):
    """Generator for connection.lkml files."""

    def generate(self, connections: List[Dict], output_dir: str) -> str:
        """
        Generate a connection.lkml file.

        Args:
            connections: List of connection data from JSON format
            output_dir: Directory to write the file to

        Returns:
            Path to the generated file
        """
        try:
            # Find the primary BigQuery connection (skip federated wrapper)
            connection_data = self._find_primary_connection(connections)

            if not connection_data:
                raise ValueError("No valid connection found")

            # Prepare template context
            context = self._build_context(connection_data)

            # Render template
            content = self.template_engine.render_template("connection.j2", context)

            # Write to file
            output_path = self._ensure_output_dir(output_dir)
            file_path = output_path / f"connection{self.lookml_extension}"

            return self._write_file(content, file_path)

        except Exception as e:
            logger.error(f"Failed to generate connection file: {str(e)}")
            raise

    def _find_primary_connection(self, connections: List[Dict]) -> Dict:
        """Find the primary connection to use."""
        # Find the primary BigQuery connection (skip federated wrapper)
        for conn in connections:
            if conn.get("type") == "bigquery" and conn.get("name"):
                return conn

        # Fallback to first non-federated connection
        for conn in connections:
            if conn.get("type") != "federated":
                return conn

        # Final fallback to first connection
        return connections[0] if connections else {}

    def _build_context(self, connection_data: Dict) -> Dict:
        """Build template context for connection."""
        return {
            "connection": connection_data,
            "connection_name": connection_data.get("name")
            or f"bigquery_{connection_data.get('dataset', 'default').lower()}",
            "database_type": connection_data.get("type", "unknown"),
            "host": connection_data.get("host") or connection_data.get("server"),
            "port": connection_data.get("port"),
            "database": connection_data.get("database")
            or connection_data.get("dataset"),
            "username": connection_data.get("username")
            or connection_data.get("service_account"),
            "schema": connection_data.get("schema"),
        }
