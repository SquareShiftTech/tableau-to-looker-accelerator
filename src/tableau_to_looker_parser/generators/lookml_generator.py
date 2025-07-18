"""
Refactored LookML generator using the new modular architecture.
This maintains backward compatibility with the original LookMLGenerator interface.
"""

from typing import Dict, Optional
import logging

from .project_generator import ProjectGenerator

logger = logging.getLogger(__name__)


class LookMLGenerator:
    """
    Main LookML generator class (refactored to use modular architecture).

    This class maintains backward compatibility with the original interface
    while internally using the new modular generator architecture.
    """

    def __init__(self, template_dir: Optional[str] = None):
        """
        Initialize the LookML generator.

        Args:
            template_dir: Directory containing template files. If None, uses default.
        """
        self.project_generator = ProjectGenerator(template_dir)
        self.lookml_extension = ".lkml"

        logger.info("LookML generator initialized (using refactored architecture)")

    def generate_project_files(
        self, migration_data: Dict, output_dir: str
    ) -> Dict[str, str]:
        """
        Generate all LookML files for a migration.

        Args:
            migration_data: Complete migration data
            output_dir: Directory to write files to

        Returns:
            Dictionary mapping file type to file path
        """
        return self.project_generator.generate_project_files(migration_data, output_dir)

    def generate_connection_file(self, connection, output_dir: str) -> str:
        """
        Generate a connection.lkml file.

        Args:
            connection: Connection data from JSON format
            output_dir: Directory to write the file to

        Returns:
            Path to the generated file
        """
        # Convert connection object to dict if needed
        if hasattr(connection, "__dict__"):
            connection_data = connection.__dict__
        else:
            connection_data = connection

        return self.project_generator.connection_generator.generate(
            [connection_data], output_dir
        )

    def generate_view_file(self, view, output_dir: str) -> str:
        """
        Generate a view.lkml file.

        Args:
            view: View data from JSON format
            output_dir: Directory to write the file to

        Returns:
            Path to the generated file
        """
        # Convert view object to migration_data format
        if hasattr(view, "__dict__"):
            view_data = view.__dict__
        else:
            view_data = view

        # Create minimal migration_data for single view
        migration_data = {
            "tables": [
                {"name": view_data["name"], "table": view_data.get("table_name", "")}
            ],
            "dimensions": view_data.get("dimensions", []),
            "measures": view_data.get("measures", []),
            "relationships": [],
        }

        view_files = self.project_generator.view_generator.generate_views(
            migration_data, output_dir
        )
        return view_files[0] if view_files else None

    def generate_model_file(self, migration_data: Dict, output_dir: str) -> str:
        """
        Generate a model.lkml file with explores and joins.

        Args:
            migration_data: Complete migration data with relationships
            output_dir: Directory to write the file to

        Returns:
            Path to the generated file
        """
        return self.project_generator.model_generator.generate(
            migration_data, output_dir
        )

    def validate_output_directory(self, output_dir: str) -> bool:
        """
        Validate that output directory is writable.

        Args:
            output_dir: Directory path to validate

        Returns:
            True if directory is valid and writable
        """
        return self.project_generator.validate_output_directory(output_dir)

    def _clean_view_name(self, name: str) -> str:
        """Clean view name for LookML (backward compatibility)."""
        return self.project_generator._clean_name(name)

    def _clean_connection_name(self, name: str) -> str:
        """Clean connection name for LookML (backward compatibility)."""
        return self.project_generator._clean_name(name)
