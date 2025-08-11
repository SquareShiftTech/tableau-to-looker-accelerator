"""
Project LookML generator - orchestrates all other generators.
"""

from pathlib import Path
from typing import Dict
import logging

from .base_generator import BaseGenerator
from .connection_generator import ConnectionGenerator
from .view_generator import ViewGenerator
from .model_generator import ModelGenerator
from .looker_native_dashboard_generator import LookerNativeDashboardGenerator

logger = logging.getLogger(__name__)


class ProjectGenerator(BaseGenerator):
    """Main generator that orchestrates all LookML file generation."""

    def __init__(self, template_dir=None):
        """Initialize the project generator with all sub-generators."""
        super().__init__(template_dir)

        self.connection_generator = ConnectionGenerator(template_dir)
        self.view_generator = ViewGenerator(template_dir)
        self.model_generator = ModelGenerator(template_dir)
        self.dashboard_generator = LookerNativeDashboardGenerator(template_dir)

        logger.info(
            "Project generator initialized with all sub-generators including Looker-native dashboard generator"
        )

    def generate_project_files(
        self, migration_data: Dict, output_dir: str
    ) -> Dict[str, str]:
        """
        Generate all LookML files for a migration project.

        Args:
            migration_data: Complete migration data
            output_dir: Directory to write files to

        Returns:
            Dictionary mapping file type to file path(s)
        """
        generated_files = {}

        try:
            # Ensure output directory exists
            self._ensure_output_dir(output_dir)

            # Generate connection file
            # generated_files["connection"] = self._generate_connection(
            # migration_data, output_dir
            # )

            # Generate view files
            generated_files["views"] = self._generate_views(migration_data, output_dir)

            # Generate model file
            generated_files["model"] = self._generate_model(migration_data, output_dir)

            # Generate dashboard files
            generated_files["dashboards"] = self._generate_dashboards(
                migration_data, output_dir
            )

            logger.info(f"Generated {len(generated_files)} file types in {output_dir}")
            return generated_files

        except Exception as e:
            logger.error(f"Failed to generate project files: {str(e)}")
            raise

    def _generate_connection(self, migration_data: Dict, output_dir: str) -> str:
        """Generate connection file if connections exist."""
        connections = migration_data.get("connections")
        if not connections:
            logger.warning("No connections found in migration data")
            return None

        return self.connection_generator.generate(connections, output_dir)

    def _generate_views(self, migration_data: Dict, output_dir: str) -> list:
        """Generate all view files."""
        return self.view_generator.generate_views(migration_data, output_dir)

    def _generate_model(self, migration_data: Dict, output_dir: str) -> str:
        """Generate model file with explores and joins."""
        return self.model_generator.generate(migration_data, output_dir)

    def _generate_dashboards(self, migration_data: Dict, output_dir: str) -> list:
        """Generate dashboard files if dashboards exist."""
        dashboards = migration_data.get("dashboards")
        if not dashboards:
            logger.info("No dashboards found in migration data")
            return []

        dashboard_files = self.dashboard_generator.generate(migration_data, output_dir)
        logger.info(f"Generated {len(dashboard_files)} dashboard files")
        return dashboard_files

    def validate_output_directory(self, output_dir: str) -> bool:
        """
        Validate that output directory is writable.

        Args:
            output_dir: Directory path to validate

        Returns:
            True if directory is valid and writable
        """
        try:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Test write access
            test_file = output_path / ".write_test"
            test_file.write_text("test")
            test_file.unlink()

            return True

        except Exception as e:
            logger.error(f"Output directory validation failed: {str(e)}")
            return False
