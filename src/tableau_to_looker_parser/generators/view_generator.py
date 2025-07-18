"""
View LookML generator.
"""

from typing import Dict, List, Set
import logging
from types import SimpleNamespace

from .base_generator import BaseGenerator

logger = logging.getLogger(__name__)


class ViewGenerator(BaseGenerator):
    """Generator for view.lkml files."""

    def generate_views(self, migration_data: Dict, output_dir: str) -> List[str]:
        """
        Generate all view.lkml files for a migration.

        Args:
            migration_data: Complete migration data
            output_dir: Directory to write files to

        Returns:
            List of paths to generated view files
        """
        try:
            view_files = []
            all_dimensions = migration_data.get("dimensions", [])
            all_measures = migration_data.get("measures", [])

            # Determine which views need to be generated
            view_names_needed = self._determine_view_names(migration_data)

            # Generate view file for each needed view
            for view_name in view_names_needed:
                view_file = self._generate_single_view(
                    view_name, migration_data, all_dimensions, all_measures, output_dir
                )
                if view_file:
                    view_files.append(view_file)

            logger.info(f"Generated {len(view_files)} view files")
            return view_files

        except Exception as e:
            logger.error(f"Failed to generate view files: {str(e)}")
            raise

    def _determine_view_names(self, migration_data: Dict) -> Set[str]:
        """Determine which view names are needed (tables + self-join aliases)."""
        view_names_needed = set()

        # Add actual table names
        for table in migration_data.get("tables", []):
            view_names_needed.add(table["name"])

        # Add self-join aliases
        self_join_aliases = self._detect_self_join_aliases(migration_data)
        view_names_needed.update(self_join_aliases)

        return view_names_needed

    def _detect_self_join_aliases(self, migration_data: Dict) -> Set[str]:
        """Detect self-join aliases that need separate view files."""
        self_join_aliases = set()
        actual_table_names = [
            table["name"] for table in migration_data.get("tables", [])
        ]

        for relationship in migration_data.get("relationships", []):
            if relationship.get("relationship_type") == "physical":
                table_aliases = relationship.get("table_aliases", {})

                # Group aliases by their table reference
                table_refs = {}
                for alias, table_ref in table_aliases.items():
                    if table_ref not in table_refs:
                        table_refs[table_ref] = []
                    table_refs[table_ref].append(alias)

                # For each table that has multiple aliases, it's a self-join
                for table_ref, aliases in table_refs.items():
                    if len(aliases) > 1:  # Multiple aliases for same table = self-join
                        for alias in aliases:
                            if (
                                alias not in actual_table_names
                            ):  # Only add non-table aliases
                                self_join_aliases.add(alias)

        return self_join_aliases

    def _generate_single_view(
        self,
        view_name: str,
        migration_data: Dict,
        all_dimensions: List[Dict],
        all_measures: List[Dict],
        output_dir: str,
    ) -> str:
        """Generate a single view file."""
        # Find the actual table this view represents
        actual_table, table_ref = self._resolve_view_table(view_name, migration_data)

        if not actual_table:
            logger.warning(f"Could not resolve table for view: {view_name}")
            return None

        actual_table_name = actual_table["name"]

        # Filter dimensions and measures for this specific table
        table_dimensions = [
            dim for dim in all_dimensions if dim.get("table_name") == actual_table_name
        ]
        table_measures = [
            measure
            for measure in all_measures
            if measure.get("table_name") == actual_table_name
        ]

        # Build view data
        view_data = {
            "name": view_name,
            "table_name": table_ref,
            "dimensions": table_dimensions,
            "measures": table_measures,
        }

        # Generate the view file
        return self._create_view_file(view_data, output_dir)

    def _resolve_view_table(self, view_name: str, migration_data: Dict) -> tuple:
        """Resolve view name to actual table and table reference."""
        # First check if it's an actual table name
        for table in migration_data.get("tables", []):
            if table["name"] == view_name:
                return table, table["table"]

        # If not found, check if it's an alias
        for relationship in migration_data.get("relationships", []):
            table_aliases = relationship.get("table_aliases", {})
            if view_name in table_aliases:
                table_ref = table_aliases[view_name]
                # Find the actual table with this reference
                for table in migration_data.get("tables", []):
                    if table["table"] == table_ref:
                        return table, table_ref

        return None, None

    def _create_view_file(self, view_data: Dict, output_dir: str) -> str:
        """Create a single view file from view data."""
        try:
            # Prepare template context
            context = {
                "view": SimpleNamespace(**view_data),
                "view_name": self._clean_name(view_data["name"]),
                "table_name": view_data["table_name"],
                "dimensions": view_data["dimensions"],
                "measures": view_data["measures"],
                "has_dimensions": len(view_data["dimensions"]) > 0,
                "has_measures": len(view_data["measures"]) > 0,
            }

            # Render template
            content = self.template_engine.render_template("basic_view.j2", context)

            # Write to file
            output_path = self._ensure_output_dir(output_dir)
            view_filename = (
                f"{self._clean_name(view_data['name'])}{self.lookml_extension}"
            )
            file_path = output_path / view_filename

            return self._write_file(content, file_path)

        except Exception as e:
            logger.error(
                f"Failed to generate view file for {view_data['name']}: {str(e)}"
            )
            raise
