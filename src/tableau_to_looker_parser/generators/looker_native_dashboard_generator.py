"""
LookerNativeDashboardGenerator - Generate Looker-native dashboard files.

Creates clean LookML dashboard files using Looker-native chart types and minimal configuration.
Replaces ECharts-based dashboard generation with simpler, maintainable approach.
"""

from typing import Dict, List, Optional
import logging
from datetime import datetime

from .base_generator import BaseGenerator
from .looker_element_generator import LookerElementGenerator
from ..models.dashboard_models import DashboardSchema, DashboardElement, ElementType
from .utils.layout_calculator import LayoutCalculator

logger = logging.getLogger(__name__)


class LookerNativeDashboardGenerator(BaseGenerator):
    """Generate Looker-native LookML dashboard files from Tableau dashboard schemas."""

    def __init__(self, template_dir: Optional[str] = None):
        """Initialize dashboard generator with clean template system."""
        super().__init__(template_dir)
        self.dashboard_extension = ".dashboard.lookml"

        # Initialize components
        self.element_generator = LookerElementGenerator()
        self.layout_calculator = LayoutCalculator()

    def generate(self, migration_data: Dict, output_dir: str) -> List[str]:
        """
        Generate Looker-native dashboard.lookml files from migration data.

        Args:
            migration_data: Migration data containing dashboards and worksheets
            output_dir: Output directory for generated files

        Returns:
            List of generated file paths
        """
        generated_files = []
        dashboards = migration_data.get("dashboards", [])

        if not dashboards:
            logger.info("No dashboards found in migration data")
            return generated_files

        # Set up model and explore context for element generation
        self._setup_element_generator_context(migration_data)

        for dashboard_data in dashboards:
            try:
                # Convert to schema if needed
                if isinstance(dashboard_data, dict):
                    dashboard = DashboardSchema(**dashboard_data)
                else:
                    dashboard = dashboard_data

                # Generate clean dashboard content
                dashboard_content = self._generate_dashboard_content(
                    dashboard, migration_data
                )

                # Write dashboard file with .dashboard.lookml extension
                file_path = self._write_dashboard_file(
                    dashboard.clean_name, dashboard_content, output_dir
                )
                generated_files.append(file_path)

                logger.info(
                    f"Generated Looker-native dashboard: {dashboard.clean_name}"
                )

            except Exception as e:
                dashboard_name = (
                    dashboard_data.get("name", "unknown")
                    if isinstance(dashboard_data, dict)
                    else "unknown"
                )
                logger.error(f"Failed to generate dashboard {dashboard_name}: {e}")

                # Log validation details for debugging
                if hasattr(e, "errors"):
                    logger.error(f"Validation errors: {e.errors()}")
                continue

        logger.info(f"Generated {len(generated_files)} Looker-native dashboard files")
        return generated_files

    def _setup_element_generator_context(self, migration_data: Dict):
        """Set up model and explore names for element generation."""
        # Get model name using same logic as model generator
        model_name = self._get_model_name(migration_data)

        # Use main table as explore name (following existing pattern)
        tables = migration_data.get("tables", [])
        if tables:
            # Use same cleaning logic as model template: clean_name filter
            raw_table_name = tables[0].get("name", "main_table")
            explore_name = self._clean_name(raw_table_name)
        else:
            explore_name = "main_table"

        self.element_generator.set_model_explore(model_name, explore_name)
        logger.debug(f"Set element generator context: {model_name}.{explore_name}")

    def _get_model_name(self, migration_data: Dict) -> str:
        """Get model name using same logic as model generator."""
        # First try to get from metadata
        metadata_name = migration_data.get("metadata", {}).get("project_name")
        if metadata_name and metadata_name != "default_model":
            return metadata_name

        # Fallback: Generate model name from connection like model generator does
        connections = migration_data.get("connections", [])
        if connections:
            connection = connections[0]
            connection_name = connection.get("name", "")
            if connection_name:
                # Use connection name as base for model name
                return f"{connection_name}_model"

        # Final fallback
        return "bigquery_super_store_sales_model"

    def _generate_dashboard_content(
        self, dashboard: DashboardSchema, migration_data: Dict
    ) -> str:
        """Generate clean Looker-native dashboard content."""

        # Convert dashboard elements to Looker-native format
        elements = self._convert_elements_to_looker_native(
            dashboard.elements, migration_data
        )

        # Skip global filters - we handle filters at element level now
        filters = []

        # Build template context with minimal properties
        context = {
            "dashboard_name": dashboard.clean_name,
            "title": dashboard.title,
            "description": getattr(dashboard, "description", ""),
            "layout_type": "newspaper",  # Standard Looker layout
            "preferred_viewer": "dashboards-next",
            "elements": elements,
            "filters": filters,
            "generation_timestamp": datetime.now().isoformat(),
            "source_dashboard": dashboard.name,
        }

        return self.template_engine.render_template(
            "looker_native_dashboard.j2", context
        )

    def _convert_elements_to_looker_native(
        self, elements: List[DashboardElement], migration_data: Dict
    ) -> List[Dict]:
        """Convert dashboard elements to Looker-native format using element generator."""
        looker_elements = []

        # Calculate height-based row positions and standardized widths for all elements
        height_based_rows = self.layout_calculator.calculate_height_based_rows(elements)

        for element in elements:
            try:
                if element.element_type == ElementType.WORKSHEET:
                    looker_element = self._convert_worksheet_element(
                        element, migration_data, height_based_rows
                    )
                    if looker_element:
                        looker_elements.append(looker_element)
                else:
                    # Skip non-worksheet elements for now (filters, text, etc.)
                    logger.debug(f"Skipping element type: {element.element_type}")
                    continue

            except Exception as e:
                logger.error(f"Failed to convert element {element.element_id}: {e}")
                continue

        return looker_elements

    def _convert_worksheet_element(
        self,
        element: DashboardElement,
        migration_data: Dict,
        height_based_rows: Dict[str, int] = None,
    ) -> Optional[Dict]:
        """Convert worksheet element using the LookerElementGenerator."""
        if not element.worksheet:
            logger.warning(f"Element {element.element_id} has no worksheet data")
            return None

        worksheet = element.worksheet

        # Calculate position using existing layout calculator with height-based rows and standardized widths
        position = self.layout_calculator.calculate_looker_position(
            element, migration_data, height_based_rows
        )

        # Generate element using the dedicated generator
        looker_element = self.element_generator.generate_element(worksheet, position)

        # Add any element-specific overrides
        if hasattr(element, "title_override") and element.title_override:
            looker_element["title"] = element.title_override

        # Add dashboard-specific filters if present
        element_filters = self._extract_element_filters(element, migration_data)
        if element_filters:
            looker_element.setdefault("filters", {}).update(element_filters)

        return looker_element

    def _extract_element_filters(
        self, element: DashboardElement, migration_data: Dict
    ) -> Dict[str, str]:
        """Extract filters specific to this element from migration data."""
        # TODO: Implement element-specific filter extraction
        # This will parse filters from the original Tableau dashboard element
        return {}

    def _convert_global_filters(self, global_filters: List) -> List[Dict]:
        """Convert global dashboard filters to simple Looker format."""
        looker_filters = []

        for filter_obj in global_filters:
            # Simplified filter format - just the essentials
            filter_config = {
                "name": filter_obj.name,
                "title": filter_obj.title,
                "type": "field_filter",  # Standard Looker filter type
                "default_value": filter_obj.default_value or "",
                "model": self.element_generator.model_name,
                "explore": self.element_generator.explore_name,
                "field": filter_obj.field,
            }
            looker_filters.append(filter_config)

        return looker_filters

    def _write_dashboard_file(
        self, dashboard_name: str, content: str, output_dir: str
    ) -> str:
        """Write dashboard content to .dashboard.lookml file."""
        output_path = self._ensure_output_dir(output_dir)

        # Use .dashboard.lookml extension for Looker-native dashboards
        filename = f"{dashboard_name}{self.dashboard_extension}"
        file_path = output_path / filename

        return self._write_file(content, file_path)
