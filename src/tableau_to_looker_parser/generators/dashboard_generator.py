"""
Dashboard LookML generator for Tableau to LookML migration.

Converts Tableau dashboard schemas into LookML dashboard files with proper
element positioning, filters, and visualization configurations.
"""

from typing import Dict, List, Optional
import logging
from datetime import datetime

from .base_generator import BaseGenerator
from ..models.dashboard_models import DashboardSchema, DashboardElement, ElementType
from .chart_configs.chart_config_factory import ChartConfigFactory
from .utils.field_mapping import FieldMapper
from .utils.layout_calculator import LayoutCalculator

logger = logging.getLogger(__name__)


class DashboardGenerator(BaseGenerator):
    """Generate LookML dashboard files from Tableau dashboard schemas."""

    def __init__(self, template_dir: Optional[str] = None):
        """Initialize dashboard generator with template engine."""
        super().__init__(template_dir)
        self.dashboard_extension = ".dashboard"

        # Initialize modular components
        self.chart_config_factory = ChartConfigFactory(prefer_echarts=True)
        self.field_mapper = FieldMapper()
        self.layout_calculator = LayoutCalculator()

    def generate(self, migration_data: Dict, output_dir: str) -> List[str]:
        """
        Generate dashboard.lkml files from migration data.

        Args:
            migration_data: Migration data containing dashboards
            output_dir: Output directory for generated files

        Returns:
            List of generated file paths
        """
        generated_files = []
        dashboards = migration_data.get("dashboards", [])

        if not dashboards:
            logger.info("No dashboards found in migration data")
            return generated_files

        for dashboard_data in dashboards:
            try:
                # Convert to schema if needed
                if isinstance(dashboard_data, dict):
                    dashboard = DashboardSchema(**dashboard_data)
                else:
                    dashboard = dashboard_data

                # Generate dashboard content
                dashboard_content = self._generate_dashboard_content(
                    dashboard, migration_data
                )

                # Write dashboard file
                file_path = self._write_dashboard_file(
                    dashboard.clean_name, dashboard_content, output_dir
                )
                generated_files.append(file_path)

            except Exception as e:
                # Enhanced error logging for validation errors
                dashboard_name = (
                    dashboard_data.get("name", "unknown")
                    if isinstance(dashboard_data, dict)
                    else "unknown"
                )
                logger.error(f"Failed to generate dashboard {dashboard_name}: {e}")
                logger.error(
                    f"Dashboard data keys: {list(dashboard_data.keys()) if isinstance(dashboard_data, dict) else 'Not a dict'}"
                )

                # Log specific validation error details if it's a pydantic error
                if hasattr(e, "errors"):
                    logger.error(f"Validation errors: {e.errors()}")
                continue

        logger.info(f"Generated {len(generated_files)} dashboard files")
        return generated_files

    def _generate_dashboard_content(
        self, dashboard: DashboardSchema, migration_data: Dict
    ) -> str:
        """Generate LookML dashboard content from schema."""

        # Convert dashboard elements to LookML format
        elements = self._convert_elements_to_lookml(dashboard.elements, migration_data)

        # Convert global filters
        filters = self._convert_filters_to_lookml(dashboard.global_filters)

        # Prepare template context
        context = {
            "dashboard_name": dashboard.clean_name,
            "title": dashboard.title,
            "source_dashboard_name": dashboard.name,
            "generation_timestamp": datetime.now().isoformat(),
            "description": getattr(dashboard, "description", ""),
            "layout_type": dashboard.layout_type,
            "elements": elements,
            "filters": filters,
            "cross_filter_enabled": dashboard.cross_filter_enabled,
            "preferred_viewer": "dashboards-next",  # Default to modern viewer
            "preferred_slug": getattr(dashboard, "preferred_slug", None),
            # Additional LookML dashboard properties
            "auto_run": True,
            "refresh_interval": None,
            "shared": True,
            "show_filters_bar": True,
            "show_title": True,
            "background_color": "#ffffff",
            "load_configuration": "wait",
        }

        return self.template_engine.render_template("dashboard.j2", context)

    def _convert_elements_to_lookml(
        self, elements: List[DashboardElement], migration_data: Dict
    ) -> List[Dict]:
        """Convert dashboard elements to LookML element format."""
        lookml_elements = []

        for element in elements:
            try:
                if element.element_type == ElementType.WORKSHEET:
                    lookml_element = self._convert_worksheet_element(
                        element, migration_data
                    )
                # lif element.element_type == ElementType.FILTER:
                #    lookml_element = self._convert_filter_element(element)
                # elif element.element_type == ElementType.PARAMETER:
                #    lookml_element = self._convert_parameter_element(element)
                # elif element.element_type == ElementType.TEXT:
                # lookml_element = self._convert_text_element(element)
                else:
                    logger.warning(f"Unsupported element type: {element.element_type}")
                    continue

                if lookml_element:
                    lookml_elements.append(lookml_element)

            except Exception as e:
                logger.error(f"Failed to convert element {element.element_id}: {e}")
                continue

        return lookml_elements

    def _convert_worksheet_element(
        self, element: DashboardElement, migration_data: Dict
    ) -> Optional[Dict]:
        """Convert worksheet element to LookML format."""
        if not element.worksheet:
            logger.warning(
                f"Worksheet element {element.element_id} has no worksheet data"
            )
            return None

        worksheet = element.worksheet

        if worksheet.name == "CD detail":
            print(f"🔧 DASHBOARD DEBUG: Processing worksheet '{worksheet.name}'")

        # Determine chart type from worksheet visualization using factory
        dashboard_context = {
            "dashboard_name": element.worksheet.name if element.worksheet else "",
            "project_name": migration_data.get("metadata", {}).get("project_name", ""),
        }
        chart_type = self.chart_config_factory.get_visualization_type(
            worksheet.visualization.chart_type, dashboard_context
        )

        # Get model and explore name
        model_name = migration_data.get("metadata", {}).get(
            "project_name", "tableau_migration"
        )
        # Use main table explore instead of worksheet-specific explores
        main_table = migration_data.get("tables", [{}])[0]
        explore_name = (
            main_table.get("name", "main_table") if main_table else "main_table"
        )

        # Build fields array from worksheet field usage
        fields = self.field_mapper.build_fields_from_worksheet(worksheet, explore_name)

        # Use existing dual-axis detection from visualization config
        is_dual_axis = getattr(worksheet.visualization, "is_dual_axis", False)

        # Build filters from worksheet
        filters = self.field_mapper.build_filters_from_worksheet(
            worksheet, explore_name
        )

        # Build sorts from worksheet
        sorts = self.field_mapper.build_sorts_from_worksheet(worksheet, explore_name)

        # Create LookML element matching the YAML format
        lookml_element = {
            "title": worksheet.name.replace("_", " ").title(),
            "name": worksheet.clean_name,
            "model": model_name,
            "explore": explore_name,
            "type": chart_type,
            "layout": self.layout_calculator.calculate_responsive_layout(
                element, migration_data
            ),
            "listen": {},  # Will be populated with filter connections
        }

        # Add optional fields
        if fields:
            lookml_element["fields"] = fields

        if filters:
            lookml_element["filters"] = filters

        if sorts:
            lookml_element["sorts"] = sorts

        # Add default limits
        lookml_element["limit"] = 500
        lookml_element["column_limit"] = 50

        # Add fill_fields for time-based charts
        fill_fields = self.field_mapper.get_fill_fields_from_worksheet(
            worksheet, explore_name
        )
        if fill_fields:
            lookml_element["fill_fields"] = fill_fields

        # Add chart-specific configurations using factory with Tableau styling
        color_palettes = migration_data.get("color_palettes", {})
        field_encodings = migration_data.get("field_encodings", {})

        print(f"🔧 DASHBOARD DEBUG: Processing worksheet '{worksheet.name}'")
        print(f"   Chart Type: {worksheet.visualization.chart_type}")
        print(f"   LookML Type: {chart_type}")
        print(f"   Color Palettes Available: {list(color_palettes.keys())}")
        print(f"   Field Encodings Available: {len(field_encodings)} worksheets")

        chart_config = self.chart_config_factory.generate_chart_config(
            worksheet.visualization.chart_type,
            worksheet,
            fields,
            explore_name,
            dashboard_context,
            color_palettes,
            field_encodings,
        )

        print(f"   Chart Config Generated: {bool(chart_config)}")
        if chart_config:
            print(f"   Chart Config Keys: {list(chart_config.keys())}")
            # Show ECharts-specific properties if present
            echarts_props = [
                "chartType",
                "colorPalette",
                "themeSelector",
                "showTooltip",
            ]
            for prop in echarts_props:
                if prop in chart_config:
                    print(f"   ✅ {prop}: {chart_config[prop]}")
                else:
                    print(f"   ❌ Missing: {prop}")
            lookml_element.update(chart_config)

        # Add dual-axis configuration if detected
        if is_dual_axis or self._is_dual_axis_chart_type(chart_type):
            dual_axis_config = self._generate_dual_axis_config(fields, explore_name)
            lookml_element.update(dual_axis_config)

        return lookml_element

    def _convert_filter_element(self, element: DashboardElement) -> Optional[Dict]:
        """Convert filter element to LookML format (usually handled as dashboard filters)."""
        # Filter elements are typically converted to dashboard-level filters
        # Rather than individual elements, but we can create a text element as placeholder
        return {
            "name": f"filter_{element.element_id}",
            "title": f"Filter: {element.filter_config.get('field', 'Unknown')}",
            "type": "text",
            "layout": {
                "column": int(element.position.x * 24),
                "row": int(element.position.y * 20),
                "width": max(1, int(element.position.width * 24)),
                "height": 1,  # Filters are typically 1 row high
            },
            "note": f"Filter element: {element.filter_config.get('field', 'Unknown')}",
        }

    def _convert_parameter_element(self, element: DashboardElement) -> Optional[Dict]:
        """Convert parameter element to LookML format."""
        return {
            "name": f"parameter_{element.element_id}",
            "title": f"Parameter: {element.parameter_config.get('name', 'Unknown')}",
            "type": "text",
            "layout": {
                "column": int(element.position.x * 24),
                "row": int(element.position.y * 20),
                "width": max(1, int(element.position.width * 24)),
                "height": 1,
            },
            "note": f"Parameter element: {element.parameter_config.get('name', 'Unknown')}",
        }

    def _convert_text_element(self, element: DashboardElement) -> Optional[Dict]:
        """Convert text element to LookML format."""
        return {
            "name": f"text_{element.element_id}",
            "title": "Text Element",
            "type": "text",
            "layout": {
                "column": int(element.position.x * 24),
                "row": int(element.position.y * 20),
                "width": max(1, int(element.position.width * 24)),
                "height": max(1, int(element.position.height * 10)),
            },
            "note": element.text_content or "Text element",
        }

    def _convert_filters_to_lookml(self, global_filters: List) -> List[Dict]:
        """Convert global dashboard filters to LookML YAML format."""
        lookml_filters = []

        for filter_obj in global_filters:
            lookml_filter = {
                "name": filter_obj.name,
                "title": filter_obj.title,
                "type": filter_obj.filter_type,
                "default_value": filter_obj.default_value or "",
                "allow_multiple_values": True,
                "required": False,
                "ui_config": {"type": "advanced", "display": "popover", "options": []},
                "model": filter_obj.explore,  # Assuming explore name is the model
                "explore": filter_obj.explore,
                "listens_to_filters": [],
                "field": filter_obj.field,
            }
            lookml_filters.append(lookml_filter)

        return lookml_filters

    def _is_dual_axis_chart_type(self, chart_type: str) -> bool:
        """Check if chart type indicates dual-axis visualization."""
        dual_axis_types = [
            "bar_and_line",
            "bar_and_area",
            "line_and_bar",
            "bar_and_scatter",
            "line_and_area",
        ]
        return chart_type in dual_axis_types

    def _generate_dual_axis_config(self, fields: List[str], explore_name: str) -> Dict:
        """Generate y_axes configuration for dual-axis charts."""
        if not fields:
            return {}

        # Extract measure fields (those with aggregation prefixes)
        measure_fields = [
            f
            for f in fields
            if any(
                prefix in f.lower() for prefix in ["total_", "sum_", "avg_", "count_"]
            )
        ]

        if len(measure_fields) < 2:
            # Single measure, still add basic y_axes for consistency
            return {
                "y_axes": [
                    {
                        "label": "",
                        "orientation": "left",
                        "series": [
                            {
                                "axisId": measure_fields[0]
                                if measure_fields
                                else fields[0],
                                "id": measure_fields[0]
                                if measure_fields
                                else fields[0],
                                "name": self._get_field_display_name(
                                    measure_fields[0] if measure_fields else fields[0]
                                ),
                            }
                        ],
                        "showLabels": True,
                        "showValues": True,
                        "valueFormat": '0,"K"',
                        "unpinAxis": False,
                        "tickDensity": "default",
                        "type": "linear",
                    }
                ]
            }

        # Multiple measures - create dual-axis configuration
        series = []
        colors = ["#5C8BB6", "#ED9149", "#4E7599", "#D56339"]  # Color palette

        for i, field in enumerate(measure_fields[:4]):  # Limit to 4 measures
            series.append(
                {
                    "axisId": field,
                    "id": field,
                    "name": self._get_field_display_name(field),
                }
            )

        config = {
            "y_axes": [
                {
                    "label": "",
                    "orientation": "left",
                    "series": series,
                    "showLabels": True,
                    "showValues": True,
                    "valueFormat": '0,"K"',
                    "unpinAxis": False,
                    "tickDensity": "default",
                    "type": "linear",
                }
            ],
            "x_axis_gridlines": False,
            "y_axis_gridlines": False,
            "show_y_axis_labels": True,
            "show_y_axis_ticks": True,
            "y_axis_combined": True,
            "series_colors": {},
        }

        # Add series colors
        for i, field in enumerate(measure_fields[: len(colors)]):
            config["series_colors"][field] = colors[i]

        return config

    def _get_field_display_name(self, field: str) -> str:
        """Convert field name to display name."""
        if not field:
            return ""

        # Remove explore prefix (e.g., "orders.total_sales" -> "total_sales")
        if "." in field:
            field = field.split(".")[-1]

        # Remove aggregation prefix and title case
        field = (
            field.replace("total_", "")
            .replace("sum_", "")
            .replace("avg_", "")
            .replace("count_", "")
        )
        return field.replace("_", " ").title()

    def _write_dashboard_file(
        self, dashboard_name: str, content: str, output_dir: str
    ) -> str:
        """Write dashboard content to file."""
        output_path = self._ensure_output_dir(output_dir)

        # Create dashboard filename - dashboards use .dashboard extension only
        filename = f"{dashboard_name}{self.dashboard_extension}"
        file_path = output_path / filename

        return self._write_file(content, file_path)
