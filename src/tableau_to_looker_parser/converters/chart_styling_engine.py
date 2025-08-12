"""
ChartStylingEngine - Apply styling configurations to dashboard elements based on chart type.

Loads YAML styling configurations and applies them to Looker dashboard elements,
using extracted Tableau styling data (colors, fonts, titles, tooltips).
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class ChartStylingEngine:
    """Apply styling configurations to dashboard elements based on chart type."""

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize styling engine with YAML configuration."""
        if config_path is None:
            # Default to config directory relative to this file
            config_dir = Path(__file__).parent.parent / "config"
            config_path = config_dir / "chart_styling.yaml"

        self.config_path = config_path
        self.styling_config = self._load_styling_config()

    def _load_styling_config(self) -> Dict[str, Any]:
        """Load chart styling configuration from YAML file."""
        try:
            with open(self.config_path, "r") as file:
                config = yaml.safe_load(file)
                logger.info(f"Loaded chart styling config from {self.config_path}")
                return config.get("chart_styling_config", {})
        except Exception as e:
            logger.error(f"Failed to load chart styling config: {e}")
            return {}

    def apply_styling(
        self,
        element: Dict[str, Any],
        worksheet_styling: Dict[str, Any],
        chart_type: str,
    ) -> Dict[str, Any]:
        """
        Apply styling configuration to a dashboard element.

        Args:
            element: Base dashboard element configuration
            worksheet_styling: Extracted Tableau styling data (colors, titles, etc.)
            chart_type: Looker chart type (looker_donut_multiples, looker_bar, etc.)

        Returns:
            Enhanced element configuration with styling applied
        """
        if not self.styling_config:
            logger.warning("No styling configuration loaded")
            return element

        # Get styling config for this chart type
        chart_config = self.styling_config.get(chart_type, {})
        if not chart_config:
            logger.debug(f"No styling configuration found for chart type: {chart_type}")
            # Try fallback to generic table styling
            chart_config = self.styling_config.get("table", {})

        if not chart_config:
            return element

        logger.debug(f"Applying styling for chart type: {chart_type}")

        # Apply styling properties
        styled_element = element.copy()

        # Apply color mappings if supported and available
        if self._supports_styling(chart_config, "color_mappings"):
            color_config = self._apply_color_mappings(worksheet_styling, chart_config)
            if color_config:
                styled_element.update(color_config)

        # Apply title styling if supported and available
        if self._supports_styling(chart_config, "title_style"):
            title_config = self._apply_title_styling(worksheet_styling, chart_config)
            if title_config:
                styled_element.update(title_config)

        # Apply table styling if supported and available
        if self._supports_styling(chart_config, "table_style"):
            table_config = self._apply_table_styling(worksheet_styling, chart_config)
            if table_config:
                styled_element.update(table_config)

        # Apply chart-specific properties
        chart_properties = chart_config.get("properties", {})
        for prop, value in chart_properties.items():
            if value and prop not in styled_element:  # Don't override existing values
                styled_element[prop] = value

        logger.debug(
            f"Applied {len(chart_properties)} styling properties to {chart_type}"
        )
        return styled_element

    def _supports_styling(
        self, chart_config: Dict[str, Any], styling_type: str
    ) -> bool:
        """Check if chart configuration supports a specific styling type."""
        supported = chart_config.get("supported_styling", [])
        return styling_type in supported

    def _apply_color_mappings(
        self, worksheet_styling: Dict[str, Any], chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply color mappings from Tableau styling to Looker element."""
        color_config = {}

        # Get color mappings from extracted Tableau styling
        color_mappings_data = worksheet_styling.get("color_mappings", {})
        if not color_mappings_data or color_mappings_data.get("type") != "categorical":
            return color_config

        mappings = color_mappings_data.get("mappings", {})
        if not mappings:
            return color_config

        # Apply series colors for categorical data
        properties = chart_config.get("properties", {})
        if properties.get("series_colors"):
            color_config["series_colors"] = mappings
            logger.debug(f"Applied color mappings: {list(mappings.keys())}")

        return color_config

    def _apply_title_styling(
        self, worksheet_styling: Dict[str, Any], chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply title styling from Tableau styling to Looker element."""
        title_config = {}

        # Get title styling from extracted Tableau styling
        title_style_data = worksheet_styling.get("title_style", {})
        if not title_style_data:
            return title_config

        # Map Tableau title properties to Looker properties
        title_text = title_style_data.get("text")
        if title_text:
            title_config["title"] = title_text

        # Apply chart-specific title styling from config
        config_title_style = chart_config.get("title_style", {})
        if config_title_style:
            # Map styling properties to Looker format
            title_formatting = {}

            if config_title_style.get("font_weight") == "bold" and title_style_data.get(
                "bold"
            ):
                title_formatting["font_weight"] = "bold"

            if config_title_style.get("font_size"):
                title_formatting["font_size"] = config_title_style["font_size"]

            if config_title_style.get("text_align"):
                title_formatting["text_align"] = config_title_style["text_align"]

            if title_formatting:
                title_config["title_style"] = title_formatting

        if title_config:
            logger.debug(f"Applied title styling: {list(title_config.keys())}")

        return title_config

    def _apply_table_styling(
        self, worksheet_styling: Dict[str, Any], chart_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Apply table styling from extracted Tableau styling to Looker element."""
        table_config = {}

        # Get table styling from extracted Tableau styling
        table_style_data = worksheet_styling.get("table_style", {})
        if not table_style_data:
            return table_config

        # Map header styling (black headers with white text)
        header_styles = table_style_data.get("header", {})
        if header_styles:
            # Header background colors
            if "column_header_bg" in header_styles:
                table_config["header_background_color"] = header_styles[
                    "column_header_bg"
                ]
            if "row_header_bg" in header_styles:
                table_config["row_header_background_color"] = header_styles[
                    "row_header_bg"
                ]

        # Map label styling (header text colors)
        label_styles = table_style_data.get("labels", {})
        if label_styles:
            # Header text colors
            if "column_text_color" in label_styles:
                table_config["header_font_color"] = label_styles["column_text_color"]
            if "row_text_color" in label_styles:
                table_config["row_header_font_color"] = label_styles["row_text_color"]
            if "text_align" in label_styles:
                table_config["header_font_align"] = label_styles["text_align"]

        # Map cell styling (ash cell colors)
        cell_styles = table_style_data.get("cells", {})
        if cell_styles:
            if "background_color" in cell_styles:
                table_config["cell_background_color"] = cell_styles["background_color"]
            if "text_align" in cell_styles:
                table_config["cell_text_align"] = cell_styles["text_align"]
            if "vertical_align" in cell_styles:
                table_config["cell_vertical_align"] = cell_styles["vertical_align"]

        # Map table background
        if "table_background" in table_style_data:
            table_config["table_background_color"] = table_style_data[
                "table_background"
            ]

        # Map data value styling (ash colors for cell values)
        data_value_styles = table_style_data.get("data_values", {})
        if data_value_styles:
            if "value_color_palette" in data_value_styles:
                # table_config["value_color_palette"] = data_value_styles[
                #    "value_color_palette"
                # ]
                pass  # TODO: implement this

        if table_config:
            logger.debug(f"Applied table styling: {list(table_config.keys())}")

        return table_config

    def get_supported_chart_types(self) -> List[str]:
        """Get list of chart types with styling configurations."""
        return list(self.styling_config.keys())

    def get_chart_styling_properties(self, chart_type: str) -> Dict[str, Any]:
        """Get styling properties for a specific chart type."""
        return self.styling_config.get(chart_type, {})

    def get_global_styling(self) -> Dict[str, Any]:
        """Get global styling defaults."""
        try:
            with open(self.config_path, "r") as file:
                config = yaml.safe_load(file)
                return config.get("global_styling", {})
        except Exception as e:
            logger.error(f"Failed to load global styling config: {e}")
            return {}
