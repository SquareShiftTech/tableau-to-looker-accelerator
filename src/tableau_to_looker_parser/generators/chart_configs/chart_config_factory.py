"""
Factory for creating chart configuration objects.

Determines which chart configuration class to use based on chart type,
dashboard requirements, or explicit preferences.
"""

from typing import Optional, Dict, Any
from .base_chart_config import BaseChartConfig
from .echarts_config import EChartsConfig
from .standard_config import StandardConfig


class ChartConfigFactory:
    """Factory for creating appropriate chart configuration objects."""

    def __init__(self, prefer_echarts: bool = True):
        """
        Initialize chart configuration factory.

        Args:
            prefer_echarts: Whether to prefer ECharts over standard Looker charts
        """
        self.prefer_echarts = prefer_echarts
        self._echarts_config = EChartsConfig()
        self._standard_config = StandardConfig()

    def get_chart_config(
        self, chart_type: str, dashboard_context: Optional[Dict[str, Any]] = None
    ) -> BaseChartConfig:
        """
        Get appropriate chart configuration object.

        Args:
            chart_type: Tableau chart type (e.g., 'heatmap', 'donut', 'bar')
            dashboard_context: Optional dashboard context for decision making

        Returns:
            Appropriate chart configuration object
        """
        # Check for explicit requirements in dashboard context
        if dashboard_context:
            # Force ECharts for Connected Devices dashboard patterns
            if self._is_connected_devices_pattern(dashboard_context):
                return self._echarts_config

            # Force standard Looker for certain dashboard types
            if dashboard_context.get("force_standard_looker", False):
                return self._standard_config

        # Handle special ECharts-only chart types
        echarts_only_types = ["heatmap", "donut", "grouped_bar"]
        if chart_type.lower() in echarts_only_types:
            return self._echarts_config

        # Use preference for charts supported by both
        if self.prefer_echarts and self._echarts_config.can_handle_chart_type(
            chart_type
        ):
            return self._echarts_config
        elif self._standard_config.can_handle_chart_type(chart_type):
            return self._standard_config
        else:
            # Fallback to ECharts as it's more flexible
            return self._echarts_config

    def _is_connected_devices_pattern(self, dashboard_context: Dict[str, Any]) -> bool:
        """Check if dashboard follows Connected Devices patterns."""
        dashboard_name = dashboard_context.get("dashboard_name", "").lower()

        # Connected Devices indicators
        connected_devices_indicators = [
            "connected_devices",
            "intraday_sales",
            "device",
            "tablet",
            "cd_detail",
            "cd_interval",
            "sales_by_hour",
        ]

        return any(
            indicator in dashboard_name for indicator in connected_devices_indicators
        )

    def get_visualization_type(
        self, chart_type: str, dashboard_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Get LookML visualization type for chart.

        Args:
            chart_type: Tableau chart type
            dashboard_context: Optional dashboard context

        Returns:
            LookML visualization type string
        """
        config = self.get_chart_config(chart_type, dashboard_context)
        return config.get_visualization_type(chart_type)

    def generate_chart_config(
        self,
        chart_type: str,
        worksheet,
        fields: list,
        explore_name: str,
        dashboard_context: Optional[Dict[str, Any]] = None,
        color_palettes: Dict = None,
        field_encodings: Dict = None,
    ) -> Dict[str, Any]:
        """
        Generate complete chart configuration using Tableau styling information.

        Args:
            chart_type: Tableau chart type
            worksheet: Worksheet schema object
            fields: List of field references
            explore_name: Name of the explore
            dashboard_context: Optional dashboard context
            color_palettes: Extracted Tableau color palettes
            field_encodings: Extracted Tableau field encodings

        Returns:
            Complete chart configuration dictionary
        """
        config = self.get_chart_config(chart_type, dashboard_context)

        print(f"   Chart type: {chart_type}")
        print(f"   Config instance: {config}")
        print(
            f"   Config supports ECharts: {isinstance(config, EChartsConfig) if hasattr(config, '__class__') else False}"
        )

        # Pass styling information to ECharts config if available
        if hasattr(config, "generate_chart_config"):
            result = config.generate_chart_config(
                worksheet, fields, explore_name, color_palettes, field_encodings
            )
            print(
                f"   Generated config keys: {list(result.keys()) if result else 'None'}"
            )
            return result
        else:
            # Fallback for standard config without styling support
            result = config.generate_chart_config(worksheet, fields, explore_name)
            print(
                f"   Generated fallback config keys: {list(result.keys()) if result else 'None'}"
            )
            return result

    def list_supported_chart_types(self) -> Dict[str, list]:
        """Get list of all supported chart types by configuration type."""
        return {
            "echarts": self._echarts_config.supported_chart_types,
            "standard": self._standard_config.supported_chart_types,
        }

    def set_echarts_preference(self, prefer_echarts: bool):
        """Update ECharts preference setting."""
        self.prefer_echarts = prefer_echarts
