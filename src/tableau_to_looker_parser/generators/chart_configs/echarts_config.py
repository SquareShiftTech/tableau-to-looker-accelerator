"""
ECharts visualization configuration generator.

Handles configuration for tableau_to_looker::echarts_visualization_prod charts
based on the Connected Devices Detail dashboard patterns.
"""

from typing import Dict, List, Any
from .base_chart_config import BaseChartConfig


class EChartsConfig(BaseChartConfig):
    """Configuration generator for ECharts visualizations."""

    def _get_supported_chart_types(self) -> List[str]:
        """Return list of chart types supported by ECharts configuration."""
        return [
            "heatmap",
            "bar",
            "donut",
            "grouped_bar",
            "line",
            "area",
            "pie",
            "scatter",
            "text_table",
            "table",
            "bar_and_line",
            "bar_and_area",
            "line_and_bar",
            "unknown",
        ]

    def get_visualization_type(self, tableau_chart_type: str) -> str:
        """Map Tableau chart type to ECharts visualization type."""
        return "tableau_to_looker::echarts_visualization_prod"

    def generate_chart_config(
        self,
        worksheet,
        fields: List[str],
        explore_name: str,
        color_palettes: Dict = None,
        field_encodings: Dict = None,
    ) -> Dict[str, Any]:
        """Generate comprehensive ECharts configuration using Tableau-extracted styling info."""
        chart_type = worksheet.visualization.chart_type.lower()
        worksheet_name = worksheet.name

        print(f"   Chart type: {chart_type}")
        print(f"   Fields: {fields}")
        print(f"   Color palettes available: {bool(color_palettes)}")

        # Start with complete ECharts configuration
        config = self._get_comprehensive_echarts_config()

        # Override with Tableau color palettes if available
        if color_palettes:
            tableau_palette = self._get_tableau_color_palette(
                color_palettes, field_encodings, worksheet_name
            )
            config["colorPalette"] = tableau_palette
            print(f"   Applied Tableau color palette: {len(tableau_palette)} colors")

        # Add chart-specific configurations
        chart_specific = self._get_chart_specific_config(
            chart_type, worksheet, fields, explore_name
        )
        config.update(chart_specific)
        print(f"   Added chart-specific config: {list(chart_specific.keys())}")

        # Add dynamic field-based configurations from Tableau encodings
        field_config = self._get_field_based_config(
            fields, worksheet, field_encodings, worksheet_name
        )
        config.update(field_config)
        print(f"   Added field-based config: {list(field_config.keys())}")

        # Add filters from worksheet
        filters = self._generate_tableau_filters(worksheet, explore_name)
        if filters:
            config["filters"] = filters
            print(f"   Added filters: {len(filters)} items")

        # Add sorts from Tableau XML data
        sorts = self._generate_tableau_sorts(worksheet, explore_name, fields)
        if sorts:
            config["sorts"] = sorts
            print(f"   Added sorts: {len(sorts)} items")

        # Add pivots ONLY for table charts (like CD detail)
        pivots = self._generate_looker_pivots(chart_type, fields, explore_name)
        if pivots:
            config["pivots"] = pivots
            print(f"   Added pivots: {len(pivots)} items")

        echarts_props = ["chartType", "colorPalette", "themeSelector", "showTooltip"]
        for prop in echarts_props:
            if prop in config:
                print(f"   ✅ Final {prop}: {config[prop]}")
            else:
                print(f"   ❌ Final Missing: {prop}")

        return config

    def _get_comprehensive_echarts_config(self) -> Dict[str, Any]:
        """Get comprehensive ECharts configuration covering all possible properties."""
        return {
            # Base LookML properties
            "limit": 500,
            "column_limit": 50,
            "hidden_fields": [],
            "hidden_points_if_no": [],
            "series_labels": {},
            "show_view_names": False,
            # ECharts core properties
            "chartType": "bar",  # Will be overridden by chart-specific config
            "themeSelector": "system",
            "simpleColorSelection": True,
            "showTooltip": True,
            "showAdvancedTooltip": True,
            "toolTipTriggerOn": "mousemove",
            # Label and text properties
            "showSeriesLabel": True,
            "labelAlignment": "horizontal",
            "labelFontSize": "10",
            "labelAngle": 0,
            "labelPosition": "top",
            "labelColor": "",
            # Center title (for donuts/pies)
            "showCenterTitle": False,
            "centerLabelTitle": "Total",
            "centerLabelCalculation": "sum",
            # Formatting
            "prefix": "",
            "postfix": "",
            "metricFormat": False,
            "decimalPlaces": 0,
            # Legend
            "showLegend": True,
            "legendPosition": "top",
            # Visual styling
            "borderRadius": 0,
            "borderWidth": 0,
            "barWidth": 50,
            # X-Axis configurations
            "xAxisSeriesToggle": True,
            "showXAxisGrid": True,
            "xAxisFormatter": True,
            "xAxisGridType": "solid",
            "xAxisReverse": False,
            "xAxisName": "",
            "xAxisNameLocation": "middle",
            "xAxisNameGap": 25,
            "xAxisNameFontSize": 12,
            "xAxisNameFontWeight": "normal",
            "xAxisMin": None,
            "xAxisMax": None,
            # Y-Axis configurations
            "yAxisSeriesToggle": True,
            "showYAxisGrid": True,
            "yAxisFormatter": False,
            "yAxisGridType": "solid",
            "yAxisReverse": False,
            "yAxisName": "",
            "yAxisNameLocation": "middle",
            "yAxisNameGap": 40,
            "yAxisNameFontSize": 12,
            "yAxisNameFontWeight": "normal",
            "yAxisMin": None,
            "yAxisMax": None,
            # Spacing and layout
            "top": 10,
            "bottom": 10,
            "left": 10,
            "right": 10,
            # Color configurations (will be overridden by Tableau colors)
            "colorPalette": self._get_default_color_palette(),
            # Tooltips
            "customTooltip": "",
            "showToolTipDefaultValue": True,
            # Table-specific (for heatmaps)
            "tableHeadBg": "#000000",
            "tableFillBg": "#000000",
            "tableHeadText": "#FFF",
            "tableHeadTextAlignment": "left",
            "tablebodyTextAlignment": "right",
            # Standard Looker compatibility
            "x_axis_gridlines": False,
            "y_axis_gridlines": True,
            "show_y_axis_labels": True,
            "show_y_axis_ticks": True,
            "y_axis_tick_density": "default",
            "y_axis_tick_density_custom": 5,
            "show_x_axis_label": True,
            "show_x_axis_ticks": True,
            "y_axis_scale_mode": "linear",
            "x_axis_reversed": False,
            "y_axis_reversed": False,
            "plot_size_by_field": False,
            "trellis": "",
            "stacking": "",
            "limit_displayed_rows": False,
            "legend_position": "center",
            "point_style": "none",
            "show_value_labels": False,
            "label_density": 25,
            "x_axis_scale": "auto",
            "y_axis_combined": True,
            "ordering": "none",
            "show_null_labels": False,
            "show_totals_labels": False,
            "show_silhouette": False,
            "totals_color": "#808080",
            "defaults_version": 0,
            "hidden_pivots": {},
            "clusterCount": 0,
            # Table formatting (for compatibility)
            "show_row_numbers": True,
            "transpose": False,
            "truncate_text": True,
            "hide_totals": False,
            "hide_row_totals": False,
            "size_to_fit": True,
            "table_theme": "white",
            "enable_conditional_formatting": False,
            "header_text_alignment": "left",
            "header_font_size": 12,
            "rows_font_size": 12,
            "conditional_formatting_include_totals": False,
            "conditional_formatting_include_nulls": False,
        }

    def _get_heatmap_config(
        self, fields: List[str], explore_name: str
    ) -> Dict[str, Any]:
        """Configuration for heatmap tables (Total sales by Hour pattern)."""
        return {
            "chartType": "table",
            "tableHeadBg": "#000000",
            "tableFillBg": "#000000",
            "tableHeadText": "#FFF",
            "tableHeadTextAlignment": "left",
            "tablebodyTextAlignment": "right",
            "pivots": self.generate_pivots(fields, explore_name, "date"),
            "decimalPlaces": 0,
            "showLegend": False,
        }

    def _get_donut_config(self, fields: List[str], explore_name: str) -> Dict[str, Any]:
        """Configuration for donut charts (Connected Devices pattern)."""
        return {
            "chartType": "doughnut",
            "showCenterTitle": True,
            "centerLabelTitle": "Total Sales",
            "centerLabelCalculation": "sum",
            "showLegend": False,
            "decimalPlaces": 2,
            "labelFontSize": "8",
            "labelPosition": "default",
            "colorPalette": [
                "#4E79A7",
                "#F28E2B",
                "#E15759",
                "#76B7B2",
                "#59A14F",
                "#EDC948",
                "#B07AA1",
                "#FF9DA7",
                "#BAB0AC",
            ],
            # Donut-specific axis settings
            "xAxisSeriesToggle": False,
            "showXAxisGrid": False,
            "yAxisSeriesToggle": False,
            "showYAxisGrid": False,
            "right": 13,
        }

    def _get_grouped_bar_config(
        self, fields: List[str], explore_name: str
    ) -> Dict[str, Any]:
        """Configuration for grouped/stacked bars (Connect total pattern)."""
        return {
            "chartType": "singleStackedBar",
            "labelPosition": "inside",
            "labelColor": "#ffff",
            "labelAlignment": "vertical",
            "showLegend": False,
            "barWidth": 50,
            "xAxisReverse": True,
            "customTooltip": self._generate_custom_tooltip(fields),
            "pivots": self.generate_pivots(fields, explore_name, "dimension")[
                :1
            ],  # Single pivot
            # Grouped bar specific colors
            "dimensionColor_C1940": "#E15759",
            "dimensionColor_C2269": "#76B7B2",
            # Hidden pivot configuration
            "hidden_pivots": {},
        }

    def _get_bar_config(self, fields: List[str], explore_name: str) -> Dict[str, Any]:
        """Configuration for simple bar charts (By Interval pattern)."""
        return {
            "chartType": "stackedBar",
            "showLegend": False,
            "decimalPlaces": 0,
            "barWidth": 142,
            "xAxisNameLocation": "start",
            "yAxisNameGap": 20,
            "showXAxisGrid": False,
            "showYAxisGrid": False,
            # Time-based coloring
            "dimensionColor_2025_04_20": "#000000",
            "dimensionColor_2025_04_21": "#000000",
            "hidden_pivots": {},
        }

    def _get_default_config(self, chart_type: str) -> Dict[str, Any]:
        """Default configuration for other chart types."""
        chart_type_configs = {
            "line": {"chartType": "line"},
            "area": {"chartType": "area"},
            "pie": {"chartType": "pie"},
            "scatter": {"chartType": "scatter"},
        }

        return chart_type_configs.get(chart_type, {"chartType": "bar"})

    def _generate_custom_tooltip(self, fields: List[str]) -> str:
        """Generate custom tooltip for grouped visualizations."""
        # Look for equipment group fields for Connected Devices pattern
        eqp_fields = []
        for field in fields:
            field_name = field.split(".")[-1] if "." in field else field
            if "eqp_grp" in field_name.lower():
                eqp_fields.append(field_name.upper())

        if len(eqp_fields) >= 2:
            return (
                f"<SHAPE[color=#76B7B2,shape=circle]> *{eqp_fields[1]}* : <SUM([{eqp_fields[1]}])>,"
                f"<SHAPE[color=#E15759,shape=circle]> *{eqp_fields[0]}* : <SUM([{eqp_fields[0]}])>"
            )

        return ""

    def generate_dimension_colors(self, fields: List[str]) -> Dict[str, str]:
        """Generate dimension color mappings for ECharts."""
        colors = {
            "#4E79A7": "blue",
            "#F28E2B": "orange",
            "#E15759": "red",
            "#76B7B2": "teal",
            "#59A14F": "green",
            "#EDC948": "yellow",
            "#B07AA1": "purple",
            "#FF9DA7": "pink",
            "#BAB0AC": "gray",
        }

        dimension_colors = {}
        color_keys = list(colors.keys())

        for i, field in enumerate(fields):
            if i < len(color_keys):
                field_key = f"dimensionColor_{field.split('.')[-1]}"
                dimension_colors[field_key] = color_keys[i]

        return dimension_colors

    def _get_tableau_color_palette(
        self, color_palettes: Dict, field_encodings: Dict, worksheet_name: str
    ) -> List[str]:
        """Extract the appropriate color palette from Tableau data."""
        # Get encodings for this worksheet
        worksheet_encodings = field_encodings.get(worksheet_name, {})
        color_palette_names = worksheet_encodings.get("color_palettes", [])

        # Use the first specified palette, or fall back to defaults
        if color_palette_names:
            palette_name = color_palette_names[0]
            if palette_name in color_palettes:
                return color_palettes[palette_name]["colors"]

        # Check for default or VZ Brand palette
        for palette_name in ["VZ Brand", "default"]:
            if palette_name in color_palettes:
                return color_palettes[palette_name]["colors"]

        # Ultimate fallback to Tableau standard colors
        return [
            "#4E79A7",
            "#F28E2B",
            "#E15759",
            "#76B7B2",
            "#59A14F",
            "#EDC948",
            "#B07AA1",
            "#FF9DA7",
            "#BAB0AC",
        ]

    def _get_chart_specific_config(
        self, chart_type: str, worksheet, fields: List[str], explore_name: str
    ) -> Dict[str, Any]:
        """Get chart-specific configuration based on Tableau chart type."""
        config = {}

        # Set chartType based on Tableau chart type
        chart_type_mapping = {
            "heatmap": "table",
            "donut": "doughnut",
            "pie": "doughnut",
            "grouped_bar": "singleStackedBar",
            "bar": "stackedBar",
            "line": "line",
            "area": "area",
            "scatter": "scatter",
            "text_table": "table",
            "table": "table",
        }

        config["chartType"] = chart_type_mapping.get(chart_type, "bar")

        # Chart-specific settings
        if chart_type in ["heatmap", "text_table", "table"]:
            config.update(
                {
                    "tableHeadBg": "#000000",
                    "tableFillBg": "#000000",
                    "tableHeadText": "#FFF",
                    "tableHeadTextAlignment": "left",
                    "tablebodyTextAlignment": "right",
                }
            )
        elif chart_type in ["donut", "pie"]:
            config.update(
                {
                    "showCenterTitle": True,
                    "centerLabelTitle": "Total Sales",
                    "centerLabelCalculation": "sum",
                    "showLegend": False,
                    "decimalPlaces": 2,
                    "labelFontSize": "8",
                    "labelPosition": "default",
                    "xAxisSeriesToggle": False,
                    "showXAxisGrid": False,
                    "yAxisSeriesToggle": False,
                    "showYAxisGrid": False,
                    "right": 13,
                }
            )
        elif chart_type == "grouped_bar":
            config.update(
                {
                    "labelPosition": "inside",
                    "labelColor": "#ffffff",
                    "labelAlignment": "vertical",
                    "showLegend": False,
                    "xAxisReverse": True,
                    "hidden_pivots": {},
                }
            )
        elif chart_type == "bar":
            config.update(
                {
                    "showLegend": False,
                    "decimalPlaces": 0,
                    "barWidth": 142,
                    "xAxisNameLocation": "start",
                    "yAxisNameGap": 20,
                    "showXAxisGrid": False,
                    "showYAxisGrid": False,
                    "hidden_pivots": {},
                }
            )

        return config

    def _get_field_based_config(
        self,
        fields: List[str],
        worksheet,
        field_encodings: Dict = None,
        worksheet_name: str = "",
    ) -> Dict[str, Any]:
        """Generate field-based configuration including dimension colors."""
        config = {}

        # Generate dimension colors based on actual field values if available
        if field_encodings and worksheet_name in field_encodings:
            encodings = field_encodings[worksheet_name]
            config.update(
                self._generate_dimension_colors_from_encodings(encodings, fields)
            )

        return config

    def _get_worksheet_filters_sorts(
        self, worksheet, explore_name: str
    ) -> Dict[str, Any]:
        """Extract filters and sorts from worksheet."""
        config = {}

        # Add filters if available
        if hasattr(worksheet, "filters") and worksheet.filters:
            filters = {}
            for filter_config in worksheet.filters:
                field_name = filter_config.get("field", "").strip("[]")
                filter_value = filter_config.get("value", "")
                filter_key = f"{explore_name.lower()}.{field_name}"
                filters[filter_key] = filter_value
            if filters:
                config["filters"] = filters

        # Add sorts if available
        if hasattr(worksheet, "sorting") and worksheet.sorting:
            sorts = []
            for sort_config in worksheet.sorting:
                field_name = sort_config.get("field", "").strip("[]")
                direction = sort_config.get("direction", "ASC").lower()
                sort_field = f"{explore_name.lower()}.{field_name}"
                sorts.append(f"{sort_field} {direction}")
            if sorts:
                config["sorts"] = sorts

        return config

    def _generate_pivots(self, fields: List[str], worksheet) -> List[str]:
        """Generate pivots based on worksheet configuration."""
        pivots = []

        # Extract pivots from visualization if available
        if hasattr(worksheet, "visualization") and hasattr(
            worksheet.visualization, "encodings"
        ):
            # Look for fields that should be pivoted (typically date fields for time series)
            for field in fields:
                field_name = field.split(".")[-1] if "." in field else field
                if any(
                    keyword in field_name.lower()
                    for keyword in ["date", "time", "hour", "day"]
                ):
                    pivots.append(field)
                    if len(pivots) >= 2:  # Limit to 2 pivots
                        break

        return pivots

    def _generate_tableau_filters(self, worksheet, explore_name: str) -> Dict[str, str]:
        """Extract filters from Tableau worksheet data."""
        filters = {}

        # Use existing base class method first
        base_filters = self.generate_filters_config(worksheet, explore_name)
        if base_filters:
            filters.update(base_filters)

        return filters

    def _generate_tableau_sorts(
        self, worksheet, explore_name: str, fields: List[str]
    ) -> List[str]:
        """Convert Tableau sorts to Looker format using extracted worksheet data."""
        sorts = []

        # Check if worksheet has sorts data from XML parsing
        if hasattr(worksheet, "sorts") and worksheet.sorts:
            for sort_config in worksheet.sorts:
                tableau_field = sort_config.get("field", "")
                direction = sort_config.get("direction", "ASC").lower()

                # Convert Tableau field name to Looker field name
                looker_field = self._convert_tableau_field_to_looker(
                    tableau_field, explore_name, fields
                )
                if looker_field:
                    if direction == "desc":
                        sorts.append(f"{looker_field} desc 0")
                    else:
                        sorts.append(looker_field)

        # If no sorts from worksheet, generate smart defaults based on field patterns
        if not sorts:
            # Add date fields first (natural sort order)
            date_fields = [
                f
                for f in fields
                if any(kw in f.lower() for kw in ["date", "time", "day", "hour"])
            ]
            sorts.extend(date_fields)

            # Add measure fields with descending sort
            measure_fields = [
                f
                for f in fields
                if any(
                    prefix in f.lower()
                    for prefix in ["sum_", "total_", "avg_", "count_"]
                )
            ]
            for measure in measure_fields:
                sorts.append(f"{measure} desc 0")

        return sorts

    def _generate_looker_pivots(
        self, chart_type: str, fields: List[str], explore_name: str
    ) -> List[str]:
        """Generate Looker pivots ONLY for table charts (text_table)."""
        pivots = []

        # ONLY generate pivots for table charts
        if chart_type.lower() not in ["text_table", "table"]:
            print(f"   No pivots for chart type: {chart_type} (only tables get pivots)")
            return pivots

        # Pattern 1: Time-based pivots (most common for tables)
        # Example: pivots: [day_rpt_dt, hour_rpt_time]
        time_fields = []
        for field in fields:
            field_name = field.split(".")[-1] if "." in field else field
            if any(
                keyword in field_name.lower()
                for keyword in [
                    "rpt_dt",
                    "day_rpt_dt",
                    "rpt_time",
                    "hour_rpt_time",
                    "day_",
                    "hour_",
                ]
            ):
                time_fields.append(field)

        if time_fields:
            pivots.extend(
                time_fields[:2]
            )  # Limit to 2 time dimensions like manual reference
            print(f"   Added time-based pivots: {time_fields[:2]}")

        # Pattern 2: Category-based pivots for comparison (only if no time pivots)
        # Example: pivots: [eqp_grp_desc]
        if not pivots:  # Only if no time pivots found
            for field in fields:
                field_name = field.split(".")[-1] if "." in field else field
                if "eqp_grp_desc" in field_name.lower():
                    pivots.append(field)
                    print(f"   Added category pivot: {field}")
                    break  # Just one category pivot

        return pivots

    def _convert_tableau_field_to_looker(
        self, tableau_field: str, explore_name: str, available_fields: List[str]
    ) -> str:
        """Convert Tableau field reference to Looker field reference."""
        if not tableau_field:
            return ""

        # Extract field name from Tableau's complex field reference format
        # Example: [federated.1fc6jd010l1f0m19s90ze0noolhe].[none:model_nm:nk] -> model_nm
        field_name = ""
        if "].[" in tableau_field:
            # Extract the part after the last ].[
            parts = tableau_field.split("].[")[-1]
            if ":" in parts:
                # Extract field name from :fieldname:
                field_parts = parts.split(":")
                if len(field_parts) >= 2:
                    field_name = field_parts[1]

        # Try to match with available fields
        if field_name:
            # Look for exact matches first
            for field in available_fields:
                if field.endswith(f".{field_name}"):
                    return field

            # Look for partial matches
            for field in available_fields:
                if field_name.lower() in field.lower():
                    return field

        return ""

    def _generate_dimension_colors_from_encodings(
        self, encodings: Dict, fields: List[str]
    ) -> Dict[str, str]:
        """Generate dimension color mappings from Tableau field encodings."""
        dimension_colors = {}

        # Default color mapping for common dimension values
        default_colors = {
            "C1940": "#E15759",
            "C2269": "#76B7B2",
            "C8730": "#76B7B2",
            "C2004": "#E15759",
            "true": "#76B7B2",
            "false": "#E15759",
        }

        # Add default dimension colors
        for key, color in default_colors.items():
            dimension_colors[f"dimensionColor_{key}"] = color

        return dimension_colors

    def _get_default_color_palette(self) -> List[str]:
        """Get default Tableau color palette."""
        return [
            "#4E79A7",
            "#F28E2B",
            "#E15759",
            "#76B7B2",
            "#59A14F",
            "#EDC948",
            "#B07AA1",
            "#FF9DA7",
            "#BAB0AC",
        ]
