"""
Field mapping utilities for dashboard generation.

Handles conversion of Tableau field references to LookML field references,
including aggregation type mapping and field validation.
"""

import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


class FieldMapper:
    """Utility class for mapping Tableau fields to LookML field references."""

    def __init__(self):
        """Initialize field mapper."""
        self.aggregation_mapping = {
            "sum": "total_",
            "avg": "avg_",
            "count": "count_",
            "countd": "count_",  # Count distinct maps to count in LookML
            "min": "min_",
            "max": "max_",
        }

    def build_fields_from_worksheet(self, worksheet, explore_name: str) -> List[str]:
        """
        Build fields array from worksheet fields for LookML dashboard.
        Only includes fields from row and column shelves (for tables/pivots) plus measures.

        Args:
            worksheet: Worksheet schema object
            explore_name: Name of the explore to reference

        Returns:
            List of LookML field references (e.g., ["orders.category", "orders.total_sales"])
        """
        fields = []

        # Get fields from the worksheet schema
        worksheet_fields = worksheet.fields if hasattr(worksheet, "fields") else []

        for field in worksheet_fields:
            # Skip internal fields
            if self._is_internal_field(field):
                field_name = self._get_field_name(field)
                logger.debug(f"Skipping internal field: {field_name}")
                continue

            # Chart-specific field inclusion logic
            shelf = self._get_field_shelf(field)
            field_type = self._get_field_type(field)
            field_name = self._get_field_name(field)

            # Check if we should include this field based on chart type
            should_include = self._should_include_field_for_chart(
                worksheet, field, shelf, field_type
            )

            if should_include:
                if field_name:
                    # Add proper measure aggregation types for dashboard fields
                    aggregated_field_name = self._add_measure_aggregation_type(
                        field_name, field
                    )
                    fields.append(f"{explore_name.lower()}.{aggregated_field_name}")
                    logger.debug(
                        f"Added field: {field_name} (shelf: {shelf}, type: {field_type})"
                    )
            else:
                logger.debug(
                    f"Skipping field: {field_name} (shelf: {shelf}, type: {field_type})"
                )

        return fields

    def _is_internal_field(self, field) -> bool:
        """Check if field is internal and should be skipped."""
        if hasattr(field, "is_internal"):
            return field.is_internal
        elif hasattr(field, "get"):
            return field.get("is_internal", False)
        return False

    def _get_field_name(self, field) -> str:
        """Extract field name from field object."""
        if hasattr(field, "name"):
            return field.name
        elif hasattr(field, "get"):
            return field.get("name", "")
        return ""

    def _get_field_shelf(self, field) -> str:
        """Extract field shelf from field object."""
        if hasattr(field, "shelf"):
            return field.shelf
        elif hasattr(field, "get"):
            return field.get("shelf", "")
        return ""

    def _should_include_field_for_chart(
        self, worksheet, field, shelf: str, field_type: str
    ) -> bool:
        """
        Determine if a field should be included based on chart type.

        For donut charts: Use color and text/detail shelf fields + measures
        For tables: Use rows and columns shelf fields + measures
        For bar/pie: Use rows and columns shelf fields + measures
        """
        # Get chart type from worksheet
        chart_type = "bar"  # default
        if hasattr(worksheet, "visualization"):
            viz = worksheet.visualization
            if hasattr(viz, "chart_type"):
                chart_type = (
                    viz.chart_type.lower()
                    if hasattr(viz.chart_type, "lower")
                    else str(viz.chart_type).lower()
                )

        # Always include measures
        if field_type == "measure":
            return True

        # Donut-specific logic: include color and detail/text shelf fields
        if chart_type in ["donut", "pie"]:
            # Include fields on color, detail, or text shelves
            if shelf in ["color", "detail", "text"]:
                logger.debug(
                    f"Including donut field from {shelf} shelf: {self._get_field_name(field)}"
                )
                return True

            # Also check if this field is used in visualization.color
            if hasattr(worksheet, "visualization") and hasattr(
                worksheet.visualization, "color"
            ):
                viz_color = worksheet.visualization.color
                field_name = self._get_field_name(field)
                if (
                    viz_color
                    and field_name
                    and field_name.lower() in str(viz_color).lower()
                ):
                    logger.debug(
                        f"Including donut field from visualization.color: {field_name}"
                    )
                    return True

            return False

        # For all other charts (tables, bars): use rows and columns
        return shelf in ["rows", "columns"]

    def _add_measure_aggregation_type(self, field_name: str, field) -> str:
        """
        Add proper aggregation type to measure field names for dashboard references.
        For dimensions with tableau_instance, generate timeframe-specific field names.

        Args:
            field_name: Base field name
            field: Field object with type and aggregation info

        Returns:
            Field name with aggregation prefix for measures, or timeframe prefix for dimensions
        """
        # Get field type and aggregation from field object
        field_type = self._get_field_type(field)
        field_aggregation = self._get_field_aggregation(field)

        # For dimensions, check if they have tableau_instance for timeframe mapping
        if field_type == "dimension":
            # Use generic tableau_instance parsing for timeframe fields
            timeframe_field = self._get_timeframe_field_name(field_name, field)
            return timeframe_field

        # Check if this is a measure field
        if field_type == "measure":
            if field_aggregation:
                aggregation_lower = field_aggregation.lower()

                # Map aggregation types to measure prefixes
                prefix = self.aggregation_mapping.get(aggregation_lower, "total_")
                return f"{prefix}{field_name.lower()}"

            # Fallback for measures without aggregation info
            return f"total_{field_name.lower()}"

        # Return other field types as-is
        return field_name

    def _get_field_type(self, field) -> str:
        """Extract field type from field object."""
        if hasattr(field, "type"):
            return field.type
        elif hasattr(field, "role"):
            return field.role
        elif hasattr(field, "get"):
            return field.get("type", field.get("role", "dimension"))
        return "dimension"

    def _get_field_aggregation(self, field) -> str:
        """Extract field aggregation from field object."""
        if hasattr(field, "aggregation"):
            return field.aggregation
        elif hasattr(field, "get"):
            return field.get("aggregation", "")
        return ""

    def _get_tableau_instance(self, field) -> str:
        """Extract tableau_instance from field object."""
        if hasattr(field, "tableau_instance"):
            return getattr(field, "tableau_instance", "")
        elif hasattr(field, "get"):
            return field.get("tableau_instance", "")
        return ""

    def _parse_derivation_from_instance(self, tableau_instance: str) -> str:
        """
        Parse derivation prefix from tableau_instance.

        Args:
            tableau_instance: Tableau instance string like "[tdy:RPT_DT:ok]"

        Returns:
            Derivation prefix like "tdy", "thr", "dy", "none"
        """
        if not tableau_instance:
            return ""

        # Remove brackets and split by colons
        # "[tdy:RPT_DT:ok]" -> ["tdy", "RPT_DT", "ok"]
        cleaned = tableau_instance.strip("[]")
        parts = cleaned.split(":")

        if len(parts) >= 1:
            return parts[0]  # Return derivation prefix

        return ""

    def _map_derivation_to_timeframe(self, derivation: str) -> str:
        """
        Map Tableau derivation to LookML timeframe prefix.

        Args:
            derivation: Tableau derivation like "tdy", "thr", "dy", "none"

        Returns:
            LookML timeframe prefix like "day_", "hour_", ""
        """
        derivation_mapping = {
            "tdy": "day_",  # Day-Trunc -> day_
            "dy": "day_",  # Day -> day_
            "thr": "hour_",  # Hour-Trunc -> hour_
            "hr": "hour_",  # Hour -> hour_
            "tyr": "year_",  # Year-Trunc -> year_
            "yr": "year_",  # Year -> year_
            "tmn": "month_",  # Month-Trunc -> month_
            "mn": "month_",  # Month -> month_
            "tqr": "quarter_",  # Quarter-Trunc -> quarter_
            "qr": "quarter_",  # Quarter -> quarter_
            "twk": "week_",  # Week-Trunc -> week_
            "wk": "week_",  # Week -> week_
            "none": "",  # No derivation -> no prefix
        }

        return derivation_mapping.get(derivation.lower(), "")

    def _get_timeframe_field_name(self, field_name: str, field) -> str:
        """
        Generate timeframe-specific field name from tableau_instance for datetime fields only.

        Args:
            field_name: Base field name
            field: Field object with tableau_instance

        Returns:
            Field name with timeframe prefix for datetime fields, otherwise unchanged
        """
        # Only apply timeframe logic to datetime fields
        datatype = self._get_field_datatype(field)
        if datatype not in ["date", "datetime"]:
            return field_name.lower()

        tableau_instance = self._get_tableau_instance(field)

        if tableau_instance:
            derivation = self._parse_derivation_from_instance(tableau_instance)
            timeframe_prefix = self._map_derivation_to_timeframe(derivation)

            if timeframe_prefix:
                return f"{timeframe_prefix}{field_name.lower()}"

        # Return field name as-is for datetime fields without tableau_instance or timeframe mapping
        return field_name.lower()

    def get_fill_fields_from_worksheet(self, worksheet, explore_name: str) -> List[str]:
        """
        Get fill_fields for time-based visualizations.

        Args:
            worksheet: Worksheet schema object
            explore_name: Name of the explore

        Returns:
            List of date/time fields to use for filling
        """
        fill_fields = []

        # Get fields from the worksheet schema
        worksheet_fields = worksheet.fields if hasattr(worksheet, "fields") else []

        # Look for date/time fields that should be filled
        for field in worksheet_fields:
            field_name = self._get_field_name(field)
            datatype = self._get_field_datatype(field)
            field_type = self._get_field_type(field)

            # Check if field is a date/time dimension
            is_date_field = (
                datatype in ["date", "datetime"] and field_type == "dimension"
            )

            if is_date_field:
                # Use the same timeframe logic for consistency
                timeframe_field = self._get_timeframe_field_name(field_name, field)
                fill_fields.append(f"{explore_name.lower()}.{timeframe_field}")

        return fill_fields

    def _get_field_datatype(self, field) -> str:
        """Extract field datatype from field object."""
        if hasattr(field, "datatype"):
            return getattr(field, "datatype", "")
        elif hasattr(field, "get"):
            return field.get("datatype", "")
        return ""

    def build_filters_from_worksheet(
        self, worksheet, explore_name: str
    ) -> Dict[str, str]:
        """
        Build filters dictionary from worksheet filters.

        Args:
            worksheet: Worksheet schema object
            explore_name: Name of the explore

        Returns:
            Dictionary of filter configurations
        """
        filters = {}

        if hasattr(worksheet, "filters") and worksheet.filters:
            for filter_config in worksheet.filters:
                field_name = filter_config.get("field", "").strip("[]")
                filter_value = filter_config.get("value", "")
                # Convert to explore.field format
                filter_key = f"{explore_name.lower()}.{field_name}"
                filters[filter_key] = filter_value

        return filters

    def build_sorts_from_worksheet(self, worksheet, explore_name: str) -> List[str]:
        """
        Build sorts array from worksheet sorting configuration.

        Args:
            worksheet: Worksheet schema object
            explore_name: Name of the explore

        Returns:
            List of sort configurations
        """
        sorts = []

        if hasattr(worksheet, "sorting") and worksheet.sorting:
            for sort_config in worksheet.sorting:
                field_name = sort_config.get("field", "").strip("[]")
                direction = sort_config.get("direction", "ASC").lower()
                # Convert to explore.field format
                sort_field = f"{explore_name.lower()}.{field_name}"
                sorts.append(f"{sort_field} {direction}")

        return sorts
