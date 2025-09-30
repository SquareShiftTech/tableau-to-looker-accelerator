"""
Filter Processor - Convert Tableau filters to LookML using Pydantic models.

Clean, type-safe filter conversion with configurable mapping rules.
"""

from typing import Dict, List, Optional, Any
import logging
import re
from ..models.filter_mapping_models import (
    FilterMappingConfig,
    TableauFilter,
    LookMLFilter,
    GroupfilterLogic,
)

logger = logging.getLogger(__name__)


class FilterProcessor:
    """Process Tableau filters to LookML element filters."""

    def __init__(self, explore_name: str, config: Optional[FilterMappingConfig] = None):
        """Initialize with explore context and mapping configuration."""
        self.explore_name = explore_name
        self.config = config or FilterMappingConfig()

    def process_worksheet_filters(
        self,
        worksheet_filters: List[Dict],
        calculated_fields: Optional[List[str]] = None,
        field_mappings: Dict = None,
    ) -> Dict[str, str]:
        """
        Convert worksheet filters to LookML element filters.

        Args:
            worksheet_filters: List of filter dicts from worksheet JSON
            calculated_fields: List of calculated field names to exclude from filtering

        Returns:
            Dict mapping field keys to filter values for LookML
        """
        lookml_filters = {}
        calculated_fields = calculated_fields or []

        for filter_data in worksheet_filters:
            try:

                if filter_data.get("field_name") in ignored_fields:
                    logger.debug(
                        f"Ignoring filter for field: {filter_data.get('field_name')}"
                    )
                    continue
                group_except = False
                group_nested_union = False

                for groupfilter_logic in filter_data.get("groupfilter_logic", []):
                    if groupfilter_logic.get("function") == "except":
                        group_except = True
                    for nested_fields in groupfilter_logic.get("nested_filters", []):
                        if nested_fields.get("function") == "union":
                            group_nested_union = True

                if group_except or group_nested_union:
                    logger.debug(
                        f"Ignoring filter for field: {filter_data.get('field_name')} because it is an except or nested union"
                    )
                    continue

                # Parse Tableau filter with Pydantic validation
                tableau_filter = TableauFilter(**filter_data)

                # Skip calculated fields if specified - normalize field names for comparison
                if calculated_fields:
                    # Normalize field name for comparison: remove spaces, parentheses, make lowercase
                    normalized_filter_name = (
                        tableau_filter.field_name.replace(" ", "_")
                        .replace("(", "")
                        .replace(")", "")
                        .lower()
                    )
                    if (
                        normalized_filter_name in calculated_fields
                        and normalized_filter_name
                        in ["calculation_1181350527289110528"]
                    ):
                        logger.debug(
                            f"Skipping calculated field filter: {tableau_filter.field_name}"
                        )
                        continue

                dimensions = field_mappings.get("dimensions", {})
                # measures = field_mappings.get("measures", {})
                calculated_fields_mappings = field_mappings.get("calculated_fields", {})

                datasource_id = tableau_filter.datasource_id
                local_name = f"[{tableau_filter.field_name}]"

                datasource_fields = dimensions.get(datasource_id, {})

                clean_name = None

                if datasource_fields:
                    clean_name = datasource_fields.get(local_name, {}).get("clean_name")
                    tableau_filter.view_mapping_name = clean_name

                if not clean_name:
                    datasource_fields = calculated_fields_mappings.get(
                        datasource_id, {}
                    )
                    if datasource_fields:
                        clean_name = datasource_fields.get(local_name, {}).get(
                            "clean_name"
                        )
                        tableau_filter.view_mapping_name = clean_name

                # Convert to LookML filter
                lookml_filter = self._convert_filter(tableau_filter)
                if lookml_filter:
                    lookml_filters[lookml_filter.field_key] = lookml_filter.field_value

            except Exception as e:
                logger.warning(
                    f"Failed to process filter {filter_data.get('field_name', 'unknown')}: {e}"
                )

        return lookml_filters

    def _convert_filter(self, tableau_filter: TableauFilter) -> Optional[LookMLFilter]:
        """Convert single Tableau filter to LookML filter."""

        # Skip action filters - these are internal Tableau UI actions, not data filters
        if tableau_filter.field_name.lower().startswith("action"):
            return None

        # Get mapping rule
        rule = self.config.get_filter_rule(
            tableau_filter.filter_type, tableau_filter.filter_class
        )

        if not rule:
            logger.warning(
                f"No mapping rule for filter type: {tableau_filter.filter_type}, class: {tableau_filter.filter_class}"
            )
            return None

        # Clean field name

        clean_field = tableau_filter.view_mapping_name
        clean_field = self._apply_fallback_timeframe_mapping(
            clean_field or tableau_filter.field_name, tableau_filter
        )
        if not clean_field:
            logger.warning(f"Invalid field name: {tableau_filter.field_name}")
            return None

        # Create field key in explore.field format
        field_key = f"{self.explore_name}.{clean_field}"

        # Process filter value based on rule
        filter_value = self._process_filter_value(tableau_filter, rule)

        return LookMLFilter(
            field_key=field_key, field_value=filter_value, filter_type=rule.lookml_type
        )

    def _process_filter_value(self, tableau_filter: TableauFilter, rule) -> str:
        """Process filter value based on mapping rule."""

        # Use existing values if available
        if tableau_filter.values:
            return tableau_filter.values

        # Process based on rule type
        if rule.processor_method == "process_categorical_filter":
            return self._process_categorical_filter(tableau_filter)
        elif rule.processor_method == "process_date_filter":
            return self._process_date_filter(tableau_filter)
        elif rule.processor_method == "process_card_filter":
            return self._process_card_filter(tableau_filter)

        return ""

    def _process_categorical_filter(self, tableau_filter: TableauFilter) -> str:
        """Process categorical filter from groupfilter logic."""
        if not tableau_filter.groupfilter_logic:
            return ""

        # Check if any of the top-level filters use "except" function
        # has_except = any(logic.function == "except" for logic in tableau_filter.groupfilter_logic)
        # if has_except:
        #     logger.debug(f"Found top-level except function for {tableau_filter.field_name}, returning -NULL only")
        #     return "-NULL"

        extracted_values = []

        for logic in tableau_filter.groupfilter_logic:
            values = self._extract_values_from_logic(logic)
            extracted_values.extend(values)

        # Remove duplicates and return
        unique_values = list(dict.fromkeys(extracted_values))  # Preserve order
        return ", ".join(unique_values) if unique_values else ""

    def _process_date_filter(self, tableau_filter: TableauFilter) -> str:
        """Process date filter."""
        # Date filters typically don't need specific values in element filters
        return ""

    def _process_card_filter(self, tableau_filter: TableauFilter) -> str:
        """Process worksheet card filter."""
        # Card filters are UI controls, typically no default value
        return ""

    def _extract_values_from_logic(self, logic: GroupfilterLogic) -> List[str]:
        """Extract filter values from groupfilter logic."""
        values = []

        # Get rule for this function
        rule = self.config.get_groupfilter_rule(logic.function)
        if not rule:
            return values

        # Skip processing for except functions - this is now handled at the categorical filter level
        # if logic.function == "except":
        #     return []

        if not rule.extract_values:
            # Functions like level-members don't extract specific values
            return [rule.default_value] if rule.default_value else []

        if rule.value_source == "member" and logic.member:
            # Extract direct member value
            clean_value = logic.member.strip("\"'")
            if clean_value:
                # Convert %null% to -NULL for LookML exclusion
                if clean_value in ["%null%"]:
                    values.append("-NULL")
                else:
                    values.append(clean_value)

        elif rule.value_source == "nested_members" and logic.nested_filters:
            # Extract from nested filters (union, etc.)
            for nested in logic.nested_filters:
                nested_values = self._extract_values_from_logic(nested)
                values.extend(nested_values)

        return values

    def _apply_fallback_timeframe_mapping(
        self, field_name: str, tableau_filter=None
    ) -> str:
        """

        Args:
            field_name: Original field name from filter
            tableau_filter: The full tableau filter object for additional context

        Returns:
            Field name with appropriate timeframe suffix for date fields

        """

        if tableau_filter:
            # Try to get field_info from extra fields
            field_info = getattr(tableau_filter, "field_info", None)
            if not field_info:
                # Try to get it from the raw data
                field_info = tableau_filter.__dict__.get("field_info", {})
            if field_info and isinstance(field_info, dict):
                field_type = field_info.get("field_type", "")
                if field_type:
                    # Map Tableau field types to LookML timeframe suffixes
                    timeframe_mapping = {
                        "yr": "year",
                        "tyr": "year",
                        "mn": "month",
                        "tmn": "month",
                        "dy": "date",
                        "tdy": "date",
                        "qr": "quarter",
                        "tqr": "quarter",
                        "wk": "week",
                        "twk": "week",
                    }
                    timeframe_suffix = timeframe_mapping.get(field_type)
                    if timeframe_suffix:
                        result = f"{field_name}_{timeframe_suffix}"
                        logger.debug(
                            f"Using field type {field_type}, mapping to: {result}"
                        )
                        return result

        return field_name

    def limit_filter(self, worksheet_filters: List[Dict]) -> Dict[str, Any]:
        metadata = {"column_limit": 50, "sorts": []}

        if not worksheet_filters:
            return metadata

        try:
            for filter_data in worksheet_filters:
                groupfilter_logic = filter_data.get("groupfilter_logic", [])
                if not groupfilter_logic:
                    continue

                # Look for count-based top N pattern
                count_value = None
                direction = None
                sort_field = None
                for value in groupfilter_logic:
                    if value.get("function") == "end" and "count" in value:
                        count_value = value["count"]
                        direction = value.get("end", "top")
                        sort_field = self._remove_special_chars((filter_data))
                        break
                if count_value is not None:
                    metadata["column_limit"] = count_value
                    if sort_field:
                        if direction == "top":
                            metadata["sorts"] = [f"{sort_field} desc"]
                        elif direction == "bottom":
                            metadata["sorts"] = [f"{sort_field} asc"]

                    break

        except Exception as e:
            logger.warning(f"Error extracting count-based metadata: {e}")

        return metadata

    def _remove_special_chars(self, filter_data):
        field_name = filter_data.get("field_name", "")

        if not field_name:
            return None

        field_name = self.config.clean_field_name(field_name)

        sort_field = r"_\d{3,}$"  # Underscore followed by 3+ digits at end
        field_name = re.sub(sort_field, "", field_name)
        return f"{self.explore_name}.{field_name}"
