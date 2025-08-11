"""
Field Validation Engine for ensuring dashboard-view field synchronization.

Validates that all dashboard field references have corresponding fields in the generated views,
and provides suggestions for missing fields.
"""

import logging
from typing import Dict, List, Set, Optional

logger = logging.getLogger(__name__)


class FieldValidationResult:
    """Result of field validation containing missing fields and suggestions."""

    def __init__(self):
        self.missing_fields: List[str] = []
        self.suggestions: List[Dict] = []
        self.validation_errors: List[str] = []
        self.is_valid: bool = True

    def add_missing_field(self, field_ref: str, suggestion: Optional[Dict] = None):
        """Add a missing field with optional suggestion."""
        self.missing_fields.append(field_ref)
        self.is_valid = False
        if suggestion:
            self.suggestions.append(suggestion)

    def add_validation_error(self, error: str):
        """Add a validation error."""
        self.validation_errors.append(error)
        self.is_valid = False


class FieldValidationEngine:
    """
    Engine for validating dashboard-view field synchronization.

    Ensures all dashboard field references exist in the generated views
    and provides actionable suggestions for missing fields.
    """

    def __init__(self):
        """Initialize the validation engine."""
        pass

    def validate_dashboard_field_sync(
        self, migration_data: Dict
    ) -> FieldValidationResult:
        """
        Validate that all dashboard field references exist in views.

        Args:
            migration_data: Complete migration data with dashboards and fields

        Returns:
            FieldValidationResult with validation status and suggestions
        """
        result = FieldValidationResult()

        # Extract dashboard field references
        dashboard_fields = self._extract_dashboard_field_references(migration_data)
        logger.info(
            f"Found {len(dashboard_fields)} unique field references in dashboards"
        )

        # Extract available view fields
        view_fields = self._extract_available_view_fields(migration_data)
        logger.info(f"Found {len(view_fields)} available fields in views")

        # Check for missing fields
        missing_fields = dashboard_fields - view_fields

        if missing_fields:
            logger.warning(f"Found {len(missing_fields)} missing field references")

            for missing_field in missing_fields:
                suggestion = self._suggest_field_derivation(
                    missing_field, migration_data
                )
                result.add_missing_field(missing_field, suggestion)

        else:
            logger.info("All dashboard field references are satisfied by view fields")

        return result

    def _extract_dashboard_field_references(self, migration_data: Dict) -> Set[str]:
        """
        Extract all field references from dashboard elements.

        Args:
            migration_data: Migration data containing dashboards

        Returns:
            Set of unique field references (view.field format)
        """
        field_references = set()

        dashboards = migration_data.get("dashboards", [])
        for dashboard in dashboards:
            elements = dashboard.get("elements", [])

            for element in elements:
                # Extract from fields array
                fields = element.get("fields", [])
                for field in fields:
                    if isinstance(field, str) and "." in field:
                        # Extract just the field name part
                        field_name = field.split(".")[-1]
                        field_references.add(field_name)

                # Extract from sorts array
                sorts = element.get("sorts", [])
                for sort in sorts:
                    if isinstance(sort, str) and "." in sort:
                        # Remove sort direction and extract field name
                        sort_field = sort.split()[0] if " " in sort else sort
                        field_name = sort_field.split(".")[-1]
                        field_references.add(field_name)

        return field_references

    def _extract_available_view_fields(self, migration_data: Dict) -> Set[str]:
        """
        Extract all available fields from dimensions, measures, and calculated fields.

        Args:
            migration_data: Migration data containing dimensions, measures, calculated fields

        Returns:
            Set of available field names
        """
        available_fields = set()

        # Add dimension fields
        dimensions = migration_data.get("dimensions", [])
        for dimension in dimensions:
            field_name = dimension.get("name")
            if field_name:
                available_fields.add(field_name)

            # Add dimension_group timeframes
            if dimension.get("field_type") == "dimension_group":
                base_name = field_name
                timeframes = dimension.get(
                    "timeframes", ["date", "week", "month", "quarter", "year"]
                )
                for timeframe in timeframes:
                    if timeframe not in ["raw", "time"]:  # Skip raw and time
                        available_fields.add(f"{base_name}_{timeframe}")

        # Add measure fields
        measures = migration_data.get("measures", [])
        for measure in measures:
            field_name = measure.get("name")
            if field_name:
                available_fields.add(field_name)

        # Add calculated fields
        calculated_fields = migration_data.get("calculated_fields", [])
        for calc_field in calculated_fields:
            field_name = calc_field.get("name")
            if field_name:
                available_fields.add(field_name)

        return available_fields

    def _suggest_field_derivation(
        self, missing_field: str, migration_data: Dict
    ) -> Optional[Dict]:
        """
        Suggest how to create a missing field.

        Args:
            missing_field: Missing field name
            migration_data: Migration data for context

        Returns:
            Suggestion dict or None
        """
        # Check if it looks like a time function
        if self._is_time_function_pattern(missing_field):
            return self._suggest_time_dimension_group(missing_field)

        # Check if it looks like an aggregation
        if self._is_aggregation_pattern(missing_field):
            return self._suggest_aggregated_measure(missing_field)

        # Check if it looks like a calculation reference
        if missing_field.startswith("calculation_"):
            return self._suggest_calculated_field_reference(missing_field)

        # Check for similar field names
        similar_field = self._find_similar_field(missing_field, migration_data)
        if similar_field:
            return {
                "type": "similar_field",
                "field_name": missing_field,
                "suggestion": f"Did you mean '{similar_field}'?",
                "action": "rename_field_reference",
                "target_field": similar_field,
            }

        # Default suggestion
        return {
            "type": "unknown",
            "field_name": missing_field,
            "suggestion": f"Create missing field '{missing_field}' in view",
            "action": "create_field",
            "suggested_type": "dimension",
        }

    def _is_time_function_pattern(self, field_name: str) -> bool:
        """Check if field name matches time function patterns."""
        time_patterns = [
            "day_",
            "hour_",
            "minute_",
            "quarter_",
            "year_",
            "month_",
            "week_",
        ]
        return any(field_name.startswith(pattern) for pattern in time_patterns)

    def _is_aggregation_pattern(self, field_name: str) -> bool:
        """Check if field name matches aggregation patterns."""
        agg_patterns = ["sum_", "avg_", "count_", "min_", "max_", "median_"]
        return any(field_name.startswith(pattern) for pattern in agg_patterns)

    def _suggest_time_dimension_group(self, field_name: str) -> Dict:
        """Suggest creating a time dimension_group."""
        # Extract time function and base field
        for pattern in [
            "day_",
            "hour_",
            "minute_",
            "quarter_",
            "year_",
            "month_",
            "week_",
        ]:
            if field_name.startswith(pattern):
                time_function = pattern.rstrip("_")
                base_field = field_name[len(pattern) :]

                return {
                    "type": "time_dimension_group",
                    "field_name": field_name,
                    "suggestion": f"Create dimension_group for '{base_field}' with '{time_function}' timeframe",
                    "action": "create_dimension_group",
                    "base_field": base_field,
                    "time_function": time_function,
                    "timeframes": [
                        "raw",
                        "time",
                        "date",
                        "week",
                        "month",
                        "quarter",
                        "year",
                    ],
                }

        return None

    def _suggest_aggregated_measure(self, field_name: str) -> Dict:
        """Suggest creating an aggregated measure."""
        # Extract aggregation and base field
        for pattern in ["sum_", "avg_", "count_", "min_", "max_", "median_"]:
            if field_name.startswith(pattern):
                aggregation = pattern.rstrip("_")
                base_field = field_name[len(pattern) :]

                return {
                    "type": "aggregated_measure",
                    "field_name": field_name,
                    "suggestion": f"Create {aggregation.upper()} measure for '{base_field}'",
                    "action": "create_measure",
                    "base_field": base_field,
                    "aggregation": aggregation,
                    "lookml_type": aggregation
                    if aggregation in ["sum", "count", "average", "min", "max"]
                    else "sum",
                }

        return None

    def _suggest_calculated_field_reference(self, field_name: str) -> Dict:
        """Suggest creating a calculated field reference."""
        return {
            "type": "calculated_field",
            "field_name": field_name,
            "suggestion": f"Ensure calculated field '{field_name}' is properly converted from Tableau",
            "action": "verify_calculation",
            "calc_id": field_name,
        }

    def _find_similar_field(
        self, missing_field: str, migration_data: Dict
    ) -> Optional[str]:
        """Find similar field names using simple string matching."""
        available_fields = self._extract_available_view_fields(migration_data)

        # Look for exact substring matches
        for available_field in available_fields:
            if (
                missing_field.lower() in available_field.lower()
                or available_field.lower() in missing_field.lower()
            ):
                return available_field

        # Look for fields with similar length and characters
        for available_field in available_fields:
            if abs(len(missing_field) - len(available_field)) <= 2:
                # Simple character overlap check
                missing_chars = set(missing_field.lower())
                available_chars = set(available_field.lower())
                overlap = len(missing_chars & available_chars)

                if overlap >= min(len(missing_chars), len(available_chars)) * 0.7:
                    return available_field

        return None

    def generate_validation_report(self, result: FieldValidationResult) -> str:
        """
        Generate a human-readable validation report.

        Args:
            result: Field validation result

        Returns:
            Formatted validation report
        """
        if result.is_valid:
            return "✅ Field validation passed: All dashboard field references are satisfied by view fields."

        report = ["❌ Field validation failed:\n"]

        if result.missing_fields:
            report.append(f"Missing {len(result.missing_fields)} field references:")
            for i, suggestion in enumerate(result.suggestions):
                field_name = suggestion.get("field_name", "unknown")
                suggestion_text = suggestion.get(
                    "suggestion", "No suggestion available"
                )
                report.append(f"  {i + 1}. {field_name}: {suggestion_text}")

        if result.validation_errors:
            report.append("\nValidation errors:")
            for error in result.validation_errors:
                report.append(f"  - {error}")

        return "\n".join(report)
