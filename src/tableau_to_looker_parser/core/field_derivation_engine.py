"""
Field Derivation Engine for mapping Tableau instances to Looker fields.

Maps Tableau instance patterns like [tdy:RPT_DT:ok] and [sum:sales:qk]
to appropriate Looker dimension_groups and measures for dashboard synchronization.
"""

import logging
from typing import Dict, List, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


class FieldDerivationType(Enum):
    """Types of field derivations supported."""

    TIME_FUNCTION = "time_function"  # day(), hour(), etc.
    AGGREGATION = "aggregation"  # sum(), avg(), etc.
    CALCULATION = "calculation"  # calculated field references
    DIRECT = "direct"  # direct field reference


class FieldDerivationEngine:
    """
    Engine for deriving Looker fields from Tableau instances.

    Handles mapping patterns like:
    - [tdy:RPT_DT:ok] → rpt_dt_date (dimension_group timeframe)
    - [sum:sales:qk] → sales (measure with sum aggregation)
    - [Calculation_123:qk] → calculation_123 (calculated field)
    """

    def __init__(self):
        """Initialize the derivation engine with pattern mappings."""
        self.time_function_patterns = {
            "tdy": "date",
            "thr": "hour",
            "tmn": "minute",
            "tqr": "quarter",
            "tyr": "year",
            "tmth": "month",
            "twk": "week",
        }

        self.aggregation_patterns = {
            "sum": "sum",
            "avg": "average",
            "cnt": "count",
            "min": "min",
            "max": "max",
            "med": "median",
        }

    def derive_fields_from_tableau_instances(
        self, worksheet_fields: List[Dict], dashboard_field_references: List[str]
    ) -> List[Dict]:
        """
        Derive missing Looker fields from Tableau instances in dashboard references.

        Args:
            worksheet_fields: Existing fields from worksheet processing
            dashboard_field_references: Field references found in dashboard elements

        Returns:
            List of derived field definitions for view generation
        """
        derived_fields = []
        existing_field_names = {field.get("name", "") for field in worksheet_fields}

        logger.info(
            f"Deriving fields from {len(dashboard_field_references)} dashboard references"
        )

        for field_ref in dashboard_field_references:
            # Extract tableau instance from field reference
            tableau_instance = self._extract_tableau_instance(field_ref)
            if not tableau_instance:
                continue

            # Skip if we already have this field
            derived_name = self._get_derived_field_name(tableau_instance)
            if derived_name in existing_field_names:
                continue

            # Derive field definition
            derived_field = self._derive_field_from_instance(tableau_instance)
            if derived_field:
                derived_fields.append(derived_field)
                existing_field_names.add(derived_field["name"])
                logger.debug(
                    f"Derived field: {tableau_instance} → {derived_field['name']}"
                )

        logger.info(f"Derived {len(derived_fields)} new fields from Tableau instances")
        return derived_fields

    def _extract_tableau_instance(self, field_reference: str) -> Optional[str]:
        """
        Extract Tableau instance from dashboard field reference.

        Args:
            field_reference: Dashboard field like "model.explore.field_name"

        Returns:
            Tableau instance pattern or None
        """
        # Field reference format: model.explore.field_name
        if "." in field_reference:
            field_name = field_reference.split(".")[-1]
        else:
            field_name = field_reference

        # Check if field name matches tableau instance patterns
        if self._is_tableau_instance_pattern(field_name):
            return field_name

        return None

    def _is_tableau_instance_pattern(self, tableau_instance: str) -> bool:
        """Check if tableau instance matches derivable patterns."""
        if not tableau_instance:
            return False

        # Parse actual Tableau instance format: [function:FIELD:qualifier]
        # Examples: [tdy:RPT_DT:ok], [sum:sales:qk], [attr:CHANNEL:nk]

        # Remove brackets and split
        if tableau_instance.startswith("[") and tableau_instance.endswith("]"):
            inner = tableau_instance[1:-1]
            parts = inner.split(":")
            if len(parts) >= 3:
                function = parts[0]
                field = parts[1]
                # qualifier = parts[2]  # Not currently used

                # Check for time functions
                if function in ["tdy", "thr", "tmn", "tqr", "tyr", "tmth", "twk"]:
                    return True

                # Check for aggregation functions
                if function in ["sum", "avg", "cnt", "min", "max", "med"]:
                    return True

                # Check for calculation references
                if field.startswith("Calculation_"):
                    return True

        return False

    def _derive_field_from_instance(self, tableau_instance: str) -> Optional[Dict]:
        """
        Derive Looker field definition from Tableau instance.

        Args:
            tableau_instance: Tableau instance pattern

        Returns:
            Field definition dict or None
        """
        derivation_type, base_field, modifier = self._parse_tableau_instance(
            tableau_instance
        )

        if derivation_type == FieldDerivationType.TIME_FUNCTION:
            return self._create_time_dimension_group(base_field, modifier)
        elif derivation_type == FieldDerivationType.AGGREGATION:
            return self._create_aggregated_measure(base_field, modifier)
        elif derivation_type == FieldDerivationType.CALCULATION:
            return self._create_calculated_field_reference(tableau_instance)
        elif derivation_type == FieldDerivationType.DIRECT:
            return self._create_direct_field_reference(base_field)

        return None

    def _parse_tableau_instance(
        self, tableau_instance: str
    ) -> Tuple[FieldDerivationType, str, str]:
        """
        Parse Tableau instance to extract derivation type and components.

        Args:
            tableau_instance: Instance like "[tdy:RPT_DT:ok]" or "[sum:sales:qk]"

        Returns:
            Tuple of (derivation_type, base_field, modifier)
        """
        if not tableau_instance.startswith("[") or not tableau_instance.endswith("]"):
            return FieldDerivationType.DIRECT, tableau_instance, ""

        # Parse [function:FIELD:qualifier] format
        inner = tableau_instance[1:-1]
        parts = inner.split(":")
        if len(parts) < 3:
            return FieldDerivationType.DIRECT, tableau_instance, ""

        function = parts[0]
        field = parts[1]
        # qualifier = parts[2]  # Not currently used

        # Time function patterns
        time_functions = {
            "tdy": "day",
            "thr": "hour",
            "tmn": "minute",
            "tqr": "quarter",
            "tyr": "year",
            "tmth": "month",
            "twk": "week",
        }
        if function in time_functions:
            return (
                FieldDerivationType.TIME_FUNCTION,
                field.lower(),
                time_functions[function],
            )

        # Aggregation patterns
        agg_functions = {
            "sum": "sum",
            "avg": "average",
            "cnt": "count",
            "min": "min",
            "max": "max",
            "med": "median",
        }
        if function in agg_functions:
            return (
                FieldDerivationType.AGGREGATION,
                field.lower(),
                agg_functions[function],
            )

        # Calculation patterns
        if field.startswith("Calculation_"):
            return FieldDerivationType.CALCULATION, field.lower(), ""

        # Direct field reference (attr, none, etc.)
        return FieldDerivationType.DIRECT, field.lower(), function

    def _create_time_dimension_group(self, base_field: str, time_function: str) -> Dict:
        """
        Create dimension_group for time functions.

        Args:
            base_field: Base field name like "rpt_dt"
            time_function: Time function like "day", "hour"

        Returns:
            Dimension group field definition
        """
        # Map time function to timeframe
        timeframe_mapping = {
            "day": "date",
            "hour": "hour",
            "minute": "minute",
            "quarter": "quarter",
            "year": "year",
            "month": "month",
            "week": "week",
        }

        primary_timeframe = timeframe_mapping.get(time_function, "date")

        return {
            "name": base_field,  # dimension_group name
            "field_type": "dimension_group",
            "role": "dimension",
            "datatype": "datetime",
            "sql_column": base_field.upper(),
            "description": f"Time dimension group for {base_field}",
            "timeframes": ["raw", "time", "date", "week", "month", "quarter", "year"],
            "primary_timeframe": primary_timeframe,
            "derivation": f"time_function:{time_function}",
            "tableau_instance": f"{time_function}_{base_field}",
            "is_derived": True,
            "source_type": "tableau_instance_derivation",
        }

    def _create_aggregated_measure(self, base_field: str, aggregation: str) -> Dict:
        """
        Create measure with aggregation.

        Args:
            base_field: Base field name like "sales"
            aggregation: Aggregation function like "sum", "avg"

        Returns:
            Measure field definition
        """
        # Use simple field name for measures since they're commonly referenced
        measure_name = base_field

        return {
            "name": measure_name,
            "field_type": "measure",
            "role": "measure",
            "datatype": "real",
            "sql_column": base_field.upper(),
            "description": f"{aggregation.title()} of {base_field}",
            "aggregation": aggregation,
            "lookml_type": aggregation
            if aggregation in ["sum", "count", "average", "min", "max"]
            else "sum",
            "derivation": f"aggregation:{aggregation}",
            "tableau_instance": f"{aggregation}_{base_field}",
            "is_derived": True,
            "source_type": "tableau_instance_derivation",
        }

    def _create_calculated_field_reference(self, calc_instance: str) -> Dict:
        """
        Create reference to calculated field.

        Args:
            calc_instance: Calculation instance like "calculation_123456"

        Returns:
            Calculated field reference
        """
        return {
            "name": calc_instance,
            "field_type": "dimension",  # Default to dimension, will be corrected by actual calc field data
            "role": "dimension",
            "datatype": "string",
            "description": f"Reference to calculated field {calc_instance}",
            "derivation": "calculation_reference",
            "tableau_instance": calc_instance,
            "is_derived": True,
            "is_calculation_reference": True,
            "source_type": "tableau_instance_derivation",
        }

    def _create_direct_field_reference(self, field_name: str) -> Dict:
        """
        Create direct field reference.

        Args:
            field_name: Direct field name

        Returns:
            Field definition
        """
        return {
            "name": field_name,
            "field_type": "dimension",
            "role": "dimension",
            "datatype": "string",
            "sql_column": field_name.upper(),
            "description": f"Direct reference to {field_name}",
            "derivation": "direct_reference",
            "tableau_instance": field_name,
            "is_derived": True,
            "source_type": "tableau_instance_derivation",
        }

    def _get_derived_field_name(self, tableau_instance: str) -> str:
        """Get the derived Looker field name for a Tableau instance."""
        derivation_type, base_field, modifier = self._parse_tableau_instance(
            tableau_instance
        )

        if derivation_type == FieldDerivationType.TIME_FUNCTION:
            return base_field  # dimension_group uses base field name
        elif derivation_type == FieldDerivationType.AGGREGATION:
            return base_field  # measure uses base field name
        else:
            return tableau_instance  # use instance name directly

    def extract_dashboard_field_references(
        self, dashboard_elements: List[Dict]
    ) -> List[str]:
        """
        Extract all field references from dashboard elements.

        Args:
            dashboard_elements: List of dashboard element dicts

        Returns:
            List of unique field references
        """
        field_references = set()

        for element in dashboard_elements:
            # Extract from fields array
            fields = element.get("fields", [])
            for field in fields:
                if isinstance(field, str):
                    field_references.add(field)

            # Extract from sorts array
            sorts = element.get("sorts", [])
            for sort in sorts:
                if isinstance(sort, str):
                    # Remove sort direction
                    field_name = sort.split()[0] if " " in sort else sort
                    field_references.add(field_name)

        return list(field_references)
