from typing import Dict, Optional
from tableau_to_looker_parser.handlers.base_handler import BaseHandler
from tableau_to_looker_parser.models.json_schema import AggregationType


class MeasureHandler(BaseHandler):
    """Handler for Tableau measure fields.

    Handles:
    - Converting raw measure data to standardized JSON format
    - Mapping aggregation types (SUM, COUNT, AVG, MIN, MAX)
    - Converting value formatting (currency, percentage, decimal)
    - Standardizing drill-down settings
    - Field type mapping
    """

    # Map Tableau roles to aggregation types
    AGGREGATION_MAP = {
        "sum": AggregationType.SUM,
        "avg": AggregationType.AVG,
        "min": AggregationType.MIN,
        "max": AggregationType.MAX,
        "count": AggregationType.COUNT,
    }

    # Map Tableau number formats to LookML formats
    FORMAT_MAP = {
        # Currency formats
        "$#,##0": "usd",
        "$#,##0.00": "usd_2",
        "€#,##0": "eur",
        "€#,##0.00": "eur_2",
        "¥#,##0": "jpy",
        "£#,##0": "gbp",
        # Percentage formats
        "0%": "percent_0",
        "0.0%": "percent_1",
        "0.00%": "percent_2",
        # Decimal formats
        "#,##0": "decimal_0",
        "#,##0.0": "decimal_1",
        "#,##0.00": "decimal_2",
    }

    def can_handle(self, data: Dict) -> float:
        """Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        # Must have role=measure
        if data.get("role") != "measure":
            return 0.0

        # Must not be a parameter
        if data.get("param_domain_type"):
            return 0.0

        # Check if this has a calculation - if so, defer to CalculatedFieldHandler
        if data.get("calculation"):
            return 0.0

        # High confidence for measures, especially quantitative
        if data.get("datatype") == "real":
            return 1.0

        return 0.5

    def convert_to_json(self, data: Dict) -> Dict:
        """Convert raw measure data to two-step pattern: dimension + measure.

        Args:
            raw_data: Raw data dict from XMLParser.extract_measure()

        Returns:
            Dict: Two-step pattern with both dimension and measure
        """
        # Use the clean field name from v2 parser, fallback to cleaning raw_name for v1
        base_name = data.get("name") or self._clean_field_name(data["raw_name"])

        # Get aggregation type
        aggregation = self.AGGREGATION_MAP.get(
            data["aggregation"].lower(), AggregationType.SUM
        )

        # CR #2 Fix: Generate proper measure name with total_ prefix for SUM aggregation
        if aggregation == AggregationType.SUM:
            measure_name = f"total_{base_name}"
        else:
            measure_name = f"{aggregation.value}_{base_name}"

        # Two-step pattern implementation
        json_data = {
            "two_step_pattern": True,
            "dimension": {
                "name": f"{base_name}_raw",
                "table_name": data.get("table_name"),
                "datatype": data.get("datatype", "real"),
                "role": "dimension",
                "hidden": True,  # Hidden raw dimension
                "sql_column": data.get("raw_name", f"[{base_name.title()}]"),
                "description": f"Raw field for {base_name}",
                "label": f"{base_name.replace('_', ' ').title()} (Raw)",
                "datasource_id": data.get("datasource_id"),
                "local_name": data.get("raw_name"),
            },
            "measure": {
                "name": measure_name,  # CR #2 Fix: Proper naming
                "aggregation": aggregation,
                "table_name": data.get("table_name"),
                "label": data.get("label")
                or f"Total {base_name.replace('_', ' ').title()}",
                "description": self._build_description(data),
                "hidden": False,
                "dimension_reference": f"{base_name}_raw",  # References the raw dimension
                "datasource_id": data.get("datasource_id"),
                "local_name": data.get("raw_name"),
            },
        }

        # Add value format if present
        if data.get("number_format"):
            json_data["measure"]["value_format"] = self._convert_format(
                data["number_format"]
            )

        # Add drill-down settings if present
        if data.get("drill_down"):
            json_data["measure"]["drill_fields"] = data["drill_down"]["fields"]
            if data["drill_down"]["default"]:
                json_data["measure"]["drill_down_default"] = True

        # Add calculation if present (goes to measure, not dimension)
        if data.get("calculation"):
            json_data["measure"]["sql"] = data["calculation"]

        return json_data

    def _clean_field_name(self, name: str) -> str:
        """Clean a Tableau field name for LookML.

        Args:
            name: Raw field name like "[Field Name]"

        Returns:
            str: Clean name like "field_name"
        """
        # Remove brackets
        name = name.strip("[]")

        # Convert to lowercase
        name = name.lower()

        # Replace spaces and special chars with underscore
        name = "".join(c if c.isalnum() else "_" for c in name)

        # Remove duplicate underscores
        while "__" in name:
            name = name.replace("__", "_")

        # Remove leading/trailing underscores
        name = name.strip("_")

        return name

    def _convert_format(self, tableau_format: str) -> Optional[str]:
        """Convert Tableau number format to LookML format.

        Args:
            tableau_format: Tableau format string

        Returns:
            str: LookML format name or None
        """
        # Try direct mapping first
        if tableau_format in self.FORMAT_MAP:
            return self.FORMAT_MAP[tableau_format]

        # Handle format variations with custom suffixes/prefixes
        base_format = tableau_format.split(";")[
            0
        ].strip()  # Take first format before custom negative

        # Try to identify format type and number of decimals
        if "$" in base_format:
            return "usd_2" if ".00" in base_format else "usd"

        if "%" in base_format:
            if ".00" in base_format:
                return "percent_2"
            elif ".0" in base_format:
                return "percent_1"
            return "percent_0"

        # Count decimal places for numeric formats
        decimal_places = 0
        if "." in base_format:
            after_decimal = base_format.split(".")[-1]
            decimal_places = len([c for c in after_decimal if c == "0"])

        if decimal_places > 0:
            return f"decimal_{min(decimal_places, 2)}"

        return "decimal_0"  # Default to whole numbers

    def _build_description(self, data: Dict) -> Optional[str]:
        """Build a description from raw measure data.

        Args:
            data: Raw data dict from XMLParser.extract_measure()

        Returns:
            str: Description or None
        """
        # Use caption from Tableau XML as primary description
        if data.get("caption"):
            return data["caption"]

        parts = []

        # Add calculation if present
        if data.get("calculation"):
            parts.append(f"Calculation: {data['calculation']}")

        # Add format if present
        if data.get("number_format"):
            parts.append(f"Format: {data['number_format']}")

        # Add drill-down info if present
        if data.get("drill_down"):
            drill = data["drill_down"]
            fields = ", ".join(drill["fields"])
            parts.append(f"Drill fields: {fields}")
            if drill["default"]:
                parts.append("Default drill-down enabled")

        # Add original name
        if data.get("raw_name"):
            parts.append(f"Original name: {data['raw_name']}")

        return " | ".join(parts) if parts else None
