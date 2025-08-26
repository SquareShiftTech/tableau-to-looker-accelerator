from typing import Dict, Optional

from tableau_to_looker_parser.handlers.base_handler import BaseHandler
from tableau_to_looker_parser.models.json_schema import (
    ParameterType,
    RangeParameterSettings,
    ListParameterSettings,
    ParameterSettings,
    DateParameterSettings,
)


class ParameterHandler(BaseHandler):
    """Handler for Tableau parameter data.

    Handles:
    - Converting raw parameter data to standardized JSON format
    - Parameter type mapping and validation
    - Range parameter settings normalization
    - List parameter settings normalization
    - Date parameter settings normalization

    Does NOT handle XML parsing - that's XMLParser's job.
    """

    # Map Tableau datatypes to schema types
    TYPE_MAP = {
        "real": "number",
        "integer": "number",
        "boolean": "boolean",
        "date": "date",
        "datetime": "datetime",
        "string": "string",
    }

    def can_handle(self, data: Dict) -> float:
        """Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        # Must have param_domain_type
        if not data.get("param-domain-type"):
            return 0.0

        # Must have datatype
        if not data.get("datatype"):
            return 0.0

        # High confidence for parameters
        return 1.0

    def convert_to_json(self, data: Dict) -> Dict:
        """Convert raw parameter data to schema-compliant JSON.

        Args:
            raw_data: Raw data dict from XMLParser.extract_parameter()

        Returns:
            Dict: Schema-compliant parameter data
        """
        # Clean field name
        name = self._clean_field_name(data["raw_name"])

        param_type = self._get_parameter_type(
            data["param-domain-type"], data["datatype"]
        )

        # Build parameter settings
        settings = {
            "type": param_type,
            "default_value": data.get("default_value"),
            "description": self._build_description(data),
            "required": False,  # Default to optional
        }

        # Handle type-specific settings
        if param_type in [ParameterType.DATE, ParameterType.DATETIME]:
            date_settings = {
                "format": "YYYY-MM-DD"
                if param_type == ParameterType.DATE
                else "YYYY-MM-DD HH:mm:ss"
            }

            # Add range if present
            if data.get("range"):
                date_settings["range"] = RangeParameterSettings(
                    **data["range"], inclusive_min=True, inclusive_max=True
                )

            # Add allowed values if present
            if data.get("values"):
                date_settings["allowed_values"] = ListParameterSettings(
                    values=data["values"], value_type="date"
                )

            settings["date"] = DateParameterSettings(**date_settings)

        elif param_type == ParameterType.RANGE:
            if data.get("range"):
                settings["range"] = RangeParameterSettings(
                    **data["range"], inclusive_min=True, inclusive_max=True
                )

        elif param_type == ParameterType.LIST:
            if data.get("values"):
                value_type = "string"
                if data["datatype"] in ["integer", "real"]:
                    value_type = "number"
                elif data["datatype"] in ["date", "datetime"]:
                    value_type = "date"

                settings["list"] = ListParameterSettings(
                    values=data["values"],
                    allow_multiple=False,
                    value_type=value_type,
                )

        # Build complete parameter
        json_data = {
            "name": name,
            "field_type": self.TYPE_MAP.get(data["datatype"], "string"),
            "label": data.get("label"),
            "description": self._build_description(data),
            "hidden": False,
            "parameter": ParameterSettings(**settings),
        }

        return json_data

    def _get_parameter_type(self, param_type: str, datatype: str) -> ParameterType:
        """Map Tableau parameter type to schema type.

        Args:
            param_type: Tableau parameter type (range, list)
            datatype: Parameter datatype (string, integer, real, boolean, date, datetime)

        Returns:
            ParameterType: Schema parameter type
        """
        # Handle explicit parameter types first
        if param_type == "range":
            if datatype == "date":
                return ParameterType.DATE
            elif datatype == "datetime":
                return ParameterType.DATETIME
            return ParameterType.RANGE

        if param_type == "list":
            return ParameterType.LIST

        # Fall back to datatype-based types
        type_map = {
            "date": ParameterType.DATE,
            "datetime": ParameterType.DATETIME,
            "integer": ParameterType.NUMBER,
            "real": ParameterType.NUMBER,
            "boolean": ParameterType.BOOLEAN,
            "string": ParameterType.STRING,
        }
        return type_map.get(datatype.lower(), ParameterType.STRING)

    def _clean_field_name(self, name: str) -> str:
        """Clean a Tableau field name for LookML.

        Args:
            name: Raw field name like "[Parameter 1]"

        Returns:
            str: Clean name like "parameter_1"
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

    def _build_description(self, raw_data: Dict) -> Optional[str]:
        """Build a description from raw parameter data.

        Args:
            raw_data: Raw data dict from XMLParser.extract_parameter()

        Returns:
            str: Description or None
        """
        parts = []

        # Add default value
        if raw_data.get("default_value"):
            parts.append(f"Default: {raw_data['default_value']}")

        # Add range info
        if raw_data.get("range"):
            range_info = raw_data["range"]
            parts.append(
                f"Range: {range_info.get('min', 'N/A')} to {range_info.get('max', 'N/A')}"
                + (f" (step: {range_info['step']})" if range_info.get("step") else "")
            )

        # Add list values
        if raw_data.get("values"):
            values = raw_data["values"]
            parts.append(f"Values: {', '.join(values)}")

        # Add original name if present
        if raw_data.get("raw_name"):
            parts.append(f"Original name: {raw_data['raw_name']}")

        return " | ".join(parts) if parts else None
