import re
from typing import Dict, Optional

from tableau_to_looker_parser.handlers.base_handler import BaseHandler
from tableau_to_looker_parser.models.json_schema import DimensionSchema, DimensionType


class DimensionHandler(BaseHandler):
    """Handler for Tableau dimension data.

    Handles:
    - Converting raw dimension data to standardized JSON format
    - Field type mapping and validation
    - Name cleaning and standardization
    - Data type standardization
    - Semantic role standardization

    Does NOT handle XML parsing - that's XMLParser's job.
    """

    # Map Tableau datatypes to our dimension types
    TYPE_MAP = {
        "string": DimensionType.STRING,
        "integer": DimensionType.INTEGER,
        "real": DimensionType.REAL,
        "boolean": DimensionType.BOOLEAN,
        "date": DimensionType.DATE,
        "datetime": DimensionType.DATETIME,
    }

    def can_handle(self, data: Dict) -> float:
        """Determine if this handler can process the raw data.

        Args:
            data: Raw data dict from XMLParser

        Returns:
            float: Confidence score between 0.0 and 1.0
        """
        # Get basic attributes
        role = data.get("role", "")
        datatype = data.get("datatype", "")
        param_type = data.get("param_domain_type")
        semantic_role = data.get("semantic_role")

        # Don't handle parameters - they go to ParameterHandler
        if param_type:
            return 0.0

        # High confidence if it's a dimension with known datatype
        if role == "dimension" and datatype in self.TYPE_MAP:
            return 1.0

        # Medium confidence if it has semantic role
        if semantic_role:
            return 0.75

        # Low confidence if it's a dimension without known datatype
        if role == "dimension":
            return 0.5

        # Skip measures and unknown roles
        if role == "measure":
            return 0.0

        # Very low confidence for anything else
        return 0.1

    def convert_to_json(self, raw_data: Dict) -> Dict:
        """Convert raw dimension data to schema-compliant JSON.

        Args:
            raw_data: Raw data dict from XMLParser.extract_dimension()

        Returns:
            Dict: Schema-compliant dimension data
        """
        # Clean field name
        name = self._clean_field_name(raw_data["raw_name"])

        # Build base dimension
        json_data = {
            "name": name,
            "field_type": self.TYPE_MAP.get(raw_data["datatype"], DimensionType.STRING),
            "label": raw_data.get("label"),
            "description": self._build_description(raw_data),
            "calculation": raw_data.get("calculation"),
            "semantic_role": raw_data.get("semantic_role"),
        }

        # Add range data if present
        if "range" in raw_data:
            json_data["range"] = raw_data["range"]

        # Remove None values
        json_data = {k: v for k, v in json_data.items() if v is not None}

        # Create and validate with schema
        dimension = DimensionSchema(**json_data)
        return dimension.model_dump()

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
        name = re.sub(r"[^a-z0-9]+", "_", name)

        # Remove duplicate underscores
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        return name

    def _build_description(self, raw_data: Dict) -> Optional[str]:
        """Build a description from raw dimension data.

        Args:
            raw_data: Raw data dict from XMLParser.extract_dimension()

        Returns:
            str: Description or None
        """
        parts = []

        # Add semantic role if present
        if raw_data.get("semantic_role"):
            parts.append(f"Semantic role: {raw_data['semantic_role']}")

        # Add calculation if present
        if raw_data.get("calculation"):
            parts.append(f"Calculation: {raw_data['calculation']}")

        # Add aggregation source
        if raw_data.get("aggregate_role_from"):
            parts.append(f"Aggregated from: {raw_data['aggregate_role_from']}")

        # Add tableau type if present
        if raw_data.get("tableau_type"):
            parts.append(f"Tableau type: {raw_data['tableau_type']}")

        # Add original name if present
        if raw_data.get("raw_name"):
            parts.append(f"Original name: {raw_data['raw_name']}")

        # Add range info if present
        if range_data := raw_data.get("range"):
            range_parts = []
            if min_val := range_data.get("min"):
                range_parts.append(f"min: {min_val}")
            if max_val := range_data.get("max"):
                range_parts.append(f"max: {max_val}")
            if gran := range_data.get("granularity"):
                range_parts.append(f"granularity: {gran}")
            if range_parts:
                parts.append(f"Range: {', '.join(range_parts)}")

        return " | ".join(parts) if parts else None
